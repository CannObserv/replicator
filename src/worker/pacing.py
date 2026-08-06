"""Per-host request spacing — the mechanism half of politeness (#12, #19).

`docs/contracts/replicator-boundaries.md` splits this capability: Replicator
**enforces** a per-host rate because it is the only process that can see the
origin's tolerance across commands, and the issuer **decides** the numbers,
which are operator policy. Since #19 the numbers arrive over
`content.fetch-policy` and reach this module through the `policy` seam;
`src/worker/policy.py` owns the map they land in. The env default did not go
away — it is what an unknown or revoked host resolves to, never "unlimited",
because a boot replay cannot tell a consumer whether the set it received is
whole.

The state is a host -> last-request timestamp map, in memory. That is one of the
three shapes the charter permits: derived, bounded, and rebuildable by replay —
a cold worker is simply polite from scratch, which errs in the safe direction.
"""

import time
from collections.abc import Callable
from urllib.parse import urlsplit

# How a host's interval is looked up. ``None`` means "no explicit policy" — an
# unknown host and a revoked one are the same answer here on purpose, since the
# fallback rule the charter fixes is the same for both.
HostPolicy = Callable[[str], float | None]

# When to prune. Each entry is a hostname and a float; the bound is not about
# memory pressure at this size but about the shape — an unbounded map that only
# ever grows is a leak whatever its constant. Pruning is O(n) and runs only on
# the record that crosses the bound, so the amortized cost is negligible.
MAX_TRACKED_HOSTS = 4096


class HostPacer:
    """How long before this host may be asked for something again.

    Deliberately *not* a token bucket: a bucket permits a burst up to its depth,
    and a burst is precisely what an origin notices. Fixed minimum spacing is the
    same rule Watcher enforces today (``DEFAULT_MIN_INTERVAL``), so the cutover
    changes who paces rather than how.

    Reports a wait; it does not spend one. The caller decides whether a wait is
    short enough to sleep through or long enough to park the message, because
    only the caller knows what else is behind it on a serial consume path.
    """

    def __init__(
        self,
        default_interval_seconds: float,
        *,
        policy: HostPolicy | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._default = default_interval_seconds
        # Defaulted rather than required, and the default resolves every host to
        # "no policy" rather than to "no pacing": an unwired seam falls back to
        # exactly the pre-#19 behaviour instead of silently disabling the
        # mechanism, which is the same reason build_handler constructs a pacer
        # when none is injected.
        self._policy: HostPolicy = policy if policy is not None else lambda _host: None
        self._clock = clock
        self._last: dict[str, float] = {}
        # Earliest a prune could reclaim anything; see _prune.
        self._prune_not_before = 0.0

    def _interval_for(self, host: str) -> float:
        """This host's minimum spacing: its policy, or the conservative default.

        ``is None`` rather than a truthiness test. ``0.0`` is a legal published
        interval meaning "this host needs no spacing" — an explicit operator
        decision — and ``policy(host) or self._default`` would read it as an
        absent one and pace the host anyway.
        """
        interval = self._policy(host)
        return self._default if interval is None else interval

    @property
    def tracked_hosts(self) -> int:
        """How many hosts currently hold an entry.

        Reported on ``handler.py::_pace``'s log line — the branch that actually
        waits — so a rising figure is visible as the corpus widening rather than
        inferred from throughput, without repeating a slow gauge once per
        command.
        """
        return len(self._last)

    def wait_seconds(self, url: str) -> float:
        """Seconds before a request to ``url``'s host may go out. ``0.0`` for now.

        A URL with no host is never paced: it is also unfetchable, and the driver
        refuses it as ``not_fetchable``. Returning a wait would park a
        permanently-bad command in the PEL instead of letting it reach the
        terminal fact its issuer is waiting for.
        """
        host = _host(url)
        if host is None:
            return 0.0
        interval = self._interval_for(host)
        if interval <= 0:
            return 0.0
        last = self._last.get(host)
        if last is None:
            return 0.0
        return max(0.0, interval - (self._clock() - last))

    def record(self, url: str) -> None:
        """Stamp a request as having gone out.

        Called after the wait, never before it — only a request that reaches the
        origin resets that origin's clock. Recording an *attempt* would let a run
        of parked redeliveries push the next real fetch out indefinitely, spacing
        the origin from requests it never received.
        """
        host = _host(url)
        if host is None:
            return
        now = self._clock()
        self._last[host] = now
        if len(self._last) > MAX_TRACKED_HOSTS and now >= self._prune_not_before:
            self._prune(now)

    def _prune(self, now: float) -> None:
        """Drop hosts whose interval has already elapsed.

        Those entries impose no wait, so removing them changes no decision this
        pacer can make. Hosts still inside their interval are kept however far
        over the bound that leaves us — enforcing the bound by forgetting a host
        that is still owed space would turn a memory limit into a politeness
        breach.

        Which means the bound can be exceeded with nothing to reclaim, and a
        prune that frees nothing must not run again on the next record: with more
        than ``MAX_TRACKED_HOSTS`` hosts all inside a long interval, that would
        be a full dict rebuild per message forever (CR #7). The retry is deferred
        until the oldest entry could plausibly have aged out.

        Both halves resolve the interval **per host** (#19). Against one global
        number a host on a 300-second policy would be dropped as soon as the
        one-second default elapsed, losing its spacing silently and in the
        loosening direction — the failure the policy stream exists to remove —
        and the deferral would be computed from a number no remaining entry is
        actually waiting on.
        """
        before = len(self._last)
        self._last = {
            host: last for host, last in self._last.items() if now - last < self._interval_for(host)
        }
        if len(self._last) < before:
            self._prune_not_before = 0.0
            return
        # Nothing was reclaimable. The earliest anything can be is when the
        # entry that frees up soonest finishes its own interval.
        self._prune_not_before = min(
            (last + self._interval_for(host) for host, last in self._last.items()),
            default=now,
        )


def _host(url: str) -> str | None:
    """The URL's hostname, or ``None`` when there is nothing to pace.

    ``urlsplit().hostname`` is already lowercased and already excludes the port,
    which is the key this wants: politeness is about how often a *server* is
    asked, so a second port is the same machine and a capitalized host is the
    same name. Keying on anything finer would let an issuer multiply its own
    rate limit by spelling the URL differently.

    **Known limitation: the host asked for, not the host reached (CR #4).** httpx
    follows redirects inside the driver, so a URL that 301s elsewhere is paced
    under the name the command carried and not at all under the name that
    actually served it. A corpus where several watched URLs funnel into one
    portal or CDN therefore hits that host at N times the intended rate — the
    failure politeness exists to prevent. Recording the landing host too
    (``FetchResult.final_url`` is available at the call site) would fix it at the
    cost of "one request, one record", which wants its own decision rather than
    a quiet change here; the policy stream is where it should land.
    """
    try:
        return urlsplit(url).hostname
    except ValueError:
        # An unparseable authority (a bad IPv6 literal, say). Unfetchable for the
        # same reason a hostless URL is, and handled the same way.
        return None
