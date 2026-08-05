"""Per-host request spacing — the mechanism half of politeness (#12).

`docs/contracts/replicator-boundaries.md` splits this capability: Replicator
**enforces** a per-host rate because it is the only process that can see the
origin's tolerance across commands, and the issuer **decides** the numbers,
which are operator policy. Until the `content.fetch.policy` stream exists there
is nothing to decide with, so this carries a single conservative default from
env — a default is mechanism, and the alternative at the Phase 4 cutover is no
politeness at all: Watcher's limiter paces its own fetches, and the moment that
fetch path becomes a publish path it silently starts pacing nothing
(CannObserv/watcher#245).

The state is a host -> last-request timestamp map, in memory. That is one of the
three shapes the charter permits: derived, bounded, and rebuildable by replay —
a cold worker is simply polite from scratch, which errs in the safe direction.
"""

import time
from collections.abc import Callable
from urllib.parse import urlsplit

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
        self, min_interval_seconds: float, *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._interval = min_interval_seconds
        self._clock = clock
        self._last: dict[str, float] = {}
        # Earliest a prune could reclaim anything; see _prune.
        self._prune_not_before = 0.0

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
        if host is None or self._interval <= 0:
            return 0.0
        last = self._last.get(host)
        if last is None:
            return 0.0
        return max(0.0, self._interval - (self._clock() - last))

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
        """
        before = len(self._last)
        self._last = {
            host: last for host, last in self._last.items() if now - last < self._interval
        }
        if len(self._last) < before:
            self._prune_not_before = 0.0
            return
        # Nothing was reclaimable. The earliest anything can be is one interval
        # after the oldest entry still held.
        self._prune_not_before = min(self._last.values(), default=now) + self._interval


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
