"""Per-host request spacing — the mechanism half of politeness (#12, #19, #25).

`docs/contracts/replicator-boundaries.md` splits this capability: Replicator
**enforces** a per-host rate because it is the only process that can see the
origin's tolerance across commands, and the issuer **decides** the numbers,
which are operator policy. Since #19 the numbers arrive over
`content.fetch-policy` and reach this module through the `policy` seam;
`src/worker/policy.py` owns the map they land in. The env default did not go
away — it is what an unknown or revoked host resolves to, never "unlimited",
because a boot replay cannot tell a consumer whether the set it received is
whole.

Since #25 the interval is also **adaptive upward**: an origin that answers 429
(or 503) raises its own spacing until a quiet window passes. That is the
escalation Watcher ran on its own fetch path and lost at the Phase 4 cutover, and
it stays mechanism rather than policy on the charter's second test — the
tolerance of an origin across commands is visible to nobody but the fetcher.
**The published number remains the floor.** Escalation may only ever raise the
effective interval above it, never lower it, so a host that once 429'd can never
come back less polite than the issuer asked for.

The state is one host -> `_HostState` map, in memory: the last request's stamp and
whatever escalation is in force. That is one of the three shapes the charter
permits: derived, bounded, and rebuildable by replay — a cold worker is simply
polite from scratch, which errs in the safe direction. Escalation shares the map
rather than growing a second one, because a second map is a second bound to get
right and the prune already has the argument for this one.
"""

import time
from collections.abc import Callable
from typing import NamedTuple
from urllib.parse import urlsplit

# How a host's interval is looked up. ``None`` means "no explicit policy" — an
# unknown host and a revoked one are the same answer here on purpose, since the
# fallback rule the charter fixes is the same for both.
HostPolicy = Callable[[str], float | None]

# When to prune. Each entry is a hostname and a small tuple; the bound is not
# about memory pressure at this size but about the shape — an unbounded map that
# only ever grows is a leak whatever its constant. Pruning is O(n) and runs only
# on the record that crosses the bound, so the amortized cost is negligible.
MAX_TRACKED_HOSTS = 4096

# The escalation curve, recovered from Watcher's pre-cutover limiter
# (``2b98989^:src/core/rate_limiter.py``) so the cluster keeps the tolerance
# profile origins have already been seeing.
#
# Module constants rather than ``REPLICATOR_*`` settings, deliberately: the
# numbers the *issuer* owns travel on ``content.fetch-policy``, and these are not
# those. Escalation is how the mechanism reacts to an origin, not a policy an
# operator tunes per deployment — a knob here would be one more way for a local
# env file to disagree with the charter's ownership split.
#
# ``BACKOFF_FIRST_STEP`` is the part easy to leave out. Doubling alone would take
# a 0.1-second host five refusals to reach one second, so the first escalation
# clears a fixed step regardless of how fast the floor is.
BACKOFF_MULTIPLIER = 2.0
BACKOFF_FIRST_STEP = 2.0

# Where escalation stops — **how far above the published floor it may go**, not an
# absolute interval (CR #14). Watcher's constant was named ``BACKOFF_MAX_INTERVAL``
# and was absolute, which was right there: its floor was a single global
# ``DEFAULT_MIN_INTERVAL = 1.0`` and no host could be published slower. Replicator
# has per-host published numbers (#19), so an absolute ceiling is silently inert
# for every host whose policy already exceeds it — and those are precisely the
# origins an issuer has already marked fragile, so the ones likeliest to go on
# refusing. Renamed rather than re-tuned: the old name is what made the reading
# plausible.
#
# Also roughly ``REPLICATOR_CLAIM_MIN_IDLE_MS``, which is not a coincidence worth
# relying on but is why the headroom lands somewhere a parked command can still
# express: past a minute the wait outlives the reclaim cadence that would bring
# the command back anyway. That reasoning is about the *added* wait, which is what
# makes headroom the right shape for it — a host already published at 300 s is
# past the reclaim cadence before any escalation happens.
BACKOFF_MAX_HEADROOM = 60.0

# How long an origin must go without refusing before its escalation is dropped —
# Watcher's ``DEFAULT_DECAY_WINDOW`` (``models/domain.py``), which lived on the
# ``Domain`` row rather than in the limiter.
#
# **Measured from the last refusal, not from the last request.** Watcher's
# ``Domain.last_request_at`` reads like the latter and was in fact written only by
# ``_persist_backoff``, i.e. on a 429 and nowhere else. The distinction is the
# difference between working and not: a host fetched every minute never has a
# half-hour gap between *requests*, so a window measured from traffic would hold
# every escalation forever on exactly the hosts busy enough to earn one.
#
# "Decay" is Watcher's word and a misnomer in both codebases — the reset is one
# step back to the floor, not a ramp. Kept because the issue and the sibling repo
# both use it.
BACKOFF_DECAY_SECONDS = 1800.0


class _HostState(NamedTuple):
    """What the pacer remembers about one host.

    ``escalated_interval`` is ``0.0`` for the ordinary case — no refusal seen, or
    one whose window has since elapsed. It holds what the *mechanism* decided and
    never the effective interval: folding the floor in would let a number this
    module invented shadow one the issuer published on the next republish.
    """

    last_request_at: float
    escalated_interval: float = 0.0
    escalated_at: float = 0.0


class HostPacer:
    """How long before this host may be asked for something again.

    Deliberately *not* a token bucket: a bucket permits a burst up to its depth,
    and a burst is precisely what an origin notices. Fixed minimum spacing is the
    same rule Watcher enforced pre-cutover (``DEFAULT_MIN_INTERVAL``), so the
    cutover changes who paces rather than how — and since #25 the spacing rises on
    a 429 the same way Watcher's did, which is the other half of "rather than how".
    Still fixed within a host: escalation moves the interval, never the burst
    allowance, which stays deliberately absent (#12).

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
        self._hosts: dict[str, _HostState] = {}
        # Earliest a prune could reclaim anything; see _prune.
        self._prune_not_before = 0.0

    def _floor_for(self, host: str) -> float:
        """This host's minimum spacing: its policy, or the conservative default.

        ``is None`` rather than a truthiness test. ``0.0`` is a legal published
        interval meaning "this host needs no spacing" — an explicit operator
        decision — and ``policy(host) or self._default`` would read it as an
        absent one and pace the host anyway.

        The floor, not the answer: an escalation can raise the effective interval
        above this and nothing can lower it below (#25). Resolved on every read
        rather than cached with the entry, so a republished policy takes effect on
        the next command instead of on the next 429.
        """
        interval = self._policy(host)
        return self._default if interval is None else interval

    def _live_escalation(self, state: _HostState, now: float) -> float:
        """The escalation still in force for this entry, or ``0.0`` (#25).

        Expiry is *derived* rather than swept: the entry stops counting the moment
        its window elapses, whether or not anything has since called ``record``.
        A read that had to wait for a write to be correct would answer
        ``wait_seconds`` from state the same call is about to invalidate.
        """
        if state.escalated_interval <= 0:
            return 0.0
        if now - state.escalated_at >= BACKOFF_DECAY_SECONDS:
            return 0.0
        return state.escalated_interval

    def _interval_for(self, host: str, state: _HostState | None, now: float) -> float:
        """The spacing actually in force: the floor, raised by any live escalation."""
        floor = self._floor_for(host)
        if state is None:
            return floor
        return max(floor, self._live_escalation(state, now))

    @property
    def tracked_hosts(self) -> int:
        """How many hosts currently hold an entry.

        Reported on ``handler.py::_pace``'s log line — the branch that actually
        waits — so a rising figure is visible as the corpus widening rather than
        inferred from throughput, without repeating a slow gauge once per
        command.
        """
        return len(self._hosts)

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
        now = self._clock()
        state = self._hosts.get(host)
        if state is None:
            return 0.0
        interval = self._interval_for(host, state, now)
        if interval <= 0:
            return 0.0
        return max(0.0, interval - (now - state.last_request_at))

    def record(self, url: str) -> None:
        """Stamp a request as having gone out.

        Called after the wait, never before it — only a request that reaches the
        origin resets that origin's clock. Recording an *attempt* would let a run
        of parked redeliveries push the next real fetch out indefinitely, spacing
        the origin from requests it never received.

        Also where an elapsed escalation is dropped (#25) — the same place
        Watcher's reset ran, minus the ``Domain`` row it read and wrote. Nothing
        depends on that happening here, because ``_live_escalation`` already
        ignores an expired one; this is the eviction that keeps the map from
        holding state no read will ever consult again.
        """
        host = _host(url)
        if host is None:
            return
        now = self._clock()
        state = self._hosts.get(host)
        escalated = 0.0 if state is None else self._live_escalation(state, now)
        if state is None or escalated <= 0:
            # Never refused, or refused long enough ago that the window has
            # closed: the one-step reset back to the floor.
            self._hosts[host] = _HostState(now)
        else:
            self._hosts[host] = _HostState(now, escalated, state.escalated_at)
        if len(self._hosts) > MAX_TRACKED_HOSTS and now >= self._prune_not_before:
            self._prune(now)

    def report_rate_limited(self, url: str, *, retry_after_seconds: float | None = None) -> float:
        """The origin asked for later. Raise this host's interval; return the one now in force.

        Takes a URL rather than a host for symmetry with the two methods above:
        every caller has a command with a URL in it, and making it split the host
        itself would duplicate this module's own parsing — and invite a caller to
        key on something finer, which is how an issuer accidentally multiplies its
        own rate limit.

        Compounds off the interval **in force**, so successive refusals double
        (Watcher's ``BACKOFF_MULTIPLIER``) up to :data:`BACKOFF_MAX_HEADROOM`
        *above this host's floor*, with :data:`BACKOFF_FIRST_STEP` as the floor on
        the first step.

        The return value is the spacing now in force, not the escalation this
        method picked (CR #15). Those agree whenever the escalation is the larger
        number, which is always — but the caller logs this, it is the only signal
        the mechanism emits (a 429 publishes no fact, and the transient raise
        behind it looks like a timeout in the journal), and resolving it through
        :meth:`_interval_for` is what keeps that line honest if the ceiling is ever
        re-shaped again.

        ``retry_after_seconds`` is the origin stating the number instead of us
        guessing it — parsed from the header by the caller, which is where the
        wall clock an HTTP-date needs lives. It can only ever raise the
        escalation, never soften one: an origin refusing while asking for a
        one-second delay is describing the cadence it is already refusing, and
        honouring that downward would let a 429 that always carries a small header
        disable escalation outright. Being *more* polite than asked violates
        nothing. ``None``, zero, and a value already in the past are the same
        answer — no evidence — and fall through to the multiplier rather than
        raising, because an origin sending rubbish here is still an origin asking
        for space.

        Returns ``0.0`` for a URL with no host, which is also unfetchable; the
        reasoning is ``wait_seconds``'.
        """
        host = _host(url)
        if host is None:
            return 0.0
        now = self._clock()
        state = self._hosts.get(host)
        floor = self._floor_for(host)
        stepped = max(self._interval_for(host, state, now) * BACKOFF_MULTIPLIER, BACKOFF_FIRST_STEP)
        asked = 0.0 if retry_after_seconds is None else retry_after_seconds
        escalated = min(max(stepped, asked), floor + BACKOFF_MAX_HEADROOM)
        # Defensive on the stamp: ``_pace`` records before the fetch, so an entry
        # always exists by the time a status comes back. A mechanism that only
        # escalated hosts it had already seen would break silently if that order
        # ever changed, and "now" is the truthful answer anyway — a request just
        # went out and was refused.
        escalation = _HostState(now if state is None else state.last_request_at, escalated, now)
        self._hosts[host] = escalation
        return self._interval_for(host, escalation, now)

    def _prune(self, now: float) -> None:
        """Drop hosts that can no longer change an answer this pacer gives.

        Two things make an entry worth keeping, and an entry needs neither to be
        reclaimable. Its interval may not have elapsed — dropping it would forget
        space the origin is still owed. Or it may hold a live escalation (#25):
        the escalated interval can be long past while the *window* is still open,
        and forgetting that an origin refused eight minutes ago is not
        recoverable, whereas the memory it costs is one tuple. Both cases are the
        same trade in the same direction — enforcing a memory bound by becoming
        less polite would turn a limit into a politeness breach.

        Which means the bound can be exceeded with nothing to reclaim, and a
        prune that frees nothing must not run again on the next record: with more
        than ``MAX_TRACKED_HOSTS`` hosts all inside a long interval, that would
        be a full dict rebuild per message forever (CR #7). The retry is deferred
        until the earliest entry could plausibly have aged out.

        Both halves resolve the interval **per host** (#19). Against one global
        number a host on a 300-second policy would be dropped as soon as the
        one-second default elapsed, losing its spacing silently and in the
        loosening direction — the failure the policy stream exists to remove —
        and the deferral would be computed from a number no remaining entry is
        actually waiting on.
        """
        before = len(self._hosts)
        self._hosts = {
            host: state
            for host, state in self._hosts.items()
            if self._still_constrains(host, state, now)
        }
        if len(self._hosts) < before:
            self._prune_not_before = 0.0
            return
        # Nothing was reclaimable. The earliest anything can be is when the entry
        # that frees up soonest finishes both of its clauses.
        self._prune_not_before = min(
            (self._reclaimable_at(host, state) for host, state in self._hosts.items()),
            default=now,
        )

    def _still_constrains(self, host: str, state: _HostState, now: float) -> bool:
        """Whether dropping this entry could change a later answer."""
        if now - state.last_request_at < self._interval_for(host, state, now):
            return True
        return self._live_escalation(state, now) > 0

    def _reclaimable_at(self, host: str, state: _HostState) -> float:
        """The earliest ``_still_constrains`` could go false for this entry.

        A true lower bound, which is what the deferral needs: once the escalation
        window has closed the interval is back to the floor, so the interval
        clause is measured against the floor rather than against an escalated
        number that will no longer be in force by then.
        """
        elapses_at = state.last_request_at + self._floor_for(host)
        if state.escalated_interval <= 0:
            return elapses_at
        return max(elapses_at, state.escalated_at + BACKOFF_DECAY_SECONDS)


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
