"""Per-host pacing: the interim politeness default (#12), escalating on 429 (#25).

The state machine only. How the handler spends the wait it is told about — sleep
below the bound, park above it — and how a ``Retry-After`` header becomes a number
are ``tests/worker/test_handler_pacing.py``.
"""

import pytest

from src.worker.pacing import (
    BACKOFF_DECAY_SECONDS,
    BACKOFF_FIRST_STEP,
    BACKOFF_MAX_HEADROOM,
    MAX_TRACKED_HOSTS,
    HostPacer,
)


def test_the_first_request_to_a_host_waits_for_nothing():
    """There is no previous request to space this one from."""
    pacer = HostPacer(1.0)

    assert pacer.wait_seconds("https://example.test/a") == 0.0


def test_a_second_request_to_the_same_host_waits_out_the_interval(clock):
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    clock.advance(0.25)

    assert pacer.wait_seconds("https://example.test/b") == pytest.approx(0.75)


def test_the_wait_is_gone_once_the_interval_has_elapsed(clock):
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    clock.advance(1.0)

    assert pacer.wait_seconds("https://example.test/b") == 0.0


def test_pacing_is_per_host(clock):
    """A slow origin must not throttle an unrelated one."""
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    assert pacer.wait_seconds("https://other.test/a") == 0.0


def test_the_host_is_the_key_regardless_of_scheme_port_or_case(clock):
    """One machine, one budget.

    Politeness is about how often a *server* is asked, so a second scheme or port
    is the same origin's socket and a capitalized host is the same name. Keying
    on anything finer would let an issuer multiply its own rate limit by spelling
    the URL differently.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    assert pacer.wait_seconds("http://EXAMPLE.test:8080/b") > 0


def test_a_hostless_url_is_never_paced():
    """It cannot be fetched either — the driver refuses it as not_fetchable.

    Returning a wait would park a permanently bad command in the PEL instead of
    letting it reach the terminal fact the issuer is waiting for.
    """
    pacer = HostPacer(1.0)
    pacer.record("not-a-url")

    assert pacer.wait_seconds("not-a-url") == 0.0


def test_an_unparseable_authority_is_never_paced():
    """``urlsplit`` raises on a malformed IPv6 literal rather than returning None.

    Same outcome as a hostless URL and for the same reason — it is unfetchable —
    but it arrives as an exception, and an uncaught one here would turn a bad URL
    into an unclassified handler failure retried to the delivery ceiling.
    """
    pacer = HostPacer(1.0)
    pacer.record("http://[::1")

    assert pacer.wait_seconds("http://[::1") == 0.0


def test_a_zero_default_leaves_an_unknown_host_unpaced(clock):
    """The operator escape hatch, narrowed by #19 to the hosts with no policy.

    Before the policy stream this disabled pacing outright. It cannot mean that
    any more without letting an env var veto a value the issuer published, which
    inverts the ownership split the charter settled.
    """
    pacer = HostPacer(0.0, clock=clock)
    pacer.record("https://example.test/a")

    assert pacer.wait_seconds("https://example.test/a") == 0.0


def test_recording_prunes_hosts_that_no_longer_constrain_anything(clock):
    """In-memory derived state still has to be bounded.

    A worker fetching a long tail of one-off hosts would otherwise grow a
    permanent entry per host. An entry older than the interval imposes no wait,
    so dropping it changes no decision this pacer can make.
    """
    pacer = HostPacer(1.0, clock=clock)
    for n in range(MAX_TRACKED_HOSTS + 1):
        pacer.record(f"https://host{n}.test/a")

    clock.advance(1.0)
    pacer.record("https://trigger.test/a")

    assert pacer.tracked_hosts == 1


def test_pruning_keeps_the_hosts_that_do_constrain_something(clock):
    """The bound must not be enforced by forgetting a host that is still waiting."""
    pacer = HostPacer(60.0, clock=clock)
    for n in range(MAX_TRACKED_HOSTS + 1):
        pacer.record(f"https://host{n}.test/a")

    assert pacer.wait_seconds("https://host0.test/a") > 0


def test_a_prune_that_reclaims_nothing_is_not_retried_every_record(clock, monkeypatch):
    """Over the bound with nothing reclaimable is a steady state, not an emergency.

    Every host inside a long interval means the bound cannot be honoured, and
    retrying the O(n) rebuild on each subsequent record would make a full dict
    copy per message for as long as that lasts (CR #7). The retry waits until the
    oldest entry could plausibly have aged out.

    **Why counting calls on a private attribute is safe here (CR #12).** The
    worry about a spy like this is that a refactor stops going through the
    patched attribute, leaving a test that passes while measuring nothing. The
    ``prunes == 1`` assertion is what forecloses that: inline ``_prune``'s body
    into ``record`` and this fails ``assert 0 == 1`` rather than passing.
    Verified by doing exactly that, not by argument. The ``tracked_hosts``
    assertions below say the same thing in observable terms, so a reader who
    distrusts the spy still has the behaviour.
    """
    pacer = HostPacer(60.0, clock=clock)
    for n in range(MAX_TRACKED_HOSTS + 1):
        pacer.record(f"https://host{n}.test/a")

    prunes = 0
    original = pacer._prune

    def counting(now: float) -> None:
        nonlocal prunes
        prunes += 1
        original(now)

    monkeypatch.setattr(pacer, "_prune", counting)
    for n in range(10):
        pacer.record(f"https://later{n}.test/a")

    assert prunes == 0
    # Observable half: nothing was reclaimable, so the set only grew — the bound
    # is deliberately exceeded rather than honoured by forgetting a waiting host.
    assert pacer.tracked_hosts == MAX_TRACKED_HOSTS + 11

    clock.advance(61.0)
    pacer.record("https://trigger.test/a")

    assert prunes == 1
    assert pacer.tracked_hosts == 1


# --------------------------------------------------------------------------- #
# Per-host policy (#19). The numbers arrive on content.fetch-policy; the pacer
# only knows how to ask for one.
# --------------------------------------------------------------------------- #


def policy_of(**intervals: float):
    """A ``policy`` seam backed by a literal map, so these stay pacer tests.

    ``FetchPolicyMap`` is the real implementation and has its own file; wiring it
    in here would make a pacing failure and a policy-application failure look
    identical.
    """
    return lambda host: intervals.get(host)


def test_a_hosts_policy_replaces_the_default_interval(clock):
    pacer = HostPacer(1.0, policy=policy_of(**{"slow.test": 30.0}), clock=clock)
    pacer.record("https://slow.test/a")

    clock.advance(5.0)

    assert pacer.wait_seconds("https://slow.test/b") == pytest.approx(25.0)


def test_a_host_with_no_policy_falls_back_to_the_default(clock):
    """Never to unlimited — the boot replay cannot tell a consumer whether the
    set it received is whole, so absence resolves conservatively."""
    pacer = HostPacer(1.0, policy=policy_of(**{"slow.test": 30.0}), clock=clock)
    pacer.record("https://unknown.test/a")

    clock.advance(0.5)

    assert pacer.wait_seconds("https://unknown.test/b") == pytest.approx(0.5)


def test_a_policy_of_zero_is_honoured_rather_than_read_as_absent(clock):
    """``0.0`` is a legal interval meaning "this host needs no spacing".

    The falsy-zero trap: a lookup written ``policy.get(host) or default`` treats
    an explicit operator decision as a missing one and paces the host anyway.
    """
    pacer = HostPacer(60.0, policy=policy_of(**{"fast.test": 0.0}), clock=clock)
    pacer.record("https://fast.test/a")

    assert pacer.wait_seconds("https://fast.test/b") == 0.0


def test_a_policy_applies_even_when_the_default_disables_pacing(clock):
    """The zero default is the fallback for unknown hosts, not a global off switch."""
    pacer = HostPacer(0.0, policy=policy_of(**{"slow.test": 30.0}), clock=clock)
    pacer.record("https://slow.test/a")

    assert pacer.wait_seconds("https://slow.test/b") == pytest.approx(30.0)


def test_pruning_uses_each_hosts_own_interval(clock):
    """The bound must not be enforced by forgetting a host that is still waiting.

    Pruning against one global interval would drop a 300-second host the moment
    the one-second default elapsed — losing its spacing silently, and in the
    loosening direction, which is the failure the policy stream exists to remove.
    """
    pacer = HostPacer(1.0, policy=policy_of(**{"slow.test": 300.0}), clock=clock)
    pacer.record("https://slow.test/a")
    for n in range(MAX_TRACKED_HOSTS):
        pacer.record(f"https://host{n}.test/a")

    clock.advance(2.0)
    pacer.record("https://trigger.test/a")

    assert pacer.wait_seconds("https://slow.test/b") == pytest.approx(298.0)


def test_the_deferred_prune_retry_waits_out_the_longest_interval_held(clock):
    """The retry bound is derived per host too, or it fires while nothing has aged.

    With a policy host far out beyond the default, the earliest a prune can
    reclaim *that* entry is its own interval — computing the deferral from the
    default would rebuild the dict on every record until the host aged out.
    """
    pacer = HostPacer(1.0, policy=policy_of(**{"slow.test": 300.0}), clock=clock)
    for n in range(MAX_TRACKED_HOSTS + 1):
        pacer.record(f"https://host{n}.test/a")
    pacer.record("https://slow.test/a")

    clock.advance(2.0)
    pacer.record("https://trigger.test/a")

    # Everything on the default aged out; the policy host did not.
    assert pacer.tracked_hosts == 2
    assert pacer.wait_seconds("https://slow.test/b") == pytest.approx(298.0)


# --------------------------------------------------------------------------- #
# Adaptive escalation (#25). The floor stays the issuer's number; a 429 raises
# the *effective* interval above it, temporarily, and a quiet window drops it.
# --------------------------------------------------------------------------- #


def test_a_rate_limited_host_is_spaced_further_than_its_floor(clock):
    """The whole point: an origin that starts refusing gets a slower cadence.

    Before #25 a 429 changed nothing about the host — only the one command that
    hit it was retried, at the reclaim cadence, while its siblings kept the
    original spacing.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    pacer.report_rate_limited("https://example.test/a")

    assert pacer.wait_seconds("https://example.test/b") == pytest.approx(2.0)


def test_the_first_escalation_clears_the_first_step_however_fast_the_floor(clock):
    """Watcher's ``max(current * MULTIPLIER, 2.0)``, recovered from ``2b98989^``.

    Doubling alone would take a 0.1-second host five 429s to reach a second. The
    first step is a floor on the *escalation*, not on the policy interval, so one
    refusal is enough to be meaningfully slower.
    """
    pacer = HostPacer(0.1, clock=clock)
    pacer.record("https://example.test/a")

    assert pacer.report_rate_limited("https://example.test/a") == pytest.approx(BACKOFF_FIRST_STEP)


def test_further_rate_limiting_compounds_from_the_escalated_interval(clock):
    """×2 per refusal, off the interval in force rather than off the floor."""
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    steps = [pacer.report_rate_limited("https://example.test/a") for _ in range(3)]

    assert steps == pytest.approx([2.0, 4.0, 8.0])


def test_the_escalation_is_capped(clock):
    """Unbounded doubling would park a host for hours off a transient refusal.

    ``BACKOFF_MAX_HEADROOM`` is also roughly ``REPLICATOR_CLAIM_MIN_IDLE_MS``, so
    for a host on the interim default the ceiling lands where parking can still
    express the wait.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    for _ in range(20):
        interval = pacer.report_rate_limited("https://example.test/a")

    assert interval == pytest.approx(1.0 + BACKOFF_MAX_HEADROOM)


def test_a_floor_above_the_ceiling_still_escalates(clock):
    """The ceiling is headroom above the published floor, not an absolute (CR #14).

    Watcher's ``BACKOFF_MAX_INTERVAL`` was an absolute 60 s and that was right
    there: its floor was ``DEFAULT_MIN_INTERVAL = 1.0`` and it had no per-host
    published numbers. Replicator does have them (#19), so an absolute ceiling
    makes the whole mechanism inert for every host whose policy already exceeds
    it — silently, and on exactly the origins an issuer has already marked
    fragile and which are therefore likeliest to keep refusing.
    """
    pacer = HostPacer(1.0, policy=policy_of(**{"slow.test": 300.0}), clock=clock)
    pacer.record("https://slow.test/a")

    first = pacer.report_rate_limited("https://slow.test/a")

    assert first > 300.0
    assert pacer.wait_seconds("https://slow.test/b") == pytest.approx(first)
    for _ in range(20):
        interval = pacer.report_rate_limited("https://slow.test/a")
    assert interval == pytest.approx(300.0 + BACKOFF_MAX_HEADROOM)


def test_the_reported_interval_is_the_one_now_in_force(clock):
    """The return value is what the caller logs, and it is the only signal this
    mechanism emits — a 429 publishes no fact, and the transient raise behind it
    is indistinguishable in the journal from a timeout (CR #15).

    So it reports the spacing actually in force, not the escalation the mechanism
    picked. The two agree today; returning the raw escalation made them disagree
    for any host whose floor outran the ceiling, and an operator reading that line
    would have drawn the wrong conclusion about what the host is being spaced at.
    """
    pacer = HostPacer(1.0, policy=policy_of(**{"slow.test": 300.0}), clock=clock)
    pacer.record("https://slow.test/a")

    reported = pacer.report_rate_limited("https://slow.test/a", retry_after_seconds=1.0)

    assert reported == pytest.approx(pacer.wait_seconds("https://slow.test/b"))


def test_escalation_is_per_host(clock):
    """One origin's tolerance says nothing about another's."""
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")
    pacer.record("https://other.test/a")

    pacer.report_rate_limited("https://example.test/a")

    assert pacer.wait_seconds("https://other.test/b") == pytest.approx(1.0)


def test_a_policy_floor_above_the_escalation_still_wins(clock):
    """Escalation may only ever raise the effective interval, never lower it.

    The published number is the issuer's decision and this mechanism is not
    allowed to overrule it downward — a host on a 300-second policy that once
    429'd must not come back at 2 seconds, which is what the multiplier alone
    would have made of the interim default.
    """
    pacer = HostPacer(1.0, policy=policy_of(**{"slow.test": 300.0}), clock=clock)
    pacer.record("https://slow.test/a")

    pacer.report_rate_limited("https://slow.test/a")

    assert pacer.wait_seconds("https://slow.test/b") >= 300.0


def test_the_floor_is_resolved_on_every_read_not_folded_into_the_escalation(clock):
    """A policy republished after a 429 takes effect on the next read.

    What the escalation stores is what the *mechanism* decided, never the
    effective interval — otherwise a number this module invented would shadow one
    the issuer published, which inverts the ownership split the charter settles.
    """
    published = {"slow.test": 0.5}
    pacer = HostPacer(1.0, policy=lambda host: published.get(host), clock=clock)
    pacer.record("https://slow.test/a")
    pacer.report_rate_limited("https://slow.test/a")

    published["slow.test"] = 10.0

    assert pacer.wait_seconds("https://slow.test/b") == pytest.approx(10.0)


def test_a_quiet_window_resets_the_escalation_in_one_step(clock):
    """Watcher's reset was one step (``current_interval = min_interval``), not a ramp.

    Re-derived in memory here rather than mirrored: Watcher's version read and
    wrote a ``Domain`` row, and Replicator has no database by charter.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")
    pacer.report_rate_limited("https://example.test/a")

    clock.advance(BACKOFF_DECAY_SECONDS)
    pacer.record("https://example.test/b")

    assert pacer.wait_seconds("https://example.test/c") == pytest.approx(1.0)


def test_the_escalation_holds_until_the_window_has_actually_elapsed(clock):
    """Asserted from below as well, or "reset immediately" would also pass."""
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")
    pacer.report_rate_limited("https://example.test/a")

    clock.advance(BACKOFF_DECAY_SECONDS - 1.0)
    pacer.record("https://example.test/b")

    assert pacer.wait_seconds("https://example.test/c") == pytest.approx(2.0)


def test_the_quiet_window_runs_from_the_last_refusal_not_the_last_request(clock):
    """The discriminating test, and the correction to Watcher's own docstring.

    Watcher measured the window from ``Domain.last_request_at`` — which, checked
    against ``2b98989^``, only ``_persist_backoff`` ever wrote, i.e. on a 429 and
    nowhere else. So the "quiet" being waited out is quiet *from refusals*, not
    from traffic.

    That distinction is the difference between working and not. A host fetched
    every 60 seconds never has a 30-minute gap between requests, so a window
    measured from the last request would hold every escalation forever on exactly
    the hosts busy enough to have earned one.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")
    pacer.report_rate_limited("https://example.test/a")

    for _ in range(30):
        clock.advance(BACKOFF_DECAY_SECONDS / 30 + 0.1)
        pacer.record("https://example.test/b")

    assert pacer.wait_seconds("https://example.test/c") == pytest.approx(1.0)


def test_a_retry_after_the_origin_sent_beats_the_multipliers_guess(clock):
    """The origin telling us the number is better evidence than doubling."""
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    pacer.report_rate_limited("https://example.test/a", retry_after_seconds=45.0)

    assert pacer.wait_seconds("https://example.test/b") == pytest.approx(45.0)


def test_a_retry_after_is_capped_like_any_other_escalation(clock):
    """An hour-long ``Retry-After`` is a wire value, and the wire is untrusted."""
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    escalated = pacer.report_rate_limited("https://example.test/a", retry_after_seconds=3600.0)

    assert escalated == pytest.approx(1.0 + BACKOFF_MAX_HEADROOM)


def test_a_retry_after_shorter_than_the_step_does_not_soften_the_escalation(clock):
    """Retry-After raises the escalation; it never unwinds one.

    An origin refusing while asking for a 1-second delay is describing the
    cadence it is already refusing. Honouring that downward would let a 429 that
    always carries a small header disable escalation outright — the failure this
    issue exists to fix. Being more polite than asked violates nothing.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")
    pacer.report_rate_limited("https://example.test/a")

    escalated = pacer.report_rate_limited("https://example.test/a", retry_after_seconds=1.0)

    assert escalated == pytest.approx(4.0)


def test_an_absent_or_unusable_retry_after_falls_back_to_the_multiplier(clock):
    """``None`` and a value past its own date are the same answer: no evidence."""
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://example.test/a")

    assert pacer.report_rate_limited("https://example.test/a") == pytest.approx(2.0)
    assert pacer.report_rate_limited(
        "https://example.test/a", retry_after_seconds=-30.0
    ) == pytest.approx(4.0)


def test_a_hostless_url_cannot_be_escalated():
    """Same reasoning as pacing one: it is unfetchable, and a wait would only
    park a permanently-bad command instead of letting it reach its fact."""
    pacer = HostPacer(1.0)

    assert pacer.report_rate_limited("not-a-url") == 0.0
    assert pacer.tracked_hosts == 0


def test_a_refusal_on_a_host_with_no_recorded_request_still_spaces_it(clock):
    """Defensive, and free: the mechanism must not depend on call order.

    ``_pace`` records before the fetch today, so the entry always exists by the
    time a 429 comes back. A pacer that only escalated known hosts would break
    silently if that order ever changed.
    """
    pacer = HostPacer(1.0, clock=clock)

    pacer.report_rate_limited("https://example.test/a")

    assert pacer.wait_seconds("https://example.test/b") == pytest.approx(2.0)


def test_an_escalation_is_held_in_the_bounded_map_the_prune_governs(clock):
    """A second unbounded map would reintroduce exactly the leak ``_prune`` exists
    to prevent, so the escalation rides the same entry as the timestamp.

    Observable as ``tracked_hosts`` — the figure the bound is enforced against and
    the one ``_pace`` reports. An escalation kept beside it would leave this at
    ``0`` while holding an entry, which is the shape of the leak.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.report_rate_limited("https://example.test/a")

    assert pacer.tracked_hosts == 1

    clock.advance(BACKOFF_DECAY_SECONDS)
    for n in range(MAX_TRACKED_HOSTS):
        pacer.record(f"https://host{n}.test/a")
    clock.advance(1.0)
    pacer.record("https://trigger.test/a")

    assert pacer.tracked_hosts == 1


def test_pruning_keeps_a_host_whose_escalation_is_still_in_force(clock):
    """Reclaiming an escalated entry would loosen politeness to honour a memory
    bound — the same trade ``_prune`` already refuses for a host still waiting.

    The escalated interval has elapsed here, so the entry imposes no wait *now*
    and the pre-#25 criterion would drop it. What it still holds is the fact that
    this origin refused recently, which is not recoverable once forgotten.
    """
    pacer = HostPacer(1.0, clock=clock)
    pacer.record("https://limited.test/a")
    pacer.report_rate_limited("https://limited.test/a")
    for n in range(MAX_TRACKED_HOSTS):
        pacer.record(f"https://host{n}.test/a")

    clock.advance(30.0)
    pacer.record("https://trigger.test/a")

    # Asserted on the *next* request, because that is where the surviving state
    # shows: the escalated interval had already elapsed, which is exactly why the
    # pre-#25 criterion would have reclaimed the entry.
    pacer.record("https://limited.test/a")

    assert pacer.wait_seconds("https://limited.test/b") == pytest.approx(2.0)


def test_the_deferred_prune_retry_waits_out_a_live_escalation(clock, monkeypatch):
    """The CR #7 deferral has to know about the decay window too.

    An escalated entry is unreclaimable until its window elapses, however long ago
    its interval ran out, so a deferral computed from the interval alone would
    rebuild the whole dict on every record for the next half hour — the precise
    regression CR #7 fixed, reintroduced through the new state.

    Same spy idiom as ``test_a_prune_that_reclaims_nothing_is_not_retried_every_record``,
    and safe for the same reason: ``prunes == 1`` at the end fails if ``_prune``
    stops being reached through the attribute.
    """
    pacer = HostPacer(1.0, clock=clock)
    for n in range(MAX_TRACKED_HOSTS + 1):
        pacer.record(f"https://host{n}.test/a")
        pacer.report_rate_limited(f"https://host{n}.test/a")

    # The prune at the crossing ran inside the last host's ``record``, one line
    # before that host was escalated, so its deferral was computed with one plain
    # entry in the map. This record recomputes it with every window open, which is
    # the state under test.
    clock.advance(61.0)
    pacer.record("https://host0.test/a")

    prunes = 0
    original = pacer._prune

    def counting(now: float) -> None:
        nonlocal prunes
        prunes += 1
        original(now)

    monkeypatch.setattr(pacer, "_prune", counting)
    # Every interval has elapsed many times over, and every window is still open.
    for n in range(10):
        pacer.record(f"https://later{n}.test/a")

    assert prunes == 0
    assert pacer.tracked_hosts == MAX_TRACKED_HOSTS + 11

    clock.advance(BACKOFF_DECAY_SECONDS)
    pacer.record("https://trigger.test/a")

    assert prunes == 1
    assert pacer.tracked_hosts == 1
