"""Per-host pacing: the interim politeness default (#12).

The state machine only. How the handler spends the wait it is told about — sleep
below the bound, park above it — is ``tests/worker/test_handler_pacing.py``.
"""

import pytest

from src.worker.pacing import MAX_TRACKED_HOSTS, HostPacer


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
