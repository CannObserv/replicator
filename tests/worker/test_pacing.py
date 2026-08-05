"""Per-host pacing: the interim politeness default (#12).

The state machine only. How the handler spends the wait it is told about — sleep
below the bound, park above it — is ``tests/worker/test_handler_pacing.py``.
"""

import pytest

from src.worker.pacing import MAX_TRACKED_HOSTS, HostPacer


class Clock:
    """A monotonic clock the test advances by hand.

    Pacing is the one thing in the byte path whose whole behaviour is a duration,
    so every assertion here would otherwise be a real sleep — slow, and flaky in
    exactly the direction that hides an off-by-one.
    """

    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock()


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


def test_a_zero_interval_disables_pacing(clock):
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
