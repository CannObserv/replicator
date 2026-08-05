"""How the byte path spends a pacing wait (#12).

Two outcomes, split by duration: a short wait is slept through in the handler, a
long one parks the message in the PEL for ``claim_stale`` to bring back. The
split exists because neither alone is correct on a serial consume path — sleeping
through a long wait blocks every other host's commands and a SIGTERM behind them,
and parking is bounded below by ``REPLICATOR_CLAIM_MIN_IDLE_MS``, so it cannot
express the sub-minute spacing that is the normal case.
"""

import asyncio

import pytest
from co_core.pure.adapters.bus import streams

from src.core.config import get_settings
from src.core.errors import TransientFetchError
from src.storage.local import LocalBlobStore
from src.worker.handler import build_handler
from src.worker.pacing import HostPacer
from tests.worker.conftest import URL, Clock, FakeFetcher, command, published_facts


@pytest.fixture
def paced(fake_redis, tmp_path):
    """A handler whose pacer and sleep bound the test controls."""

    def build(
        pacer: HostPacer,
        park_above_seconds: float = 5.0,
        stop: asyncio.Event | None = None,
    ):
        fetcher = FakeFetcher()
        return build_handler(
            fetcher=fetcher,
            store=LocalBlobStore(tmp_path),
            client=fake_redis,
            settings=get_settings(),
            pacer=pacer,
            park_above_seconds=park_above_seconds,
            stop=stop,
        ), fetcher

    return build


async def test_a_short_wait_is_slept_through_and_the_fetch_still_happens(paced, fake_redis):
    """The normal case. A sub-second space between two commands is not a failure."""
    handler, fetcher = paced(HostPacer(0.05))

    await handler(command("cmd-1"))
    await handler(command("cmd-2"))

    assert fetcher.urls == [URL, URL]
    assert len(await published_facts(fake_redis)) == 2


async def test_two_requests_to_one_host_are_actually_spaced(paced):
    """Measured across both fetches, not across the sleep.

    The wait is the *remainder* of the interval — the store and publish between
    the two commands have already spent part of it. An assertion on the sleep
    alone would fail for the handler doing its job quickly, which is backwards.
    """
    handler, _ = paced(HostPacer(0.2))
    loop = asyncio.get_running_loop()

    started = loop.time()
    await handler(command("cmd-1"))
    await handler(command("cmd-2"))

    assert loop.time() - started >= 0.2


async def test_a_wait_longer_than_the_bound_parks_the_message(paced, fake_redis):
    """Transient, so the delivery ceiling is not burned for being polite.

    The command comes back through ``claim_stale`` — the same idiom the disk
    ceiling uses, and the reason a paced command can never dead-letter for pacing.
    """
    handler, fetcher = paced(HostPacer(60.0), park_above_seconds=5.0)
    await handler(command("cmd-1"))

    with pytest.raises(TransientFetchError, match="60.0-second"):
        await handler(command("cmd-2"))

    assert fetcher.urls == [URL], "the parked command must not have been fetched"
    assert len(await published_facts(fake_redis)) == 1


async def test_a_shutdown_during_the_sleep_parks_rather_than_fetches(paced):
    """SIGTERM must not buy an unpaced request on the way out.

    ``park`` returns early when the stop event is set, and the handler cannot
    treat that as the wait having elapsed — the origin has not had its space. The
    message stays in the PEL, which is where an interrupted command belongs.
    """
    stop = asyncio.Event()
    handler, fetcher = paced(HostPacer(30.0), park_above_seconds=60.0, stop=stop)

    await handler(command("cmd-1"))
    stop.set()
    with pytest.raises(TransientFetchError, match="stopping"):
        await handler(command("cmd-2"))

    assert fetcher.urls == [URL]


async def test_pacing_runs_after_the_option_guards(paced, fake_redis):
    """A command that can never succeed must not wait first.

    Same ordering argument as the storage ceiling: validation is pure and free,
    and a permanently-bad command deserves its terminal fact now rather than
    after a reclaim cycle.
    """
    handler, fetcher = paced(HostPacer(60.0), park_above_seconds=0.0)
    await handler(command("cmd-1"))

    with pytest.raises(Exception, match="invalid|refused") as caught:
        await handler(command("cmd-2", headers={"Host": "elsewhere.test"}))

    assert not isinstance(caught.value, TransientFetchError)
    assert fetcher.urls == [URL]


async def test_a_parked_command_does_not_consume_its_own_space(paced):
    """Only a request that goes out resets the clock.

    Recording the attempt would let a burst of parked redeliveries push the next
    real fetch out indefinitely — the origin would be spaced from requests it
    never received.

    Driven by a hand-advanced clock (CR #6). The earlier form compared two live
    ``wait_seconds`` readings, which did kill the mutant but only via an implicit
    argument about which of two elapsed intervals was larger — sound, and
    impossible to see. Here the remaining wait is exact: 60 s minus the 10 s
    advanced, and unchanged by the park in between.
    """
    clock = Clock()
    pacer = HostPacer(60.0, clock=clock)
    handler, _ = paced(pacer, park_above_seconds=5.0)
    await handler(command("cmd-1"))

    clock.advance(10.0)
    with pytest.raises(TransientFetchError):
        await handler(command("cmd-2"))

    assert pacer.wait_seconds(URL) == pytest.approx(50.0)


async def test_an_uninjected_pacer_still_paces(fake_redis, tmp_path, monkeypatch):
    """The seam fails *open*, so the default must not be "no pacing".

    A byte path that quietly stopped pacing looks exactly like one that is
    working — no error, no fact, nothing in the journal — so the pacer is built
    from settings rather than left to the caller to remember. This is the
    ``BlobUsage`` argument one seam over: wired wrong, both halves stay
    individually correct and the guard is simply never reached.
    """
    monkeypatch.setenv("REPLICATOR_MIN_HOST_INTERVAL_SECONDS", "30")
    get_settings.cache_clear()
    handler = build_handler(
        fetcher=(fetcher := FakeFetcher()),
        store=LocalBlobStore(tmp_path),
        client=fake_redis,
        settings=get_settings(),
    )

    await handler(command("cmd-1"))
    with pytest.raises(TransientFetchError, match="politeness window"):
        await handler(command("cmd-2"))

    assert fetcher.urls == [URL]


async def test_the_default_park_bound_is_the_poll_window(fake_redis, tmp_path):
    """Sleep up to one blocking read, park beyond it.

    Derived from ``REPLICATOR_READ_BLOCK_MS`` rather than given its own knob: a
    wait no longer than a poll the loop already performs adds nothing to the
    worst-case shutdown latency ``TimeoutStopSec`` is sized for
    (``tests/test_deploy.py``), and a second setting would be one more number to
    keep in agreement with the unit.
    """
    settings = get_settings()
    handler = build_handler(
        fetcher=FakeFetcher(),
        store=LocalBlobStore(tmp_path),
        client=fake_redis,
        settings=settings,
        pacer=HostPacer(settings.read_block_ms / 1000 + 1),
    )

    await handler(command("cmd-1"))

    with pytest.raises(TransientFetchError, match="politeness window"):
        await handler(command("cmd-2"))


async def test_a_wait_inside_the_poll_window_sleeps_rather_than_parks(fake_redis, tmp_path):
    """The other half of the bound (CR #11).

    Asserted from below as well as above, because the park assertion alone would
    also pass with a default of ``0`` — which would park every paced command and
    put the whole corpus on a 60-second reclaim cadence. That is the 60x failure
    the split exists to avoid, and it must not be one line from passing.
    """
    settings = get_settings()
    handler = build_handler(
        fetcher=(fetcher := FakeFetcher()),
        store=LocalBlobStore(tmp_path),
        client=fake_redis,
        settings=settings,
        pacer=HostPacer(0.05),
    )

    await handler(command("cmd-1"))
    await handler(command("cmd-2"))

    assert fetcher.urls == [URL, URL]


async def test_an_unpaced_handler_is_the_pre_12_byte_path(fake_redis, tmp_path):
    """Zero interval is the escape hatch, and it must cost nothing."""
    fetcher = FakeFetcher()
    handler = build_handler(
        fetcher=fetcher,
        store=LocalBlobStore(tmp_path),
        client=fake_redis,
        settings=get_settings(),
        pacer=HostPacer(0.0),
        blobs_topic=streams.CONTENT_BLOBS,
    )

    await handler(command("cmd-1"))
    await handler(command("cmd-2"))

    assert fetcher.urls == [URL, URL]
