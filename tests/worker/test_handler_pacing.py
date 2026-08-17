"""How the byte path spends a pacing wait (#12), and what a 429 does to it (#25).

Two outcomes, split by duration: a short wait is slept through in the handler, a
long one parks the message in the PEL for ``claim_stale`` to bring back. The
split exists because neither alone is correct on a serial consume path — sleeping
through a long wait blocks every other host's commands and a SIGTERM behind them,
and parking is bounded below by ``REPLICATOR_CLAIM_MIN_IDLE_MS``, so it cannot
express the sub-minute spacing that is the normal case.

The escalation half is the wiring plus the ``Retry-After`` parse: which statuses
reach the pacer, and how a header value becomes seconds. The state machine it
feeds — compounding, the cap, the quiet window — is ``test_pacing.py``.
"""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from co_core.pure.adapters.bus import streams

from src.core.config import get_settings
from src.core.errors import (
    CompletedWithoutBlobError,
    PermanentFetchError,
    TransientFetchError,
)
from src.storage.local import LocalBlobStore
from src.worker.handler import _retry_after_seconds, build_handler
from src.worker.pacing import BACKOFF_MAX_INTERVAL, HostPacer
from tests.worker.conftest import URL, Clock, FakeFetcher, command, fetch_result, published_facts


@pytest.fixture
def paced(fake_redis, tmp_path):
    """A handler whose pacer and sleep bound the test controls."""

    def build(
        pacer: HostPacer,
        park_above_seconds: float = 5.0,
        stop: asyncio.Event | None = None,
        fetcher: FakeFetcher | None = None,
    ):
        fetcher = fetcher if fetcher is not None else FakeFetcher()
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


async def test_a_wait_longer_than_the_bound_parks_the_message(paced, fake_redis, clock):
    """Transient, so the delivery ceiling is not burned for being polite.

    The command comes back through ``claim_stale`` — the same idiom the disk
    ceiling uses, and the reason a paced command can never dead-letter for pacing.

    On the injected clock, not the real one (CR #24). The wait is the *remainder*
    of the interval and the message formats it to one decimal, so with a live
    clock the first handler's store-and-publish spending 50ms turns
    ``60.0-second`` into ``59.9-second`` and this fails — about one run in ten,
    and it fails identically to a real regression in the wait calculation.
    Freezing the clock makes the number exact by construction, which is what
    ``test_the_pacer_reports_the_remaining_wait`` below already does.
    """
    handler, fetcher = paced(HostPacer(60.0, clock=clock), park_above_seconds=5.0)
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


# --------------------------------------------------------------------------- #
# Escalation wiring (#25). Which statuses reach the pacer, and the Retry-After
# parse. Reported from the call site rather than from _raise_for_status — see the
# module docstring on src/worker/handler.py::_report_rate_limited.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("status_code", [429, 503])
async def test_the_statuses_that_ask_for_later_raise_the_hosts_interval(paced, clock, status_code):
    """A 429 is the origin refusing, a 503 is it struggling; both want more space.

    The escalation outlives the command that earned it, which is the whole point:
    before #25 only the one command that hit the 429 was slowed, by the accident
    of the reclaim cadence, while every sibling command to that host kept the
    original spacing.
    """
    pacer = HostPacer(1.0, clock=clock)
    refused = FakeFetcher(fetch_result(status_code=status_code, headers={}))
    handler, _ = paced(pacer, fetcher=refused)

    with pytest.raises(TransientFetchError, match=f"HTTP {status_code}"):
        await handler(command("cmd-1"))

    assert pacer.wait_seconds(URL) == pytest.approx(2.0)


@pytest.mark.parametrize("status_code", [304, 404, 412, 500, 502])
async def test_no_other_status_touches_the_hosts_interval(paced, clock, status_code):
    """Escalation is for the two statuses that mean "later", not for failure at
    large. A 404 is a settled answer, a 304 is a success, and a 500 is an origin
    bug; none is evidence about how often this origin tolerates being asked.

    500 is the interesting row: it is *transient* like a 429, so a mechanism keyed
    on the exception type rather than on the status would escalate here too.
    """
    pacer = HostPacer(1.0, clock=clock)
    answered = FakeFetcher(fetch_result(status_code=status_code, headers={}))
    handler, _ = paced(pacer, fetcher=answered)

    with pytest.raises((PermanentFetchError, TransientFetchError, CompletedWithoutBlobError)):
        await handler(command("cmd-1"))

    assert pacer.wait_seconds(URL) == pytest.approx(1.0)


async def test_a_successful_fetch_leaves_the_interval_at_its_floor(paced, clock):
    """The other half of the above, on the path that does not raise at all."""
    pacer = HostPacer(1.0, clock=clock)
    handler, _ = paced(pacer)

    await handler(command("cmd-1"))

    assert pacer.wait_seconds(URL) == pytest.approx(1.0)


async def test_a_retry_after_in_seconds_sets_the_interval(paced, clock):
    """Delta-seconds, the common wire form. 45 is well past what doubling a
    1-second floor would have guessed, which is why the header is worth reading."""
    pacer = HostPacer(1.0, clock=clock)
    handler, _ = paced(
        pacer,
        fetcher=FakeFetcher(fetch_result(status_code=429, headers={"Retry-After": "45"})),
    )

    with pytest.raises(TransientFetchError):
        await handler(command("cmd-1"))

    assert pacer.wait_seconds(URL) == pytest.approx(45.0)


async def test_the_retry_after_header_is_read_through_the_case_fold(paced, clock):
    """``_folded_headers``, not ``result.headers``. The fold exists because
    ``FetchResult.headers`` is a plain ``Mapping`` with no casing guarantee, and a
    raw lookup would silently miss every lowercase spelling."""
    pacer = HostPacer(1.0, clock=clock)
    handler, _ = paced(
        pacer,
        fetcher=FakeFetcher(fetch_result(status_code=429, headers={"retry-after": "30"})),
    )

    with pytest.raises(TransientFetchError):
        await handler(command("cmd-1"))

    assert pacer.wait_seconds(URL) == pytest.approx(30.0)


async def test_a_malformed_retry_after_falls_back_to_the_multiplier(paced, clock):
    """It must not raise. An unparseable header is an origin being sloppy, and
    turning that into an unclassified handler failure would burn the delivery
    ceiling on a command whose only problem is that the origin is busy."""
    pacer = HostPacer(1.0, clock=clock)
    handler, _ = paced(
        pacer,
        fetcher=FakeFetcher(fetch_result(status_code=429, headers={"Retry-After": "soon"})),
    )

    with pytest.raises(TransientFetchError):
        await handler(command("cmd-1"))

    assert pacer.wait_seconds(URL) == pytest.approx(2.0)


async def test_an_http_date_retry_after_is_honoured(paced, clock):
    """The second wire form (RFC 9110 §10.2.3), and the one easy to forget.

    Read as a duration from now, so the assertion is a window rather than a point:
    the pacer's clock is monotonic and the header's is wall-clock, and only the
    handler can bridge them.
    """
    when = datetime.now(UTC) + timedelta(seconds=40)
    pacer = HostPacer(1.0, clock=clock)
    handler, _ = paced(
        pacer,
        fetcher=FakeFetcher(
            fetch_result(
                status_code=429,
                headers={"Retry-After": when.strftime("%a, %d %b %Y %H:%M:%S GMT")},
            )
        ),
    )

    with pytest.raises(TransientFetchError):
        await handler(command("cmd-1"))

    assert 35.0 <= pacer.wait_seconds(URL) <= 41.0


async def test_a_retry_after_past_the_ceiling_is_capped(paced, clock):
    """An untrusted number off the wire, bounded by the same ceiling as any other
    escalation. A day-long value would otherwise park the host indefinitely."""
    pacer = HostPacer(1.0, clock=clock)
    handler, _ = paced(
        pacer,
        fetcher=FakeFetcher(fetch_result(status_code=429, headers={"Retry-After": "86400"})),
    )

    with pytest.raises(TransientFetchError):
        await handler(command("cmd-1"))

    assert pacer.wait_seconds(URL) == pytest.approx(BACKOFF_MAX_INTERVAL)


async def test_the_escalated_wait_parks_the_next_command(paced, fake_redis, clock):
    """End to end: the escalation is only worth having if it reaches the wait.

    The multiplier's 2 seconds is *under* this fixture's 5-second park bound, so
    every assertion above is satisfied by an escalation that reaches
    ``wait_seconds`` and stops there. A 45-second ``Retry-After`` puts the wait
    over the bound, which is what turns the mechanism into an observable outcome —
    a parked command, and an origin not asked again.
    """
    pacer = HostPacer(1.0, clock=clock)
    limited, ok = (
        FakeFetcher(fetch_result(status_code=429, headers={"Retry-After": "45"})),
        FakeFetcher(),
    )
    handler, _ = paced(pacer, fetcher=limited)
    with pytest.raises(TransientFetchError):
        await handler(command("cmd-1"))

    handler, _ = paced(pacer, fetcher=ok, park_above_seconds=5.0)
    with pytest.raises(TransientFetchError, match="45.0-second politeness window"):
        await handler(command("cmd-2"))

    assert ok.urls == [], "the escalated host must not have been asked again"
    assert await published_facts(fake_redis) == []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("120", 120.0),
        ("0", 0.0),
        (" 30 ", 30.0),
        ("-5", -5.0),
        ("", None),
        ("   ", None),
        (None, None),
        ("soon", None),
        ("1.5", None),
        ("0x10", None),
        ("Wed, 01 Jan 2025 01:00:00 GMT", 3600.0),
        ("Tue, 31 Dec 2024 23:00:00 GMT", -3600.0),
    ],
)
def test_the_retry_after_parse_covers_both_wire_forms_and_the_rubbish(value, expected):
    """Delta-seconds is ``1*DIGIT`` per RFC 9110 — not a float.

    ``1.5`` and ``1e3`` are rejected rather than accepted leniently, because a
    value that is not a legal delta-seconds is not evidence about anything; it
    falls through to the date parse and then to ``None``, which the pacer reads as
    "no header" and answers with the multiplier.
    """
    now = datetime(2025, 1, 1, tzinfo=UTC)

    assert _retry_after_seconds(value, now) == expected


def test_a_retry_after_date_with_no_zone_is_read_as_utc():
    """``-0000`` is "unknown zone" in RFC 5322 and ``parsedate_to_datetime``
    returns a *naive* datetime for it, which would raise on subtraction from an
    aware one. Everything in this service is UTC, so that is the reading."""
    now = datetime(2025, 1, 1, tzinfo=UTC)

    assert _retry_after_seconds("Wed, 01 Jan 2025 00:01:00 -0000", now) == pytest.approx(60.0)


async def test_a_permanent_status_is_still_permanent_with_the_report_ahead_of_it(paced, clock):
    """The report is a side effect ahead of the classifier, not a replacement for
    it: ``_raise_for_status`` still owns every outcome (#17's three arms included)."""
    pacer = HostPacer(1.0, clock=clock)
    handler, _ = paced(pacer, fetcher=FakeFetcher(fetch_result(status_code=404, headers={})))

    with pytest.raises(PermanentFetchError, match="HTTP 404"):
        await handler(command("cmd-1"))


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
