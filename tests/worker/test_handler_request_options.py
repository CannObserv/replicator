"""Per-fetch request options: ``headers`` and ``timeout_seconds`` (#11).

Two halves. The **merge** half is what an issuer asked for reaching the driver
— case-folded so "issuer wins" actually holds against a lowercase default. The
**guard** half is everything Replicator refuses to send on an issuer's behalf,
and every one of those refusals is terminal: a silently-dropped header is a
fingerprint-affecting change nobody can see, so the command dies with a reason
instead of fetching something the issuer cannot account for.
"""

import math

import pytest

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.storage.sweeper import BlobUsage
from src.worker.handler import MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS
from src.worker.handler import REFUSED_HEADERS as REFUSED
from src.worker.loop import Outcome, poll_once
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    FakeFetcher,
    collected_reports,
    command,
    make_command,
    process_one,
    published_facts,
)


async def assert_refused(handler, cmd, fetcher=None):
    """Run the handler expecting a terminal request-options refusal.

    The ``effects == []`` assertion is half the point: a refusal that happened
    *after* the request went out would have already changed what the origin saw.
    """
    fetcher = fetcher if fetcher is not None else FakeFetcher()
    with pytest.raises(PermanentFetchError) as caught:
        await handler(fetcher)(cmd)
    assert caught.value.reason is FailureReason.INVALID_REQUEST_OPTIONS
    assert fetcher.effects == []
    return caught.value


# --------------------------------------------------------------------------- #
# The merge
# --------------------------------------------------------------------------- #


async def test_an_omitted_options_command_reaches_the_driver_unchanged(handler):
    """The compatibility promise: omitted means today's behaviour byte-for-byte.

    ``None`` rather than ``{}`` — the driver distinguishes them only by accident
    today, but passing an empty mapping asserts "the issuer said nothing about
    headers" in the one shape a future driver might read as "send none".
    """
    fetcher = FakeFetcher()

    await handler(fetcher)(command())

    assert fetcher.effect.headers is None
    assert fetcher.effect.timeout is None


async def test_command_headers_reach_the_driver(handler):
    fetcher = FakeFetcher()

    await handler(fetcher)(command(headers={"accept": "text/html"}))

    assert fetcher.effect.headers == {"accept": "text/html"}


async def test_header_names_are_case_folded_so_the_issuer_wins(handler):
    """The whole reason the fold exists (cannobserv#272 CR).

    ``AsyncFetchDriver`` merges ``{"user-agent": DEFAULT, **effect.headers}`` —
    a plain, case-*sensitive* dict. Without the fold a capitalized ``User-Agent``
    leaves both keys in the mapping and httpx sends **both field lines**, the
    default first, leaving the origin to decide which applies. Measured against
    a real driver and socket, not inferred.

    That is exactly the fingerprint-continuity case Watcher cares about
    (watcher#241): the UA it pins would arrive alongside the one it is trying to
    replace.
    """
    fetcher = FakeFetcher()

    await handler(fetcher)(command(headers={"User-Agent": "watcher/0.1.0"}))

    assert fetcher.effect.headers == {"user-agent": "watcher/0.1.0"}


async def test_surrounding_whitespace_is_not_part_of_a_header_value(handler):
    """OWS is excluded from a field value by RFC 9110, so stripping it is not a
    modification — the same rule ``_passthrough`` applies to the response side."""
    fetcher = FakeFetcher()

    await handler(fetcher)(command(headers={"accept": "  text/html  "}))

    assert fetcher.effect.headers == {"accept": "text/html"}


async def test_the_timeout_reaches_the_driver(handler):
    """``timeout_seconds`` on the wire, ``timeout`` on the effect — a boundary
    translation co-core spells out deliberately."""
    fetcher = FakeFetcher()

    await handler(fetcher)(command(timeout_seconds=2.5))

    assert fetcher.effect.timeout == 2.5


# --------------------------------------------------------------------------- #
# The guards — headers
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name", sorted(REFUSED))
async def test_a_framing_header_is_refused(handler, name):
    """Refused, not stripped.

    Dropping one silently changes what the origin saw with no signal an issuer
    could act on — the same argument that makes an over-long value a drop rather
    than a truncation, applied to the request side.
    """
    await assert_refused(handler, command(headers={name: "x"}))


async def test_a_proxy_header_is_refused(handler):
    await assert_refused(handler, command(headers={"Proxy-Authorization": "Basic x"}))


async def test_a_case_collision_is_refused(handler):
    """Folding these would silently discard one of them — see the module docstring."""
    await assert_refused(handler, command(headers={"User-Agent": "a", "user-agent": "b"}))


async def test_a_case_collision_is_refused_even_when_the_values_agree(handler):
    """The issuer is confused either way, and "agrees today" is not a rule worth
    writing: it makes the refusal depend on the values rather than the shape."""
    await assert_refused(handler, command(headers={"Accept": "text/html", "accept": "text/html"}))


@pytest.mark.parametrize(
    "name",
    [
        "user agent",  # SP is not a token character
        "user:agent",  # the field separator itself
        "user\nagent",
        "",
        "acc€pt",
    ],
)
async def test_a_header_name_that_is_not_a_token_is_refused(handler, name):
    await assert_refused(handler, command(headers={name: "x"}))


@pytest.mark.parametrize(
    "value",
    [
        "one\r\nX-Injected: two",  # request splitting, the reason this guard exists
        "one\nX-Injected: two",
        "one\rtwo",
        "one\x00two",
        "one\x1btwo",
        "snőw",  # beyond latin-1: not encodable as a field value
    ],
)
async def test_a_header_value_that_cannot_be_sent_verbatim_is_refused(handler, value):
    """Terminal on purpose.

    httpx raises ``LocalProtocolError`` for these, which subclasses
    ``httpx.HTTPError`` and would therefore land in ``_fetch``'s catch-all as
    *transient* — exempt from the delivery ceiling, so the command would retry at
    the reclaim cadence forever and never close.
    """
    await assert_refused(handler, command(headers={"x-test": value}))


async def test_too_many_headers_are_refused(handler):
    headers = {f"x-h{index}": "v" for index in range(MAX_REQUEST_HEADERS + 1)}

    await assert_refused(handler, command(headers=headers))


async def test_the_maximum_header_count_is_allowed(handler):
    """The bound is a ceiling, not an off-by-one."""
    fetcher = FakeFetcher()
    headers = {f"x-h{index}": "v" for index in range(MAX_REQUEST_HEADERS)}

    await handler(fetcher)(command(headers=headers))

    assert fetcher.effect.headers == headers


async def test_headers_over_the_total_size_bound_are_refused(handler):
    await assert_refused(handler, command(headers={"x-big": "v" * MAX_REQUEST_HEADER_BYTES}))


# --------------------------------------------------------------------------- #
# The guards — timeout
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("seconds", [0.0, -1.0, math.nan, math.inf])
async def test_a_nonsensical_timeout_is_refused(handler, seconds):
    await assert_refused(handler, command(timeout_seconds=seconds))


async def test_a_timeout_over_the_ceiling_is_refused(handler, settings):
    """The consume path is serial (``count=1``), so an issuer's timeout is a lien
    on every other command in the group, not only its own."""
    await assert_refused(handler, command(timeout_seconds=settings.max_fetch_timeout_seconds + 1))


async def test_the_ceiling_itself_is_allowed(handler, settings):
    fetcher = FakeFetcher()

    await handler(fetcher)(command(timeout_seconds=settings.max_fetch_timeout_seconds))

    assert fetcher.effect.timeout == settings.max_fetch_timeout_seconds


# --------------------------------------------------------------------------- #
# Ordering and side effects
# --------------------------------------------------------------------------- #


async def test_bad_options_are_refused_before_the_storage_ceiling_parks_the_command(
    handler, settings
):
    """Validation is pure and free; the ceiling raise is transient and parks the
    message in the PEL for a sweep interval. A permanently-bad command must not
    wait minutes to reach a conclusion available immediately."""
    usage = BlobUsage(total_bytes=settings.blob_max_total_bytes)

    with pytest.raises(PermanentFetchError) as caught:
        await handler(FakeFetcher(), usage=usage)(command(headers={"Host": "elsewhere.test"}))

    assert not isinstance(caught.value, TransientFetchError)
    assert caught.value.reason is FailureReason.INVALID_REQUEST_OPTIONS


async def test_a_refused_command_stores_nothing_and_publishes_nothing(
    handler, fake_redis, tmp_path
):
    await assert_refused(handler, command(headers={"Connection": "close"}))

    assert await published_facts(fake_redis) == []
    assert list(tmp_path.iterdir()) == []


async def test_a_refused_command_closes_with_the_new_taxonomy_row(
    fake_redis, consumer, settings, handler
):
    """The refusal reaches the issuer, which is the whole reason it is a refusal.

    Pins the row the contract's failure taxonomy gained: terminal fact naming
    ``invalid_request_options``, then the DLQ. A guard that dead-lettered
    silently would leave an issuer waiting on its reaper for a mistake
    Replicator diagnosed instantly.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-bad", headers={"Host": "x.test"}))
    reports = collected_reports()

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, handler(), reporter=reports
    )

    assert outcome is Outcome.DEAD_LETTERED
    (report,) = reports.reports
    assert report.reason is FailureReason.INVALID_REQUEST_OPTIONS
    assert report.command_id == "cmd-bad"
    # No status_code: nothing was ever sent, so there is no origin answer to
    # report. An issuer branching on it must see the absence, not a zero.
    assert report.status_code is None
