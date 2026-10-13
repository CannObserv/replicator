"""Boot's broker calls ride out an outage the way the consume loop does (#133).

A start that boots *into* a broker that is still down used to exit on the first
``ensure_group``: co-core swallows ``BUSYGROUP`` only, and nothing in ``run()``
caught the ``ConnectionError``. Each such start cost ~45 s of the unit's start
budget instead of the loop's ~7.5 min, so six starts absorbed ~12 min of outage
rather than the hour #94 sized them for.

The retry shares the loop's backoff and its consecutive-failure ceiling, so
``worst_case_outage_seconds`` describes every start, not only the first. What it
retries is narrower than the loop's "any exception": only the transient set, and
never ``NoPermissionError`` — boot's ``XGROUP CREATE … MKSTREAM`` refusal on an
ungranted stream must keep failing fast (broker#64).
"""

import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoPermissionError, OutOfMemoryError, ResponseError

from src.worker.loop import retry_at_boot

# Every test either patches park or drives sub-millisecond backoffs.
DEADLINE = 1.0


def _brisk(settings, **update):
    return settings.model_copy(
        update={"error_backoff_base_seconds": 0.001, "error_backoff_max_seconds": 0.001, **update}
    )


class _Flaky:
    """A boot call that raises ``errors`` in order, then succeeds."""

    def __init__(self, *errors: Exception) -> None:
        self.errors = list(errors)
        self.attempts = 0

    async def __call__(self) -> None:
        self.attempts += 1
        if self.errors:
            raise self.errors.pop(0)


async def _retry(call, settings, stop=None) -> bool:
    return await asyncio.wait_for(
        retry_at_boot(call, what="ensure_group", settings=settings, stop=stop or asyncio.Event()),
        DEADLINE,
    )


async def test_a_broker_down_at_boot_is_ridden_out(settings):
    call = _Flaky(RedisConnectionError("refused"), ConnectionError("refused"))

    assert await _retry(call, _brisk(settings)) is True
    assert call.attempts == 3


async def test_a_capped_broker_at_boot_is_ridden_out(settings):
    """``XGROUP CREATE … MKSTREAM`` is a write; a capped broker refuses it (#79)."""
    call = _Flaky(OutOfMemoryError("command not allowed when used memory > 'maxmemory'"))

    assert await _retry(call, _brisk(settings)) is True
    assert call.attempts == 2


async def test_boot_waits_longer_between_repeated_failures(settings, monkeypatch):
    """The wiring, not the arithmetic: the failure count reaches the backoff."""
    waits: list[float] = []

    async def recording_park(event: asyncio.Event, seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr("src.worker.loop.park", recording_park)
    call = _Flaky(*(RedisConnectionError("refused") for _ in range(3)))

    await _retry(call, settings)

    base = settings.error_backoff_base_seconds
    assert waits == [base, base * 2, base * 4]


async def test_boot_gives_up_at_the_loops_own_ceiling(settings):
    """Same ceiling as the loop, so ``worst_case_outage_seconds`` covers boot too."""
    doomed = _brisk(settings, max_consecutive_cycle_failures=3)
    call = _Flaky(*(RedisConnectionError("refused") for _ in range(10)))

    with pytest.raises(RedisConnectionError):
        await _retry(call, doomed)
    assert call.attempts == 3


async def test_a_success_resets_the_count_for_the_next_call(settings):
    """One counter per call: a broker that answered is no longer down."""
    forgiving = _brisk(settings, max_consecutive_cycle_failures=3)

    for _ in range(3):
        call = _Flaky(RedisConnectionError("blip"), RedisConnectionError("blip"))
        assert await _retry(call, forgiving) is True


async def test_an_ungranted_stream_fails_fast(settings):
    """broker#64: the one refusal at boot that must not retry."""
    call = _Flaky(NoPermissionError("No permissions to access a key"))

    with pytest.raises(NoPermissionError):
        await _retry(call, _brisk(settings))
    assert call.attempts == 1


async def test_a_refusal_that_is_not_an_outage_fails_fast(settings):
    """A wrong start id is a configuration bug; retrying it only delays the exit."""
    call = _Flaky(ResponseError("ERR Invalid stream ID specified as stream command argument"))

    with pytest.raises(ResponseError):
        await _retry(call, _brisk(settings))
    assert call.attempts == 1


async def test_a_stop_during_the_backoff_ends_boot_without_another_attempt(settings, monkeypatch):
    stop = asyncio.Event()

    async def stopping_park(event: asyncio.Event, seconds: float) -> None:
        event.set()

    monkeypatch.setattr("src.worker.loop.park", stopping_park)
    call = _Flaky(*(RedisConnectionError("refused") for _ in range(5)))

    assert await _retry(call, settings, stop) is False
    assert call.attempts == 1


async def test_a_call_made_after_a_stop_is_still_attempted_once(settings):
    """A pre-set stop is how the scaffold tests run ``run()`` to its startup work."""
    stop = asyncio.Event()
    stop.set()
    call = _Flaky()

    assert await _retry(call, settings, stop) is True
    assert call.attempts == 1


async def test_a_refusing_broker_is_not_logged_as_unreachable(settings, caplog):
    """CR 4: a capped or loading broker answered — the line must not say otherwise."""
    call = _Flaky(OutOfMemoryError("command not allowed when used memory > 'maxmemory'"))

    with caplog.at_level("INFO", logger="src.worker.loop"):
        await _retry(call, _brisk(settings))

    messages = [record.getMessage() for record in caplog.records]
    assert messages
    assert not any("unreachable" in message for message in messages), messages
