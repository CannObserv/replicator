"""Outage resilience: a failing *cycle* (not a failing message) is the loop's own.

A broker that refuses reads, acks, or DLQ writes must be ridden out with
backoff — and, if it never comes back, must eventually surface as an exit rather
than a worker that looks alive to systemd while doing nothing.
"""

import asyncio

import pytest
from co_core.pure.models.changes import ContentFetchCommand
from redis.exceptions import ConnectionError as RedisConnectionError

from src.worker.loop import (
    ERROR_BACKOFF_BASE_SECONDS,
    ERROR_BACKOFF_MAX_SECONDS,
    _error_backoff_seconds,
)
from tests.worker.conftest import TOPIC, drive_loop, make_command, unreachable_handler


def _always_failing_read(monkeypatch, consumer):
    """Make every read raise as a down broker would."""

    async def dead_read(**kwargs):
        raise RedisConnectionError("broker went away")

    monkeypatch.setattr(consumer, "read", dead_read)


async def test_a_broker_failure_does_not_kill_the_loop(fake_redis, consumer, settings, monkeypatch):
    """CR #1: a Redis blip must back off and retry, not terminate the worker."""
    monkeypatch.setattr("src.worker.loop.ERROR_BACKOFF_BASE_SECONDS", 0.01)
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-after-outage"))
    stop = asyncio.Event()
    seen: list[str] = []
    failures = {"left": 2}

    real_read = consumer.read

    async def flaky_read(**kwargs):
        if failures["left"]:
            failures["left"] -= 1
            raise RedisConnectionError("broker went away")
        return await real_read(**kwargs)

    monkeypatch.setattr(consumer, "read", flaky_read)

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)
        stop.set()

    await drive_loop(fake_redis, consumer, settings, handler, stop)

    assert failures["left"] == 0
    assert seen == ["cmd-after-outage"]


async def test_the_backoff_escalates_and_caps():
    """Consecutive failures must escalate, capped — not hammer a down broker."""
    first = _error_backoff_seconds(1, ERROR_BACKOFF_BASE_SECONDS)
    second = _error_backoff_seconds(2, ERROR_BACKOFF_BASE_SECONDS)
    huge = _error_backoff_seconds(10_000, ERROR_BACKOFF_BASE_SECONDS)

    assert first == ERROR_BACKOFF_BASE_SECONDS
    assert second > first
    assert huge == ERROR_BACKOFF_MAX_SECONDS


async def test_the_loop_actually_waits_longer_between_repeated_failures(
    fake_redis, consumer, settings, monkeypatch
):
    """CR #13: assert the wiring, not just the arithmetic.

    Patching the base to something tiny (as the resilience tests must, to stay
    fast) hides whether the loop passes the failure count to the backoff at all
    — parking on the idle interval instead would leave every other test green
    while the worker hammered a down broker.
    """
    waits: list[float] = []
    stop = asyncio.Event()

    async def recording_park(event: asyncio.Event, seconds: float) -> None:
        waits.append(seconds)
        if len(waits) == 3:
            event.set()

    monkeypatch.setattr("src.worker.loop._park", recording_park)
    _always_failing_read(monkeypatch, consumer)

    await drive_loop(fake_redis, consumer, settings, unreachable_handler, stop)

    assert waits == [
        ERROR_BACKOFF_BASE_SECONDS,
        ERROR_BACKOFF_BASE_SECONDS * 2,
        ERROR_BACKOFF_BASE_SECONDS * 4,
    ]


async def test_a_sustained_outage_eventually_exits(fake_redis, consumer, settings, monkeypatch):
    """CR #12: absorbing forever would hide a permanently wrong REDIS_URL.

    Nothing exits, so systemd's Restart=on-failure never fires and the worker
    looks alive while doing no work. Re-raising hands the decision back to the
    unit, whose ExecStartPre re-runs the Redis floor check.
    """
    monkeypatch.setattr("src.worker.loop.ERROR_BACKOFF_BASE_SECONDS", 0.001)
    monkeypatch.setattr("src.worker.loop.ERROR_BACKOFF_MAX_SECONDS", 0.001)
    monkeypatch.setattr("src.worker.loop.MAX_CONSECUTIVE_CYCLE_FAILURES", 3)
    _always_failing_read(monkeypatch, consumer)
    stop = asyncio.Event()

    with pytest.raises(RedisConnectionError):
        await drive_loop(fake_redis, consumer, settings, unreachable_handler, stop)


async def test_a_recovered_cycle_resets_the_failure_count(
    fake_redis, consumer, settings, monkeypatch
):
    """One blip every few polls must never accumulate toward the exit ceiling."""
    monkeypatch.setattr("src.worker.loop.ERROR_BACKOFF_BASE_SECONDS", 0.001)
    monkeypatch.setattr("src.worker.loop.MAX_CONSECUTIVE_CYCLE_FAILURES", 3)
    stop = asyncio.Event()
    polls = {"count": 0}
    real_read = consumer.read

    async def intermittent_read(**kwargs):
        polls["count"] += 1
        if polls["count"] % 2:  # fail, succeed, fail, succeed, ...
            raise RedisConnectionError("blip")
        if polls["count"] >= 8:
            stop.set()
        return await real_read(**kwargs)

    monkeypatch.setattr(consumer, "read", intermittent_read)

    await drive_loop(fake_redis, consumer, settings, unreachable_handler, stop)

    assert polls["count"] >= 8  # never hit the ceiling despite 4 failures
