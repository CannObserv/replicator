"""Outage resilience: a failing *cycle* (not a failing message) is the loop's own.

A broker that refuses reads, acks, or DLQ writes must be ridden out with
backoff — and, if it never comes back, must eventually surface as an exit rather
than a worker that looks alive to systemd while doing nothing.

The backoff and the give-up ceiling are Settings, so these tests configure them
rather than patching module globals. The one exception is ``_park``, patched
where a test needs to observe the wait it would otherwise sleep through.
"""

import asyncio

import pytest
from co_core.pure.models.changes import ContentFetchCommand
from redis.exceptions import ConnectionError as RedisConnectionError

from src.worker.loop import _error_backoff_seconds
from tests.worker.conftest import TOPIC, drive_loop, make_command, unreachable_handler

# Resilience tests either patch _park or drive sub-millisecond backoffs, so a
# hang here is a hang in this file — fail fast rather than inherit the 5s
# default and point one frame away from the test that caused it.
DEADLINE = 1.0


def _always_failing_read(monkeypatch, consumer):
    """Make every read raise as a down broker would."""

    async def dead_read(**kwargs):
        raise RedisConnectionError("broker went away")

    monkeypatch.setattr(consumer, "read", dead_read)


async def test_a_broker_failure_does_not_kill_the_loop(fake_redis, consumer, settings, monkeypatch):
    """CR #1: a Redis blip must back off and retry, not terminate the worker."""
    brisk = settings.model_copy(update={"error_backoff_base_seconds": 0.01})
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

    await drive_loop(fake_redis, consumer, brisk, handler, stop, deadline=DEADLINE)

    assert failures["left"] == 0
    assert seen == ["cmd-after-outage"]


async def test_the_backoff_escalates_and_caps():
    """Consecutive failures must escalate, capped — not hammer a down broker."""
    first = _error_backoff_seconds(1, base=1.0, maximum=30.0)
    second = _error_backoff_seconds(2, base=1.0, maximum=30.0)
    huge = _error_backoff_seconds(10_000, base=1.0, maximum=30.0)

    assert first == 1.0
    assert second > first
    assert huge == 30.0


async def test_the_loop_actually_waits_longer_between_repeated_failures(
    fake_redis, consumer, settings, monkeypatch
):
    """CR #13: assert the wiring, not just the arithmetic.

    Configuring a tiny base (as the other resilience tests must, to stay fast)
    hides whether the loop passes the failure count to the backoff at all —
    parking on the idle interval instead would leave every other test green
    while the worker hammered a down broker. Patching ``_park`` observes the
    requested wait without spending it, so this one runs at the real defaults.
    """
    waits: list[float] = []
    stop = asyncio.Event()

    async def recording_park(event: asyncio.Event, seconds: float) -> None:
        waits.append(seconds)
        if len(waits) == 3:
            event.set()

    monkeypatch.setattr("src.worker.loop._park", recording_park)
    _always_failing_read(monkeypatch, consumer)

    await drive_loop(fake_redis, consumer, settings, unreachable_handler, stop, deadline=DEADLINE)

    base = settings.error_backoff_base_seconds
    assert waits == [base, base * 2, base * 4]


async def test_a_sustained_outage_eventually_exits(fake_redis, consumer, settings, monkeypatch):
    """CR #12: absorbing forever would hide a permanently wrong REDIS_URL.

    Nothing exits, so systemd's Restart=on-failure never fires and the worker
    looks alive while doing no work. Re-raising hands the decision back to the
    unit, whose ExecStartPre re-runs the Redis floor check.
    """
    doomed = settings.model_copy(
        update={
            "error_backoff_base_seconds": 0.001,
            "error_backoff_max_seconds": 0.001,
            "max_consecutive_cycle_failures": 3,
        }
    )
    _always_failing_read(monkeypatch, consumer)
    stop = asyncio.Event()

    with pytest.raises(RedisConnectionError):
        await drive_loop(fake_redis, consumer, doomed, unreachable_handler, stop, deadline=DEADLINE)


async def test_a_recovered_cycle_resets_the_failure_count(
    fake_redis, consumer, settings, monkeypatch
):
    """One blip every few polls must never accumulate toward the exit ceiling.

    The ceiling is 3 and the read fails every other poll, so a loop that failed
    to reset would re-raise on the fifth poll. Surviving strictly more failures
    than the ceiling is the property — asserting on the poll count alone would
    only restate this test's own stop condition (CR #18).
    """
    forgiving = settings.model_copy(
        update={"error_backoff_base_seconds": 0.001, "max_consecutive_cycle_failures": 3}
    )
    stop = asyncio.Event()
    polls = {"count": 0}
    failures = {"count": 0}
    real_read = consumer.read

    async def intermittent_read(**kwargs):
        polls["count"] += 1
        if polls["count"] % 2:  # fail, succeed, fail, succeed, ...
            failures["count"] += 1
            raise RedisConnectionError("blip")
        if failures["count"] >= 4:
            stop.set()
        return await real_read(**kwargs)

    monkeypatch.setattr(consumer, "read", intermittent_read)

    await drive_loop(fake_redis, consumer, forgiving, unreachable_handler, stop, deadline=DEADLINE)

    assert failures["count"] > forgiving.max_consecutive_cycle_failures
