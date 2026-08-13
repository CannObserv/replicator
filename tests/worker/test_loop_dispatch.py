"""Dispatch and cadence: a good command is handled, acked, and stops on request.

``src/worker/loop.py``'s tests are split by concern rather than kept in one
``test_loop.py`` — dispatch, DLQ routing, dedupe, failure classification, crash
recovery, and outage resilience each have their own file.

Assertions read the broker's own view (``xpending``, ``xlen``) rather than
co-core internals, which are private and not a stable contract.
"""

import asyncio

from co_core.pure.models.changes import ContentFetchCommand

from src.worker.loop import FETCH_SPEC, Outcome, poll_once, process_batch, process_message
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    collected_reports,
    drive_loop,
    make_command,
    unreachable_handler,
)


async def test_a_well_formed_command_is_dispatched_and_acked(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-a", url="https://example.test/a"))
    seen: list[ContentFetchCommand] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command)

    messages = await poll_once(fake_redis, consumer, settings, group=GROUP)
    outcome = await process_message(
        messages[0],
        client=fake_redis,
        consumer=consumer,
        group=GROUP,
        handler=handler,
        settings=settings,
        reporter=collected_reports(),
        spec=FETCH_SPEC,
    )

    assert outcome is Outcome.ACKED
    assert [c.command_id for c in seen] == ["cmd-a"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_run_loop_drains_then_exits_when_stopped(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-b"))
    stop = asyncio.Event()
    seen: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)
        stop.set()

    await drive_loop(fake_redis, consumer, settings, handler, stop)

    assert seen == ["cmd-b"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_run_loop_finishes_the_in_flight_message_after_stop(fake_redis, consumer, settings):
    """SIGTERM mid-handler must still ack — restarts should not strand the PEL."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-c"))
    stop = asyncio.Event()
    finished: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        stop.set()  # the signal lands while this message is in flight
        await asyncio.sleep(0)
        finished.append(command.command_id)

    await drive_loop(fake_redis, consumer, settings, handler, stop)

    assert finished == ["cmd-c"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_run_loop_returns_promptly_when_idle_and_stopped(fake_redis, consumer, settings):
    """An empty stream must not pin the loop past the stop signal."""
    stop = asyncio.Event()
    stop.set()

    await drive_loop(fake_redis, consumer, settings, unreachable_handler, stop)


async def test_the_loop_stops_between_messages_in_one_batch(fake_redis, consumer, settings):
    """CR #7: the stop check must not depend on count=1 staying true."""
    stop = asyncio.Event()
    stop.set()
    handled: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        handled.append(command.command_id)

    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-batch-1"))
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-batch-2"))
    messages = await consumer.read(count=2, block_ms=1)
    assert len(messages) == 2

    await process_batch(
        messages,
        client=fake_redis,
        consumer=consumer,
        group=GROUP,
        settings=settings,
        handler=handler,
        reporter=collected_reports(),
        spec=FETCH_SPEC,
        stop=stop,
    )

    assert handled == ["cmd-batch-1"]
