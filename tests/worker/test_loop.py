"""The consume path: read -> dispatch -> ack, and shutdown behaviour.

Assertions read the broker's own view (``xpending``, ``xlen``) rather than
co-core internals, which are private and not a stable contract.
"""

import asyncio

from co_core.pure.models.changes import ContentFetchCommand

from src.worker.loop import Outcome, poll_once, process_message, run_loop
from tests.worker.conftest import GROUP, TOPIC, make_command


async def test_a_well_formed_command_is_dispatched_and_acked(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-a", url="https://example.test/a"))
    seen: list[ContentFetchCommand] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command)

    messages = await poll_once(fake_redis, consumer, settings)
    outcome = await process_message(
        messages[0],
        client=fake_redis,
        consumer=consumer,
        handler=handler,
        settings=settings,
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

    await asyncio.wait_for(
        run_loop(
            client=fake_redis, consumer=consumer, settings=settings, handler=handler, stop=stop
        ),
        timeout=5,
    )

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

    await asyncio.wait_for(
        run_loop(
            client=fake_redis, consumer=consumer, settings=settings, handler=handler, stop=stop
        ),
        timeout=5,
    )

    assert finished == ["cmd-c"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_run_loop_returns_promptly_when_idle_and_stopped(fake_redis, consumer, settings):
    """An empty stream must not pin the loop past the stop signal."""
    stop = asyncio.Event()
    stop.set()

    await asyncio.wait_for(
        run_loop(
            client=fake_redis,
            consumer=consumer,
            settings=settings,
            handler=_unreachable_handler,
            stop=stop,
        ),
        timeout=5,
    )


async def _unreachable_handler(command: ContentFetchCommand) -> None:
    raise AssertionError("handler must not run when the loop is already stopped")
