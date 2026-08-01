"""Helpers for driving the consumer loop, against the fake broker and the real one.

Messages are built through co-core's own ``to_wire`` rather than a hand-written
field map, so a producer-side envelope change breaks these tests instead of
silently drifting from what the archiver actually publishes.

``scratch_topic`` is the live-broker half: every ``@pytest.mark.integration``
test in this package works on a stream key it alone owns.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import ContentFetchCommand

from src.core.config import get_settings
from src.worker.loop import process_message, run_loop
from src.worker.main import build_consumer

TOPIC = streams.CONTENT_FETCH
GROUP = "replicator.fetch"


def make_command(command_id: str = "cmd-1", url: str = "https://example.test/a") -> dict[str, str]:
    """A well-formed ``content.fetch`` wire frame."""
    return to_wire(
        ContentFetchCommand(
            occurred_at=datetime.now(UTC),
            command_id=command_id,
            url=url,
        )
    )


@pytest.fixture
async def scratch_topic(real_redis) -> AsyncGenerator[str]:
    """A per-test stream key on the scratch database, deleted afterwards.

    The uuid keeps concurrent runs (and a run that died before teardown) from
    colliding on a group whose PEL would otherwise leak into the next test.

    The DLQ goes with it. ``dead_letter`` XADDs to ``<topic>.dlq``, so a test
    that dead-letters anything creates a second key — one the session sweeper
    would eventually expire, but which has no reason to outlive the test that
    made it.
    """
    topic = f"replicator.itest.{uuid.uuid4().hex}"
    try:
        yield topic
    finally:
        await real_redis.delete(topic, dlq_name(topic))


@pytest.fixture
def worker_env(monkeypatch):
    """Pin the group and consumer name so assertions can name them literally."""
    monkeypatch.setenv("REPLICATOR_CONSUMER_GROUP", GROUP)
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator@test")
    get_settings.cache_clear()


@pytest.fixture
async def consumer(fake_redis, worker_env):
    """An ``AsyncBusConsumer`` on ``content.fetch`` with its group created."""
    consumer = build_consumer(fake_redis, get_settings())
    await consumer.ensure_group(start_id="0")
    return consumer


@pytest.fixture
def settings(worker_env):
    """Settings built from the same env the ``consumer`` fixture reads."""
    return get_settings()


async def unreachable_handler(command: ContentFetchCommand) -> None:
    """A handler the test asserts is never called."""
    raise AssertionError(f"handler must not run (got {command.command_id})")


async def process_one(fake_redis, consumer, settings, message, handler):
    """``process_message`` with the fixture wiring filled in."""
    return await process_message(
        message,
        client=fake_redis,
        consumer=consumer,
        group=GROUP,
        handler=handler,
        settings=settings,
    )


async def drive_loop(fake_redis, consumer, settings, handler, stop, deadline: float = 5):
    """Run the loop to completion under a deadline, with the fixture wiring.

    The deadline is a test guard, not a feature of the loop: fakeredis does not
    honour ``block``, so a loop that failed to notice ``stop`` would spin rather
    than hang, and the suite would sit at pytest's 300s timeout instead.
    """
    async with asyncio.timeout(deadline):
        await run_loop(
            client=fake_redis,
            consumer=consumer,
            group=GROUP,
            settings=settings,
            handler=handler,
            stop=stop,
        )


async def noop_handler(command: ContentFetchCommand) -> None:
    """A handler that succeeds without doing anything.

    Loop tests are about what the loop does with a handler's outcome, not about
    the byte path — that has its own tests in ``test_handler.py``.
    """
