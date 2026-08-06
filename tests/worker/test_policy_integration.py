"""The policy reader against the live Archiver-operated broker (#19).

Split off by *environment* rather than concern, which is the one axis that earns
its own file: what these assert is what fakeredis cannot attest to — that the
groupless read really blocks, and that tailing this stream leaves no consumer
group behind on it.

Everything runs on ``replicator.itest.*`` scratch keys. The real
``content.fetch-policy`` is a live stream whose entries the running service
applies to its own pacing.
"""

import asyncio
import time
import uuid
from collections.abc import AsyncGenerator

import pytest
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import FetchPolicyState

from src.core.config import get_settings
from src.worker.policy import (
    FetchPolicyMap,
    build_policy_reader,
    replay_policies,
    run_policy_reader,
)
from tests.worker.conftest import now

pytestmark = pytest.mark.integration

DEFAULT = 1.0


@pytest.fixture
async def policy_topic(real_redis) -> AsyncGenerator[str]:
    """A per-test policy stream key, deleted afterwards.

    No ``.dlq`` sibling to clean up, unlike ``scratch_topic``: this stream has no
    consumer group, so nothing here can dead-letter.
    """
    topic = f"replicator.itest.{uuid.uuid4().hex}"
    try:
        yield topic
    finally:
        await real_redis.delete(topic)


@pytest.fixture
def settings(monkeypatch):
    monkeypatch.setenv("REPLICATOR_READ_BLOCK_MS", "300")
    get_settings.cache_clear()
    return get_settings()


async def publish(client, topic: str, host: str, min_interval_seconds: float) -> None:
    await client.xadd(
        topic,
        to_wire(
            FetchPolicyState(
                occurred_at=now(), host=host, min_interval_seconds=min_interval_seconds
            )
        ),
    )


async def test_replay_then_tail_against_a_real_stream(real_redis, policy_topic, settings):
    """The boot sequence end to end: everything already published, then everything after."""
    await publish(real_redis, policy_topic, "before.test", 30.0)
    reader = build_policy_reader(real_redis, topic=policy_topic)
    policies = FetchPolicyMap(DEFAULT)

    await replay_policies(reader, policies)
    assert policies.interval_for("before.test") == 30.0

    stop = asyncio.Event()
    task = asyncio.create_task(
        run_policy_reader(reader, policies=policies, settings=settings, stop=stop)
    )
    try:
        await publish(real_redis, policy_topic, "after.test", 5.0)
        async with asyncio.timeout(5):
            # Polled: the map is plain state with nothing to signal on.
            while policies.interval_for("after.test") is None:  # noqa: ASYNC110
                await asyncio.sleep(0.01)
    finally:
        stop.set()
        await task

    assert policies.interval_for("after.test") == 5.0


async def test_tailing_the_stream_creates_no_consumer_group(real_redis, policy_topic, settings):
    """The invariant the driver exists to protect.

    Every worker needs every message, so there is no group to compete over — and
    a group here would grow a PEL that nothing acks and nothing drains, on a
    stream the producer republishes onto periodically.
    """
    await publish(real_redis, policy_topic, "slow.test", 30.0)
    reader = build_policy_reader(real_redis, topic=policy_topic)
    policies = FetchPolicyMap(DEFAULT)
    await replay_policies(reader, policies)

    stop = asyncio.Event()
    task = asyncio.create_task(
        run_policy_reader(reader, policies=policies, settings=settings, stop=stop)
    )
    await asyncio.sleep(0.1)
    stop.set()
    await task

    assert await real_redis.xinfo_groups(policy_topic) == []


async def test_the_read_really_blocks(real_redis, policy_topic, settings):
    """fakeredis cannot attest to this, and a read that returned immediately
    would spin the tail against the broker for as long as the worker is up."""
    await real_redis.xadd(
        policy_topic,
        to_wire(FetchPolicyState(occurred_at=now(), host="seed.test", min_interval_seconds=1.0)),
    )
    reader = build_policy_reader(real_redis, topic=policy_topic)
    await reader.replay(count=1)

    started = time.monotonic()
    assert await reader.read(count=1, block_ms=500) == []

    assert time.monotonic() - started >= 0.4


async def test_shutdown_is_bounded_by_the_poll_window(real_redis, policy_topic, settings):
    """The tail rides the same stop event as the consume loop and blocks for the
    same configured window, so it adds no term to ``TimeoutStopSec`` — the two
    block concurrently and the worst case is the larger of them, not the sum."""
    reader = build_policy_reader(real_redis, topic=policy_topic)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_policy_reader(reader, policies=FetchPolicyMap(DEFAULT), settings=settings, stop=stop)
    )
    await asyncio.sleep(0.05)

    started = time.monotonic()
    stop.set()
    await task

    # settings pins REPLICATOR_READ_BLOCK_MS to 300ms; the margin is scheduling.
    assert time.monotonic() - started < 2.0


async def test_a_malformed_frame_on_a_real_stream_is_skipped(real_redis, policy_topic):
    """With no group there is no ack to move past one — the cursor has to be
    forced, or every policy published after it is permanently unreachable.

    The frame ahead of the poison is what makes this more than a smoke test
    (CR #1): a replay that surfaces messages only on a clean finish loses it,
    silently, with the cursor already past it.
    """
    await publish(real_redis, policy_topic, "ahead.test", 5.0)
    await real_redis.xadd(policy_topic, {"not": "a frame"})
    await publish(real_redis, policy_topic, "behind.test", 30.0)
    policies = FetchPolicyMap(DEFAULT)

    await replay_policies(build_policy_reader(real_redis, topic=policy_topic), policies)

    assert policies.interval_for("ahead.test") == 5.0
    assert policies.interval_for("behind.test") == 30.0
