"""Consumer-lifecycle facts the fake broker cannot prove (GH #3).

fakeredis is sound for consumer-group *mechanics* but diverges on *lifecycle*:
it registers a consumer on an empty ``XREADGROUP``; real Redis registers one only
on the first delivered message. The default suite therefore asserts the positive
half (``tests/worker/test_main.py``) and the negative half lives here, where a
live broker can contradict it.

These tests drive ``AsyncBusConsumer`` directly against a scratch stream rather
than through ``build_consumer``: the property under test belongs to the broker,
and ``build_consumer`` is hard-wired to ``content.fetch`` — the live command
stream, which an integration test must never write to.
"""

import uuid
from collections.abc import AsyncGenerator

import pytest
from co_core_aio.bus import AsyncBusConsumer

from tests.worker.conftest import make_command

pytestmark = pytest.mark.integration

GROUP = "replicator.itest"
CONSUMER = "replicator@itest"


@pytest.fixture
async def scratch_topic(real_redis) -> AsyncGenerator[str]:
    """A per-test stream key on the scratch database, deleted afterwards.

    The uuid keeps concurrent runs (and a run that died before teardown) from
    colliding on a group whose PEL would otherwise leak into the next test.
    """
    topic = f"replicator.itest.{uuid.uuid4().hex}"
    try:
        yield topic
    finally:
        await real_redis.delete(topic)


def _consumer(client, topic: str) -> AsyncBusConsumer:
    return AsyncBusConsumer(client, topic=topic, group=GROUP, consumer=CONSUMER)


async def test_empty_poll_does_not_register_the_consumer(real_redis, scratch_topic):
    """A polling worker is invisible in XINFO CONSUMERS until work arrives.

    This is what fakeredis gets wrong, and what a naive "is my consumer alive?"
    health check would read as a dead worker.
    """
    consumer = _consumer(real_redis, scratch_topic)
    await consumer.ensure_group(start_id="$")

    assert await consumer.read(count=1, block_ms=1) == []

    assert await real_redis.xinfo_consumers(scratch_topic, GROUP) == []


async def test_first_delivery_registers_the_consumer(real_redis, scratch_topic):
    """Delivery — not polling — is what creates the consumer entry."""
    consumer = _consumer(real_redis, scratch_topic)
    await consumer.ensure_group(start_id="$")
    await real_redis.xadd(scratch_topic, make_command())

    assert await consumer.read(count=1, block_ms=100)

    consumers = await real_redis.xinfo_consumers(scratch_topic, GROUP)
    assert [c["name"] for c in consumers] == [CONSUMER.encode()]
