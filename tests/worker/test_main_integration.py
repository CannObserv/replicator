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

import pytest
from co_core_aio.bus import AsyncBusConsumer
from redis.asyncio import Redis

from tests.worker.conftest import make_command

pytestmark = pytest.mark.integration

GROUP = "replicator.itest"
CONSUMER = "replicator@itest"


def _consumer(client: Redis, topic: str) -> AsyncBusConsumer:
    """An ``AsyncBusConsumer`` on a scratch topic, under the integration group."""
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


async def test_a_group_name_is_scoped_to_its_stream(real_redis, scratch_topic):
    """Two streams, one group *name*, two entirely separate PELs.

    Pinned because the tree asserted the opposite in four places (CR round 2):
    that one group spanning both command streams would let ``claim_stale`` on one
    "reach into the other's pending entries". It cannot. A consumer group is
    identified by **(stream key, group name)**, so a shared name creates two
    unrelated groups that merely spell the same — and the rule those comments were
    defending (one group per stream) is about legibility and about keeping the
    name-override key unambiguous, not about PEL safety.

    A false reason in a comment outlives the comment: it gets cited, and #77's
    round-1 validator hardened this one into a runtime guard before anyone checked
    it against a broker. Hence a live-broker assertion rather than a rewritten
    paragraph — fakeredis is not the authority on what Redis scopes.
    """
    other_topic = f"{scratch_topic}.second"
    try:
        await real_redis.xadd(scratch_topic, make_command())
        await real_redis.xadd(other_topic, make_command())
        first = _consumer(real_redis, scratch_topic)
        second = _consumer(real_redis, other_topic)
        await first.ensure_group(start_id="0")
        await second.ensure_group(start_id="0")

        # Deliver on the first stream only.
        assert await first.read(count=1, block_ms=100)

        assert (await real_redis.xpending(scratch_topic, GROUP))["pending"] == 1
        assert (await real_redis.xpending(other_topic, GROUP))["pending"] == 0

        # Reclaiming on the first stream cannot see the second's entries either.
        claimed = await real_redis.xautoclaim(scratch_topic, GROUP, "other-member", 0, "0-0")
        assert claimed[1], "the first stream's own entry is reclaimable"
        assert (await real_redis.xpending(other_topic, GROUP))["pending"] == 0
    finally:
        await real_redis.delete(other_topic)
