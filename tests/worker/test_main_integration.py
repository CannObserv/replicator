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

    **Both streams carry a pending entry on purpose.** The first version of this
    test delivered on one stream only, which left the other's PEL empty and made
    the reclaim half prove nothing: an ``XAUTOCLAIM`` that takes nothing from an
    empty list is not evidence it would leave a full one alone, so that half
    passed whether or not Redis crossed streams (CR round 3). In a test written to
    stop exactly this kind of unchecked claim, that was the failure it exists to
    prevent.
    """
    other_topic = f"{scratch_topic}.second"
    other_consumer = "replicator@itest-second"
    try:
        await real_redis.xadd(scratch_topic, make_command())
        await real_redis.xadd(other_topic, make_command())
        first = _consumer(real_redis, scratch_topic)
        second = AsyncBusConsumer(
            real_redis, topic=other_topic, group=GROUP, consumer=other_consumer
        )
        await first.ensure_group(start_id="0")
        await second.ensure_group(start_id="0")

        # **Both** streams must have a pending entry, or the reclaim assertion
        # below is vacuous: an XAUTOCLAIM cannot be shown to leave another
        # stream's entries alone while that stream has none (CR round 3).
        assert await first.read(count=1, block_ms=100)
        assert await second.read(count=1, block_ms=100)

        assert (await real_redis.xpending(scratch_topic, GROUP))["pending"] == 1
        before = await real_redis.xpending_range(other_topic, GROUP, "-", "+", 10)
        assert [entry["consumer"] for entry in before] == [other_consumer.encode()]

        # Reclaim everything claimable on the first stream, as a third member.
        # The second stream's entry is untouched — still pending, still owned by
        # the consumer that read it, despite the identically named group.
        _, claimed, _ = await real_redis.xautoclaim(
            scratch_topic, GROUP, "replicator@itest-third", 0, "0-0"
        )
        assert claimed, "the first stream's own entry is reclaimable"

        after = await real_redis.xpending_range(other_topic, GROUP, "-", "+", 10)
        assert [entry["consumer"] for entry in after] == [other_consumer.encode()]
        assert [entry["message_id"] for entry in after] == [entry["message_id"] for entry in before]
    finally:
        await real_redis.delete(other_topic)
