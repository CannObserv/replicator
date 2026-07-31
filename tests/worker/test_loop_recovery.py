"""Crash recovery: reclaiming what a dead worker left in the pending list.

``claim_stale`` restarts at ``0-0`` on every call, so these tests pin the two
properties that follow from it — a poison entry must not jam the pass, and the
pass must give up rather than spin when the PEL is pathological.
"""

from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import ContentFetchCommand

from src.worker.loop import MAX_POISON_SKIPS, Outcome, claim_once, poll_once
from tests.worker.conftest import GROUP, TOPIC, make_command, process_one


async def test_a_message_from_a_dead_consumer_is_reclaimed_and_processed(
    fake_redis, consumer, settings
):
    """AC: crash recovery — an abandoned PEL entry comes back via claim_stale."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-orphan"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=1)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    seen: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)

    messages = await poll_once(fake_redis, consumer, eager, group=GROUP)
    assert await process_one(fake_redis, consumer, eager, messages[0], handler) is Outcome.ACKED

    assert seen == ["cmd-orphan"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_a_poison_pel_entry_does_not_jam_recovery(fake_redis, consumer, settings):
    """claim_stale restarts at 0-0 every call, so a poison entry would block it."""
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-behind-poison"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=2)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})

    messages = await poll_once(fake_redis, consumer, eager, group=GROUP)

    assert len(messages) == 1
    command = messages[0].payload
    assert isinstance(command, ContentFetchCommand)
    assert command.command_id == "cmd-behind-poison"
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_recovery_gives_up_after_the_poison_skip_bound(fake_redis, consumer, settings):
    """CR #3: a pathological PEL must not starve the read path within one tick."""
    for _ in range(MAX_POISON_SKIPS + 2):
        await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=100)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})

    reclaimed = await claim_once(fake_redis, consumer, eager, group=GROUP)

    assert reclaimed == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == MAX_POISON_SKIPS
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 2  # the bound stopped the pass; the rest wait for the next
