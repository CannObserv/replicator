"""Command dedupe: a redelivered ``command_id`` must not re-run the handler.

The key is written after the handler succeeds, never before — these tests pin
that ordering, since reversing it would turn a crash into permanent loss.
"""

import json

from co_core.pure.models.changes import ContentFetchCommand

from src.worker.loop import DEDUPE_KEY_PREFIX, Outcome, log_only_handler, poll_once
from tests.worker.conftest import GROUP, TOPIC, make_command, process_one, unreachable_handler


async def test_a_redelivered_command_is_acked_without_rerunning_the_handler(
    fake_redis, consumer, settings
):
    """AC: dedupe on command_id, durable in Redis so it survives a restart."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-dup"))
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-dup"))
    calls: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        calls.append(command.command_id)

    first = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    assert await process_one(fake_redis, consumer, settings, first, handler) is Outcome.ACKED

    second = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    assert await process_one(fake_redis, consumer, settings, second, unreachable_handler) is (
        Outcome.DEDUPED
    )

    assert calls == ["cmd-dup"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_the_dedupe_key_carries_the_configured_ttl(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-ttl"))

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_one(fake_redis, consumer, settings, message, log_only_handler)

    ttl = await fake_redis.ttl(f"{DEDUPE_KEY_PREFIX}cmd-ttl")
    assert 0 < ttl <= settings.dedupe_ttl_seconds


async def test_a_failed_handler_leaves_no_dedupe_key(fake_redis, consumer, settings):
    """Set-after-success: a crash between handler and key must re-run, not skip."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-boom"))

    async def handler(command: ContentFetchCommand) -> None:
        raise RuntimeError("handler exploded")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    assert not await fake_redis.exists(f"{DEDUPE_KEY_PREFIX}cmd-boom")


async def test_a_dead_lettered_command_leaves_no_dedupe_key(fake_redis, consumer, settings):
    """A DLQ'd command was never handled — replay must not be short-circuited."""
    fields = make_command(command_id="cmd-future-2")
    payload = json.loads(fields["payload"])
    payload["schema_version"] = 99
    fields["payload"] = json.dumps(payload)
    await fake_redis.xadd(TOPIC, fields)

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_one(fake_redis, consumer, settings, message, unreachable_handler)

    assert not await fake_redis.exists(f"{DEDUPE_KEY_PREFIX}cmd-future-2")
