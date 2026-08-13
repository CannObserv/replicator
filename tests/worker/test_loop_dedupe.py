"""Command dedupe: a redelivered ``command_id`` must not re-run the handler.

The key is written after the handler succeeds, never before — these tests pin
that ordering, since reversing it would turn a crash into permanent loss.
"""

import json

from co_core.pure.models.changes import ContentFetchCommand

from src.worker.loop import FETCH_SPEC, Outcome, poll_once
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    make_command,
    noop_handler,
    process_one,
    unreachable_handler,
)


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
    await process_one(fake_redis, consumer, settings, message, noop_handler)

    ttl = await fake_redis.ttl(FETCH_SPEC.dedupe_key("cmd-ttl"))
    assert 0 < ttl <= settings.dedupe_ttl_seconds


async def test_a_failed_handler_leaves_no_dedupe_key(fake_redis, consumer, settings):
    """Set-after-success: a crash between handler and key must re-run, not skip."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-boom"))

    async def handler(command: ContentFetchCommand) -> None:
        raise RuntimeError("handler exploded")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    assert not await fake_redis.exists(FETCH_SPEC.dedupe_key("cmd-boom"))


async def test_a_dead_lettered_command_leaves_no_dedupe_key(fake_redis, consumer, settings):
    """A DLQ'd command was never handled — replay must not be short-circuited."""
    fields = make_command(command_id="cmd-future-2")
    payload = json.loads(fields["payload"])
    payload["schema_version"] = 99
    fields["payload"] = json.dumps(payload)
    await fake_redis.xadd(TOPIC, fields)

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_one(fake_redis, consumer, settings, message, unreachable_handler)

    assert not await fake_redis.exists(FETCH_SPEC.dedupe_key("cmd-future-2"))


async def test_request_options_are_not_part_of_the_command_identity(fake_redis, consumer, settings):
    """MUST-1 survives #11: ``command_id`` alone is the identity, options included.

    The redelivery here carries *different* headers, which is the direction that
    could actually break — a dedupe key that folded in the options would treat
    this as a new command and fetch twice for one occasion, while still passing
    every test that varies only the ``command_id``.

    The converse (two distinct ids, same URL, different options ⇒ two fetches)
    needs no test: distinct ids are distinct keys by construction, and MUST-1
    already requires the issuer to mint one per occasion.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-opt", headers={"accept": "*/*"}))
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-opt", timeout_seconds=5))
    calls: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        calls.append(command.command_id)

    first = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    assert await process_one(fake_redis, consumer, settings, first, handler) is Outcome.ACKED

    second = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    assert await process_one(fake_redis, consumer, settings, second, unreachable_handler) is (
        Outcome.DEDUPED
    )

    assert calls == ["cmd-opt"]
