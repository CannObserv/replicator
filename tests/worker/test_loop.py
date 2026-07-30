"""The consume path: read -> dispatch -> ack, and shutdown behaviour.

Assertions read the broker's own view (``xpending``, ``xlen``) rather than
co-core internals, which are private and not a stable contract.
"""

import asyncio
import json
from datetime import UTC, datetime

import pytest
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.adapters.bus.exceptions import BusMessageAnomaly
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import BlobAvailableEvent, ContentFetchCommand
from redis.exceptions import ConnectionError as RedisConnectionError

from src.core.errors import PermanentFetchError, TransientFetchError
from src.worker.loop import (
    DEDUPE_KEY_PREFIX,
    Outcome,
    dead_letter_anomaly,
    log_only_handler,
    poll_once,
    process_message,
    run_loop,
)
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


async def test_a_malformed_frame_is_dead_lettered_and_acked(fake_redis, consumer, settings):
    """AC: poison goes to content.fetch.dlq, the original is acked, the loop lives."""
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})

    messages = await poll_once(fake_redis, consumer, settings)

    assert messages == []
    dlq = await fake_redis.xrange(dlq_name(TOPIC))
    assert len(dlq) == 1
    assert dlq[0][1][b"payload"] == b"not json"
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_an_unknown_event_type_is_dead_lettered(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, {"event_type": "who_knows", "payload": "{}"})

    assert await poll_once(fake_redis, consumer, settings) == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_a_deleted_poison_entry_still_dead_letters(fake_redis, consumer, settings):
    """XADD rejects an empty field map, so a trimmed entry needs synthesized fields."""
    message_id = await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "{"})
    await fake_redis.xreadgroup(GROUP, settings.consumer_name, {TOPIC: ">"}, count=1)
    await fake_redis.xdel(TOPIC, message_id)  # entry gone; still pending

    await dead_letter_anomaly(
        fake_redis,
        consumer,
        BusMessageAnomaly("boom", topic=TOPIC, message_id=message_id.decode()),
    )

    dlq = await fake_redis.xrange(dlq_name(TOPIC))
    assert len(dlq) == 1
    assert dlq[0][1][b"original_message_id"] == message_id
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_a_foreign_payload_type_is_dead_lettered(fake_redis, consumer, settings):
    """from_wire's dispatch table is global — a fact on the command stream decodes."""
    await fake_redis.xadd(
        TOPIC,
        to_wire(
            BlobAvailableEvent(
                occurred_at=datetime.now(UTC),
                content_fingerprint="f" * 64,
                blob_uri="file:///blobs/f",
                size_bytes=1,
                media_type="text/html",
                url="https://example.test/a",
            )
        ),
    )

    messages = await poll_once(fake_redis, consumer, settings)
    outcome = await process_message(
        messages[0],
        client=fake_redis,
        consumer=consumer,
        handler=_unreachable_handler,
        settings=settings,
    )

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_an_unknown_schema_version_is_dead_lettered(fake_redis, consumer, settings):
    """Branch on schema_version before destructuring — an unknown one is not ours."""
    fields = make_command(command_id="cmd-future")
    payload = json.loads(fields["payload"])
    payload["schema_version"] = 2
    fields["payload"] = json.dumps(payload)
    fields["schema_version"] = "2"
    await fake_redis.xadd(TOPIC, fields)

    messages = await poll_once(fake_redis, consumer, settings)
    outcome = await process_message(
        messages[0],
        client=fake_redis,
        consumer=consumer,
        handler=_unreachable_handler,
        settings=settings,
    )

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_the_loop_keeps_running_past_a_poison_frame(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-after-poison"))
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

    assert seen == ["cmd-after-poison"]
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def _process(fake_redis, consumer, settings, message, handler):
    return await process_message(
        message,
        client=fake_redis,
        consumer=consumer,
        handler=handler,
        settings=settings,
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

    first = (await poll_once(fake_redis, consumer, settings))[0]
    assert await _process(fake_redis, consumer, settings, first, handler) is Outcome.ACKED

    second = (await poll_once(fake_redis, consumer, settings))[0]
    assert await _process(fake_redis, consumer, settings, second, _unreachable_handler) is (
        Outcome.DEDUPED
    )

    assert calls == ["cmd-dup"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_the_dedupe_key_carries_the_configured_ttl(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-ttl"))

    message = (await poll_once(fake_redis, consumer, settings))[0]
    await _process(fake_redis, consumer, settings, message, log_only_handler)

    ttl = await fake_redis.ttl(f"{DEDUPE_KEY_PREFIX}cmd-ttl")
    assert 0 < ttl <= settings.dedupe_ttl_seconds


async def test_a_failed_handler_leaves_no_dedupe_key(fake_redis, consumer, settings):
    """Set-after-success: a crash between handler and key must re-run, not skip."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-boom"))

    async def handler(command: ContentFetchCommand) -> None:
        raise RuntimeError("handler exploded")

    message = (await poll_once(fake_redis, consumer, settings))[0]
    outcome = await _process(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    assert not await fake_redis.exists(f"{DEDUPE_KEY_PREFIX}cmd-boom")


async def test_a_dead_lettered_command_leaves_no_dedupe_key(fake_redis, consumer, settings):
    """A DLQ'd command was never handled — replay must not be short-circuited."""
    fields = make_command(command_id="cmd-future-2")
    payload = json.loads(fields["payload"])
    payload["schema_version"] = 99
    fields["payload"] = json.dumps(payload)
    await fake_redis.xadd(TOPIC, fields)

    message = (await poll_once(fake_redis, consumer, settings))[0]
    await _process(fake_redis, consumer, settings, message, _unreachable_handler)

    assert not await fake_redis.exists(f"{DEDUPE_KEY_PREFIX}cmd-future-2")


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

    messages = await poll_once(fake_redis, consumer, eager)
    assert await _process(fake_redis, consumer, eager, messages[0], handler) is Outcome.ACKED

    assert seen == ["cmd-orphan"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_a_poison_pel_entry_does_not_jam_recovery(fake_redis, consumer, settings):
    """claim_stale restarts at 0-0 every call, so a poison entry would block it."""
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-behind-poison"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=2)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})

    messages = await poll_once(fake_redis, consumer, eager)

    assert len(messages) == 1
    command = messages[0].payload
    assert isinstance(command, ContentFetchCommand)
    assert command.command_id == "cmd-behind-poison"
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_a_transient_failure_leaves_the_message_pending(fake_redis, consumer, settings):
    """AC: transient => retry. Redelivery is claim_stale's job, so do not ack."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-transient"))

    async def handler(command: ContentFetchCommand) -> None:
        raise TransientFetchError("broker or origin is having a moment")

    message = (await poll_once(fake_redis, consumer, settings))[0]
    outcome = await _process(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    assert not await fake_redis.exists(f"{DEDUPE_KEY_PREFIX}cmd-transient")


async def test_a_redis_connection_error_counts_as_transient(fake_redis, consumer, settings):
    """redis-py's error types are disjoint from the builtins — both are listed."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-redis-down"))

    async def handler(command: ContentFetchCommand) -> None:
        raise RedisConnectionError("connection refused")

    message = (await poll_once(fake_redis, consumer, settings))[0]

    assert await _process(fake_redis, consumer, settings, message, handler) is Outcome.RETRY


async def test_a_permanent_failure_is_dead_lettered(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-permanent"))

    async def handler(command: ContentFetchCommand) -> None:
        raise PermanentFetchError("this url will never be fetchable")

    message = (await poll_once(fake_redis, consumer, settings))[0]
    outcome = await _process(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_an_unclassified_failure_retries_below_the_ceiling(fake_redis, consumer, settings):
    """A handler bug must not discard a valid command on its first failure."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-bug"))

    async def handler(command: ContentFetchCommand) -> None:
        raise AttributeError("NoneType has no attribute 'content'")

    message = (await poll_once(fake_redis, consumer, settings))[0]
    outcome = await _process(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0


async def test_an_unclassified_failure_dead_letters_at_the_ceiling(fake_redis, consumer, settings):
    """The ceiling is read from XPENDING's delivery counter, not a side counter."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-persistent-bug"))
    strict = settings.model_copy(update={"max_delivery_attempts": 1})

    async def handler(command: ContentFetchCommand) -> None:
        raise AttributeError("still broken")

    message = (await poll_once(fake_redis, consumer, strict))[0]
    outcome = await _process(fake_redis, consumer, strict, message, handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_transient_failures_are_exempt_from_the_ceiling(fake_redis, consumer, settings):
    """A long outage must never drop a valid command (archiver#107, CR #2)."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-outage"))
    strict = settings.model_copy(update={"max_delivery_attempts": 1})

    async def handler(command: ContentFetchCommand) -> None:
        raise TransientFetchError("still down")

    message = (await poll_once(fake_redis, consumer, strict))[0]

    assert await _process(fake_redis, consumer, strict, message, handler) is Outcome.RETRY
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0


async def test_cancellation_is_never_classified(fake_redis, consumer, settings):
    """Shutdown is not a message failure — CancelledError must propagate."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-cancelled"))

    async def handler(command: ContentFetchCommand) -> None:
        raise asyncio.CancelledError

    message = (await poll_once(fake_redis, consumer, settings))[0]
    with pytest.raises(asyncio.CancelledError):
        await _process(fake_redis, consumer, settings, message, handler)

    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1
