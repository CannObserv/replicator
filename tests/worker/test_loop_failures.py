"""Failure classification: which handler failures retry, and which dead-letter.

Three tiers — permanent (DLQ now), transient (retry forever, exempt from the
ceiling), and unclassified (retry against the ceiling). The ceiling itself is
read from XPENDING rather than a side counter, so the tests that cover it also
cover what happens when that row is gone.
"""

import asyncio

import pytest
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import ContentFetchCommand
from redis.exceptions import ConnectionError as RedisConnectionError

from src.core.errors import PermanentFetchError, TransientFetchError
from src.storage.local import LocalBlobStore
from src.storage.sweeper import BlobUsage
from src.worker.handler import build_handler
from src.worker.loop import DEDUPE_KEY_PREFIX, Outcome, _delivery_count, poll_once
from tests.worker.conftest import GROUP, TOPIC, FakeFetcher, make_command, process_one


async def test_a_transient_failure_leaves_the_message_pending(fake_redis, consumer, settings):
    """AC: transient => retry. Redelivery is claim_stale's job, so do not ack."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-transient"))

    async def handler(command: ContentFetchCommand) -> None:
        raise TransientFetchError("broker or origin is having a moment")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

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

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]

    assert await process_one(fake_redis, consumer, settings, message, handler) is Outcome.RETRY


async def test_a_permanent_failure_is_dead_lettered(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-permanent"))

    async def handler(command: ContentFetchCommand) -> None:
        raise PermanentFetchError("this url will never be fetchable")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_an_unclassified_failure_retries_below_the_ceiling(fake_redis, consumer, settings):
    """A handler bug must not discard a valid command on its first failure."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-bug"))

    async def handler(command: ContentFetchCommand) -> None:
        raise AttributeError("NoneType has no attribute 'content'")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

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

    message = (await poll_once(fake_redis, consumer, strict, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, strict, message, handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_transient_failures_are_exempt_from_the_ceiling(fake_redis, consumer, settings):
    """A long outage must never drop a valid command (archiver#107, CR #2)."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-outage"))
    strict = settings.model_copy(update={"max_delivery_attempts": 1})

    async def handler(command: ContentFetchCommand) -> None:
        raise TransientFetchError("still down")

    message = (await poll_once(fake_redis, consumer, strict, group=GROUP))[0]

    assert await process_one(fake_redis, consumer, strict, message, handler) is Outcome.RETRY
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0


async def test_cancellation_is_never_classified(fake_redis, consumer, settings):
    """Shutdown is not a message failure — CancelledError must propagate."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-cancelled"))

    async def handler(command: ContentFetchCommand) -> None:
        raise asyncio.CancelledError

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    with pytest.raises(asyncio.CancelledError):
        await process_one(fake_redis, consumer, settings, message, handler)

    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1


async def test_a_missing_pending_row_is_logged_not_silently_retried(
    fake_redis, consumer, settings, caplog
):
    """CR #5: no PEL row means the ceiling cannot be read — say so.

    The entry leaving the PEL underneath us is the reachable case; a *wrong*
    group is not, since Redis answers XPENDING for an unknown group with NOGROUP
    rather than an empty result.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-no-row"))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await consumer.ack(message.message_id)  # gone from the PEL

    with caplog.at_level("WARNING"):
        attempts = await _delivery_count(fake_redis, message, group=GROUP)

    assert attempts == 1
    assert "no pending entry" in caplog.text


async def test_the_missing_pending_row_warning_is_undamped(fake_redis, consumer, settings, caplog):
    """CR #16: one warning per occurrence, unlike run_loop's every-Nth logging.

    Safe today because retries are gated by claim_min_idle_ms, so occurrences
    are minutes apart. Pinned so that dropping that gate — a much faster reclaim
    cadence — shows up here as a failing test rather than as journal flood.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-no-row-twice"))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await consumer.ack(message.message_id)

    with caplog.at_level("WARNING"):
        await _delivery_count(fake_redis, message, group=GROUP)
        await _delivery_count(fake_redis, message, group=GROUP)
        await _delivery_count(fake_redis, message, group=GROUP)

    warnings = [r for r in caplog.records if "no pending entry" in r.getMessage()]
    assert len(warnings) == 3


async def test_a_command_blocked_by_the_blob_ceiling_stays_pending(fake_redis, consumer, settings):
    """Backpressure only works if the command survives to be retried.

    The byte path refuses to fetch once the blob tree is over its ceiling. That
    refusal is transient by design — dead-lettering instead would discard work
    over a condition a later sweep clears, and the DLQ would fill with perfectly
    good commands during any period of disk pressure.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-over-ceiling"))
    store = LocalBlobStore(settings.blob_dir)
    usage = BlobUsage()
    usage.observe(settings.blob_max_total_bytes)
    fetcher = FakeFetcher()
    handler = build_handler(
        fetcher=fetcher, store=store, client=fake_redis, settings=settings, usage=usage
    )

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    assert fetcher.urls == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
