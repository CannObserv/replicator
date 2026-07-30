"""The ``content.fetch`` consume path: poll -> dispatch -> ack.

Split from ``main`` so each outcome is unit-testable without driving the loop:
``process_message`` decides the fate of one message, ``poll_once`` sources them,
and ``run_loop`` owns only cadence and shutdown.

The byte path (fetch, fingerprint, temp-store, ``blob_available``) lives behind
the ``Handler`` seam and arrives in the next issue; this module ships a logging
handler so the loop is exercisable end to end without it.
"""

import asyncio
from collections.abc import Awaitable, Callable
from enum import StrEnum

from co_core.effects.bus import BusMessage
from co_core.pure.adapters.bus.exceptions import BusMessageAnomaly
from co_core.pure.models.changes import ContentFetchCommand
from co_core_aio.bus import AsyncBusConsumer
from redis.asyncio import Redis
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.core.config import Settings
from src.core.errors import PermanentFetchError, TransientFetchError
from src.core.logging import get_logger

logger = get_logger(__name__)

# The only command schema this worker understands. co-core's model validates
# any integer here (schema_version is a plain int, not a Literal), so an
# unrecognized version is ours to catch — branch before destructuring.
SUPPORTED_SCHEMA_VERSION = 1

# Namespace for the command dedupe keys. Redis, not an in-memory set: the point
# is to survive the restart that redelivery follows.
DEDUPE_KEY_PREFIX = "replicator:cmd:"

# Handler failures that are *transient* (broker or origin down / slow / loading)
# and must retry indefinitely — EXEMPT from the delivery ceiling, so a long-but-
# genuine outage can never silently drop a valid command. Mirrors archiver's
# publisher tuple; redis-py's error types are disjoint from the builtins, so
# both are listed.
#
# NOTE (co-core coupling): this gate assumes co-core-aio propagates the
# underlying redis exception types *unwrapped* (its current behavior). If it
# ever wraps them in its own error type, a real outage would fall through to the
# ceiling and the cliff reopens — this tuple must then track that wrapper.
_TRANSIENT_ERRORS: tuple[type[Exception], ...] = (
    TransientFetchError,
    ConnectionError,  # builtin
    TimeoutError,  # builtin (asyncio.TimeoutError since 3.11)
    RedisConnectionError,
    RedisTimeoutError,
    BusyLoadingError,
)

# Bound on consecutive poison frames skipped in one recovery pass. claim_stale
# restarts at 0-0 on every call, so each poison entry must be routed away before
# the next claim can reach a good message; the bound keeps a pathological PEL
# from starving the read path within a single tick.
MAX_POISON_SKIPS = 10

# The consume-path handler seam. Raising signals failure; the loop — not the
# handler — decides whether that means retry or dead-letter.
Handler = Callable[[ContentFetchCommand], Awaitable[None]]

# How long an idle poll parks before looking again. The blocking XREADGROUP is
# the real wait on a live broker; this exists because fakeredis returns from a
# blocking read immediately, which would otherwise busy-spin the loop in tests.
# Waiting on the stop event rather than sleeping keeps shutdown prompt.
IDLE_SLEEP_SECONDS = 0.05


class Outcome(StrEnum):
    """What ``process_message`` did with one message."""

    ACKED = "acked"
    DEDUPED = "deduped"
    DEAD_LETTERED = "dead_lettered"
    RETRY = "retry"


async def log_only_handler(command: ContentFetchCommand) -> None:
    """Placeholder for the fetch/fingerprint/store path (next issue)."""
    logger.info(
        "content.fetch received",
        extra={"command_id": command.command_id, "url": command.url},
    )


async def process_message(
    message: BusMessage,
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    handler: Handler,
    settings: Settings,
) -> Outcome:
    """Dispatch one decoded message and decide its fate.

    Acks only after the handler returns — an unacked message stays in the PEL and
    comes back via ``claim_stale``, which is what makes delivery at-least-once.
    """
    command = message.payload
    # from_wire's event_type -> model table is global, so a fact XADDed to the
    # command stream decodes cleanly into the wrong type rather than raising.
    if not isinstance(command, ContentFetchCommand):
        return await _dead_letter(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="payload is not a content.fetch command",
            detail={"event_type": command.event_type},
        )
    if command.schema_version != SUPPORTED_SCHEMA_VERSION:
        return await _dead_letter(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="unsupported schema_version",
            detail={"command_id": command.command_id, "schema_version": command.schema_version},
        )

    dedupe_key = f"{DEDUPE_KEY_PREFIX}{command.command_id}"
    if await client.exists(dedupe_key):
        await consumer.ack(message.message_id)
        logger.info(
            "skipped an already-handled command",
            extra={"command_id": command.command_id, "message_id": message.message_id},
        )
        return Outcome.DEDUPED

    # asyncio.CancelledError is a BaseException, so shutdown propagates through
    # these handlers untouched — it is not a message failure.
    try:
        await handler(command)
    except PermanentFetchError as exc:
        return await _dead_letter(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="handler reported a permanent failure",
            detail={"command_id": command.command_id, "error": str(exc)},
        )
    except _TRANSIENT_ERRORS as exc:
        logger.warning(
            "transient failure — leaving the message pending for redelivery",
            extra={
                "command_id": command.command_id,
                "message_id": message.message_id,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        return Outcome.RETRY
    except Exception as exc:
        return await _handle_unclassified(
            message, client=client, consumer=consumer, settings=settings, command=command, exc=exc
        )

    # Written *after* the handler, deliberately. Marking first would turn a crash
    # between the mark and a completed handle into permanent loss: redelivery
    # would short-circuit to ack having never done the work. This ordering can
    # only re-run a handler that already succeeded, which content-addressed
    # storage absorbs — the key is a cheap short-circuit, not the correctness
    # mechanism.
    await client.set(dedupe_key, message.message_id, nx=True, ex=settings.dedupe_ttl_seconds)
    await consumer.ack(message.message_id)
    return Outcome.ACKED


async def _handle_unclassified(
    message: BusMessage,
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
    command: ContentFetchCommand,
    exc: Exception,
) -> Outcome:
    """Retry a failure the loop could not classify, up to the delivery ceiling.

    Defense in depth, not the primary DLQ route: a handler bug must not discard
    a valid command on its first failure, but it must not spin forever either.
    """
    attempts = await _delivery_count(client, message, group=settings.consumer_group)
    error = f"{type(exc).__name__}: {exc}"
    if attempts >= settings.max_delivery_attempts:
        return await _dead_letter(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="unclassified failure hit the delivery ceiling",
            detail={"command_id": command.command_id, "attempts": attempts, "error": error},
        )
    logger.warning(
        "unclassified failure — leaving the message pending for redelivery",
        extra={
            "command_id": command.command_id,
            "message_id": message.message_id,
            "attempts": attempts,
            "error": error,
        },
        exc_info=exc,
    )
    return Outcome.RETRY


async def _delivery_count(client: Redis, message: BusMessage, *, group: str) -> int:
    """How many times this entry has been delivered, per the broker's own PEL.

    XPENDING is the source of truth — no side counter to keep, expire, or
    reconcile. Note it only advances on a claim_stale reclaim (a ``>`` read never
    redelivers), which is what makes the ceiling a bound in time.
    """
    entries = await client.xpending_range(
        message.topic, group, min=message.message_id, max=message.message_id, count=1
    )
    return int(entries[0]["times_delivered"]) if entries else 1


async def _dead_letter(
    consumer: AsyncBusConsumer,
    message_id: str,
    fields: dict[str, str],
    *,
    reason: str,
    detail: dict[str, object] | None = None,
) -> Outcome:
    """Copy a frame to ``<topic>.dlq``, ack the original, and say why."""
    dlq_id = await consumer.dead_letter(message_id, fields)
    logger.warning(
        "dead-lettered a content.fetch frame",
        extra={"reason": reason, "message_id": message_id, "dlq_id": dlq_id, **(detail or {})},
    )
    return Outcome.DEAD_LETTERED


async def dead_letter_anomaly(
    client: Redis,
    consumer: AsyncBusConsumer,
    exc: BusMessageAnomaly,
) -> Outcome:
    """Route a frame that failed to decode at all.

    ``from_wire`` raises from inside ``read``/``claim_stale``, so there is no
    ``BusMessage`` and no field map — the anomaly carries only ``topic`` and
    ``message_id``. ``dead_letter`` XADDs the fields it is given and ``XADD``
    rejects an empty map, so the raw frame is re-read by id; a trimmed or
    ``XDEL``-ed entry (still pending, no longer in the stream) falls back to a
    synthesized record so the message can never get stuck in the PEL.
    """
    raw = await client.xrange(exc.topic, min=exc.message_id, max=exc.message_id)
    if raw:
        fields = {_as_str(k): _as_str(v) for k, v in raw[0][1].items()}
    else:
        fields = {
            "error": str(exc),
            "original_message_id": exc.message_id,
            "original_topic": exc.topic,
        }
    return await _dead_letter(
        consumer,
        exc.message_id,
        fields,
        reason="frame failed to decode",
        detail={"anomaly": type(exc).__name__, "recovered_fields": bool(raw)},
    )


def _as_str(value: bytes | str) -> str:
    """Decode a raw Redis field (the client is not in decode_responses mode)."""
    return value.decode() if isinstance(value, bytes) else value


async def claim_once(
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
) -> list[BusMessage]:
    """Reclaim one message abandoned by a crashed (or transiently failing) worker.

    ``count=1`` for the batch-poison reason plus a sharper one: XAUTOCLAIM
    transfers ownership and resets the idle clock on every entry it returns
    *before* co-core decodes them, so one poison frame in a batch of ten would
    strand nine good messages with their timers restarted.

    A poison entry would otherwise jam recovery permanently — ``claim_stale``
    restarts at ``0-0`` on every call, so the same bad frame is re-claimed and
    re-raised forever. Routing it to the DLQ and re-claiming is what lets the
    entries behind it through.
    """
    for _ in range(MAX_POISON_SKIPS):
        try:
            return await consumer.claim_stale(
                min_idle_ms=settings.claim_min_idle_ms, count=1, start_id="0-0"
            )
        except BusMessageAnomaly as exc:
            await dead_letter_anomaly(client, consumer, exc)
    logger.warning(
        "recovery pass hit the poison-skip bound",
        extra={"skipped": MAX_POISON_SKIPS, "group": settings.consumer_group},
    )
    return []


async def poll_once(
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
) -> list[BusMessage]:
    """Source the next message, dead-lettering anything that will not decode.

    ``count=1`` throughout: ``read`` decodes with the fail-loud ``from_wire``, so
    a poison frame in a ``count>1`` batch raises before the well-formed messages
    in that batch are returned — reading one at a time keeps a single bad frame
    from swallowing good ones. An empty list means "nothing to do this tick",
    whether the stream was idle or a poison frame was just routed away.

    Recovery comes first: work already delivered to a worker that died (or that
    failed transiently here) is older than anything unread, and leaving it while
    new messages flow would let it age indefinitely.
    """
    reclaimed = await claim_once(client, consumer, settings)
    if reclaimed:
        return reclaimed
    try:
        return await consumer.read(count=1, block_ms=settings.read_block_ms)
    except BusMessageAnomaly as exc:
        await dead_letter_anomaly(client, consumer, exc)
        return []


async def run_loop(
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
    handler: Handler,
    stop: asyncio.Event,
) -> None:
    """Poll and dispatch until ``stop`` is set.

    The stop check is between messages, never inside one: a SIGTERM arriving
    mid-handler finishes that message and acks it, so ``systemctl restart`` does
    not strand work in the pending entries list.
    """
    while not stop.is_set():
        messages = await poll_once(client, consumer, settings)
        if not messages:
            await _park(stop, IDLE_SLEEP_SECONDS)
            continue
        for message in messages:
            await process_message(
                message,
                client=client,
                consumer=consumer,
                handler=handler,
                settings=settings,
            )


async def _park(stop: asyncio.Event, seconds: float) -> None:
    """Wait out an idle poll, returning early once ``stop`` is set."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass
