"""The ``content.fetch`` consume path: poll -> dispatch -> ack.

Split from ``main`` so each outcome is unit-testable without driving the loop:
``process_message`` decides the fate of one message, ``poll_once`` sources them,
and ``run_loop`` owns only cadence and shutdown.

The byte path (fetch, fingerprint, temp-store, ``blob_available``) lives behind
the ``Handler`` seam in ``src.worker.handler`` — this module stays ignorant of
what a handler does, and decides only what its success or failure means.
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

# How long an idle poll parks before looking again. On a live broker the
# blocking XREADGROUP is the real wait and this adds ~50ms per idle tick; it is
# kept as insurance against a client that does not honour `block` — fakeredis
# returns from a blocking read immediately, which would otherwise busy-spin the
# loop in tests. Waiting on the stop event rather than sleeping keeps shutdown
# prompt.
IDLE_SLEEP_SECONDS = 0.05

# Outage handling is operator-tunable and lives in Settings
# (REPLICATOR_ERROR_BACKOFF_BASE_SECONDS / _MAX_SECONDS,
# REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES) — these two are shape, not policy:
# the exponent cap keeps 2**shift from overflowing before the max is applied,
# and the log interval keeps a long outage from flooding the journal at one
# line per retry. Mirrors archiver's drain-loop backoff (archiver#107, CR #13).
ERROR_BACKOFF_MAX_SHIFT = 5
ERROR_LOG_EVERY = 15


class Outcome(StrEnum):
    """What ``process_message`` did with one message."""

    ACKED = "acked"
    DEDUPED = "deduped"
    DEAD_LETTERED = "dead_lettered"
    RETRY = "retry"


async def process_message(
    message: BusMessage,
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    group: str,
    handler: Handler,
    settings: Settings,
) -> Outcome:
    """Dispatch one decoded message and decide its fate.

    Acks only after the handler returns — an unacked message stays in the PEL and
    comes back via ``claim_stale``, which is what makes delivery at-least-once.

    ``group`` is passed rather than re-read from ``settings`` so the PEL this
    consults is provably the one ``consumer`` acks against: co-core keeps the
    consumer's group private, and two independent reads of the same setting can
    drift, in which case the ceiling would query an unrelated PEL and silently
    never trip.
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
            message,
            client=client,
            consumer=consumer,
            group=group,
            settings=settings,
            command=command,
            exc=exc,
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
    group: str,
    settings: Settings,
    command: ContentFetchCommand,
    exc: Exception,
) -> Outcome:
    """Retry a failure the loop could not classify, up to the delivery ceiling.

    Defense in depth, not the primary DLQ route: a handler bug must not discard
    a valid command on its first failure, but it must not spin forever either.
    """
    attempts = await _delivery_count(client, message, group=group)
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
    if not entries:
        # Nothing to count against: the entry left the PEL underneath us, or the
        # group is not the one it was delivered to. Treat it as a first attempt
        # (retry rather than discard) but say so — a silent 1 here would mean a
        # ceiling that can never trip.
        logger.warning(
            "no pending entry for this message — cannot read its delivery count",
            extra={"message_id": message.message_id, "topic": message.topic, "group": group},
        )
        return 1
    return int(entries[0]["times_delivered"])


async def _dead_letter(
    consumer: AsyncBusConsumer,
    message_id: str,
    fields: dict[str, str],
    *,
    reason: str,
    detail: dict[str, object] | None = None,
) -> Outcome:
    """Copy a frame to ``<topic>.dlq``, ack the original, and say why.

    The reason travels *in the entry*, not only in the log line: the DLQ is the
    operator's triage surface, and correlating a stream entry back to a journal
    timestamp to learn which of the five routes sent it there is avoidable work.
    Consumers use ``extra="ignore"`` models, so the added envelope keys cannot
    break a replay tool that re-reads the original ``payload``.
    """
    dlq_id = await consumer.dead_letter(
        message_id,
        {**fields, "dlq_reason": reason, "dlq_original_id": message_id},
    )
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
    *,
    group: str,
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
        extra={"skipped": MAX_POISON_SKIPS, "group": group},
    )
    return []


async def poll_once(
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
    *,
    group: str,
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
    reclaimed = await claim_once(client, consumer, settings, group=group)
    if reclaimed:
        return reclaimed
    try:
        return await consumer.read(count=1, block_ms=settings.read_block_ms)
    except BusMessageAnomaly as exc:
        await dead_letter_anomaly(client, consumer, exc)
        return []


async def process_batch(
    messages: list[BusMessage],
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    group: str,
    settings: Settings,
    handler: Handler,
    stop: asyncio.Event,
) -> None:
    """Dispatch a polled batch, stopping between messages once asked to.

    The stop check is between messages, never inside one: a SIGTERM arriving
    mid-handler finishes that message and acks it, so ``systemctl restart`` does
    not strand work in the pending entries list. Polling is ``count=1`` today, so
    the loop body runs once — the check is here so that raising the count later
    cannot silently extend shutdown latency.
    """
    for message in messages:
        await process_message(
            message,
            client=client,
            consumer=consumer,
            group=group,
            handler=handler,
            settings=settings,
        )
        if stop.is_set():
            break


async def run_loop(
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    group: str,
    settings: Settings,
    handler: Handler,
    stop: asyncio.Event,
) -> None:
    """Poll and dispatch until ``stop`` is set.

    A failing *message* is decided by ``process_message``; a failing *cycle* —
    the broker refusing a read, an ack, or a DLQ write — is this loop's problem.
    It backs off and retries rather than propagating, because the alternative is
    the process dying on every Redis restart and leaning on systemd to bring it
    back through the full ExecStartPre chain. ``asyncio.CancelledError`` is a
    ``BaseException`` and still propagates: shutdown is not a cycle failure.
    """
    consecutive_failures = 0
    while not stop.is_set():
        # Bound before the try: the read below is only valid by the except
        # branch always continuing, and a future branch that falls through
        # would hit UnboundLocalError on a path that runs only during an outage.
        messages: list[BusMessage] = []
        try:
            messages = await poll_once(client, consumer, settings, group=group)
            await process_batch(
                messages,
                client=client,
                consumer=consumer,
                group=group,
                settings=settings,
                handler=handler,
                stop=stop,
            )
        except Exception as exc:
            consecutive_failures += 1
            if consecutive_failures >= settings.max_consecutive_cycle_failures:
                logger.error(
                    "poll cycle has failed continuously — exiting so the unit restarts",
                    extra={
                        "consecutive_failures": consecutive_failures,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    exc_info=exc,
                )
                raise
            if consecutive_failures == 1 or consecutive_failures % ERROR_LOG_EVERY == 0:
                logger.error(
                    "poll cycle failed — backing off",
                    extra={
                        "consecutive_failures": consecutive_failures,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    exc_info=exc,
                )
            await _park(
                stop,
                _error_backoff_seconds(
                    consecutive_failures,
                    base=settings.error_backoff_base_seconds,
                    maximum=settings.error_backoff_max_seconds,
                ),
            )
            continue

        if consecutive_failures:
            logger.info("poll cycle recovered", extra={"after_failures": consecutive_failures})
            consecutive_failures = 0
        if not messages:
            await _park(stop, IDLE_SLEEP_SECONDS)


def _error_backoff_seconds(consecutive_failures: int, *, base: float, maximum: float) -> float:
    """Exponential backoff (``base * 2**(n-1)``) capped at ``maximum``.

    The exponent is clamped so the intermediate cannot overflow before the cap
    is applied — a loop that has been failing for hours must not compute
    ``2**10000``.
    """
    if consecutive_failures <= 1:
        return base
    shift = min(consecutive_failures - 1, ERROR_BACKOFF_MAX_SHIFT)
    return min(base * 2**shift, maximum)


async def _park(stop: asyncio.Event, seconds: float) -> None:
    """Wait out an idle poll, returning early once ``stop`` is set."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass
