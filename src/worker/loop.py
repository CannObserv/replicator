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
from co_core.pure.models.changes import ContentFetchCommand
from co_core_aio.bus import AsyncBusConsumer
from redis.asyncio import Redis

from src.core.config import Settings
from src.core.logging import get_logger

logger = get_logger(__name__)

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
    await handler(command)
    await consumer.ack(message.message_id)
    return Outcome.ACKED


async def poll_once(
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
) -> list[BusMessage]:
    """Source the next message.

    ``count=1`` throughout: ``read`` decodes with the fail-loud ``from_wire``, so
    a poison frame in a ``count>1`` batch raises before the well-formed messages
    in that batch are returned — reading one at a time keeps a single bad frame
    from swallowing good ones.
    """
    return await consumer.read(count=1, block_ms=settings.read_block_ms)


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
