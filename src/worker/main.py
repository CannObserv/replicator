"""Bus consumer entry point — the primary Replicator process.

Consumes ``content.fetch`` commands from the Redis change bus and (once the
message loop lands) fetches, fingerprints, temp-stores, and emits a
``blob_available`` fact. See ``docs/plans/2026-06-25-replicator-mvp-design.md``.

Scaffold status: this module establishes the bus connection and the consumer
group. The read -> fetch -> fingerprint -> store -> publish handler is the first
feature increment and is built test-first; nothing here pre-empts it.

Bus clients are **injection-only** — the co-core driver never opens or closes the
``redis.asyncio.Redis`` client, so this module owns one for the worker lifetime
and closes it on the way out.
"""

import asyncio

from co_core.pure.adapters.bus import streams
from co_core_aio.bus import AsyncBusConsumer
from redis.asyncio import Redis

from src.core.config import Settings, get_settings
from src.core.logging import configure_logging, get_logger

logger = get_logger(__name__)


def build_consumer(client: Redis, settings: Settings) -> AsyncBusConsumer:
    """Wire an ``AsyncBusConsumer`` for the ``content.fetch`` command stream.

    ``content.fetch`` carries command semantics, so there is exactly one group
    cluster-wide (``replicator.fetch``) whose members compete for messages —
    unlike a fact stream, where each consuming service gets its own group.
    """
    return AsyncBusConsumer(
        client,
        topic=streams.CONTENT_FETCH,
        group=settings.consumer_group,
        consumer=settings.consumer_name,
    )


async def run() -> None:
    """Connect to the bus and ensure the consumer group exists."""
    settings = get_settings()
    configure_logging(settings.log_level)

    # systemd's StateDirectory= creates the parent only, so the leaf is ours to
    # make. Doing it at startup rather than at first write means a
    # misconfigured path fails loudly on boot, not mid-fetch. The failure is
    # logged structurally before re-raising: an uncaught OSError would put the
    # one line that matters into the journal as a bare traceback, unparseable by
    # a pipeline expecting JSON, right before the unit flaps to its restart
    # limit.
    try:
        settings.blob_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.error(
            "blob directory is not usable",
            extra={"blob_dir": str(settings.blob_dir), "errno": exc.errno},
        )
        raise

    client = Redis.from_url(settings.redis_url)
    try:
        consumer = build_consumer(client, settings)
        # start_id="$" reads only messages added after group creation. The MVP
        # seed harness controls when commands appear, so a backlog drain ("0")
        # is not needed; revisit when a live issuer exists.
        await consumer.ensure_group(start_id="$")
        logger.info(
            "worker ready",
            extra={
                "group": settings.consumer_group,
                "consumer": settings.consumer_name,
                "build": settings.build_id,
            },
        )
        logger.info("message loop not yet implemented — exiting cleanly")
    finally:
        await client.aclose()


def main() -> None:
    """Console entry point."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
