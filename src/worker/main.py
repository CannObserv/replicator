"""Bus consumer entry point — the primary Replicator process.

Consumes ``content.fetch`` commands from the Redis change bus. See
``docs/plans/2026-06-25-replicator-mvp-design.md``.

This module is wiring: client lifetime, consumer group, signal handling. The
consume path itself lives in ``src.worker.loop``, and the fetch → fingerprint →
temp-store → ``blob_available`` work behind that module's handler seam lives in
``src.worker.handler``.

Bus clients are **injection-only** — the co-core driver never opens or closes the
``redis.asyncio.Redis`` client, so this module owns one for the worker lifetime
and closes it on the way out.
"""

import asyncio
import signal
import stat
from pathlib import Path

from co_core.pure.adapters.bus import streams
from co_core_aio.bus import AsyncBusConsumer
from co_core_aio.fetch import AsyncFetchDriver
from redis.asyncio import Redis

from src.core.config import Settings, get_settings
from src.core.logging import configure_logging, get_logger
from src.storage.local import LocalBlobStore, ensure_directory
from src.worker.handler import build_handler
from src.worker.loop import run_loop

logger = get_logger(__name__)

# Signals that mean "stop taking new work". SIGINT is included so an interactive
# Ctrl-C drains the same way systemd's SIGTERM does.
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def warn_if_unreachable(blob_dir: Path) -> None:
    """Say so when the blob root cannot be traversed by another service.

    A pre-existing directory keeps whatever mode its operator gave it — the
    store deliberately does not widen what it did not create. The cost of that
    choice is a misconfiguration with no local symptom: blobs store fine, the
    fact publishes fine, and the failure appears in *another repo* as a
    ``blob_uri`` nothing can open. This is the one place it can be noticed.

    A warning rather than a failure: a single-user deployment where nothing else
    reads the blobs is legitimate, and the operator may mean it.
    """
    mode = stat.S_IMODE(blob_dir.stat().st_mode)
    if mode & 0o011:  # group- or other-executable ⇒ traversable
        return
    logger.warning(
        "blob directory is not traversable by other services",
        extra={
            "blob_dir": str(blob_dir),
            "mode": oct(mode),
            "detail": "blob_uri will be announced on content.blobs but cannot be opened",
        },
    )


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


def install_signal_handlers(stop: asyncio.Event) -> None:
    """Route SIGTERM/SIGINT to ``stop`` instead of killing the loop mid-message.

    Setting an event rather than cancelling means the in-flight message finishes
    and acks; a cancelled handler would leave it in the PEL for a stale-claim
    round-trip that a clean restart has no reason to need.
    """
    loop = asyncio.get_running_loop()
    for sig in _STOP_SIGNALS:
        loop.add_signal_handler(sig, stop.set)


def remove_signal_handlers() -> None:
    """Restore default signal disposition (mirrors ``install_signal_handlers``)."""
    loop = asyncio.get_running_loop()
    for sig in _STOP_SIGNALS:
        loop.remove_signal_handler(sig)


async def run(stop: asyncio.Event | None = None) -> None:
    """Connect to the bus, ensure the consumer group, and consume until stopped.

    ``stop`` is injectable so tests drive the loop without signals; left unset,
    the process owns its own event and wires SIGTERM/SIGINT to it.
    """
    settings = get_settings()
    configure_logging(settings.log_level)

    # systemd's StateDirectory= creates the parent only, so the leaf is ours to
    # make. Doing it at startup rather than at first write means a
    # misconfigured path fails loudly on boot, not mid-fetch. The failure is
    # logged structurally before re-raising: an uncaught OSError would put the
    # one line that matters into the journal as a bare traceback, unparseable by
    # a pipeline expecting JSON, right before the unit flaps to its restart
    # limit.
    #
    # ensure_directory rather than a bare mkdir so a directory this process
    # creates is left readable by the service that reads the blobs, while one
    # that already exists keeps whatever mode its operator gave it.
    try:
        ensure_directory(settings.blob_dir)
    except OSError as exc:
        logger.error(
            "blob directory is not usable",
            extra={"blob_dir": str(settings.blob_dir), "errno": exc.errno},
        )
        raise
    warn_if_unreachable(settings.blob_dir)

    owns_signals = stop is None
    client = Redis.from_url(settings.redis_url)
    # Constructed inside the try, like the signal handlers: anything opened
    # between here and the try would leak the Redis client if it raised.
    fetcher: AsyncFetchDriver | None = None
    try:
        # One driver for the worker's lifetime, not one per message: it wraps an
        # httpx.AsyncClient whose connection pool is the point, and a per-message
        # driver would open and discard a pool per fetch. Closed in the same
        # finally as the Redis client — both are ours because we opened them.
        fetcher = AsyncFetchDriver()
        # Installed inside the try so the handlers are always removed again —
        # outside it, a failure between install and the try would leak global
        # signal state (harmless for a dying process, not for an in-process test).
        if stop is None:
            stop = asyncio.Event()
            install_signal_handlers(stop)

        consumer = build_consumer(client, settings)
        # Default start_id="$" reads only messages added after group creation.
        # The MVP seed harness controls when commands appear, so a backlog drain
        # ("0") is not needed; REPLICATOR_CONSUMER_START_ID flips it once a live
        # issuer exists — see the setting for the XGROUP SETID caveat.
        await consumer.ensure_group(start_id=settings.consumer_start_id)
        logger.info(
            "worker ready",
            extra={
                "group": settings.consumer_group,
                "consumer": settings.consumer_name,
                "build": settings.build_id,
                # How long this worker will absorb a broker outage before
                # exiting. In the journal at every boot because the unit's
                # StartLimitIntervalSec is sized against it, and a config change
                # that widens it would otherwise be invisible.
                "worst_case_outage_seconds": settings.worst_case_outage_seconds,
            },
        )
        await run_loop(
            client=client,
            consumer=consumer,
            # The same value build_consumer used — threaded explicitly so the
            # PEL the ceiling reads is provably the one the consumer acks against.
            group=settings.consumer_group,
            settings=settings,
            handler=build_handler(
                fetcher=fetcher,
                store=LocalBlobStore(settings.blob_dir),
                client=client,
                settings=settings,
            ),
            stop=stop,
        )
        logger.info("worker stopped", extra={"consumer": settings.consumer_name})
    finally:
        if owns_signals:
            remove_signal_handlers()
        if fetcher is not None:
            await fetcher.aclose()
        await client.aclose()


def main() -> None:
    """Console entry point."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
