"""The retention task: run the blob sweep on a cadence, alongside the consume loop.

An in-worker task rather than a systemd timer. A timer's one real advantage is
surviving a crashed worker, and that case is self-limiting — a worker that is not
running is not writing blobs either, so the tree stops growing. Against it: a
second unit repeating the wheelhouse and ``uv --frozen`` ``ExecStartPre`` chain,
and a second configuration surface, for a delete loop. The worker already has a
stop-event cadence this can ride.

The sweep itself is synchronous filesystem work, so it runs off the event loop
thread — a tree of any size walks for long enough to stall a poll otherwise.
"""

import asyncio
from pathlib import Path

from src.core.config import Settings
from src.core.logging import get_logger
from src.storage.sweeper import BlobUsage, sweep
from src.worker.loop import park

logger = get_logger(__name__)


async def run_sweeper(
    *, root: Path, settings: Settings, usage: BlobUsage, stop: asyncio.Event
) -> None:
    """Sweep ``root`` until ``stop`` is set, keeping ``usage`` measured.

    The first sweep runs immediately rather than after one interval: a worker
    restarting behind a long outage should reclaim on boot, not a quarter of an
    hour later. The cost is a window at startup — until that first pass returns,
    ``usage`` is the byte path's own running estimate from zero, so the ceiling
    is briefly optimistic. Sub-second in practice, and erring toward fetching.
    """
    while not stop.is_set():
        await _sweep_once(root, settings=settings, usage=usage)
        await park(stop, settings.blob_sweep_interval_seconds)


async def _sweep_once(root: Path, *, settings: Settings, usage: BlobUsage) -> None:
    """One pass, with its outcome logged and its failure absorbed.

    A failing sweep is not a reason to take the worker down. Retention is not
    load-bearing for correctness — the consume path is — and a tree that cannot
    be walked degrades to a tree that grows until the ceiling stops the byte
    path, which is the guard that exists for exactly this. Exiting instead would
    trade a bounded degradation for an outage.

    ``usage`` is left at its previous measurement on failure. A stale number is a
    better ceiling input than a zero, which would read as an empty tree and
    re-open the tap at precisely the wrong moment.
    """
    try:
        # Off the loop thread: the walk is blocking filesystem work, and a tree
        # of any size would otherwise stall a poll — turning retention into a
        # source of consume-path latency, which is exactly backwards.
        result = await asyncio.to_thread(
            sweep,
            root,
            ttl_seconds=settings.blob_ttl_seconds,
            temp_grace_seconds=settings.blob_temp_grace_seconds,
        )
    except OSError as exc:
        logger.warning(
            "blob sweep failed — retrying next cycle",
            extra={"blob_dir": str(root), "errno": exc.errno},
        )
        return

    usage.observe(result.bytes_remaining)
    if result.blobs_reaped or result.temps_reaped or result.shards_removed:
        # Silent when there was nothing to do: this runs every cycle forever, and
        # an idle tree must not be the loudest thing in the journal.
        #
        # The populations are counted separately because they mean different
        # things. Reaped blobs are the policy working; reaped temporaries are
        # writes that were SIGKILLed mid-store, and a rising count of those is a
        # symptom, not housekeeping.
        logger.info(
            "swept the blob directory",
            extra={
                "blobs_reaped": result.blobs_reaped,
                "bytes_reclaimed": result.bytes_reclaimed,
                "temps_reaped": result.temps_reaped,
                "shards_removed": result.shards_removed,
                "blobs_remaining": result.blobs_remaining,
                "bytes_remaining": result.bytes_remaining,
            },
        )
    if usage.is_over(settings.blob_max_total_bytes):
        # The byte path stops fetching from here until a sweep brings this back
        # under. Said plainly, because the visible symptom is otherwise commands
        # sitting on the bus with no local error to explain them.
        logger.warning(
            "blob directory is over its ceiling — fetching is paused until it drains",
            extra={
                "bytes_remaining": result.bytes_remaining,
                "ceiling_bytes": settings.blob_max_total_bytes,
                "blob_ttl_seconds": settings.blob_ttl_seconds,
                "detail": "blobs inside their TTL are never reaped to make room",
            },
        )
