"""Retention over the local blob tree — the reap that makes "temporary" true.

Replicator is the producer in archiver's temp-cache protocol, where the producer
cleans up. This module is the filesystem half of that: a single synchronous pass
over the tree, returning what it did. The cadence, the logging, and the decision
to run at all belong to ``src.worker.retention``.

The tree holds **three** populations, and they are not interchangeable:

* finished blobs — ``<ab>/<cd>/<sha256>.bin``, reaped once their mtime is older
  than the TTL;
* in-flight temporaries — ``.<sha256>.<random>.tmp``, which are **not garbage**;
  a writer holds one between the write and the ``os.replace`` that publishes it,
  and reaping one makes that rename fail with ENOENT, dead-lettering a command
  whose bytes were fine. They are matched by their own pattern, on their own
  much longer grace, so only genuine SIGKILL debris is caught;
* empty shard directories, removed last for the same reason — an ``rmdir`` under
  a writer breaks its rename just as surely as unlinking the temp would.

Orphans — blobs whose ``blob_available`` never published — are deliberately not
a fourth case. They are ordinary aged blobs to the sweep; the exact signal is
recorded at the moment one is created (``src.worker.handler``), which costs
nothing and avoids making a *delete* decision depend on another service's stream
retention.

Reaping never runs the clock faster under disk pressure. Deleting a blob still
inside its TTL would turn a local disk problem into a ``blob_uri`` that cannot
be opened in another repo — the one failure with no local symptom. The ceiling
is enforced instead by ``BlobUsage``, which the byte path reads to stop fetching.
"""

import time
from dataclasses import dataclass
from pathlib import Path

from src.core.logging import get_logger

logger = get_logger(__name__)

# Finished blobs. Two levels of two characters mirrors LocalBlobStore's sharding,
# and the `.bin` suffix is what excludes the dot-prefixed temporaries by
# construction — the reason the temp naming was chosen.
BLOB_GLOB = "??/??/*.bin"

# In-flight temporaries, matched only to age out debris.
TEMP_GLOB = "??/??/.*.tmp"

# Shard levels, innermost first: a leaf has to go before its parent can be empty.
SHARD_GLOBS = ("??/??", "??")


@dataclass(frozen=True)
class SweepResult:
    """What one pass over the tree did, and what it left behind."""

    blobs_reaped: int = 0
    bytes_reclaimed: int = 0
    temps_reaped: int = 0
    shards_removed: int = 0
    blobs_remaining: int = 0
    bytes_remaining: int = 0


@dataclass
class BlobUsage:
    """How many bytes the blob tree is holding, shared by the sweep and the byte path.

    Two writers with different authority. ``observe`` is the sweep's measured
    total and replaces whatever was there; ``add`` is the byte path's estimate
    between sweeps, which exists because a burst can cross the ceiling long
    before the next pass re-measures. Drift is bounded by the sweep interval —
    the estimate is only ever used to stop fetching sooner, never to reap.

    It starts at zero and therefore under any ceiling: nothing is known before
    the first sweep, and refusing to fetch on that basis would make an unmeasured
    tree indistinguishable from a full one.
    """

    total_bytes: int = 0

    def observe(self, total_bytes: int) -> None:
        """Replace the running total with the sweep's measurement."""
        self.total_bytes = total_bytes

    def add(self, size_bytes: int) -> None:
        """Account for bytes stored since the last sweep."""
        self.total_bytes += size_bytes

    def is_over(self, ceiling_bytes: int) -> bool:
        """Whether the tree has grown past what this deployment will hold."""
        return self.total_bytes >= ceiling_bytes


def sweep(root: Path, *, ttl_seconds: float, temp_grace_seconds: float) -> SweepResult:
    """Reap expired blobs, stale temporaries, and the shards they emptied.

    Synchronous and blocking: a tree of any size walks for long enough to stall
    an event loop, so the caller runs this off the loop thread.

    A missing root sweeps to nothing rather than raising. The worker creates it
    at startup, but the sweep must not be the thing that fails if it has not yet.
    """
    if not root.is_dir():
        return SweepResult()

    now = time.time()
    reaped, reclaimed, remaining, held = _reap_blobs(root, cutoff=now - ttl_seconds)
    temps = _reap_files(root, TEMP_GLOB, cutoff=now - temp_grace_seconds)
    return SweepResult(
        blobs_reaped=reaped,
        bytes_reclaimed=reclaimed,
        temps_reaped=temps,
        # Last, and only after both file passes: an empty directory is only
        # safely empty once nothing is about to be renamed into it.
        shards_removed=_remove_empty_shards(root),
        blobs_remaining=remaining,
        bytes_remaining=held,
    )


def _reap_blobs(root: Path, *, cutoff: float) -> tuple[int, int, int, int]:
    """Unlink blobs last referenced before ``cutoff``; measure what survives.

    The survivors are counted in the same walk that reaps, because the ceiling
    reads that number and a second walk would report a tree the sweep has
    already changed.
    """
    reaped = reclaimed = remaining = held = 0
    for path in root.glob(BLOB_GLOB):
        size = _reap_if_older(path, cutoff=cutoff)
        if size is None:
            # Either still live or gone from under us; only the former is worth
            # counting, and a vanished file weighs nothing either way.
            if path.exists():
                remaining += 1
                held += path.stat().st_size
            continue
        reaped += 1
        reclaimed += size
    return reaped, reclaimed, remaining, held


def _reap_files(root: Path, pattern: str, *, cutoff: float) -> int:
    """Unlink everything matching ``pattern`` that is older than ``cutoff``."""
    return sum(1 for path in root.glob(pattern) if _reap_if_older(path, cutoff=cutoff) is not None)


def _reap_if_older(path: Path, *, cutoff: float) -> int | None:
    """Unlink ``path`` if it has not been touched since ``cutoff``; return its size.

    ``None`` means nothing was reaped — the file is still live, or it vanished
    between the stat and the unlink. The second case is ordinary: another worker
    on the same tree reaps the same expired blob, and the loser must not fail
    the sweep over it.
    """
    try:
        stat_result = path.stat()
        if stat_result.st_mtime >= cutoff:
            return None
        path.unlink()
    except FileNotFoundError:
        return None
    except OSError as exc:
        # Worth a line each: a tree this process cannot reap will keep growing
        # until the ceiling stops the byte path, and the errno says why.
        logger.warning("could not reap a blob", extra={"path": str(path), "errno": exc.errno})
        return None
    return stat_result.st_size


def _remove_empty_shards(root: Path) -> int:
    """Remove shard directories nothing is left in, innermost level first.

    ``rmdir`` rather than a recursive delete, and its refusal to touch a
    non-empty directory is the safety property: a shard holding a live
    temporary is left exactly where it is. The root is never a candidate — it
    is the store's construction-time invariant, not a shard.
    """
    removed = 0
    for pattern in SHARD_GLOBS:
        for path in root.glob(pattern):
            if not path.is_dir():
                continue
            try:
                path.rmdir()
            except OSError:
                continue  # not empty, or gone — both mean leave it
            removed += 1
    return removed
