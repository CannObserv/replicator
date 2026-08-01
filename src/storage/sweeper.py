"""Retention over the local blob tree — the reap that makes "temporary" true.

Replicator is the producer in archiver's temp-cache protocol, where the producer
cleans up. This module is the filesystem half of that: a single synchronous pass
over the tree, returning what it did — including its failures, which it counts
rather than logs. The cadence, the logging, and the decision to run at all
belong to ``src.worker.retention``.

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
    """What one pass over the tree did, and what it left behind.

    ``bytes_remaining`` is what the ceiling reads, so it counts **everything the
    tree is still holding** — surviving blobs and the temporaries the sweep is
    waiting out alike. Measuring only ``*.bin`` would let a crash loop fill the
    disk with debris while the ceiling reported an empty tree.

    The per-population counts are kept alongside it rather than folded in.
    ``blobs_remaining`` and ``temps_remaining`` describe different things — one
    is the tree doing its job, the other is debris from interrupted writes — and
    a single total would hide a rising second number inside a healthy first.

    ``reap_failures`` is counted rather than logged per file: the realistic
    trigger is a permissions problem across the whole tree, and a line each would
    be tens of thousands per cycle. ``reap_failure_sample`` keeps one example so
    the errno is still reachable from the journal.
    """

    blobs_reaped: int = 0
    bytes_reclaimed: int = 0
    temps_reaped: int = 0
    shards_removed: int = 0
    blobs_remaining: int = 0
    temps_remaining: int = 0
    temp_bytes_remaining: int = 0
    bytes_remaining: int = 0
    reap_failures: int = 0
    reap_failure_sample: str | None = None


@dataclass(frozen=True)
class _PassResult:
    """One glob pattern's worth of reaping — blobs or temporaries."""

    reaped: int = 0
    reclaimed: int = 0
    survivors: int = 0
    survivor_bytes: int = 0
    failures: int = 0
    failure_sample: str | None = None


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
    blobs = _reap_older_than(root, BLOB_GLOB, cutoff=now - ttl_seconds)
    temps = _reap_older_than(root, TEMP_GLOB, cutoff=now - temp_grace_seconds)
    return SweepResult(
        blobs_reaped=blobs.reaped,
        # Both passes: "how much did this free" is a question about the disk, and
        # the disk does not care which population the bytes belonged to.
        bytes_reclaimed=blobs.reclaimed + temps.reclaimed,
        temps_reaped=temps.reaped,
        # Last, and only after both file passes: an empty directory is only
        # safely empty once nothing is about to be renamed into it.
        shards_removed=_remove_empty_shards(root),
        blobs_remaining=blobs.survivors,
        temps_remaining=temps.survivors,
        temp_bytes_remaining=temps.survivor_bytes,
        bytes_remaining=blobs.survivor_bytes + temps.survivor_bytes,
        reap_failures=blobs.failures + temps.failures,
        reap_failure_sample=blobs.failure_sample or temps.failure_sample,
    )


def _reap_older_than(root: Path, pattern: str, *, cutoff: float) -> _PassResult:
    """Unlink everything matching ``pattern`` last touched before ``cutoff``.

    Survivors are tallied in the same walk that reaps: the ceiling reads that
    total, and a second walk would be measuring a tree this one has already
    changed.

    Exactly one ``stat`` per file. That is the pass's whole per-file cost on a
    tree walked forever on a timer — and re-stat-ing what was already read is
    what opens a window for a concurrent reap to raise between the two calls.

    Every step tolerates the file vanishing underneath it. Two workers can share
    a tree, so losing the race to reap the same expired entry is ordinary, and
    the loser must not take down a pass that still has thousands of files to go.
    """
    reaped = reclaimed = survivors = survivor_bytes = failures = 0
    sample: str | None = None
    for path in root.glob(pattern):
        try:
            stat_result = path.stat()
        except FileNotFoundError:
            continue  # reaped between the glob and the stat
        except OSError as exc:
            failures += 1
            sample = sample or f"{path} (errno {exc.errno})"
            continue
        if stat_result.st_mtime >= cutoff:
            survivors += 1
            survivor_bytes += stat_result.st_size
            continue
        try:
            path.unlink()
        except FileNotFoundError:
            continue
        except OSError as exc:
            failures += 1
            sample = sample or f"{path} (errno {exc.errno})"
            # Expired but still on disk, so still the ceiling's problem.
            survivors += 1
            survivor_bytes += stat_result.st_size
            continue
        reaped += 1
        reclaimed += stat_result.st_size
    return _PassResult(
        reaped=reaped,
        reclaimed=reclaimed,
        survivors=survivors,
        survivor_bytes=survivor_bytes,
        failures=failures,
        failure_sample=sample,
    )


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
