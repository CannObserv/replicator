"""The retention sweep over the blob tree."""

import os
import time
from pathlib import Path

import pytest

from src.storage.local import LocalBlobStore
from src.storage.sweeper import BlobUsage, sweep

TTL = 600.0
TEMP_GRACE = 3600.0

# Two fingerprints landing in different shards, so a test can age one and leave
# the other alone without the two sharing a directory.
FRESH = "9f2a7c1e" + "0" * 56
AGED = "1b3d5f70" + "0" * 56


@pytest.fixture
def store(tmp_path):
    """A store rooted at a fresh temp directory."""
    return LocalBlobStore(tmp_path)


def blob_path(root: Path, fingerprint: str) -> Path:
    """Where the store puts a blob — spelled out rather than asked of the store."""
    return root / fingerprint[0:2] / fingerprint[2:4] / f"{fingerprint}.bin"


def age(path: Path, seconds: float) -> None:
    """Backdate ``path`` so a TTL measured from mtime considers it old."""
    when = time.time() - seconds
    os.utime(path, (when, when))


def run(root: Path, *, ttl: float = TTL, temp_grace: float = TEMP_GRACE):
    """``sweep`` with the test defaults filled in."""
    return sweep(root, ttl_seconds=ttl, temp_grace_seconds=temp_grace)


def test_a_blob_older_than_the_ttl_is_removed(store, tmp_path):
    store.store(b"hello", AGED, "text/plain")
    age(blob_path(tmp_path, AGED), TTL + 1)

    run(tmp_path)

    assert not blob_path(tmp_path, AGED).exists()


def test_a_blob_younger_than_the_ttl_is_left_alone(store, tmp_path):
    store.store(b"hello", FRESH, "text/plain")

    run(tmp_path)

    assert blob_path(tmp_path, FRESH).exists()


def test_a_blob_whose_fact_was_just_republished_is_not_reaped(store, tmp_path):
    """The regression the whole design turns on.

    Content-addressed storage short-circuits a re-fetch of unchanged bytes, but
    a fresh ``blob_available`` is published for it either way. If the sweep read
    the *first* store's mtime, the blob announced a moment ago would be inside
    the reap window immediately.
    """
    store.store(b"hello", AGED, "text/plain")
    age(blob_path(tmp_path, AGED), TTL + 1)

    store.store(b"hello", AGED, "text/plain")  # the re-fetch that re-announces it
    run(tmp_path)

    assert blob_path(tmp_path, AGED).exists()


def test_the_result_counts_what_was_reaped_and_how_much_it_freed(store, tmp_path):
    store.store(b"hello", AGED, "text/plain")
    age(blob_path(tmp_path, AGED), TTL + 1)

    result = run(tmp_path)

    assert (result.blobs_reaped, result.bytes_reclaimed) == (1, len(b"hello"))


def test_the_result_reports_what_is_left(store, tmp_path):
    """The ceiling reads this number, so it has to exclude what the sweep just freed."""
    store.store(b"hello", AGED, "text/plain")
    store.store(b"world!", FRESH, "text/plain")
    age(blob_path(tmp_path, AGED), TTL + 1)

    result = run(tmp_path)

    assert (result.blobs_remaining, result.bytes_remaining) == (1, len(b"world!"))


def test_an_in_flight_temporary_is_never_matched_as_a_blob(store, tmp_path):
    """The failure mode the ``*.bin`` glob exists to prevent.

    A writer holds ``.<sha256>.<random>.tmp`` between the write and the
    ``os.replace`` that publishes it. Reaping one makes that rename fail with
    ENOENT and dead-letters a command whose bytes were fine — so the sweep must
    not see dot-prefixed temporaries as candidates at all, whatever their age.
    """
    shard = tmp_path / "9f" / "2a"
    shard.mkdir(parents=True)
    temp = shard / f".{FRESH}.abc123.tmp"
    temp.write_bytes(b"partial")
    age(temp, TTL + 1)

    result = run(tmp_path)

    assert temp.exists()
    assert result.blobs_reaped == 0


def test_a_temporary_older_than_the_grace_is_debris_and_is_removed(store, tmp_path):
    """A temp outlives one write only if the writer was SIGKILLed mid-store."""
    shard = tmp_path / "9f" / "2a"
    shard.mkdir(parents=True)
    temp = shard / f".{FRESH}.abc123.tmp"
    temp.write_bytes(b"partial")
    age(temp, TEMP_GRACE + 1)

    result = run(tmp_path)

    assert not temp.exists()
    assert result.temps_reaped == 1


def test_an_emptied_shard_directory_does_not_accumulate(store, tmp_path):
    store.store(b"hello", AGED, "text/plain")
    age(blob_path(tmp_path, AGED), TTL + 1)

    run(tmp_path)

    assert not (tmp_path / AGED[0:2]).exists()


def test_a_shard_still_holding_a_blob_is_kept(store, tmp_path):
    store.store(b"hello", FRESH, "text/plain")

    run(tmp_path)

    assert (tmp_path / FRESH[0:2] / FRESH[2:4]).is_dir()


def test_a_shard_holding_an_in_flight_temporary_is_kept(tmp_path):
    """Removing the directory under a writer breaks its rename just as surely."""
    shard = tmp_path / "9f" / "2a"
    shard.mkdir(parents=True)
    (shard / f".{FRESH}.abc123.tmp").write_bytes(b"partial")

    run(tmp_path)

    assert shard.is_dir()


def test_the_root_itself_is_never_removed(tmp_path):
    """It is the store's construction-time invariant, not a shard."""
    run(tmp_path)

    assert tmp_path.is_dir()


def test_a_missing_root_sweeps_to_nothing(tmp_path):
    """Startup order is not the sweeper's to guarantee."""
    result = run(tmp_path / "not-yet")

    assert (result.blobs_reaped, result.bytes_remaining) == (0, 0)


def test_a_blob_that_vanishes_mid_sweep_does_not_fail_the_sweep(store, tmp_path, monkeypatch):
    """Another worker on the same tree can reap the same expired blob first."""
    store.store(b"hello", AGED, "text/plain")
    age(blob_path(tmp_path, AGED), TTL + 1)
    real_unlink = Path.unlink

    def racing_unlink(self, *args, **kwargs):
        real_unlink(self, *args, **kwargs)
        real_unlink(self, *args, **kwargs)  # the second raises, as the loser would

    monkeypatch.setattr(Path, "unlink", racing_unlink)

    result = run(tmp_path)

    assert result.blobs_reaped == 0


def test_usage_is_over_the_ceiling_once_the_sweep_says_so():
    usage = BlobUsage()

    usage.observe(1_000)

    assert usage.is_over(999) is True


def test_usage_starts_under_any_ceiling():
    """Nothing is known before the first sweep, and fetching must not be blocked by that."""
    assert BlobUsage().is_over(1) is False


def test_usage_grows_as_blobs_are_stored_between_sweeps():
    """A burst can cross the ceiling long before the next sweep re-measures."""
    usage = BlobUsage()
    usage.observe(500)

    usage.add(600)

    assert usage.is_over(1_000) is True


def test_a_sweep_re_measures_rather_than_accumulating():
    """``observe`` is authoritative — the increments between sweeps are the estimate."""
    usage = BlobUsage()
    usage.add(10_000)

    usage.observe(42)

    assert usage.total_bytes == 42


def test_a_blob_that_cannot_be_reaped_is_named_rather_than_skipped(
    store, tmp_path, monkeypatch, caplog
):
    """A tree this process cannot reap grows until the ceiling stops the byte path.

    That is a survivable degradation, but only if the journal says which file
    and which errno — otherwise the visible symptom is fetching pausing for no
    stated reason a week later.
    """
    store.store(b"hello", AGED, "text/plain")
    age(blob_path(tmp_path, AGED), TTL + 1)

    def denied(self, *args, **kwargs):
        raise PermissionError(1, "Operation not permitted", str(self))

    monkeypatch.setattr(Path, "unlink", denied)

    with caplog.at_level("WARNING", logger="src.storage.sweeper"):
        result = run(tmp_path)

    assert result.blobs_reaped == 0
    assert "could not reap" in caplog.text


def test_a_stray_file_shaped_like_a_shard_is_left_alone(tmp_path):
    """``??`` matches any two-character name, and not all of them are directories."""
    stray = tmp_path / "ab"
    stray.write_bytes(b"not a shard")

    result = run(tmp_path)

    assert stray.exists()
    assert result.shards_removed == 0
