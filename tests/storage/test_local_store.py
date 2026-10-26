"""The local store the worker builds: one more try when a sweep took its shard (#159)."""

import hashlib
import logging
from pathlib import Path

import pytest
from co_core_sync.drivers.blobstore import LocalBlobStore

from src.storage.local_store import SweepSafeLocalBlobStore

DATA = b"bytes"
FINGERPRINT = hashlib.sha256(DATA).hexdigest()


def write(store, method: str, tmp_path: Path) -> str:
    """Store ``DATA`` through either write method, the copy one from a source file."""
    if method == "store":
        return store.store(DATA, FINGERPRINT, "text/plain")
    source = tmp_path / "source"
    source.write_bytes(DATA)
    return store.store_file(source, FINGERPRINT, "text/plain")


def lost_shard(tmp_path: Path) -> FileNotFoundError:
    """What the store raises when a sweep took its shard: a path under the root."""
    return FileNotFoundError(2, "No such file or directory", str(tmp_path / "blobs" / "ab" / "cd"))


def failing_first(monkeypatch, *errors: BaseException) -> list:
    """Make the shared store's write raise ``errors`` in turn, then succeed."""
    real = LocalBlobStore._publish
    calls: list = []

    def publish(self, fingerprint, write_bytes):
        calls.append(fingerprint)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]
        return real(self, fingerprint, write_bytes)

    monkeypatch.setattr(LocalBlobStore, "_publish", publish)
    return calls


@pytest.mark.parametrize("method", ["store", "store_file"])
def test_a_lost_shard_is_stored_again_once(monkeypatch, tmp_path, method):
    calls = failing_first(monkeypatch, lost_shard(tmp_path))
    store = SweepSafeLocalBlobStore(tmp_path / "blobs")

    uri = write(store, method, tmp_path)

    assert len(calls) == 2
    assert store.open(FINGERPRINT) == DATA
    assert uri == store.uri_for(FINGERPRINT)


@pytest.mark.parametrize("method", ["store", "store_file"])
def test_a_shard_lost_twice_is_the_callers_failure(monkeypatch, tmp_path, method):
    """One retry, not a loop: after a lost race every level is fresh, so a second loss is
    something else — and ``in_store`` classifies it as it would have anyway."""
    calls = failing_first(monkeypatch, lost_shard(tmp_path), lost_shard(tmp_path))

    with pytest.raises(FileNotFoundError):
        write(SweepSafeLocalBlobStore(tmp_path / "blobs"), method, tmp_path)

    assert len(calls) == 2


def test_any_other_failure_is_not_retried(monkeypatch, tmp_path):
    calls = failing_first(monkeypatch, PermissionError(13, "Permission denied", "ab"))

    with pytest.raises(PermissionError):
        SweepSafeLocalBlobStore(tmp_path / "blobs").store(DATA, FINGERPRINT, "text/plain")

    assert len(calls) == 1


def test_a_missing_source_file_is_not_taken_for_a_lost_shard(tmp_path):
    """Only a path under the root is the sweep's doing; anything else is raised as it was."""
    calls = []
    real = LocalBlobStore.store_file

    def counting(self, *args, **kwargs):
        calls.append(args)
        return real(self, *args, **kwargs)

    store = SweepSafeLocalBlobStore(tmp_path / "blobs")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(LocalBlobStore, "store_file", counting)
        with pytest.raises(FileNotFoundError):
            store.store_file(tmp_path / "absent", FINGERPRINT, "text/plain")

    assert len(calls) == 1


def test_the_retry_is_said_once(monkeypatch, tmp_path, caplog):
    """Rare and self-healing, but a rising count would mean a sweep is too eager."""
    failing_first(monkeypatch, lost_shard(tmp_path))

    with caplog.at_level(logging.INFO, logger="src.storage.local_store"):
        SweepSafeLocalBlobStore(tmp_path / "blobs").store(DATA, FINGERPRINT, "text/plain")

    (record,) = caplog.records
    assert record.content_fingerprint == FINGERPRINT
    assert "FileNotFoundError" in record.error
