"""The local-filesystem blob backend."""

import stat
from pathlib import Path

import pytest

from src.storage.local import LocalBlobStore

# A stand-in fingerprint: the real one is a 64-char sha256, and the shard split
# takes the first four characters, so the literal has to be long enough to slice.
FINGERPRINT = "9f2a7c1e" + "0" * 56


@pytest.fixture
def store(tmp_path):
    """A store rooted at a fresh temp directory."""
    return LocalBlobStore(tmp_path)


def test_store_writes_the_bytes_to_a_two_level_sharded_path(store, tmp_path):
    store.store(b"hello", FINGERPRINT, "text/plain")

    blob = tmp_path / "9f" / "2a" / f"{FINGERPRINT}.bin"
    assert blob.read_bytes() == b"hello"


def test_store_returns_a_file_uri_for_the_blob(store, tmp_path):
    uri = store.store(b"hello", FINGERPRINT, "text/plain")

    assert uri == f"file://{tmp_path}/9f/2a/{FINGERPRINT}.bin"


def test_a_relative_root_still_yields_an_absolute_uri(tmp_path, monkeypatch):
    """``file://`` requires an absolute path, and REPLICATOR_BLOB_DIR defaults to ``blobs``."""
    monkeypatch.chdir(tmp_path)

    uri = LocalBlobStore(Path("blobs")).store(b"hello", FINGERPRINT, "text/plain")

    assert uri.startswith(f"file://{tmp_path}/blobs/")


def test_exists_is_false_before_the_blob_is_stored(store):
    assert store.exists(FINGERPRINT) is False


def test_exists_is_true_after_the_blob_is_stored(store):
    store.store(b"hello", FINGERPRINT, "text/plain")

    assert store.exists(FINGERPRINT) is True


def test_open_returns_the_stored_bytes(store):
    store.store(b"hello", FINGERPRINT, "text/plain")

    assert store.open(FINGERPRINT) == b"hello"


def test_storing_an_already_stored_fingerprint_does_not_rewrite_it(store, tmp_path):
    """The short-circuit the consume loop's at-least-once delivery leans on.

    Proven by writing different bytes to the addressed path behind the store's
    back: if the second ``store`` re-wrote, those bytes would be replaced.
    """
    store.store(b"hello", FINGERPRINT, "text/plain")
    blob = tmp_path / "9f" / "2a" / f"{FINGERPRINT}.bin"
    blob.write_bytes(b"sentinel")

    store.store(b"hello", FINGERPRINT, "text/plain")

    assert blob.read_bytes() == b"sentinel"


def test_storing_an_already_stored_fingerprint_still_returns_its_uri(store):
    first = store.store(b"hello", FINGERPRINT, "text/plain")

    assert store.store(b"hello", FINGERPRINT, "text/plain") == first


def test_a_write_that_fails_to_publish_leaves_nothing_at_the_addressed_path(store, monkeypatch):
    """Presence at a content-addressed path has to mean "complete".

    Readers — and ``store``'s own short-circuit — treat the file existing as
    proof the bytes are there, so a partial write must never be reachable at
    that path. The publish step is faked as failing to stand in for the crash.
    """

    def failing_replace(*args, **kwargs):
        raise OSError("no space left on device")

    monkeypatch.setattr("src.storage.local.os.replace", failing_replace)

    with pytest.raises(OSError):
        store.store(b"hello", FINGERPRINT, "text/plain")

    assert not store.exists(FINGERPRINT)


def test_a_failed_write_leaves_no_temporary_file_behind(store, tmp_path, monkeypatch):
    """A retried command must not accrete a temp file per attempt."""

    def failing_replace(*args, **kwargs):
        raise OSError("no space left on device")

    monkeypatch.setattr("src.storage.local.os.replace", failing_replace)

    with pytest.raises(OSError):
        store.store(b"hello", FINGERPRINT, "text/plain")

    assert list((tmp_path / "9f" / "2a").iterdir()) == []


def test_a_stored_blob_is_readable_by_another_service(store, tmp_path):
    """The ``file://`` URI is only useful if the reader can open it.

    ``mkstemp`` creates at 0600, so without an explicit widening the cross-service
    contract would hold only while every cluster unit runs as the same user —
    true today, and nothing in the code would say so.
    """
    store.store(b"hello", FINGERPRINT, "text/plain")

    blob = tmp_path / "9f" / "2a" / f"{FINGERPRINT}.bin"
    assert stat.S_IMODE(blob.stat().st_mode) == 0o644
