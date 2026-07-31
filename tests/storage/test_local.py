"""The local-filesystem blob backend."""

import errno
import os
import stat
from pathlib import Path
from urllib.parse import unquote, urlparse

import pytest

from src.storage.local import LocalBlobStore, ensure_directory

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


def test_the_shard_directories_are_traversable_under_a_restrictive_umask(tmp_path):
    """A 0644 blob inside a 0700 directory is unreachable — traversal needs +x.

    ``mkdir``'s mode is masked by the umask, so a readable file alone does not
    make the cross-service read work. Run under ``umask 0077`` because the
    permissive default hides exactly this.
    """
    previous = os.umask(0o077)
    try:
        root = tmp_path / "blobs"
        LocalBlobStore(root).store(b"hello", FINGERPRINT, "text/plain")

        for directory in (root, root / "9f", root / "9f" / "2a"):
            assert stat.S_IMODE(directory.stat().st_mode) == 0o755, directory
    finally:
        os.umask(previous)


def test_a_root_needing_encoding_yields_a_parseable_uri(tmp_path):
    """``blob_uri`` crosses a service boundary as a URI, not as a path.

    An f-string emits the space verbatim, so a consumer parsing the value gets a
    different path than the one written — or nothing.
    """
    root = tmp_path / "my blobs"

    uri = LocalBlobStore(root).store(b"hello", FINGERPRINT, "text/plain")

    assert uri == f"file://{tmp_path}/my%20blobs/9f/2a/{FINGERPRINT}.bin"
    assert Path(unquote(urlparse(uri).path)).read_bytes() == b"hello"


def test_a_preexisting_root_keeps_the_mode_its_operator_gave_it(tmp_path):
    """Only directories the store creates are the store's to set modes on.

    A root that already exists belongs to whoever provisioned it — and may be a
    shared mount this process can write into but does not own, where ``chmod``
    raises ``EPERM`` even though the write itself would have worked.
    """
    root = tmp_path / "blobs"
    root.mkdir(mode=0o700)

    LocalBlobStore(root).store(b"hello", FINGERPRINT, "text/plain")

    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / "9f").stat().st_mode) == 0o755


def test_a_root_that_is_a_regular_file_is_rejected(tmp_path):
    """``mkdir`` reports a plain FileExistsError for a file as well as a directory.

    Swallowing that would let REPLICATOR_BLOB_DIR point at a regular file, pass
    the worker's startup check, and fail at the first fetch instead of on boot.
    ENOTDIR rather than EEXIST because that is what the journal should say.
    """
    blocker = tmp_path / "blobs"
    blocker.write_bytes(b"")

    with pytest.raises(NotADirectoryError) as caught:
        ensure_directory(blocker)

    assert caught.value.errno == errno.ENOTDIR


def test_every_level_a_nested_root_creates_is_traversable(tmp_path):
    """``mkdir(parents=True)`` would create the intermediates and own none of them.

    A leaf at 0755 under parents at 0700 is reachable in principle and blocked
    in practice — the same unreadable-blob outcome, one level up.
    """
    previous = os.umask(0o077)
    try:
        root = tmp_path / "state" / "replicator" / "blobs"

        ensure_directory(root)

        for level in (root.parent.parent, root.parent, root):
            assert stat.S_IMODE(level.stat().st_mode) == 0o755, level
    finally:
        os.umask(previous)


def test_a_level_another_worker_created_first_keeps_its_mode(tmp_path, monkeypatch):
    """Two workers can race to create the same shard; the loser must not chmod.

    Whoever won owns that directory's mode, and re-applying ours would be the
    same overreach as widening a pre-existing root.
    """
    root = tmp_path / "blobs"
    real_mkdir = Path.mkdir

    def racing_mkdir(self, *args, **kwargs):
        real_mkdir(self, *args, **kwargs)
        os.chmod(self, 0o700)  # stand in for the other worker's mode
        raise FileExistsError(errno.EEXIST, "File exists", str(self))

    monkeypatch.setattr(Path, "mkdir", racing_mkdir)

    ensure_directory(root)

    assert stat.S_IMODE(root.stat().st_mode) == 0o700
