"""Local-filesystem blob backend."""

import os
import tempfile
from pathlib import Path

# A sibling service reads these bytes by the ``file://`` URI announced on
# ``blob_available``, so both the file and every directory above it have to be
# reachable by a reader that is not this process. Two defaults work against that
# and neither is visible at the call site: ``mkstemp`` creates at 0600, and
# ``mkdir``'s mode argument is masked by the umask. Both are therefore set with
# an explicit ``chmod``, which the umask does not touch.
#
# Widened deliberately: these are fetched public bytes. Nothing secret is ever
# stored here.
BLOB_MODE = 0o644
DIR_MODE = 0o755


class LocalBlobStore:
    """Content-addressed blob storage under a filesystem root."""

    def __init__(self, root: Path) -> None:
        # Resolved once, at construction: a ``file://`` URI must be absolute, and
        # ``REPLICATOR_BLOB_DIR`` defaults to the relative ``blobs``. Binding the
        # root to the working directory here rather than per-store also means a
        # later chdir cannot silently move where blobs land.
        self._root = Path(root).resolve()

    def store(self, data: bytes, fingerprint: str, media_type: str) -> str:
        """Write ``data`` at the path its ``fingerprint`` addresses."""
        path = self._path_for(fingerprint)
        # Content-addressed, so identical bytes are already the right bytes. The
        # consume path relies on this: the command dedupe key is written *after*
        # the handler returns, making a re-run of an already-successful handler
        # an expected outcome rather than an error.
        if path.is_file():
            return path.as_uri()
        self._ensure_shard(path.parent)
        self._write_atomically(path, data)
        # as_uri() rather than an f-string: it percent-encodes, so a blob dir
        # containing a space yields a URI a consumer can actually parse. It also
        # raises on a relative path, which makes __init__'s resolve() enforced
        # rather than merely conventional.
        return path.as_uri()

    def _ensure_shard(self, leaf: Path) -> None:
        """Create the shard directories, readable by whoever reads the blobs.

        The mode is applied with ``chmod`` rather than ``mkdir(mode=...)``
        because the umask masks the latter: under ``umask 0077`` the shards come
        out 0700, and a 0644 blob inside a 0700 directory is unreachable —
        traversal needs ``+x`` on every parent. Re-applied on each store, so a
        directory someone tightened by hand heals on the next write.
        """
        leaf.mkdir(parents=True, exist_ok=True)
        # The layout is exactly two levels deep (see _path_for), so this is the
        # full chain from the root down.
        for directory in (self._root, leaf.parent, leaf):
            os.chmod(directory, DIR_MODE)

    @staticmethod
    def _write_atomically(path: Path, data: bytes) -> None:
        """Publish ``data`` at ``path`` in one indivisible step.

        Presence at a content-addressed path is what readers — and ``store``'s
        own short-circuit — take as proof the bytes are complete, so a partially
        written file must never be reachable there. Writing to a sibling
        temporary and renaming makes the appearance atomic; the temp lives in the
        same directory so the rename stays within one filesystem, which is what
        ``os.replace`` needs to be atomic at all.

        The ``fsync`` before the rename is what extends that guarantee past a
        crashed *process* to a crashed *machine*: without it ext4 can expose the
        renamed name with unflushed contents after power loss, leaving a
        zero-length file whose name asserts the sha256 of real bytes. A consumer
        trusting the path over re-hashing would read silent corruption.

        The parent directory is deliberately **not** synced, so power loss can
        still lose the rename itself. That failure is the safe one: the blob is
        absent rather than wrong, the short-circuit misses, and the reclaim
        re-runs a handler that content-addressed storage makes a no-op. Only
        losing the *contents* behind a name that claims them was worth the sync.

        The temp is removed on any failure, otherwise a command retried against a
        full disk would leave one behind per attempt.
        """
        fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temp_name, BLOB_MODE)  # see BLOB_MODE — mkstemp creates at 0600
            os.replace(temp_name, path)
        except BaseException:
            Path(temp_name).unlink(missing_ok=True)
            raise

    def exists(self, fingerprint: str) -> bool:
        """Whether these bytes are already stored."""
        return self._path_for(fingerprint).is_file()

    def open(self, fingerprint: str) -> bytes:
        """Read back the bytes stored under ``fingerprint``."""
        return self._path_for(fingerprint).read_bytes()

    def _path_for(self, fingerprint: str) -> Path:
        """Shard two levels deep by the fingerprint's first four hex characters.

        A flat directory degrades past roughly ten thousand entries on ext4
        without ``dir_index`` tuning; two levels of two hex characters gives
        65,536 leaves from a uniformly distributed hash. Purely a local-backend
        detail — an object store drops the sharding and the loop is untouched.
        """
        return self._root / fingerprint[0:2] / fingerprint[2:4] / f"{fingerprint}.bin"
