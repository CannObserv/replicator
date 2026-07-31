"""Local-filesystem blob backend."""

import os
import tempfile
from pathlib import Path


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
            return f"file://{path}"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._write_atomically(path, data)
        return f"file://{path}"

    @staticmethod
    def _write_atomically(path: Path, data: bytes) -> None:
        """Publish ``data`` at ``path`` in one indivisible step.

        Presence at a content-addressed path is what readers — and ``store``'s
        own short-circuit — take as proof the bytes are complete, so a partially
        written file must never be reachable there. Writing to a sibling
        temporary and renaming makes the appearance atomic; the temp lives in the
        same directory so the rename stays within one filesystem, which is what
        ``os.replace`` needs to be atomic at all.

        The temp is removed on any failure, otherwise a command retried against a
        full disk would leave one behind per attempt.
        """
        fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.stem}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
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
