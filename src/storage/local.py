"""Local-filesystem blob backend."""

import errno
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


def ensure_directory(path: Path) -> None:
    """Create ``path`` and every missing level above it, owning each one's mode.

    The mode is set with ``chmod`` rather than ``mkdir(mode=...)`` because the
    umask masks the latter: under ``umask 0077`` a fresh directory comes out
    0700, and a 0644 blob inside a 0700 directory is unreachable — traversal
    needs ``+x`` on every parent. That is why the levels are created one at a
    time rather than by ``mkdir(parents=True)``: the latter creates the
    intermediates too but gives no chance to set their modes, so a nested root
    would end up reachable at the leaf and blocked two levels up.

    A directory that **already exists** is left exactly as it is, at any level.
    It belongs to whoever provisioned it — an operator's deliberate 0750, or a
    shared mount this process can write into but does not own, where ``chmod``
    raises ``EPERM`` even though the write would have succeeded.
    """
    if path.exists() and not path.is_dir():
        # Caught here rather than left to mkdir, which reports a plain
        # FileExistsError for this: the operator debugging a bad
        # REPLICATOR_BLOB_DIR should see ENOTDIR in the journal, not EEXIST.
        raise NotADirectoryError(errno.ENOTDIR, os.strerror(errno.ENOTDIR), str(path))
    for level in _missing_levels(path):
        try:
            level.mkdir()
        except FileExistsError:
            continue  # a concurrent worker got there first; the mode is theirs
        os.chmod(level, DIR_MODE)


def _missing_levels(path: Path) -> list[Path]:
    """The directories that have to be created, outermost first.

    The walk terminates on the first ancestor that exists, which is guaranteed
    to happen — ``/`` (or ``.`` for a relative path) always does. The
    ``parent != current`` half of the condition is belt-and-braces against a
    filesystem where that stops being true: an infinite loop here would hang the
    worker mid-message, which is a far worse failure than one redundant check.
    """
    missing: list[Path] = []
    current = path
    while not current.exists() and current.parent != current:
        missing.append(current)
        current = current.parent
    return list(reversed(missing))


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
        """Create the shard directories a blob is about to land in.

        Level by level rather than one ``mkdir(parents=True)``, because the mode
        is only ours to set on the levels we actually create — see
        ``ensure_directory``. The root is included since a store may be the first
        thing to touch it, but it is skipped when it already exists.
        """
        ensure_directory(self._root)
        ensure_directory(leaf.parent)
        ensure_directory(leaf)

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
