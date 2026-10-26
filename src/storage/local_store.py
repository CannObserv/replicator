"""The local store as the worker builds it: the shared one, made safe to sweep under (#159).

co-core's ``LocalBlobStore`` finds or makes the shard levels, then ``mkstemp``s
its temporary into them, and the retention sweep ``rmdir``s empty shards from
another thread. The sweep leaves a shard younger than its grace
(``src.storage.sweeper``), which covers one the store has just made. It cannot
cover an *old* empty one the store has just found: that can go between the
store's look and its next step, failing the write with ``FileNotFoundError``.

So the write is tried once more on exactly that error: a ``FileNotFoundError``
naming a path under the root. A missing ``store_file`` source is not the
sweep's doing and is raised untouched. One retry is enough: the second attempt
makes every level it needs afresh — and the ``rmdir`` that lost the race
touched the level above — so all of them are inside the grace. A second
``FileNotFoundError`` is something else, and is raised.

Here rather than in ``in_store``, which takes an awaitable already running and
serves the object store too, where there are no directories to lose. Fetch and
capture both write through this class.
"""

from collections.abc import Callable
from functools import partial
from pathlib import Path, PurePath

from co_core_sync.drivers.blobstore import LocalBlobStore

from src.core.logging import get_logger

logger = get_logger(__name__)


class SweepSafeLocalBlobStore(LocalBlobStore):
    """``LocalBlobStore`` whose writes survive a sweep removing their shard once."""

    def store(self, data: bytes, fingerprint: str, media_type: str) -> str:
        return self._once_more(partial(super().store, data, fingerprint, media_type), fingerprint)

    def store_file(self, path: PurePath, fingerprint: str, media_type: str) -> str:
        return self._once_more(
            partial(super().store_file, path, fingerprint, media_type), fingerprint
        )

    def _once_more(self, write: Callable[[], str], fingerprint: str) -> str:
        """Run ``write``, and again if a shard vanished under it."""
        try:
            return write()
        except FileNotFoundError as exc:
            if exc.filename is None or not Path(exc.filename).is_relative_to(self.root):
                raise
            # Said, because it is rare and heals itself — a count that rises means
            # a sweep is removing shards writers are still using.
            logger.info(
                "a sweep removed the shard mid-store — storing again",
                extra={"content_fingerprint": fingerprint, "error": f"{type(exc).__name__}: {exc}"},
            )
        return write()
