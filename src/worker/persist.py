"""The ``content.persist`` handler: keep a blob's bytes in the permanent store (#114 step 6).

The digest is the address (cannobserv#493), so there is no destination to guard
and no URL to report. What the handler must prove is that the command names
bytes this host holds under that digest, and then put them in the permanent
store under the same one:

1. **T3a on the source**, the replicate guard reused: ``blob_uri`` must be one a
   store here minted — the temp store, or the permanent store itself — matched
   exactly, never parsed into a path.
2. **The two digests agree.** The URI's fingerprint must be
   ``content_fingerprint``; a disagreement, or a malformed digest, is
   ``invalid_source``, and the consumer decides that rather than the issuer.
   Decided *before* the guard asks whether the blob exists, so a gone blob
   cannot mask it as ``blob_expired`` (#114 CR 1).
3. **Read, then create if absent**, touch off. The permanent store hashes the
   bytes it is about to write (cannobserv#492), so a corrupted temp blob is
   refused before it can reach the tier nothing may delete from.
4. **Then the fact.** Store, then publish: a ``blob_persisted`` naming absent
   bytes would be unrepairable by the issuer.

A second persist of the same bytes is a no-op success that re-emits the fact,
and so is a persist whose ``blob_uri`` already names the permanent store, or
names a temp blob that has expired since an earlier persist kept its digest. The
address is the digest, so an object already there holds these bytes by
construction, and nothing like ``destination_conflict`` exists here.

**No domain echo**, by co-core's contract: a persisted blob belongs to every
revision whose bytes hash to it, so the issuer correlates on ``command_id``.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable

from co_core.pure.models.changes import ContentPersistCommand
from co_core.pure.util.blobstore import BlobStore, FingerprintMismatch

from src.core.errors import (
    PermanentPersistError,
    PermanentReplicateError,
    PersistReason,
    TransientPersistError,
    is_terminal_provider_status,
)
from src.core.logging import get_logger
from src.worker.replicate import LocatedBlob, fingerprint_in, locate_blob

logger = get_logger(__name__)

# The handler seam the loop dispatches to.
type PersistHandler = Callable[[ContentPersistCommand], Awaitable[None]]

# The success-fact seam: the command and the stored size. The handler publishes
# its own success, as the other two command handlers do; the loop sees failures.
type PersistedPublisher = Callable[[ContentPersistCommand, int], Awaitable[None]]

# Defects on this side of the seam: left unclassified, so the delivery ceiling
# turns them into a ``handler_error`` fact rather than an endless retry (#114 CR 7).
# ``FingerprintMismatch`` is a ``ValueError`` and is caught before this.
_DEFECTS = (ValueError, TypeError, AttributeError, LookupError)

# The guard's two refusals, in this stream's vocabulary. ``locate_blob`` is
# replicate's and raises replicate's leaf; the tokens are the same wire strings.
_FROM_LOCATE = {
    "invalid_source": PersistReason.INVALID_SOURCE,
    "blob_expired": PersistReason.BLOB_EXPIRED,
}


def build_persist_handler(
    *,
    store: BlobStore,
    permanent: BlobStore,
    complete: PersistedPublisher,
) -> PersistHandler:
    """Wire the persist byte path: T3a, the digest check, the copy, the fact.

    ``store`` is the temp store. ``permanent`` is where bytes are kept, built with
    touch off; it is also a source, for a ``blob_uri`` that already names it.
    """

    async def already_kept(command: ContentPersistCommand) -> bool:
        # Its own classification: raised inside the refusal's handler, a failure
        # here would otherwise escape the one below it unclassified.
        try:
            return await asyncio.to_thread(permanent.exists, command.content_fingerprint)
        except _DEFECTS:
            raise
        except Exception as exc:
            raise _classify(exc, "the permanent store could not be checked") from exc

    async def handle(command: ContentPersistCommand) -> None:
        started = time.monotonic()
        # Before existence, not after (#114 CR 1): a gone blob would otherwise
        # answer first with `blob_expired`, whose remedy — a re-fetch — cannot fix
        # a command whose two digests disagree. `None` (not a blob URI at all) is
        # left to the guard, which refuses it `invalid_source` in the same words.
        named = fingerprint_in(command.blob_uri)
        if named is not None and named != command.content_fingerprint:
            raise PermanentPersistError(
                "blob_uri names a different digest from content_fingerprint",
                reason=PersistReason.INVALID_SOURCE,
            )
        # Off the loop thread: `exists` is a network round trip on the object store.
        try:
            source = await asyncio.to_thread(
                locate_blob, command.blob_uri, store=store, permanent=(permanent,)
            )
        except PermanentReplicateError as exc:
            reason = _FROM_LOCATE[exc.reason]
            if reason is not PersistReason.BLOB_EXPIRED or not await already_kept(command):
                raise PermanentPersistError(str(exc), reason=reason) from exc
            # Gone from where the command pointed, but already kept (#114 CR 2):
            # the permanent store is the authority on the outcome this command
            # asks for, and `blob_expired` would tell the issuer its bytes are lost.
            source = LocatedBlob(command.content_fingerprint, permanent)
        except _DEFECTS:
            raise
        except Exception as exc:
            raise _classify(exc, "the source could not be located") from exc

        try:
            data = await asyncio.to_thread(source.store.open, source.fingerprint)
        except FileNotFoundError as exc:
            # Swept between the guard's existence check and this read.
            raise PermanentPersistError(
                "the blob for this command was swept before it could be read",
                reason=PersistReason.BLOB_EXPIRED,
            ) from exc
        except OSError as exc:
            raise TransientPersistError(f"the blob could not be read: {exc}") from exc
        except _DEFECTS:
            raise
        except Exception as exc:
            raise _classify(exc, "the blob could not be read") from exc

        try:
            await asyncio.to_thread(permanent.store, data, source.fingerprint, command.media_type)
        except FingerprintMismatch as exc:
            raise PermanentPersistError(
                f"the stored bytes do not hash to their fingerprint: {exc}",
                reason=PersistReason.SOURCE_CORRUPT,
            ) from exc
        except _DEFECTS:
            raise
        except Exception as exc:
            if is_terminal_provider_status(exc):
                raise PermanentPersistError(
                    f"the permanent store refused the write ({getattr(exc, 'code', None)}): {exc}",
                    reason=PersistReason.STORE_REFUSED,
                ) from exc
            raise TransientPersistError(
                f"the permanent store write failed: {type(exc).__name__}: {exc}"
            ) from exc

        # After the object exists. A failed publish re-raises: the command stays
        # pending and the redelivery re-runs a no-op, so the fact gets another
        # chance rather than the command closing silently.
        await complete(command, len(data))
        logger.info(
            "persisted a blob",
            extra={
                "command_id": command.command_id,
                "content_fingerprint": source.fingerprint,
                "size_bytes": len(data),
                "duration_ms": round((time.monotonic() - started) * 1000, 1),
            },
        )

    return handle


def _classify(exc: Exception, what: str) -> Exception:
    """A failure reaching the source: transient unless the status is terminal.

    Only the transient half is claimed, as for replicate's source read: a terminal
    status is re-raised unclassified for the delivery ceiling to close, because no
    persist token describes "this worker cannot read its own temp store".
    """
    if is_terminal_provider_status(exc):
        return exc
    return TransientPersistError(f"{what}: {type(exc).__name__}: {exc}")
