"""Failure vocabulary for the consume path.

The loop — not the handler — decides retry vs dead-letter, but the handler is
the only thing that knows *why* it failed. These two types let it say so
directly, so classification does not rest on the loop recognizing every
exception type the byte path might raise (see the transient tuple in
``src.worker.loop``, which is the fallback for what it did not anticipate).

Since #9 the vocabulary has a second dimension. *Retry-or-not* is the exception
type; *why* is the ``reason``, which the loop copies onto the ``fetch_failed``
fact so an issuer can close a pending command with a cause instead of a timeout.
The distinction matters because three unrelated permanent conditions — a 404, an
unfetchable scheme, an oversized body — all raise one exception type, and by the
time ``process_message`` catches it the only thing separating them would be the
message string. Naming the reason at the raise site is what keeps the loop from
having to parse it back out.
"""

from enum import StrEnum


class FailureReason(StrEnum):
    """The ``reason`` token on a ``fetch_failed`` fact.

    A **wire contract** with every ``content.blobs`` consumer, not a local label:
    co-core deliberately types ``FetchFailedEvent.reason`` as a plain ``str``
    rather than a ``Literal`` so a producer adding a token cannot crash an older
    ``extra="ignore"`` consumer — which puts the whole of the compatibility
    burden on this end. Adding a member is additive and safe; renaming one
    changes what Watcher branches on.

    ``StrEnum`` so ``model_dump_json`` writes the token itself. Mirrors the
    taxonomy in ``docs/contracts/content-fetch-issuer-reference.md``; the two are
    edited together.
    """

    HTTP_STATUS = "http_status"
    """A non-2xx the origin meant: 4xx, or a body-less 304."""

    NOT_FETCHABLE = "not_fetchable"
    """Bad scheme or invalid URL — not a URL, and it will not become one."""

    TOO_LARGE = "too_large"
    """Body over ``REPLICATOR_MAX_BLOB_BYTES``."""

    INVALID_REQUEST_OPTIONS = "invalid_request_options"
    """The command's ``headers`` or ``timeout_seconds`` are not sendable (#11).

    One token for both fields deliberately: the issuer's remedy is identical
    either way — fix the command and re-issue it under a fresh ``command_id`` —
    and ``detail`` carries which guard refused it for the journal.
    """

    UNSUPPORTED_SCHEMA_VERSION = "unsupported_schema_version"
    """The command decoded, but at a ``schema_version`` this worker does not know."""

    HANDLER_ERROR = "handler_error"
    """Unclassified, and it exhausted the delivery ceiling."""

    # Deliberately absent: ``wrong_payload_type``. co-core's FetchFailedEvent
    # docstring lists it, but Replicator cannot emit it correctly (CR #1). A
    # frame that decoded to a non-command payload carries, at most, somebody
    # else's command_id — BlobAvailableEvent's names a command that *succeeded*,
    # which is why a blob exists for it. Announcing a terminal failure against
    # that id would tell an issuer its good bytes are never coming: a wrong
    # correlation, applied silently, which is the one failure shape the contract
    # spends three MUSTs preventing. The frame dead-letters and stays silent.


class HandlerError(RuntimeError):
    """Base for failures a ``content.fetch`` handler reports deliberately."""


class TransientFetchError(HandlerError):
    """The work may succeed later: leave the message unacked for redelivery.

    Exempt from the delivery ceiling — a long-but-genuine outage must never
    silently drop a valid command.

    Carries no ``reason``: a transient failure closes nothing, and Replicator
    emits no non-terminal fact today (#9 §3, deferred — the cost is that a 429
    retrying at the reclaim cadence stays invisible for as long as it retries).
    """


class PermanentFetchError(HandlerError):
    """The work will never succeed for this command: dead-letter it now.

    Retrying a deterministically bad command only burns the ceiling and delays
    the operator seeing it in ``<topic>.dlq``.

    ``reason`` is required. It is what the ``fetch_failed`` fact reports, and a
    default would quietly relabel a specific failure as a generic one on the
    wire — the sort of drift no test notices and every consumer inherits.
    ``status_code`` is set only where there was one (``HTTP_STATUS``).
    """

    def __init__(
        self, message: str, *, reason: FailureReason, status_code: int | None = None
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code
