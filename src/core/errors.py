"""Failure vocabulary for the consume path.

The loop — not the handler — decides retry vs dead-letter, but the handler is
the only thing that knows *why* it failed. These two types let it say so
directly, so classification does not rest on the loop recognizing every
exception type the byte path might raise (see the transient tuple in
``src.worker.loop``, which is the fallback for what it did not anticipate).

Since #9 the vocabulary has a second dimension. *Retry-or-not* is the exception
type; *why* is the ``reason``, which the loop copies onto the stream's failure
fact so an issuer can close a pending command with a cause instead of a timeout.
The distinction matters because three unrelated permanent conditions — a 404, an
unfetchable scheme, an oversized body — all raise one exception type, and by the
time ``process_message`` catches it the only thing separating them would be the
message string. Naming the reason at the raise site is what keeps the loop from
having to parse it back out.

**Two layers since #29.** ``TransientError`` / ``PermanentError`` are what the
loop catches, so a second command stream classifies its own failures without the
retry-or-not decision learning it exists; the ``*FetchError`` leaves are what the
byte path raises, so a raise site still says which handler is speaking.

**Three fates since #17.** Retry, dead-letter, and *complete without bytes* —
``CompletedWithoutBlobError``, which publishes a fact and acks and reaches no
DLQ. It is a sibling of the other two rather than a leaf of either, because the
hierarchy is what selects the fate: subclassing ``PermanentError`` would put it
straight back on the dead-letter path.
"""

from enum import StrEnum

# HTTP statuses that mean "come back later" rather than "this is wrong" (CR #27).
# Everything else in 4xx closes the command; everything in 5xx, and everything
# with no status at all, leaves it open.
_RETRYABLE_STATUSES = frozenset({408, 429})


def is_terminal_provider_status(exc: Exception) -> bool:
    """Whether a provider failure will fail the same way on every retry (CR #27).

    Shared by the replicate *write* path and the object-store *storage* path,
    which classify the same provider's failures into different error families —
    so the decision lives here and each caller keeps its own vocabulary. It was
    written for the write path and left there; a second caller is what makes a
    common home the right place rather than a speculative one.

    The status is read as a **number off the exception** rather than matched
    against ``google.api_core.exceptions.*``, and that is deliberate twice over:
    that package is a transitive dependency this project never declares, and both
    call sites sit behind provider-agnostic seams, so a second provider raising
    its own error type with an HTTP status is classified correctly without
    touching this function. 408 in particular has no named class in api_core at
    all.

    Anything with no status — a socket dying mid-upload — is **not** terminal.
    The generous default is what makes an unrecognized failure retry rather than
    close a command an issuer is waiting on.
    """
    status = getattr(exc, "code", None)
    return isinstance(status, int) and 400 <= status < 500 and status not in _RETRYABLE_STATUSES


class FailureReason(StrEnum):
    """Reason tokens: ``content.fetch``'s, plus the two the loop owns.

    A **wire contract** with every ``content.blobs`` consumer, not a local label:
    co-core deliberately types ``FetchFailedEvent.reason`` as a plain ``str``
    rather than a ``Literal`` so a producer adding a token cannot crash an older
    ``extra="ignore"`` consumer — which puts the whole of the compatibility
    burden on this end. Adding a member is additive and safe; renaming one
    changes what Watcher branches on.

    **Not every member belongs to fetch (#29).** ``UNSUPPORTED_SCHEMA_VERSION``
    and ``HANDLER_ERROR`` are raised by ``src.worker.loop`` rather than by the
    byte path, so they are emitted for *whatever* stream the loop is running and
    will reach ``content.artifacts`` as well once replicate lands. The rest are
    fetch's own. A second stream's refusals (``alias_unknown``,
    ``invalid_destination``, …) do **not** belong here — the vocabulary is
    producer-owned per stream, which is why ``ReportBuilder`` types ``reason`` as
    a plain ``str``.

    ``StrEnum`` so ``model_dump_json`` writes the token itself. Mirrors the
    taxonomy in ``docs/contracts/content-fetch-issuer-reference.md``; the two are
    edited together.
    """

    HTTP_STATUS = "http_status"
    """A non-2xx the origin meant, and meant as a refusal: a 4xx.

    Includes **412 Precondition Failed** — the other conditional-request status,
    and unlike a 304 it is an issuer error: a precondition that fails on a GET
    means the ``If-Match`` sent was one the origin could not satisfy. Decided
    with #17 rather than left to the first issuer using ``If-Match`` to discover.
    """

    NOT_MODIFIED = "not_modified"
    """A conditional GET that succeeded: 304, and the issuer's bytes still stand.

    **Not a failure**, and the honest cost of putting it on ``fetch_failed`` (#17,
    shape A). The event's real meaning is "this command will not produce a blob";
    ``terminal`` is the field that matters, and a consumer branching on it first —
    as the contract has always required — handles this token before it has heard
    of it. A dedicated ``content_unchanged`` fact would have cost every consumer a
    dispatch arm for an outcome structurally identical to the others here.

    The consequence, which belongs next to the token rather than only in a
    docstring: wherever conditional GET is in use this dominates the stream, so
    ``fetch_failed`` volume stops being a failure signal. ``fetch_failed where
    reason != "not_modified"`` is the one to count.
    """

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


class ReplicateReason(StrEnum):
    """The ``reason`` token on a ``replication_failed`` fact (#29).

    Separate from ``FailureReason`` because the vocabulary is **producer-owned
    per stream**, which is why ``ReportBuilder`` types ``reason`` as a plain
    ``str``: merging the two would give every fetch consumer a set of tokens it
    can never see and every replicate consumer the same, and the first token
    needed by only one of them would reopen the question anyway.

    The two the *loop* owns — ``unsupported_schema_version`` and
    ``handler_error`` — stay on ``FailureReason`` and are emitted for whichever
    stream is running, so they reach ``content.artifacts`` too. Only the
    handler's own refusals live here.

    Normative source: ``docs/contracts/content-replicate-issuer-contract.md``,
    "What Replicator refuses". co-core registers these in
    ``ReplicationFailedEvent``'s docstring; the two are edited together, and
    ``invalid_source`` is the one still awaiting its row there
    (cannobserv#330).
    """

    ALIAS_UNKNOWN = "alias_unknown"
    """The ``credentials_alias`` is not provisioned on this host (T2)."""

    PROVIDER_DISABLED = "provider_disabled"
    """The ``provider`` is not enabled here (T5).

    Separate from ``ALIAS_UNKNOWN`` because the remedies differ: fix the spec
    versus act on the host.
    """

    INVALID_DESTINATION = "invalid_destination"
    """The rendered ``destination`` escapes the alias root, or ``object_options``
    names a container the alias does not allow (T3).

    One token for both guards, ``INVALID_REQUEST_OPTIONS``' reasoning: the
    issuer's remedy is identical either way, and ``detail`` names which refused.
    """

    INVALID_SOURCE = "invalid_source"
    """``blob_uri`` is not a reference this store minted (T3a).

    Not ``BLOB_EXPIRED``: the bytes were never named, so re-fetching fixes
    nothing. Not ``INVALID_DESTINATION``: the fault is in the issuer's plumbing,
    not the RepSpec.
    """

    BLOB_EXPIRED = "blob_expired"
    """The blob is gone. Terminal — the remedy is the issuer's (MUST-7 inverted).

    Replicator has no URL on a replicate command, and issuing a fetch for itself
    would make the consumer an issuer.
    """

    DESTINATION_CONFLICT = "destination_conflict"
    """The destination already holds **different** bytes (T4).

    The one refusal that is *not* pre-credential: learning that a destination
    holds differing bytes takes an authenticated read.
    """


class HandlerError(RuntimeError):
    """Base for failures a command handler reports deliberately."""


class TransientError(HandlerError):
    """The work may succeed later: leave the message unacked for redelivery.

    Exempt from the delivery ceiling — a long-but-genuine outage must never
    silently drop a valid command.

    Carries no ``reason``: a transient failure closes nothing, and Replicator
    emits no non-terminal fact today (#9 §3, deferred — the cost is that a 429
    retrying at the reclaim cadence stays invisible for as long as it retries).
    """


class PermanentError(HandlerError):
    """The work will never succeed for this command: dead-letter it now.

    Retrying a deterministically bad command only burns the ceiling and delays
    the operator seeing it in ``<topic>.dlq``.

    ``reason`` is required. It is what the failure fact reports, and a default
    would quietly relabel a specific failure as a generic one on the wire — the
    sort of drift no test notices and every consumer inherits. ``status_code``
    is set only where there was one (``HTTP_STATUS``); a command stream whose
    failure fact models no status leaves it ``None``, which is why the loop
    passes it through rather than requiring it.

    ``reason`` is typed ``str`` **here** because the token vocabulary is
    producer-owned per stream (CR #5): replicate refuses with ``alias_unknown``
    and friends, which are deliberately not ``FailureReason`` members. The
    per-stream leaves narrow it back — see ``PermanentFetchError`` — so a raise
    site still gets its own vocabulary checked rather than accepting any string.
    """

    def __init__(self, message: str, *, reason: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


class CompletedWithoutBlobError(HandlerError):
    """The command is finished and there are no bytes: publish, ack, no DLQ (#17).

    The third fate, and a **sibling** of the two above rather than a leaf of
    either. Descended from ``PermanentError`` it would be swallowed by
    ``process_message``'s existing arm and dead-letter exactly as before; the
    alternative — leaving it a ``PermanentFetchError`` and branching on
    ``exc.reason is FailureReason.NOT_MODIFIED`` in that arm — would make a
    **wire token load-bearing for local control flow**, so renaming a string
    Watcher branches on would silently change retry and DLQ behaviour. The fate
    is structural, exactly as the other two are.

    Named in the loop's vocabulary rather than HTTP's: the token is the issuer's
    word, the exception type is the loop's. A 304 is the only condition raising
    this today, and the name leaves room for the next body-less-but-fine outcome
    without a second one-off type.

    ``reason`` is required and typed ``str`` for ``PermanentError``'s reasons: a
    default would relabel a specific outcome as a generic one on the wire, and
    the token vocabulary is producer-owned per stream. ``status_code`` is set
    where there was one.
    """

    def __init__(self, message: str, *, reason: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


# The per-stream leaves. The loop catches the **bases** above, so a second
# command stream reports failure in its own vocabulary without the retry-or-not
# decision learning that it exists (#29). Kept as distinct types rather than
# collapsed into the bases because the raise sites are what a reader greps for:
# ``PermanentFetchError`` in the byte path says which handler is speaking.
class TransientFetchError(TransientError):
    """A ``content.fetch`` handler's transient failure."""


class TransientReplicateError(TransientError):
    """A ``content.replicate`` handler's transient failure.

    A provider 5xx or a broker hiccup: still retrying, and still silent, which is
    why the replicate contract keeps MUST-6's reaper obligation verbatim.
    """


class PermanentReplicateError(PermanentError):
    """A ``content.replicate`` handler's permanent failure.

    Narrows ``reason`` to ``ReplicateReason`` for the reason
    ``PermanentFetchError`` narrows it to ``FailureReason``: the base stays
    permissive so each stream carries its own tokens, and the leaf is where a
    raise site gets its own vocabulary checked.

    No ``status_code`` in the signature: ``ReplicationFailedEvent`` models none,
    because no documented refusal reports a provider's HTTP status as its own
    outcome — ``destination_conflict`` consumes the 412 rather than reporting it.
    A provider's status belongs in ``detail`` until a consumer branches on it.
    """

    def __init__(self, message: str, *, reason: ReplicateReason) -> None:
        super().__init__(message, reason=reason)


class PermanentFetchError(PermanentError):
    """A ``content.fetch`` handler's permanent failure.

    Narrows ``reason`` back to ``FailureReason``. The base is permissive so a
    second stream can carry its own tokens; this leaf is where fetch's raise
    sites get the enum checked, so a typo'd token is still a type error rather
    than a string that reaches the wire (CR #5).
    """

    def __init__(
        self, message: str, *, reason: FailureReason, status_code: int | None = None
    ) -> None:
        super().__init__(message, reason=reason, status_code=status_code)
