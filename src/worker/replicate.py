"""The ``content.replicate`` handler and its two path guards.

Structurally the fetch handler's counterpart: the loop decides a command's fate,
this decides what the work *is* and refuses what it must. What differs is the
direction — fetch reads an arbitrary origin into temp storage, replicate writes
our own permanent stores — and every refusal here exists because that inversion
removes the bound the fetch trust model rests on
(``docs/contracts/content-replicate-issuer-contract.md``).

**This writes for ``gcs``** (#29), through ``AsyncGcsDriver.create_if_absent`` —
T4's primitive, behind every guard below. ``gdrive`` and ``ia`` have no
conditional create yet, so a command naming one is refused ``provider_disabled``,
the same path a host with no binding at all takes.

**A writer is a bucket, so the writers are keyed by alias** (CR #26). Keying them
by provider collapsed every ``gcs`` binding onto one driver and let an alias write
into a bucket it was never bound to: ``validate_destination`` guards the *prefix*
half of the T3 root, and nothing downstream re-checks the bucket, because by then
it is baked into the driver that was chosen.

**Both guards are allow-lists.** The source is resolved from a validated
fingerprint through the store's own mapping, so a path never comes from the
message; the destination must sit under the alias root. Deny-lists were the
obvious shape for both and lose the same way ``tests/test_boundaries.py``'s echo
scan lost three times: they are only ever as complete as the last probe.
"""

import re
import string
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol
from urllib.parse import urlsplit

from co_core.effects.gcs import GcsCreateIfAbsent, GcsCreateResult
from co_core.pure.models.changes import ContentReplicateCommand
from co_core.pure.util.gcs import GcsCreateOutcome

from src.core.errors import (
    PermanentReplicateError,
    ReplicateReason,
    TransientReplicateError,
)
from src.core.logging import get_logger
from src.storage.base import BlobStore
from src.worker.aliases import AliasBinding, AliasTable

logger = get_logger(__name__)

# A sha256 as the blob tree spells it. Lower-case only: the store derives paths
# from this exact string, so accepting upper-case would make two spellings of one
# fingerprint address two different files on a case-sensitive filesystem.
_FINGERPRINT = re.compile(r"^[0-9a-f]{64}$")

# What a rendered destination segment may contain. Printable ASCII minus the
# separators and qualifiers the guard rejects outright — deliberately narrower
# than any provider would accept, because widening it later is additive and
# narrowing it is not.
#
# Braces are **excluded on purpose**, and they are the interesting exclusion: a
# provider would accept them, but under T3 the issuer renders, so a ``{`` here
# almost certainly means an unrendered ``{ns.field}`` got through and R1 was
# skipped. Refusing costs an exotic-but-legal key and buys the issuer a failure
# fact instead of a permanent artifact named after a template.
_ALLOWED_IN_SEGMENT = frozenset(string.ascii_letters + string.digits + "._-+=@,()[]~ ")


# How much of a message-derived value reaches the journal. One constant rather
# than a literal at each site, so the two cannot drift into disagreeing about how
# much of an untrusted string is safe to record (CR #20).
_LOGGED_VALUE_CHARS = 120


def _refuse(message: str, reason: ReplicateReason) -> PermanentReplicateError:
    """Build the refusal. Every one of these is terminal and pre-credential."""
    return PermanentReplicateError(message, reason=reason)


def locate_blob(blob_uri: str, *, store: BlobStore) -> str:
    """The fingerprint ``blob_uri`` names, or a terminal refusal (contract T3a).

    **The message's path is never used.** The fingerprint is extracted, validated
    against ``_FINGERPRINT``, and the URI is then compared against one derived
    from the store — so the only string that reaches the filesystem is one the
    store itself built. An implementation that parsed the URI into a path would
    be a read-side traversal on a service whose destinations include public,
    undeletable archive.org items: ``file:///etc/replicator/co-pypi-reader.json``
    as a ``blob_uri`` would publish this host's GCS reader key permanently.

    Two failures, kept distinguishable because the remedies are opposite:
    ``invalid_source`` means the issuer named something this store never minted
    and re-fetching fixes nothing; ``blob_expired`` means it named the right blob
    too late, which a fresh fetch under a new ``command_id`` does fix (MUST-7
    inverted — for replicate the scheduling obligation is the issuer's).
    """
    fingerprint = _fingerprint_in(blob_uri)
    if fingerprint is None or store.uri_for(fingerprint) != blob_uri:
        # One branch for "not a fingerprint" and "not our URI" on purpose: both
        # mean the same thing to an issuer — this is not a reference we handed
        # out — and splitting them would invite a caller to treat the second as
        # recoverable. `detail` carries the value for the journal; it is bounded
        # because an unbounded message value should not reach a log line whole.
        raise _refuse(
            f"blob_uri is not a reference this store minted: {blob_uri[:_LOGGED_VALUE_CHARS]!r}",
            ReplicateReason.INVALID_SOURCE,
        )
    if not store.exists(fingerprint):
        raise _refuse("the blob for this command is no longer stored", ReplicateReason.BLOB_EXPIRED)
    return fingerprint


def _fingerprint_in(blob_uri: str) -> str | None:
    """The fingerprint a ``file://`` blob URI ends with, if it is one at all.

    Scheme-checked here rather than by the caller because #7 (object-store
    backend) changes what a valid ``blob_uri`` looks like, and co-core leaves the
    scheme "the consumer's business" precisely so this stays one decision in one
    place. When #7 lands this grows a branch; the comparison against
    ``store.uri_for`` above does not change at all.
    """
    try:
        parts = urlsplit(blob_uri)
    except ValueError:
        return None
    if parts.scheme != "file" or not parts.path:
        return None
    stem = parts.path.rsplit("/", 1)[-1].removesuffix(".bin")
    return stem if _FINGERPRINT.match(stem) else None


def validate_destination(destination: str, *, binding: AliasBinding) -> str:
    """The alias-rooted key for a rendered destination, or a terminal refusal (T3).

    Returns the joined key rather than a bare "ok" so there is exactly one place
    the root is applied — a caller that validated here and joined elsewhere could
    write outside a root it had just checked.

    **Refused, never repaired.** Same argument as refusing an unsendable header
    rather than stripping it, and sharper here: under T4 the rendered path *is*
    the idempotency key, so a silently-normalized destination would make a
    redelivery target a different key and defeat the no-op that stops a
    redelivery from destroying an artifact.

    **Any ``%`` is refused, not decoded** (CR #16, #22). The first cut decoded and
    then checked, which is the obvious way to catch ``%2e%2e`` — and it silently
    *repaired* a mid-path ``%2F`` into a separator, so ``a/b%2Fc.pdf`` landed at
    ``a/b/c.pdf``. That breaks the "refused, never repaired" rule this function is
    built on, using the very decode the traversal check needed. Under T3 the
    issuer renders, so a rendered path has no business carrying escapes at all;
    refusing them makes the traversal question moot rather than answering it, and
    "how many rounds do we decode" stops being a question anyone needs an opinion
    about.

    The test is on the character, not on ``unquote``: ``unquote`` only rewrites
    well-formed ``%XX``, so ``%zz`` and a trailing ``%`` pass through it
    unchanged and would have been refused by the segment allow-list instead —
    the right outcome reached by a rule the docstring did not describe (CR #22).
    One check now owns every ``%``, and the refusal says which rule refused it.

    Checked on the rendered string, because T3 puts the render on the issuer — a
    surviving ``{ns.field}`` means the issuer skipped R1, and the brace is
    refused as an ordinary disallowed character rather than recognised as a
    placeholder, because this service never learns that vocabulary.
    """
    if "%" in destination:
        raise _refuse(
            "destination carries percent-encoding; send the rendered path",
            ReplicateReason.INVALID_DESTINATION,
        )
    why = _why_bad_destination(destination)
    if why is not None:
        raise _refuse(
            f"destination is not a usable key: {why}", ReplicateReason.INVALID_DESTINATION
        )
    return f"{binding.prefix}/{destination}" if binding.prefix else destination


def _why_bad_destination(rendered: str) -> str | None:
    """Why this rendered path cannot be written, or ``None`` if it can.

    Segment-wise rather than by string comparison against the root: a prefix check
    admits ``reps-other`` under root ``reps``, which is the containment bug this
    shape does not have.
    """
    if not rendered or not rendered.strip():
        return "it is empty"
    if rendered != rendered.strip():
        return "it has leading or trailing whitespace"
    if rendered.startswith("/"):
        return "it is absolute"
    if rendered.endswith("/"):
        return "it ends with a separator"
    if "\\" in rendered:
        return "it contains a backslash"
    if re.match(r"^[A-Za-z]:", rendered):
        return "it carries a drive qualifier"
    segments = rendered.split("/")
    for segment in segments:
        if not segment:
            return "it has an empty segment"
        if segment in (".", ".."):
            return "it has a relative segment"
        bad = {char for char in segment if char not in _ALLOWED_IN_SEGMENT}
        if bad:
            # Bounded like every other message-derived value (CR #24). This string
            # does not only reach the journal: it becomes the failure fact's
            # `detail` on the wire and the `dlq_reason` on the DLQ entry, so an
            # unbounded segment would reach two places the bound exists to
            # protect. The character set is the actionable part anyway.
            return (
                f"segment {segment[:_LOGGED_VALUE_CHARS]!r} has "
                f"disallowed characters {sorted(bad)[:8]!r}"
            )
    return None


# The provider-write seam. Structurally ``AsyncGcsDriver.create_if_absent``, kept
# as a Protocol so the handler is testable without a bucket and so a second
# provider satisfies it without importing anything from here.
class ConditionalWriter(Protocol):
    """Write only if absent; never overwrite. The contract's T4 primitive."""

    async def create_if_absent(self, effect: GcsCreateIfAbsent) -> GcsCreateResult: ...


# The archiver ``gcs`` sub-schema's fields, and the only keys read out of
# ``object_options``. co-core models nothing inside that dict, so an issuer may
# put anything there; forwarding it blindly would turn a typo into a provider
# error, and reading a fixed set keeps this a pass-through rather than an
# interpretation — Replicator never learns what a storage class *means*.
_GCS_OPTIONS = ("cache_control", "content_disposition", "storage_class")


# How long a single conditional create may run. Surfaced as a setting rather
# than inherited from the SDK's 120s default (CR #38), for the reason the fetch
# path surfaces its own timeout: a hung write holds a PEL entry for the whole
# window, and the operator who has to change that number should not need a code
# change to do it.
DEFAULT_WRITE_TIMEOUT_SECONDS = 120


# HTTP statuses that mean "come back later" rather than "this is wrong" (CR #27).
# Everything else in 4xx closes the command; everything in 5xx, and everything
# with no status at all, leaves it open.
_RETRYABLE_STATUSES = frozenset({408, 429})


# The handler seam the loop dispatches to, matching ``src.worker.handler``'s.
type ReplicateHandler = Callable[[ContentReplicateCommand], Awaitable[None]]


# The success-fact seam. It takes the command and the URL rather than a built
# event, deliberately: constructing ``ReplicationCompleteEvent`` means naming the
# three correlators, and this module is **not** on the charter's echo allowlist.
# Keeping the construction in ``replicate_reporter`` — which is — leaves the
# handler unable to name a domain field even by accident, which is the property
# ``DOMAIN_ECHO_MODULES`` records by leaving this file out (#29).
type CompletePublisher = Callable[[ContentReplicateCommand, str], Awaitable[None]]


def build_replicate_handler(
    *,
    store: BlobStore,
    aliases: AliasTable,
    writers: Mapping[str, ConditionalWriter],
    complete: CompletePublisher,
    write_timeout_seconds: int = DEFAULT_WRITE_TIMEOUT_SECONDS,
) -> ReplicateHandler:
    """Wire the replicate byte path: guards, then T4's conditional create.

    Order is the contract's and is load-bearing: the alias resolves first, then
    the provider is checked, then the alias's own writer, then the destination,
    then the source. Every one of those is decided against host config or the
    message alone, so a refused command is refused **before any credential is
    touched** — the guarantee T1 offers the issuer, and it holds only if nothing
    reorders these.

    ``destination_conflict`` is the one refusal that cannot join them: learning
    that a destination holds *differing* bytes takes an authenticated read, which
    is why it is decided from the write's own outcome rather than up here.

    ``writers`` is keyed **by alias, not by provider** (CR #26). A driver holds
    exactly one bucket for its lifetime, so the key has to be whatever selects a
    bucket — keyed by provider, two ``gcs`` bindings collapsed onto one driver and
    a command could land outside the root its own binding declared. An alias with
    no writer is refused, which is what ``gdrive`` and ``ia`` get today and what a
    binding whose driver failed to build at startup gets too.

    ``complete`` publishes the success fact: the handler owns it the way the byte
    path owns ``blob_available``, because the loop's seam sees failures only.
    """

    async def handle(command: ContentReplicateCommand) -> None:
        binding = aliases.resolve(command.credentials_alias)
        if binding is None:
            # The alias name is a *key* here and nowhere else — never logged as a
            # value, never branched on beyond "did it resolve". The charter's
            # replicate invariant asserts that mechanically.
            raise _refuse(
                "the alias named by this command is not provisioned on this host",
                ReplicateReason.ALIAS_UNKNOWN,
            )
        if command.provider != binding.provider:
            raise _refuse(
                f"the alias is bound to {binding.provider!r}, not {command.provider!r}",
                ReplicateReason.PROVIDER_DISABLED,
            )
        # ``binding.alias`` and not ``command.credentials_alias``: the same string
        # by construction, but taking it from the resolved binding means the only
        # value that ever selects a driver came out of host config.
        writer = writers.get(binding.alias)
        if writer is None:
            raise _refuse(
                f"no {command.provider!r} writer is enabled on this host",
                ReplicateReason.PROVIDER_DISABLED,
            )
        key = validate_destination(command.destination, binding=binding)
        # Located, not read: the guard answers "is this ours, still here" without
        # touching the bytes (CR #15). The stream below is the read, and it is
        # handed straight to the driver rather than materialized here.
        fingerprint = locate_blob(command.blob_uri, store=store)

        result = await _write(
            writer,
            command,
            key=key,
            fingerprint=fingerprint,
            store=store,
            timeout_seconds=write_timeout_seconds,
        )
        if result.outcome is GcsCreateOutcome.CONFLICT:
            raise _refuse(
                f"the destination already holds different bytes: {result.detail}",
                ReplicateReason.DESTINATION_CONFLICT,
            )
        if result.outcome is GcsCreateOutcome.INDETERMINATE:
            # The contract's T4 table has three rows and this outcome is a
            # fourth: a 412 whose confirming read found *no object* is a race,
            # not a conflict — the object can be deleted between the two calls,
            # and an unfinalized resumable upload is invisible as an object, so
            # they are indistinguishable from here. Closing it terminally would
            # tell an issuer no artifact is coming, about a destination that is
            # empty and would take the very next attempt.
            #
            # Transient means the entry stays pending and **nothing is
            # published** (CR #28) — there is no non-terminal fact on this wire;
            # ``build_replicate_reporter`` stamps ``terminal=True`` on every fact
            # it emits. The issuer learns when the retry resolves the race, and
            # MUST-6's reaper is the backstop if it never does.
            raise TransientReplicateError(
                f"the conditional create could not be resolved: {result.detail}"
            )

        # Written (or already identical) — publish *after* the object exists, the
        # same ordering the byte path uses: a fact pointing at an artifact that is
        # not there is unrepairable by the consumer, while an object with no fact
        # repairs itself on the redelivery T4 makes safe.
        #
        # ``result.public_url`` and not anything off the command (#36): it is
        # present only where there was a successful write or a confirming read
        # that found the object.
        #
        # Checked rather than assumed, because the seam permits what the wire
        # refuses: ``GcsCreateResult`` types the field ``str | None`` and
        # ``ConditionalWriter`` is a Protocol any provider may satisfy, while
        # ``ReplicationCompleteEvent.public_url`` is a required ``str``. Unchecked,
        # a success carrying no URL raised a ValidationError inside the publisher,
        # which re-raises — so the command stayed pending and retried forever
        # against a driver that would answer the same way every time.
        #
        # Raised **unclassified**, like the driver's own ValueError (CR #27):
        # this is a defect on our side of the seam, retrying cannot fix it, and
        # the delivery ceiling is what turns it into a `handler_error` fact
        # rather than silence.
        if result.public_url is None:
            raise ValueError(
                f"the provider reported {result.outcome.value} with no public_url; "
                "a success fact cannot be published without one"
            )
        await complete(command, result.public_url)
        logger.info(
            "replicated a blob",
            extra={
                "command_id": command.command_id,
                "provider": command.provider,
                "outcome": result.outcome.value,
                "key": key[:_LOGGED_VALUE_CHARS],
            },
        )

    return handle


async def _write(
    writer: ConditionalWriter,
    command: ContentReplicateCommand,
    *,
    key: str,
    fingerprint: str,
    store: BlobStore,
    timeout_seconds: int,
) -> GcsCreateResult:
    """Hand the blob to the provider, classifying whatever comes back.

    The stream is closed on every path — a leaked handle per failed command is a
    slow descriptor exhaustion, and the failing paths are the ones that repeat.

    ``ValueError`` is deliberately **not** caught: it is the driver's own guard
    against a non-seekable or text-mode stream, which is a bug on this side of
    the seam rather than a provider condition. It reaches
    ``_handle_unclassified``, where the delivery ceiling can see it (CR #27).
    """
    # Hoisted out of the comprehension (CR #35): the test is on
    # ``object_options`` as a whole, not on each name, and written as a filter it
    # read like a per-key condition.
    supplied = command.object_options if isinstance(command.object_options, dict) else {}
    options = {name: supplied.get(name) for name in _GCS_OPTIONS}

    try:
        data = store.open_stream(fingerprint)
    except FileNotFoundError as exc:
        # The retention sweep runs concurrently with this loop, so the window
        # between ``locate_blob``'s existence check and this open is real
        # (CR #33). Uncaught, it closed the command as ``handler_error`` after
        # burning the ceiling — losing the one reason whose remedy the issuer
        # owns, which is to fetch again under a new command_id.
        raise _refuse(
            "the blob for this command was swept before it could be read",
            ReplicateReason.BLOB_EXPIRED,
        ) from exc
    except OSError as exc:
        # A disk that is full, read-only or gone: this host's problem, not the
        # command's, and a different host may well succeed.
        raise TransientReplicateError(f"the blob could not be opened: {exc}") from exc

    with data:
        try:
            return await writer.create_if_absent(
                GcsCreateIfAbsent(
                    blob_name=key,
                    data=data,
                    # **As** the command says, never inferred from the
                    # destination's extension — the reason co-core made this
                    # field required.
                    content_type=command.media_type,
                    timeout_seconds=timeout_seconds,
                    **options,
                )
            )
        except ValueError:
            raise
        except Exception as exc:
            raise _classify_provider_failure(exc) from exc


def _classify_provider_failure(exc: Exception) -> Exception:
    """Does this provider failure close the command, or leave it open (CR #27)?

    Everything used to be transient, which is exempt from the delivery ceiling —
    so a 403 on a misprovisioned bucket retried forever and **published no fact
    at all**, leaving the issuer waiting on a command that could never succeed.
    It was reachable from the wire, too: ``object_options`` is an opaque dict, so
    a bad ``storage_class`` is a 400 any bus writer could use to park a command
    in the PEL indefinitely.

    The rule is the HTTP status, read off the exception rather than matched
    against a class:

    - **4xx, except 408 and 429** — the same command will fail the same way on
      every retry, so it closes. A 400 is the issuer's (its rendered key or its
      ``object_options``), which is ``invalid_destination``; the rest are the
      host's — no such bucket, no permission to create — and their remedy is an
      operator act, which is what ``provider_disabled`` already means.
    - **anything else** — 5xx, the two retryable 4xx, and every failure with no
      status at all (a socket dying mid-upload) — stays open. T4 makes retrying
      the write safe, which is what lets this default be the generous one.

    Read as a number rather than caught as ``google.api_core.exceptions.*`` for
    two reasons: that package is a transitive dependency this project never
    declares, and ``ConditionalWriter`` is a provider-agnostic seam, so a second
    provider raising its own error type with an HTTP status is classified
    correctly without touching this function. 408 in particular has no named
    class in api_core at all.
    """
    status = getattr(exc, "code", None)
    terminal = (
        isinstance(status, int)
        and not isinstance(status, bool)
        and 400 <= status < 500
        and status not in _RETRYABLE_STATUSES
    )
    if not terminal:
        return TransientReplicateError(f"the provider write failed: {type(exc).__name__}: {exc}")
    reason = (
        ReplicateReason.INVALID_DESTINATION if status == 400 else ReplicateReason.PROVIDER_DISABLED
    )
    return _refuse(f"the provider refused the write ({status}): {exc}", reason)
