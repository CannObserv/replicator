"""The ``content.replicate`` handler and its two path guards.

Structurally the fetch handler's counterpart: the loop decides a command's fate,
this decides what the work *is* and refuses what it must. What differs is the
direction — fetch reads an arbitrary origin into temp storage, replicate writes
our own permanent stores — and every refusal here exists because that inversion
removes the bound the fetch trust model rests on
(``docs/contracts/content-replicate-issuer-contract.md``).

**Nothing here writes yet.** No provider client is wired, so a command that
survives every guard is refused with ``provider_disabled`` — accurately, because
no provider *is* enabled on any host today. That is a complete and honest half of
the capability rather than a stub: an issuer gets a real fact with a real reason
for every command it sends, and the byte-writing half changes what happens after
the guards, not the guards themselves.

**Both guards are allow-lists.** The source is resolved from a validated
fingerprint through the store's own mapping, so a path never comes from the
message; the destination must sit under the alias root. Deny-lists were the
obvious shape for both and lose the same way ``tests/test_boundaries.py``'s echo
scan lost three times: they are only ever as complete as the last probe.
"""

import re
import string
from collections.abc import Awaitable, Callable
from urllib.parse import unquote, urlsplit

from co_core.pure.models.changes import ContentReplicateCommand

from src.core.errors import PermanentReplicateError, ReplicateReason
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


def _refuse(message: str, reason: ReplicateReason) -> PermanentReplicateError:
    """Build the refusal. Every one of these is terminal and pre-credential."""
    return PermanentReplicateError(message, reason=reason)


def resolve_blob(blob_uri: str, *, store: BlobStore) -> bytes:
    """The bytes ``blob_uri`` names, or a terminal refusal (contract T3a).

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
            f"blob_uri is not a reference this store minted: {blob_uri[:120]!r}",
            ReplicateReason.INVALID_SOURCE,
        )
    if not store.exists(fingerprint):
        raise _refuse("the blob for this command is no longer stored", ReplicateReason.BLOB_EXPIRED)
    return store.open(fingerprint)


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

    Checked **after** percent-decoding and on the rendered string. Decoding first
    is what catches ``%2e%2e``; a guard that ran before it would pass the one
    encoding that matters. The string is rendered because T3 puts the render on
    the issuer, so a surviving ``{ns.field}`` means the issuer skipped R1 — and
    the brace is refused as an ordinary disallowed character rather than
    recognised as a placeholder, because this service never learns that
    vocabulary.
    """
    decoded = unquote(destination)
    if decoded != destination and unquote(decoded) != decoded:
        # Double-encoded. Refused rather than decoded again: "how many times do we
        # decode" has no defensible answer, and one round is what a provider will
        # do.
        raise _refuse(
            "destination is multiply percent-encoded", ReplicateReason.INVALID_DESTINATION
        )
    why = _why_bad_destination(decoded)
    if why is not None:
        raise _refuse(
            f"destination is not a usable key: {why}", ReplicateReason.INVALID_DESTINATION
        )
    return f"{binding.prefix}/{decoded}" if binding.prefix else decoded


def _why_bad_destination(decoded: str) -> str | None:
    """Why this rendered path cannot be written, or ``None`` if it can.

    Segment-wise rather than by string comparison against the root: a prefix check
    admits ``reps-other`` under root ``reps``, which is the containment bug this
    shape does not have.
    """
    if not decoded or not decoded.strip():
        return "it is empty"
    if decoded != decoded.strip():
        return "it has leading or trailing whitespace"
    if decoded.startswith("/"):
        return "it is absolute"
    if decoded.endswith("/"):
        return "it ends with a separator"
    if "\\" in decoded:
        return "it contains a backslash"
    if re.match(r"^[A-Za-z]:", decoded):
        return "it carries a drive qualifier"
    segments = decoded.split("/")
    for segment in segments:
        if not segment:
            return "it has an empty segment"
        if segment in (".", ".."):
            return "it has a relative segment"
        bad = {char for char in segment if char not in _ALLOWED_IN_SEGMENT}
        if bad:
            return f"segment {segment!r} has disallowed characters {sorted(bad)!r}"
    return None


# The handler seam the loop dispatches to, matching ``src.worker.handler``'s.
type ReplicateHandler = Callable[[ContentReplicateCommand], Awaitable[None]]


def build_replicate_handler(*, store: BlobStore, aliases: AliasTable) -> ReplicateHandler:
    """Wire the replicate byte path — which today refuses everything, accurately.

    Order is the contract's and is load-bearing: the alias resolves first, then
    the provider is checked, then the destination, then the source. Every one of
    those is decided against host config or the message alone, so a refused
    command is refused **before any credential is touched** — the guarantee T1
    offers the issuer, and it holds only if nothing reorders these.

    ``destination_conflict`` is the one refusal that cannot join them: learning
    that a destination holds *differing* bytes takes an authenticated read. It
    arrives with the first provider.
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
        key = validate_destination(command.destination, binding=binding)
        blob = resolve_blob(command.blob_uri, store=store)

        # Everything above is settled; the write is not built. Refusing here is
        # accurate rather than a placeholder — no provider is enabled on any host
        # today — and it keeps the issuer's remedy correct: act on the host.
        logger.info(
            "replicate command passed every pre-flight guard",
            extra={
                "command_id": command.command_id,
                "provider": command.provider,
                "key": key,
                "bytes": len(blob),
                "detail": "no provider writer is wired yet; refusing rather than dropping",
            },
        )
        raise _refuse(
            f"no {command.provider!r} writer is enabled on this host",
            ReplicateReason.PROVIDER_DISABLED,
        )

    return handle
