"""The gcs write path: four outcomes, two facts, and one that publishes nothing.

The contract's T4 table has three rows. ``GcsCreateOutcome`` has **four**, and
the extra one is the point of these tests: a 412 whose confirming read finds *no
object* is neither a success nor a conflict. It is a race — the object can be
deleted between the two calls, and an unfinalized resumable upload is invisible
as an object, so both look identical from here. Closing that terminally would
tell an issuer its artifact will never arrive, against a destination that is
currently empty and would accept a retry.

**Not closing it publishes nothing at all** (CR #28). ``TransientReplicateError``
is exempt from the delivery ceiling, so the loop logs, returns ``RETRY`` and
leaves the entry pending — there is no non-terminal fact on this wire, and
``build_replicate_reporter`` still stamps ``terminal=True`` on every fact it
emits. An issuer learns nothing until the retry resolves the race one way or the
other; MUST-6's reaper is the backstop if it never does. Whether that silence
should become a ``terminal=false`` fact is a contract change, not an
implementation detail, and it is not made here.

Which failures *are* terminal is the other half of the file (CR #27), and it is
the half that had a bug: every provider exception was transient, so a 403 on a
misprovisioned bucket retried forever and the issuer waited forever.
"""

import pytest
from co_core.effects.gcs import GcsCreateResult
from co_core.pure.models.changes import ReplicationCompleteEvent, ReplicationFailedEvent
from co_core.pure.util.gcs import GcsCreateOutcome
from google.api_core import exceptions as gexc

from src.core.errors import PermanentReplicateError, ReplicateReason, TransientReplicateError
from src.storage.local import LocalBlobStore
from src.worker.aliases import AliasBinding, AliasTable
from src.worker.replicate import build_replicate_handler
from tests.worker.conftest import now
from tests.worker.test_loop_spec import make_replicate_command_model

FINGERPRINT = "d" * 64
BINDING = AliasBinding(provider="gcs", bucket="co-gcs-replication")
PUBLIC_URL = "https://storage.googleapis.com/co-gcs-replication/organizations/x/report.pdf"


class FakeGcs:
    """Stands in for ``AsyncGcsDriver``, recording the effect it was handed."""

    def __init__(self, result=None, raises=None):
        self._result = result
        self._raises = raises
        self.effects = []
        self.streams = []

    async def create_if_absent(self, effect):
        self.effects.append(effect)
        # Recorded here, not asserted later: the handler closes the handle on the
        # way out, and a closed file raises on ``seekable()``. What matters is
        # what the *driver* was given, which is only observable now.
        data = effect.data
        self.streams.append(
            {
                "is_bytes": isinstance(data, bytes),
                "seekable": getattr(data, "seekable", lambda: False)(),
                "mode": getattr(data, "mode", ""),
            }
        )
        if self._raises is not None:
            raise self._raises
        return self._result


def result(outcome, **kw):
    return GcsCreateResult(outcome=outcome, blob_name="organizations/x/report.pdf", **kw)


@pytest.fixture
def store(tmp_path):
    return LocalBlobStore(tmp_path)


@pytest.fixture
def blob_uri(store):
    return store.store(b"artifact bytes", FINGERPRINT, "application/pdf")


class Completions:
    """Collects the ``replication_complete`` facts the handler publishes.

    The handler publishes its own success fact, mirroring the byte path — the
    loop's ``Handler`` seam returns ``None`` and the loop only ever sees
    failures, through the reporter.
    """

    def __init__(self):
        self.facts = []

    async def __call__(self, command, public_url):
        # Built here the way the real publisher builds it, so these tests still
        # assert on the fact's shape rather than on a tuple.
        self.facts.append(
            ReplicationCompleteEvent(
                occurred_at=now(),
                command_id=command.command_id,
                public_url=public_url,
                info_item_rep_spec_id=command.info_item_rep_spec_id,
                source_revision_id=command.source_revision_id,
                info_source_id=command.info_source_id,
            )
        )


def handler_for(store, writer, aliases=None, complete=None):
    # Keyed by alias, not by provider (CR #26): a driver *is* a bucket, so the
    # key has to be whatever selects one.
    return build_replicate_handler(
        store=store,
        aliases=aliases or AliasTable({"primary": BINDING}),
        writers={"primary": writer},
        complete=complete or Completions(),
    )


def command(blob_uri, **overrides):
    fields = {"blob_uri": blob_uri, "destination": "organizations/x/report.pdf"}
    return make_replicate_command_model(**{**fields, **overrides})


async def test_an_absent_destination_is_written_and_completes(store, blob_uri):
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL, generation=1))
    done = Completions()
    await handler_for(store, writer, complete=done)(command(blob_uri))

    (fact,) = done.facts
    assert isinstance(fact, ReplicationCompleteEvent)
    assert fact.public_url == PUBLIC_URL
    assert fact.info_item_rep_spec_id == "iirs-1"


async def test_a_redelivery_onto_matching_bytes_re_emits_the_same_url(store, blob_uri):
    """T4 row two, and the reason the envelope key is not the bare command_id.

    A no-op still emits — the issuer may have missed the first fact — and it must
    carry the *same* ``public_url``, because the registry row it writes back to
    should not change when nothing about the artifact did.
    """
    writer = FakeGcs(result(GcsCreateOutcome.ALREADY_IDENTICAL, public_url=PUBLIC_URL))
    done = Completions()
    await handler_for(store, writer, complete=done)(command(blob_uri))

    (fact,) = done.facts
    assert isinstance(fact, ReplicationCompleteEvent)
    assert fact.public_url == PUBLIC_URL


async def test_differing_bytes_are_a_terminal_conflict(store, blob_uri):
    """T4 row three. Refused rather than overwritten — and the IAM grant on this
    bucket has no delete either, so the refusal is belt and braces."""
    writer = FakeGcs(result(GcsCreateOutcome.CONFLICT, remote_md5="zzz", detail="md5 differs"))
    with pytest.raises(Exception) as caught:
        await handler_for(store, writer)(command(blob_uri))

    assert caught.value.reason is ReplicateReason.DESTINATION_CONFLICT


async def test_an_indeterminate_create_is_retried_not_closed(store, blob_uri):
    """The fourth outcome: retried, and therefore silent.

    A 412 whose confirming read finds nothing is a race, not a conflict. Closing
    it terminally would tell the issuer no artifact is coming, about a
    destination that is empty and would accept the very next attempt — so it
    raises transient and the entry stays pending. No fact is published while that
    is true (CR #28); see the module docstring.
    """
    writer = FakeGcs(result(GcsCreateOutcome.INDETERMINATE, detail="412 but no object present"))
    with pytest.raises(TransientReplicateError):
        await handler_for(store, writer)(command(blob_uri))


async def test_a_provider_error_is_transient_not_terminal(store, blob_uri):
    """The driver lets anything that is not a 412 propagate, deliberately — "they
    are the caller's non-terminal failure fact". A 503 must not close a command."""
    writer = FakeGcs(raises=RuntimeError("503 Service Unavailable"))
    with pytest.raises(TransientReplicateError):
        await handler_for(store, writer)(command(blob_uri))


async def test_the_write_carries_the_commands_media_type(store, blob_uri):
    """The field co-core made required because the consumer cannot recover it.

    Written **as** the command's value, never inferred from the destination's
    extension — otherwise a permanent store fills with
    ``application/octet-stream``.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    await handler_for(store, writer)(command(blob_uri, media_type="application/pdf"))

    (effect,) = writer.effects
    assert effect.content_type == "application/pdf"


async def test_the_write_is_handed_a_seekable_binary_stream(store, blob_uri):
    """Not bytes, and not a path.

    A path would make the provider copy something already on disk; bytes would
    pull a whole artifact into memory for a service whose only reason to hold it
    is to pass it on. Seekable because the driver reads the local md5 *after* the
    failed conditional create has moved the position.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    await handler_for(store, writer)(command(blob_uri))

    (seen,) = writer.streams
    assert not seen["is_bytes"]
    assert seen["seekable"]
    assert "b" in seen["mode"]


async def test_the_stream_is_closed_even_when_the_write_fails(store, blob_uri):
    """A leaked handle per failed command is a slow file-descriptor exhaustion,
    and the failing paths are the ones that repeat."""
    writer = FakeGcs(raises=RuntimeError("503"))
    with pytest.raises(TransientReplicateError):
        await handler_for(store, writer)(command(blob_uri))

    (effect,) = writer.effects
    assert effect.data.closed


async def test_the_object_options_reach_the_write(store, blob_uri):
    """The archiver ``gcs`` sub-schema's fields, passed through opaquely.

    co-core models nothing inside ``object_options``, so this is a pass-through
    and not an interpretation — Replicator never learns what a storage class
    means, only that the provider takes one.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    await handler_for(store, writer)(
        command(
            blob_uri,
            object_options={
                "storage_class": "ARCHIVE",
                "cache_control": "public, max-age=3600",
                "content_disposition": "inline",
            },
        )
    )

    (effect,) = writer.effects
    assert effect.storage_class == "ARCHIVE"
    assert effect.cache_control == "public, max-age=3600"
    assert effect.content_disposition == "inline"


async def test_an_unknown_object_option_is_ignored_rather_than_passed(store, blob_uri):
    """``object_options`` is an opaque dict on the wire, so an issuer can put
    anything in it. Only the fields this provider takes are read; the rest are
    freight, and forwarding them blindly would make a typo a provider error."""
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    await handler_for(store, writer)(
        command(blob_uri, object_options={"storage_class": "ARCHIVE", "folder_id": "not-for-gcs"})
    )

    (effect,) = writer.effects
    assert effect.storage_class == "ARCHIVE"
    assert not hasattr(effect, "folder_id")


async def test_the_destination_written_is_the_guarded_key(store, blob_uri):
    """The blob name comes from ``validate_destination``, not from the raw
    command — so the alias root is applied exactly once, by the guard."""
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    await handler_for(store, writer)(command(blob_uri, destination="organizations/y/r.pdf"))

    (effect,) = writer.effects
    assert effect.blob_name == "organizations/y/r.pdf"


async def test_each_alias_reaches_its_own_bucket_and_no_other(store, blob_uri):
    """CR #26, at the level where the damage would be done.

    Two aliases, both ``gcs``, different buckets. Keyed by provider they
    collapsed to one driver, so a command naming ``public`` wrote into
    ``private``'s bucket — outside the root its binding declared, into a store
    that cannot delete what it accepts. The prefix half of T3 is guarded by
    ``validate_destination``; nothing downstream re-checks the bucket, because by
    then it is baked into the driver.
    """
    public = AliasBinding(provider="gcs", bucket="co-gcs-replication")
    private = AliasBinding(provider="gcs", bucket="co-gcs-internal")
    to_public = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    to_private = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    handler = build_replicate_handler(
        store=store,
        aliases=AliasTable({"public": public, "private": private}),
        writers={"public": to_public, "private": to_private},
        complete=Completions(),
    )
    await handler(command(blob_uri, credentials_alias="public"))

    assert len(to_public.effects) == 1
    assert to_private.effects == []


async def test_an_alias_with_no_writer_of_its_own_is_refused(store, blob_uri):
    """The per-alias version of the provider check: another alias having a driver
    does not lend it to this one, which is the failure keying-by-provider had."""
    handler = build_replicate_handler(
        store=store,
        aliases=AliasTable(
            {
                "primary": BINDING,
                "spare": AliasBinding(provider="gcs", bucket="co-gcs-other"),
            }
        ),
        writers={"primary": FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))},
        complete=Completions(),
    )

    with pytest.raises(PermanentReplicateError) as caught:
        await handler(command(blob_uri, credentials_alias="spare"))

    assert caught.value.reason is ReplicateReason.PROVIDER_DISABLED


async def test_a_provider_with_no_writer_is_still_refused(store, blob_uri):
    """``gdrive`` and ``ia`` have no conditional create yet, so an alias bound to
    one must refuse rather than reach for a writer that is not there."""
    aliases = AliasTable({"drive": AliasBinding(provider="gdrive")})
    handler = build_replicate_handler(
        store=store, aliases=aliases, writers={}, complete=Completions()
    )

    with pytest.raises(Exception) as caught:
        await handler(command(blob_uri, credentials_alias="drive", provider="gdrive"))

    assert caught.value.reason is ReplicateReason.PROVIDER_DISABLED


async def test_a_refused_command_never_reaches_the_writer(store, blob_uri):
    """T1's guarantee, now that there *is* something a credential could touch.

    Every pre-flight refusal must happen before the driver is called at all —
    which is observable for the first time here, because until this commit there
    was no call to be made.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    handler = handler_for(store, writer)

    for cmd in (
        command(blob_uri, credentials_alias="unknown"),
        command(blob_uri, destination="../escape"),
        command("file:///etc/passwd"),
    ):
        with pytest.raises(Exception):
            await handler(cmd)

    assert writer.effects == []


async def test_the_write_timeout_is_the_configured_one(store, blob_uri):
    """CR #38. The SDK's 120s default was inherited rather than chosen.

    Surfaced as a setting for the reason the fetch path surfaces its own: a write
    that hangs holds a PEL entry for the whole window, and the operator who has
    to change that number should not have to change code to do it.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    handler = build_replicate_handler(
        store=store,
        aliases=AliasTable({"primary": BINDING}),
        writers={"primary": writer},
        complete=Completions(),
        write_timeout_seconds=45,
    )
    await handler(command(blob_uri))

    (effect,) = writer.effects
    assert effect.timeout_seconds == 45


# --------------------------------------------------------------------------
# Which provider failures close a command, and which leave it open (CR #27)
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("exc", "reason"),
    [
        # The issuer's own input: object_options is an opaque dict, so a bad
        # storage_class is a 400 a bus writer can produce at will. Terminal, or
        # it parks a command in the PEL forever with nothing to fix it.
        pytest.param(
            gexc.BadRequest("400 invalid storage class"),
            ReplicateReason.INVALID_DESTINATION,
            id="400-bad-request",
        ),
        # Host-side: the SA lacks storage.objects.create, or the bucket named by
        # the binding does not exist. The remedy is an operator act, which is
        # what provider_disabled already means.
        pytest.param(
            gexc.Unauthorized("401 invalid credentials"),
            ReplicateReason.PROVIDER_DISABLED,
            id="401-unauthorized",
        ),
        pytest.param(
            gexc.Forbidden("403 caller lacks storage.objects.create"),
            ReplicateReason.PROVIDER_DISABLED,
            id="403-forbidden",
        ),
        pytest.param(
            gexc.NotFound("404 no such bucket"),
            ReplicateReason.PROVIDER_DISABLED,
            id="404-no-bucket",
        ),
    ],
)
async def test_a_permanent_provider_error_closes_the_command(store, blob_uri, exc, reason):
    """A 4xx is not a retry. It is the same command failing the same way forever.

    Every one of these used to become ``TransientReplicateError``, which
    ``loop.py`` exempts from the delivery ceiling — so the command retried
    indefinitely and **no fact was ever published**. The issuer waits forever and
    MUST-6's reaper is the only thing that notices.
    """
    writer = FakeGcs(raises=exc)

    with pytest.raises(PermanentReplicateError) as caught:
        await handler_for(store, writer)(command(blob_uri))

    assert caught.value.reason is reason


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(gexc.ServiceUnavailable("503 backend error"), id="503"),
        pytest.param(gexc.InternalServerError("500"), id="500"),
        pytest.param(gexc.GatewayTimeout("504"), id="504"),
        # The two 4xx that are explicitly "come back later" rather than "this is
        # wrong": rate limiting and a request timeout.
        pytest.param(gexc.TooManyRequests("429 rate limited"), id="429"),
        # 408 has no named class in api_core, which is the reason the rule is
        # written against the numeric code rather than an exception tuple.
        pytest.param(gexc.from_http_status(408, "request timeout"), id="408"),
        # No status at all — a socket died mid-upload. Unclassifiable as
        # permanent, so it stays open.
        pytest.param(OSError("connection reset by peer"), id="no-status"),
    ],
)
async def test_a_transient_provider_error_leaves_the_command_open(store, blob_uri, exc):
    """5xx, 429, 408 and bare network failures must not close a command: the
    write is expected to succeed on a later attempt, and T4 makes retrying it
    safe."""
    writer = FakeGcs(raises=exc)

    with pytest.raises(TransientReplicateError):
        await handler_for(store, writer)(command(blob_uri))


async def test_a_programming_error_is_not_disguised_as_transient(store, blob_uri):
    """``ValueError`` is the driver's own guard against a non-seekable or
    text-mode stream — a bug here, not a provider condition.

    Swallowed into ``TransientReplicateError`` it retried forever and silently,
    because transient failures are exempt from the delivery ceiling. Left to
    propagate it reaches ``_handle_unclassified``, where the ceiling can see it
    and eventually dead-letters with a ``handler_error`` fact.
    """
    writer = FakeGcs(raises=ValueError("data must be bytes or a seekable binary stream"))

    with pytest.raises(ValueError):
        await handler_for(store, writer)(command(blob_uri))


async def test_a_blob_swept_between_the_guard_and_the_open_is_expired(store, blob_uri, monkeypatch):
    """CR #33. The retention sweep runs concurrently with this loop.

    ``locate_blob`` answers "still here" and the open happens a moment later, so
    the window is real. Uncaught, the ``FileNotFoundError`` reached
    ``_handle_unclassified`` and closed the command as ``handler_error`` after
    burning the delivery ceiling — losing the one reason the issuer can act on,
    which is that a fresh fetch fixes this.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    def swept(self, fingerprint):
        raise FileNotFoundError(f"no such blob: {fingerprint}")

    monkeypatch.setattr(LocalBlobStore, "open_stream", swept)

    with pytest.raises(PermanentReplicateError) as caught:
        await handler_for(store, writer)(command(blob_uri))

    assert caught.value.reason is ReplicateReason.BLOB_EXPIRED
    assert writer.effects == []


async def test_a_blob_that_cannot_be_opened_for_another_reason_stays_open(
    store, blob_uri, monkeypatch
):
    """A permission or I/O error on the blob tree is *this host's* problem.

    Distinguished from the swept case above because the remedies differ: a gone
    blob is terminal and the issuer must fetch again, while a read-only or full
    disk is a condition another worker on another host does not share. Closing it
    would tell the issuer to re-fetch something that is sitting right there.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    def unreadable(self, fingerprint):
        raise PermissionError("the blob tree is not readable by this process")

    monkeypatch.setattr(LocalBlobStore, "open_stream", unreadable)

    with pytest.raises(TransientReplicateError):
        await handler_for(store, writer)(command(blob_uri))


async def test_a_complete_fact_carries_no_url_the_command_supplied(store, blob_uri):
    """T6, in the form that actually holds (#36).

    ``public_url`` is not response-derived for gcs — the SDK formats it
    client-side — so the promise is narrower and real: the URL is present only
    where there was a successful write or a confirming read, and it comes from
    the driver's result rather than from anything on the command.
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    done = Completions()
    await handler_for(store, writer, complete=done)(command(blob_uri))

    (fact,) = done.facts
    assert fact.public_url == PUBLIC_URL
    assert not isinstance(fact, ReplicationFailedEvent)


async def test_a_success_with_no_url_is_not_published_at_all(store, blob_uri):
    """``ReplicationCompleteEvent.public_url`` is a required ``str``, so ``None``
    is a ValidationError inside the publisher rather than a fact.

    Not reachable through ``AsyncGcsDriver`` — it sets the URL on both success
    rows — but ``ConditionalWriter`` is a Protocol and ``GcsCreateResult`` types
    the field ``str | None``, so the seam permits what the wire refuses. Left to
    fall through, the publisher raised, the command stayed pending, and it
    retried forever against a driver that would return the same thing every time.

    Raised **unclassified**, deliberately: it is a defect on this side of the
    seam, retrying cannot fix it, and the delivery ceiling is what turns it into
    a ``handler_error`` fact instead of silence. Same reasoning as letting the
    driver's own ``ValueError`` propagate (CR #27).
    """
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=None))
    done = Completions()

    with pytest.raises(ValueError, match="no public_url"):
        await handler_for(store, writer, complete=done)(command(blob_uri))

    assert done.facts == []
