"""The gcs write path: four outcomes, three facts, one of them non-terminal.

The contract's T4 table has three rows. ``GcsCreateOutcome`` has **four**, and
the extra one is the point of these tests: a 412 whose confirming read finds *no
object* is neither a success nor a conflict. It is a race — the object can be
deleted between the two calls, and an unfinalized resumable upload is invisible
as an object, so both look identical from here. Closing that terminally would
tell an issuer its artifact will never arrive, against a destination that is
currently empty and would accept a retry.

That row is also the first non-terminal fact this service has ever emitted, which
is why ``terminal`` is asserted on every one of them rather than only where it
is False.
"""

import pytest
from co_core.effects.gcs import GcsCreateResult
from co_core.pure.models.changes import ReplicationCompleteEvent, ReplicationFailedEvent
from co_core.pure.util.gcs import GcsCreateOutcome

from src.core.errors import ReplicateReason, TransientReplicateError
from src.storage.local import LocalBlobStore
from src.worker.aliases import AliasBinding, AliasTable
from src.worker.replicate import build_replicate_handler
from tests.worker.conftest import now
from tests.worker.test_loop_spec import make_replicate_command_model

FINGERPRINT = "d" * 64
BINDING = AliasBinding(alias="primary", provider="gcs", bucket="co-gcs-replication")
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
    return build_replicate_handler(
        store=store,
        aliases=aliases or AliasTable({"primary": BINDING}),
        writers={"gcs": writer},
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
    """The fourth outcome, and the first non-terminal failure this service emits.

    A 412 whose confirming read finds nothing is a race, not a conflict. Closing
    it terminally would tell the issuer no artifact is coming, about a
    destination that is empty and would accept the very next attempt.
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


async def test_a_provider_with_no_writer_is_still_refused(store, blob_uri):
    """``gdrive`` and ``ia`` have no conditional create yet, so an alias bound to
    one must refuse rather than reach for a writer that is not there."""
    aliases = AliasTable({"drive": AliasBinding(alias="drive", provider="gdrive")})
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
