"""The server-side copy: a GCS source reaches a GCS destination without transiting this host.

#114 item 5 (cannobserv#485). When the blob a command names sits in a bucket —
the temp tier under the ``gcs`` backend, or the permanent store — the handler
hands the provider a ``GcsCopyIfAbsent`` instead of downloading the bytes and
uploading them. T4 is unchanged: the copy is still create-if-absent, and its 412
is resolved by comparing the two objects' reported md5s, so the outcomes and
their facts are the ones ``test_replicate_writer.py`` pins for the upload.

What is new here, and asserted below: which method a source selects, that the
source object is the one the *store* minted (never the message's path), that a
missing source is ``blob_expired`` as a missing read is, and that the provider's
failures close or stay open by the same status rule.
"""

import hashlib

import pytest
from co_core.pure.util.blobstore import METADATA_CONTENT_SHA256, gcs_uri
from co_core.pure.util.gcs import GcsCreateOutcome
from co_core_sync.drivers.blobstore import LocalBlobStore
from google.api_core import exceptions as gexc

from src.core.errors import PermanentReplicateError, ReplicateReason, TransientReplicateError
from src.worker.aliases import AliasTable
from src.worker.replicate import build_replicate_handler
from tests.worker.test_replicate_writer import (
    BINDING,
    PUBLIC_URL,
    Completions,
    FakeGcs,
    command,
    result,
)

FINGERPRINT = hashlib.sha256(b"artifact bytes").hexdigest()
SOURCE_BUCKET = "example-permanent-bucket"


class BucketStore:
    """A GCS-shaped store: mints ``gs://`` URIs, and refuses to be downloaded from.

    ``open_stream`` raising is the assertion that the copy path never reads the
    bytes onto this host — the whole point of item 5.
    """

    def __init__(self, bucket=SOURCE_BUCKET):
        self.bucket = bucket

    def uri_for(self, fingerprint):
        return gcs_uri(self.bucket, fingerprint)

    def exists(self, fingerprint):
        return True

    def open_stream(self, fingerprint):
        raise AssertionError("the copy path downloaded the blob")


class FakeCopier(FakeGcs):
    """``FakeGcs`` plus the copy, recording its effects apart from the creates."""

    def __init__(self, result=None, raises=None):
        super().__init__(result=result, raises=raises)
        self.copies = []

    async def copy_if_absent(self, effect):
        self.copies.append(effect)
        if self._raises is not None:
            raise self._raises
        return self._result


def handler_with(writer, *, store=None, permanent=(), **kw):
    return build_replicate_handler(
        store=store if store is not None else BucketStore("example-temp-bucket"),
        permanent_stores=permanent,
        aliases=AliasTable({"primary": BINDING}),
        writers={"primary": writer},
        complete=kw.pop("complete", Completions()),
        **kw,
    )


@pytest.fixture
def permanent():
    return BucketStore()


@pytest.fixture
def uri(permanent):
    return permanent.uri_for(FINGERPRINT)


async def test_a_bucket_source_is_copied_not_downloaded(tmp_path, permanent, uri):
    writer = FakeCopier(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))
    done = Completions()

    await handler_with(
        writer, store=LocalBlobStore(tmp_path), permanent=(permanent,), complete=done
    )(command(uri, media_type="application/pdf"))

    (effect,) = writer.copies
    assert writer.effects == []
    # The store's own object, derived from its URI — never the message's path.
    assert (effect.source_bucket, effect.source_blob_name) == (
        SOURCE_BUCKET,
        f"blobs/{FINGERPRINT}.bin",
    )
    assert effect.blob_name == "organizations/x/report.pdf"
    assert effect.content_type == "application/pdf"
    assert [fact.public_url for fact in done.facts] == [PUBLIC_URL]


async def test_a_temp_tier_in_a_bucket_is_copied_too():
    """The ``gcs`` backend's temp store is a bucket as well, so production's
    common case — publishing within the seven days — takes the copy."""
    temp = BucketStore("example-temp-bucket")
    writer = FakeCopier(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    await handler_with(writer, store=temp)(command(temp.uri_for(FINGERPRINT)))

    (effect,) = writer.copies
    assert effect.source_bucket == "example-temp-bucket"


async def test_a_local_source_is_still_uploaded(tmp_path):
    """A filesystem temp tier — every dev host — has no bucket to copy from."""
    store = LocalBlobStore(tmp_path)
    blob_uri = store.store(b"artifact bytes", FINGERPRINT, "application/pdf")
    writer = FakeCopier(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    await handler_with(writer, store=store)(command(blob_uri))

    assert writer.copies == []
    assert len(writer.effects) == 1


async def test_the_copy_carries_the_stamp_the_options_and_the_timeout(tmp_path, permanent, uri):
    writer = FakeCopier(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    await handler_with(
        writer, store=LocalBlobStore(tmp_path), permanent=(permanent,), write_timeout_seconds=45
    )(command(uri, object_options={"storage_class": "ARCHIVE", "unknown": "x"}))

    (effect,) = writer.copies
    assert effect.metadata == {METADATA_CONTENT_SHA256: FINGERPRINT}
    assert effect.storage_class == "ARCHIVE"
    assert effect.timeout_seconds == 45


async def test_differing_bytes_are_still_a_terminal_conflict(tmp_path, permanent, uri):
    writer = FakeCopier(result(GcsCreateOutcome.CONFLICT, detail="md5 differs"))

    with pytest.raises(PermanentReplicateError) as caught:
        await handler_with(writer, store=LocalBlobStore(tmp_path), permanent=(permanent,))(
            command(uri)
        )

    assert caught.value.reason is ReplicateReason.DESTINATION_CONFLICT


async def test_a_source_gone_before_the_copy_is_expired(tmp_path, permanent, uri):
    """co-core raises ``FileNotFoundError`` for a missing source only, the blob
    store's word for gone — the same remedy as a read that found nothing."""
    writer = FakeCopier(raises=FileNotFoundError(uri))

    with pytest.raises(PermanentReplicateError) as caught:
        await handler_with(writer, store=LocalBlobStore(tmp_path), permanent=(permanent,))(
            command(uri)
        )

    assert caught.value.reason is ReplicateReason.BLOB_EXPIRED


@pytest.mark.parametrize(
    ("exc", "reason"),
    [
        # A 403 is either end — the source read or the destination create —
        # and both are the host's grant, an operator act (runbook phase A).
        pytest.param(gexc.Forbidden("403"), ReplicateReason.PROVIDER_DISABLED, id="403"),
        pytest.param(gexc.BadRequest("400"), ReplicateReason.INVALID_DESTINATION, id="400"),
    ],
)
async def test_a_terminal_provider_error_on_the_copy_closes(tmp_path, permanent, uri, exc, reason):
    writer = FakeCopier(raises=exc)

    with pytest.raises(PermanentReplicateError) as caught:
        await handler_with(writer, store=LocalBlobStore(tmp_path), permanent=(permanent,))(
            command(uri)
        )

    assert caught.value.reason is reason


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(gexc.ServiceUnavailable("503"), id="503"),
        pytest.param(ConnectionError("reset"), id="no-status"),
    ],
)
async def test_a_transient_provider_error_on_the_copy_stays_open(tmp_path, permanent, uri, exc):
    writer = FakeCopier(raises=exc)

    with pytest.raises(TransientReplicateError):
        await handler_with(writer, store=LocalBlobStore(tmp_path), permanent=(permanent,))(
            command(uri)
        )

    # The copy was the call that failed, not a download the copy path must never make.
    assert len(writer.copies) == 1


async def test_a_defect_on_the_copy_reaches_the_ceiling(tmp_path, permanent, uri):
    """The driver's own ``ValueError`` (a refused metadata key) is ours, not an outage."""
    writer = FakeCopier(raises=ValueError("bad metadata"))

    with pytest.raises(ValueError):
        await handler_with(writer, store=LocalBlobStore(tmp_path), permanent=(permanent,))(
            command(uri)
        )


@pytest.mark.parametrize("source", ["bucket", "filesystem"])
async def test_the_success_line_names_the_method(tmp_path, caplog, source):
    """Journal-only: how an operator confirms the copy is live (runbook phase C)."""
    if source == "bucket":
        store = BucketStore("example-temp-bucket")
        blob_uri = store.uri_for(FINGERPRINT)
    else:
        store = LocalBlobStore(tmp_path)
        blob_uri = store.store(b"artifact bytes", FINGERPRINT, "application/pdf")
    writer = FakeCopier(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    with caplog.at_level("INFO", logger="src.worker.replicate"):
        await handler_with(writer, store=store)(command(blob_uri))

    [record] = [r for r in caplog.records if r.message == "replicated a blob"]
    assert record.method == ("copy" if source == "bucket" else "upload")
