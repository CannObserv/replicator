"""A capture's two tiers in one bucket, against a real one (#189, archiver#380 D4).

The production shape: the temp tier under ``temp/`` with touch on, the kept tier
under ``blobs/`` with touch off, both in the captures bucket and nowhere else.
The permanent test twin stands in for the captures bucket — the test SA holds
``objectAdmin`` there, so no twin could test the production IAM condition anyway;
that is the host read-back's object-level probe. No raw store is involved: the
raw family is the empty temp bucket's, and it must stay empty of these bytes.

Unique bytes per run, so the keys are this run's own; every object is deleted in
the ``finally`` and asserted gone.
"""

import hashlib
import uuid

import pytest
from co_core_sync.drivers.blobstore import GcsBlobStore
from google.cloud import storage

from src.core.config import CAPTURE_KEPT_PREFIX, CAPTURE_TEMP_PREFIX
from src.worker.persist import StoreFamily, build_persist_handler
from tests.worker.test_loop_spec import make_persist_command_model

pytestmark = pytest.mark.gcs


async def test_a_capture_is_kept_beside_its_temp_copy_and_never_in_the_raw_bucket(
    gcs_blob_bucket, gcs_permanent_bucket
):
    data = b"\x89PNG\r\n\x1a\n captured by the gcs job " + uuid.uuid4().hex.encode()
    fingerprint = hashlib.sha256(data).hexdigest()
    client = storage.Client()
    raw = StoreFamily(
        temp=GcsBlobStore(gcs_blob_bucket, client=client, touch_on_rereference=True),
        kept=GcsBlobStore(gcs_permanent_bucket, prefix="raw-kept-never-written", client=client),
    )
    captures = StoreFamily(
        temp=GcsBlobStore(
            gcs_permanent_bucket,
            prefix=CAPTURE_TEMP_PREFIX,
            client=client,
            touch_on_rereference=True,
        ),
        kept=GcsBlobStore(gcs_permanent_bucket, prefix=CAPTURE_KEPT_PREFIX, client=client),
    )
    bucket = client.bucket(gcs_permanent_bucket)
    temp_object = bucket.blob(f"temp/{fingerprint}.bin")
    kept_object = bucket.blob(f"blobs/{fingerprint}.bin")
    strays = (
        client.bucket(gcs_blob_bucket).blob(raw.temp.key_for(fingerprint)),
        bucket.blob(raw.kept.key_for(fingerprint)),
    )
    facts: list[tuple[str, int]] = []

    async def complete(command, size_bytes):
        facts.append((command.command_id, size_bytes))

    try:
        uri = captures.temp.store(data, fingerprint, "image/png")
        assert uri == f"gs://{gcs_permanent_bucket}/temp/{fingerprint}.bin"
        handle = build_persist_handler(
            store=raw.temp, permanent=raw.kept, captures=captures, complete=complete
        )

        await handle(
            make_persist_command_model(
                command_id="cap-1",
                blob_uri=uri,
                content_fingerprint=fingerprint,
                media_type="image/png",
            )
        )
        temp_object.reload()
        kept_object.reload()

        assert facts == [("cap-1", len(data))]
        assert kept_object.download_as_bytes() == data
        assert kept_object.content_type == "image/png"
        assert temp_object.custom_time is not None, "the temp tier's lifecycle rule reads it"
        assert kept_object.custom_time is None, "the kept tier never matches that rule"
        assert not any(blob.exists() for blob in strays), "no capture byte in a raw store"
    finally:
        for blob in (temp_object, kept_object, *strays):
            if blob.exists():
                blob.delete()
            assert not blob.exists(), f"teardown left {blob.name} behind"
        client.close()
