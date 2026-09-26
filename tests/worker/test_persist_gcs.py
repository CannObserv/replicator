"""Plan step 6's done condition, against real buckets (#114).

A blob stored in the temp test bucket is persisted into the permanent test twin
twice. The first is a create; the second finds the object and is a no-op
success that re-emits its fact. The object's generation proves the second wrote
nothing. Touch is off on the permanent store, so no ``customTime`` is stamped.

Unique bytes per run, so the content-addressed key is this run's own, and both
objects are deleted in the ``finally`` and asserted gone.
"""

import hashlib
import uuid

import pytest
from co_core_sync.drivers.blobstore import GcsBlobStore
from google.cloud import storage

from src.worker.persist import build_persist_handler
from tests.worker.test_loop_spec import make_persist_command_model

pytestmark = pytest.mark.gcs


async def test_a_temp_blob_is_persisted_and_a_second_persist_is_a_no_op(
    gcs_blob_bucket, gcs_permanent_bucket
):
    data = b"%PDF-1.4 persisted by the gcs job " + uuid.uuid4().hex.encode()
    fingerprint = hashlib.sha256(data).hexdigest()
    client = storage.Client()
    temp = GcsBlobStore(gcs_blob_bucket, client=client, touch_on_rereference=True)
    permanent = GcsBlobStore(gcs_permanent_bucket, client=client)
    temp_object = client.bucket(gcs_blob_bucket).blob(temp.key_for(fingerprint))
    kept = client.bucket(gcs_permanent_bucket).blob(permanent.key_for(fingerprint))
    facts: list[tuple[str, int]] = []

    async def complete(command, size_bytes):
        facts.append((command.command_id, size_bytes))

    try:
        uri = temp.store(data, fingerprint, "application/pdf")
        handle = build_persist_handler(store=temp, permanent=permanent, complete=complete)

        await handle(
            make_persist_command_model(
                command_id="per-1", blob_uri=uri, content_fingerprint=fingerprint
            )
        )
        kept.reload()
        generation = kept.generation

        await handle(
            make_persist_command_model(
                command_id="per-2", blob_uri=uri, content_fingerprint=fingerprint
            )
        )
        kept.reload()

        assert facts == [("per-1", len(data)), ("per-2", len(data))]
        assert kept.download_as_bytes() == data
        assert kept.content_type == "application/pdf"
        assert kept.generation == generation, "the second persist must write nothing"
        assert kept.custom_time is None, "touch is off on the permanent store"
    finally:
        for blob in (temp_object, kept):
            if blob.exists():
                blob.delete()
            assert not blob.exists(), f"teardown left {blob.name} behind"
        client.close()
