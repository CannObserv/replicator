"""The object-store backend against a real bucket (#7, `@pytest.mark.gcs`).

Named `_bucket` rather than `_gcs`, which is the convention for a marked file
(`test_replicate_writer_gcs.py`): the module under test is already `gcs.py`, so
the suffix would double into `test_gcs_gcs.py`. `_integration` was the other
candidate and is worse — that suffix names the *other* marker, and this file
carries `gcs`, not `integration`.

`test_gcs.py` owns the *decisions* — which key, which precondition, what a lost
race means — and answers them against a fake, because none of them need a
network to be wrong. What a fake cannot answer is whether the SDK accepts these
arguments at all: `if_generation_match=0` on `upload_from_string`, `custom_time`
as a settable property, `patch()` as the way to move it, and `NotFound` as what
comes back for an object that is gone. Every one of those is a real call whose
signature a fake asserts nothing about.

Excluded by default (`-m 'not integration and not gcs'`) and skipped unless the
host has provisioned `REPLICATOR_TEST_BLOB_BUCKET` and
`REPLICATOR_TEST_GCS_CREDENTIALS`. Run with `uv run pytest --no-cov -m gcs`.

**The bucket is not the replicate one**, and the tests here are why the split is
not fussiness: these create objects and clean them up, which needs
`storage.objects.delete` — the exact permission the replicate destination's grant
withholds so that T4's "never overwrite, never delete" is enforced at IAM rather
than only in our code.
"""

import uuid

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage

from src.storage.gcs import GcsBlobStore

pytestmark = pytest.mark.gcs

# A prefix nothing else writes under, so a failed run cannot be mistaken for
# production litter and a concurrent run cannot collide with this one.
TEST_PREFIX = "pytest-blobs"


@pytest.fixture
def fingerprint() -> str:
    """A unique content-addressed name per test.

    Deliberately not a fixed literal: these tests create real objects, and a
    shared key would make a crashed run's leftovers decide whether the next run's
    ``store`` takes the create path or the short-circuit — the two branches this
    module exists to tell apart.
    """
    return uuid.uuid4().hex + uuid.uuid4().hex  # 64 hex characters, like a sha256


@pytest.fixture
def store(gcs_blob_bucket, fingerprint):
    """A store over the provisioned test bucket, cleaned up after itself."""
    store = GcsBlobStore(gcs_blob_bucket, prefix=TEST_PREFIX)
    yield store
    bucket = storage.Client().bucket(gcs_blob_bucket)
    try:
        bucket.blob(store.key_for(fingerprint)).delete()
    except NotFound:
        pass


def test_preflight_passes_against_the_provisioned_bucket(store):
    """The boot check, against the thing it is a check on.

    A green here is what makes a failure at boot mean "this host is
    misconfigured" rather than "the check never worked".
    """
    store.preflight()


def test_a_round_trip_stores_reads_and_records_the_content_type(
    store, fingerprint, gcs_blob_bucket
):
    uri = store.store(b"integration bytes", fingerprint, "text/plain")

    assert uri == store.uri_for(fingerprint)
    assert store.exists(fingerprint) is True
    assert store.open(fingerprint) == b"integration bytes"
    # The field the filesystem backend had nowhere to put, read back off the
    # object rather than off the request we sent: a consumer fetching these bytes
    # over the API gets the type from here, and nothing else carries it.
    assert (
        storage.Client().bucket(gcs_blob_bucket).get_blob(store.key_for(fingerprint)).content_type
        == "text/plain"
    )


def test_a_second_store_short_circuits_rather_than_rewriting(store, fingerprint):
    """The ordinary redelivery path: `exists` answers yes and no write happens.

    The byte path relies on this — the dedupe key is written *after* the handler
    returns, so re-running an already-successful handler is an expected outcome
    rather than an error.
    """
    store.store(b"integration bytes", fingerprint, "text/plain")

    assert store.store(b"integration bytes", fingerprint, "text/plain") == store.uri_for(
        fingerprint
    )
    assert store.open(fingerprint) == b"integration bytes"


def test_a_lost_create_race_is_a_success_against_the_real_api(store, fingerprint, monkeypatch):
    """`if_generation_match=0` against the live service — the point of this file.

    The short-circuit above never reaches the precondition, so on its own it
    would leave the 412 handling untested against anything but a fake that
    raises whatever it was told to. Here `exists` is forced to report the state
    *both* workers saw before either wrote, which is exactly the race: the second
    upload really is refused by GCS, and the store really does have to read that
    refusal as a success with a different author.

    The failure this guards is unmissable if it happens and invisible until it
    does — a real 412 reaching the handler dead-letters a command whose blob is
    sitting at the key it names.
    """
    store.store(b"integration bytes", fingerprint, "text/plain")
    monkeypatch.setattr(GcsBlobStore, "exists", lambda self, fp: False)

    assert store.store(b"integration bytes", fingerprint, "text/plain") == store.uri_for(
        fingerprint
    )


def test_the_retention_clock_is_a_real_settable_field(store, fingerprint, gcs_blob_bucket):
    """`customTime` is what the lifecycle rule reads, so it has to actually be set.

    The one property in this module with no local symptom if it is wrong: a
    `custom_time` that silently failed to persist would leave every blob reaped
    on creation age instead, which looks identical until a re-referenced blob
    disappears inside its announced window.
    """
    store.store(b"integration bytes", fingerprint, "text/plain")
    bucket = storage.Client().bucket(gcs_blob_bucket)

    first = bucket.get_blob(store.key_for(fingerprint)).custom_time
    store.store(b"integration bytes", fingerprint, "text/plain")
    second = bucket.get_blob(store.key_for(fingerprint)).custom_time

    assert first is not None
    assert second is not None
    assert second >= first


def test_a_missing_object_reads_as_a_file_not_found(store, fingerprint):
    """The translation CR #3 introduced, against the SDK that motivates it.

    `src/worker/replicate.py` turns a swept blob into `blob_expired` rather than
    a burnt delivery ceiling, and it does that on one catch for both backends —
    which only works because the store translates. The unit tests assert the
    translation against a fake; what needs a real bucket is that the thing being
    translated is genuinely what the SDK raises, so the **cause** is asserted
    too. Without that this would pass just as happily if `open` raised
    `FileNotFoundError` for some entirely different reason.

    This test is also the reason the marked suite matters: it shipped asserting
    the pre-CR-#3 behaviour and stayed green for a week, because no host had a
    `REPLICATOR_TEST_BLOB_BUCKET` to run it against.
    """
    assert store.exists(fingerprint) is False

    with pytest.raises(FileNotFoundError) as caught:
        store.open(fingerprint)

    assert isinstance(caught.value.__cause__, NotFound)

    with pytest.raises(FileNotFoundError) as streamed:
        store.open_stream(fingerprint)

    assert isinstance(streamed.value.__cause__, NotFound)


def test_a_stream_of_a_stored_blob_is_seekable(store, fingerprint):
    store.store(b"integration bytes", fingerprint, "application/pdf")

    with store.open_stream(fingerprint) as handle:
        assert handle.seekable()
        assert handle.read() == b"integration bytes"
