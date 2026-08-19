"""The object-store blob backend (#7).

Every test here runs against a fake client. That is not a shortcut around the
``@pytest.mark.gcs`` machinery — it is the same division ``test_replicate_*``
draws: what this module owns is the *decisions* (which key, which precondition,
what a lost race means), and none of them need a network to be wrong. The one
thing a fake cannot check is that the SDK accepts these arguments, which is what
the marked test in ``test_gcs_bucket.py`` is for.
"""

import io
import re

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed, ServiceUnavailable

from src.storage.gcs import GcsBlobStore

# `FakeBlob`, `FakeBucket`, `FakeClient` and the `store` / `bucket` / `client`
# fixtures live in `tests/storage/conftest.py` (CR #10) — three modules build a
# store now, and a fake defined in whichever one got there first is a fake whose
# edits reach further than its author can see.
from tests.storage.conftest import FINGERPRINT, FakeBlob, FakeClient

# The shape `src.worker.replicate` validates a `blob_uri`'s fingerprint against.
# Spelled again here rather than imported: this module is about the store, and a
# test that the probe key cannot be mistaken for content should not pass merely
# because the guard's regex was loosened.
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")


def test_store_writes_the_bytes_under_a_flat_content_addressed_key(store, bucket):
    store.store(b"hello", FINGERPRINT, "text/plain")

    assert bucket.objects == {f"blobs/{FINGERPRINT}.bin": b"hello"}


def test_the_key_is_not_sharded(store, bucket):
    """Sharding is a local-filesystem remedy, and an object store has no directories.

    ``LocalBlobStore._path_for`` splits two levels deep because a flat ext4
    directory degrades past ~10k entries. A bucket namespace is flat by
    construction, so carrying the shards over would be cargo — and it would put
    two derivations of the same key in the tree, which is the shape ``uri_for``
    exists to keep singular.
    """
    key = store.key_for(FINGERPRINT)

    assert key == f"blobs/{FINGERPRINT}.bin"
    assert "9f/2a" not in key


def test_store_returns_a_gs_uri(store):
    uri = store.store(b"hello", FINGERPRINT, "text/plain")

    assert uri == f"gs://a-temp-bucket/blobs/{FINGERPRINT}.bin"


def test_uri_for_derives_the_same_string_without_writing(store, bucket):
    """T3a: the replicate guard compares a message's value against this, never parses it."""
    assert store.uri_for(FINGERPRINT) == store.store(b"hello", FINGERPRINT, "text/plain")
    assert store.uri_for(FINGERPRINT) == f"gs://a-temp-bucket/blobs/{FINGERPRINT}.bin"


def test_an_empty_prefix_yields_a_bucket_rooted_key(bucket):
    store = GcsBlobStore("a-temp-bucket", prefix="", client=FakeClient(bucket))

    assert store.uri_for(FINGERPRINT) == f"gs://a-temp-bucket/{FINGERPRINT}.bin"


def test_the_media_type_becomes_the_object_content_type(store, bucket):
    """The one field the filesystem backend has nowhere to put.

    It matters beyond tidiness: a consumer reading these bytes over the GCS API
    gets the type from the object, where a `file://` reader had to carry
    `media_type` from the fact and keep the two associated itself.
    """
    store.store(b"%PDF-1.7", FINGERPRINT, "application/pdf")

    assert bucket.content_types[f"blobs/{FINGERPRINT}.bin"] == "application/pdf"


def test_store_never_overwrites_an_existing_object(store, bucket):
    """`if_generation_match=0` — a create, not a put.

    Content-addressed, so a second write of the same fingerprint is by
    definition the same bytes and overwriting would be a pointless round trip.
    The precondition also removes the interrupted-write window the local backend
    needs a temp-and-rename dance for: a failed upload leaves no object, rather
    than a truncated one at a name that asserts a sha256.
    """
    store.store(b"hello", FINGERPRINT, "text/plain")

    uri = store.store(b"hello", FINGERPRINT, "text/plain")

    assert uri == f"gs://a-temp-bucket/blobs/{FINGERPRINT}.bin"
    assert bucket.objects[f"blobs/{FINGERPRINT}.bin"] == b"hello"


def test_store_takes_one_round_trip_when_the_blob_is_new(store, bucket):
    """CR #6: the precondition *is* the existence check.

    An `exists()` first was a second network call on every command of a serial
    consume path, and it was the weaker construction besides — check-then-write
    races where `if_generation_match=0` cannot.
    """
    store.store(b"hello", FINGERPRINT, "text/plain")

    assert bucket.timeouts == [store._timeout]  # exactly one upload, no probe before it


def test_a_lost_create_race_is_a_success(store, bucket):
    """Two workers store the same new fingerprint at once; the loser is not wrong.

    Both upload with ``if_generation_match=0`` and one gets a 412. The bytes at
    that key are the same bytes by construction, so the only correct outcome is
    the URI — raising would dead-letter a command whose blob is sitting right
    there. Indistinguishable from an ordinary redelivery from here, which is
    why one branch serves both.
    """
    bucket.objects[f"blobs/{FINGERPRINT}.bin"] = b"hello"

    assert store.store(b"hello", FINGERPRINT, "text/plain") == store.uri_for(FINGERPRINT)


def test_storing_an_existing_blob_restarts_the_retention_clock(store, bucket):
    """The `_touch` equivalent, and the reason the lifecycle rule reads customTime.

    A re-fetch of unchanged bytes short-circuits the write but still publishes a
    fresh `blob_available`. Without moving the clock the fact would point at an
    object partway through its window — the local backend calls `os.utime` here
    for exactly this reason, and a creation-age lifecycle rule could not express
    it at all.
    """
    store.store(b"hello", FINGERPRINT, "text/plain")
    first = bucket.custom_times[f"blobs/{FINGERPRINT}.bin"]

    store.store(b"hello", FINGERPRINT, "text/plain")
    second = bucket.custom_times[f"blobs/{FINGERPRINT}.bin"]

    assert first is not None
    assert second is not None and second >= first


def test_a_touch_on_a_vanished_object_is_swallowed(store, bucket):
    """Lifecycle can delete between the 412 and the patch that follows it.

    Same window `LocalBlobStore._touch` swallows `FileNotFoundError` for, same
    verdict: the fallout is a `blob_uri` the reader re-issues against, where
    raising would dead-letter a command whose bytes were fine a moment earlier.
    """

    class VanishingBlob(FakeBlob):
        def upload_from_string(self, *args, **kwargs):
            # The object was there when the precondition was evaluated...
            raise PreconditionFailed("object already exists")

        def patch(self):
            # ...and gone by the time its clock was restarted.
            raise NotFound("no such object")

    bucket.blob = lambda name: VanishingBlob(bucket, name)

    assert store.store(b"hello", FINGERPRINT, "text/plain") == store.uri_for(FINGERPRINT)


def test_exists_asks_the_bucket(store, bucket):
    assert store.exists(FINGERPRINT) is False

    bucket.objects[f"blobs/{FINGERPRINT}.bin"] = b"hello"

    assert store.exists(FINGERPRINT) is True


def test_open_reads_the_bytes_back(store):
    store.store(b"hello", FINGERPRINT, "text/plain")

    assert store.open(FINGERPRINT) == b"hello"


def test_open_stream_is_seekable_and_positioned_at_the_start(store):
    """Seekable is a hard requirement, not a preference (co-core `GcsCreateIfAbsent`).

    The replicate driver computes the local md5 only on the 412 path, *after* the
    failed conditional create has moved the position — so a non-seekable handle
    fails on exactly the redelivery T4 exists to handle.
    """
    store.store(b"hello", FINGERPRINT, "text/plain")

    with store.open_stream(FINGERPRINT) as handle:
        assert handle.seekable()
        assert handle.read() == b"hello"
        handle.seek(0)
        assert handle.read() == b"hello"


def test_open_stream_spills_a_large_blob_to_disk(store):
    """A `SpooledTemporaryFile`, not a `BytesIO`.

    The whole reason co-core takes a stream is that Replicator's only use for the
    bytes is to hand them on. Pulling a 64 MiB artifact into memory to feed a
    driver that streams it would give the seam back the cost it was built to
    avoid — while still satisfying every assertion above, which is why the spill
    is asserted rather than assumed.
    """
    big = b"x" * (GcsBlobStore.SPOOL_MAX_BYTES + 1)
    store.store(big, FINGERPRINT, "application/octet-stream")

    with store.open_stream(FINGERPRINT) as handle:
        assert handle._rolled is True
        assert handle.read() == big


def test_a_small_blob_stays_in_memory(store):
    store.store(b"hello", FINGERPRINT, "text/plain")

    with store.open_stream(FINGERPRINT) as handle:
        assert handle._rolled is False


def test_the_store_satisfies_the_blob_store_protocol(store, tmp_path):
    """Structural, not nominal — the seam is a ``Protocol`` on purpose.

    Asserted against ``LocalBlobStore`` in the same breath, because what the loop
    depends on is that the two are *substitutable*: a member the object store
    grew and the filesystem one did not is a call site that works until an
    operator flips ``REPLICATOR_BLOB_BACKEND`` back.
    """
    from src.storage.base import BlobStore
    from src.storage.local import LocalBlobStore

    members = BlobStore.__protocol_attrs__
    assert members  # a check over an empty set passes forever while proving nothing

    for name in members:
        assert callable(getattr(store, name)), name
        assert callable(getattr(LocalBlobStore(tmp_path), name)), name


def test_reads_do_not_go_through_a_shared_handle(store, bucket):
    """Every operation gets a fresh blob handle.

    A cached one carries a generation and metadata from whenever it was
    fetched, and this store is shared by both command loops and the retention
    task. Stale state there is a read against an object that no longer exists at
    that generation — a failure that appears only under concurrency.
    """
    store.store(b"hello", FINGERPRINT, "text/plain")

    # Held rather than counted inline: CPython reuses the id of an object that
    # was already collected, so a set comprehension over three short-lived
    # handles collapses to one entry and the assertion measures nothing.
    handles = [store._blob(FINGERPRINT) for _ in range(3)]

    assert len({id(handle) for handle in handles}) == 3


def test_download_to_file_receives_a_real_handle(store, bucket):
    """Guards the shape the SDK is handed, which the fake would otherwise fake away."""
    store.store(b"hello", FINGERPRINT, "text/plain")
    sink = io.BytesIO()

    bucket.blob(store.key_for(FINGERPRINT)).download_to_file(sink)

    assert sink.getvalue() == b"hello"


def test_preflight_probes_with_a_one_object_listing(store, client):
    """CR #1: the probe has to be able to fail.

    A listing scoped to the store's own prefix, capped at one object, and
    **iterated** — the SDK's listing is lazy, so a probe that never advances the
    iterator issues no request at all and checks nothing.
    """
    store.preflight()

    assert client.listings == [{"max_results": 1, "prefix": "blobs", "timeout": store._timeout}]


def test_preflight_raises_when_the_bucket_is_not_there(bucket):
    """The failure the old probe could not see, and the reason it is a listing.

    `Blob.exists()` catches `NotFound` and returns `False` — for a missing bucket
    exactly as for a missing object — so an existence-based preflight passed on a
    misspelled `REPLICATOR_BLOB_BUCKET` and the worker booted announcing a
    `blob_uri` into a bucket that does not exist. Listing raises.
    """
    store = GcsBlobStore("a-temp-bucket", prefix="blobs", client=FakeClient(bucket, missing=True))

    with pytest.raises(NotFound):
        store.preflight()


def test_preflight_passes_on_an_empty_bucket(store):
    """Empty is a healthy state, not a missing one — a fresh deployment's normal case."""
    store.preflight()


def test_a_missing_object_reads_as_a_file_not_found(store):
    """CR #3: the protocol's vocabulary for gone, not the SDK's.

    `src.worker.replicate` catches one exception for a swept blob. Translating
    here is what keeps that a single catch instead of one per backend — and keeps
    `google.api_core` out of a module that is deliberately provider-agnostic.
    """
    with pytest.raises(FileNotFoundError):
        store.open(FINGERPRINT)

    with pytest.raises(FileNotFoundError):
        store.open_stream(FINGERPRINT)


def test_a_provider_failure_keeps_its_status_for_the_caller_to_classify(store, bucket):
    """Everything that is not "gone" propagates intact (CR #3).

    The store holds no retry policy: a 503 is transient and a 403 is not, and
    which one closes a command is a decision that needs to know which command is
    in flight. `src.core.errors.is_terminal_provider_status` is where that is
    made, from the status this exception still carries.
    """

    class Unavailable(FakeBlob):
        def download_as_bytes(self, timeout=None):
            raise ServiceUnavailable("backend error")

    bucket.blob = lambda name: Unavailable(bucket, name)

    with pytest.raises(ServiceUnavailable) as caught:
        store.open(FINGERPRINT)

    assert caught.value.code == 503


def test_a_failed_download_does_not_leak_its_spool_file(store, bucket, monkeypatch):
    """The spool is created before the download, so a failure has to close it.

    On the disk-spilled path that handle is a real file. Leaking one per failed
    read is not hypothetical — a bucket a worker cannot reach fails every read on
    a loop, and the local backend's `_write_atomically` carries the same
    unlink-on-failure for exactly this reason.
    """
    closed = []

    class Failing(FakeBlob):
        def download_to_file(self, handle, timeout=None):
            monkeypatch.setattr(handle, "close", lambda: closed.append(True))
            # A provider failure rather than a missing object: both close the
            # handle, and this one keeps the assertion about *leaking* separate
            # from the translation `NotFound` now gets (CR #3).
            raise ServiceUnavailable("backend error mid-read")

    bucket.blob = lambda name: Failing(bucket, name)

    with pytest.raises(ServiceUnavailable):
        store.open_stream(FINGERPRINT)

    assert closed == [True]
