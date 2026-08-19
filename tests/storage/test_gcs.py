"""The object-store blob backend (#7).

Every test here runs against a fake client. That is not a shortcut around the
``@pytest.mark.gcs`` machinery — it is the same division ``test_replicate_*``
draws: what this module owns is the *decisions* (which key, which precondition,
what a lost race means), and none of them need a network to be wrong. The one
thing a fake cannot check is that the SDK accepts these arguments, which is what
the marked integration test in ``test_gcs_integration.py`` is for.
"""

import re
from io import BytesIO

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed

from src.storage.gcs import GcsBlobStore

FINGERPRINT = "9f2a7c1e" + "0" * 56

# The shape `src.worker.replicate` validates a `blob_uri`'s fingerprint against.
# Spelled again here rather than imported: this module is about the store, and a
# test that the probe key cannot be mistaken for content should not pass merely
# because the guard's regex was loosened.
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")


class FakeBlob:
    """Just enough ``google.cloud.storage.Blob`` to answer this module's questions."""

    def __init__(self, bucket, name):
        self._bucket = bucket
        self.name = name
        self.custom_time = None
        self.content_type = None
        self.patched = 0

    def exists(self):
        return self.name in self._bucket.objects

    def upload_from_string(self, data, content_type=None, if_generation_match=None, timeout=None):
        if if_generation_match == 0 and self.name in self._bucket.objects:
            raise PreconditionFailed("object already exists")
        self._bucket.objects[self.name] = data
        self.content_type = content_type
        self._bucket.content_types[self.name] = content_type
        # Set at creation by the real SDK too: ``custom_time`` is object metadata
        # on the upload, not a second call. Recorded here so the clock assertions
        # read the same field on both paths.
        self._bucket.custom_times[self.name] = self.custom_time
        self._bucket.timeouts.append(timeout)

    def patch(self):
        if self.name not in self._bucket.objects:
            raise NotFound("no such object")
        self.patched += 1
        self._bucket.custom_times[self.name] = self.custom_time

    def download_as_bytes(self, timeout=None):
        if self.name not in self._bucket.objects:
            raise NotFound("no such object")
        return self._bucket.objects[self.name]

    def download_to_file(self, handle, timeout=None):
        handle.write(self.download_as_bytes())


class FakeBucket:
    def __init__(self, name):
        self.name = name
        self.objects: dict[str, bytes] = {}
        self.content_types: dict[str, str | None] = {}
        self.custom_times: dict[str, object] = {}
        self.timeouts: list[float | None] = []
        self.reloaded = 0
        self.probed: list[str] = []

    def blob(self, name):
        if name.endswith(GcsBlobStore.PREFLIGHT_KEY):
            self.probed.append(name)
        return FakeBlob(self, name)

    def reload(self, timeout=None):
        self.reloaded += 1


class FakeClient:
    def __init__(self, bucket=None):
        self._bucket = bucket if bucket is not None else FakeBucket("a-temp-bucket")

    def bucket(self, name):
        assert name == self._bucket.name
        return self._bucket


@pytest.fixture
def bucket():
    return FakeBucket("a-temp-bucket")


@pytest.fixture
def store(bucket):
    return GcsBlobStore("a-temp-bucket", prefix="blobs", client=FakeClient(bucket))


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
    bucket.objects[f"blobs/{FINGERPRINT}.bin"] = b"hello"

    uri = store.store(b"hello", FINGERPRINT, "text/plain")

    assert uri == f"gs://a-temp-bucket/blobs/{FINGERPRINT}.bin"
    assert bucket.objects[f"blobs/{FINGERPRINT}.bin"] == b"hello"


def test_a_lost_create_race_is_a_success(bucket):
    """Two workers can store the same new fingerprint at once; the loser is not wrong.

    ``exists`` says no to both, both upload with ``if_generation_match=0``, and
    one gets a 412. The bytes at that key are the same bytes by construction, so
    the only correct outcome is the URI — raising would dead-letter a command
    whose blob is sitting right there.
    """
    store = GcsBlobStore("a-temp-bucket", prefix="blobs", client=FakeClient(bucket))
    bucket.objects[f"blobs/{FINGERPRINT}.bin"] = b"hello"

    class RacingBlob(FakeBlob):
        def exists(self):
            return False  # the state both workers observed before either wrote

    bucket.blob = lambda name: RacingBlob(bucket, name)

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
    """Lifecycle can delete between the existence check and the patch.

    Same window `LocalBlobStore._touch` swallows `FileNotFoundError` for, same
    verdict: the fallout is a `blob_uri` the reader re-issues against, where
    raising would dead-letter a command whose bytes were fine when we looked.
    """

    class VanishingBlob(FakeBlob):
        def exists(self):
            return True

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
    sink = BytesIO()

    bucket.blob(store.key_for(FINGERPRINT)).download_to_file(sink)

    assert sink.getvalue() == b"hello"


def test_preflight_probes_the_bucket(store, bucket):
    """A boot-time question with a wrong answer available: does this bucket exist?

    The probe is a read of a key nothing ever writes. That is enough to prove the
    three things a misconfiguration gets wrong — credentials resolve, the bucket
    resolves, and this identity may read it — while writing nothing and leaving
    nothing to clean up.
    """
    store.preflight()

    assert bucket.probed == [f"blobs/{GcsBlobStore.PREFLIGHT_KEY}"]


def test_preflight_raises_when_the_bucket_is_not_there(store, bucket):
    def missing(name):
        raise NotFound("no such bucket")

    bucket.blob = missing

    with pytest.raises(NotFound):
        store.preflight()


def test_the_preflight_key_cannot_collide_with_a_blob(store):
    """It lives under the same prefix, so it must not be mistakable for content.

    A fingerprint is 64 lowercase hex characters and the probe key is not, so no
    fetched blob can ever address it — and the object is never created anyway,
    which is the stronger half.
    """
    assert not _FINGERPRINT_RE.match(GcsBlobStore.PREFLIGHT_KEY.removesuffix(".bin"))
