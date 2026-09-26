"""Pins on the properties Replicator consumes from the cluster's shared store (#114).

The store itself is cannobserv's — ``co_core_sync.drivers.blobstore`` behind
``co_core.pure.util.blobstore.BlobStore`` (cannobserv#475) — and its decisions
are tested there. What is pinned *here* is narrower and ours: the handful of
behaviours the consume and replicate paths rely on, asserted against the real
classes over the SDK fakes, so a change to the lifted store breaks this suite
rather than a running worker.

- **URI spellings are byte-identical to what the bus already carries.** A
  ``blob_available`` fact's ``blob_uri`` is compared, never parsed, by the
  replicate guard (T3a); a changed spelling would refuse every in-flight
  command as ``invalid_source``.
- **``touch_on_rereference=True`` is the temp tier's shape, and not the
  store's default.** Off, the object store stamps no ``customTime`` at all,
  so the bucket's ``daysSinceCustomTime`` rule never matches and the temp
  bucket empties only through its cost backstop; locally, the sweep's "since
  last referenced" clock stops moving.
- **A missing blob is ``FileNotFoundError`` on both backends** — the one catch
  ``src.worker.replicate`` keeps for "these bytes are gone".
- **Every create is checksummed**, so a body corrupted in transit is refused
  rather than stored under a name that asserts its digest.
- **``preflight()`` raises on a missing bucket** — what ``preflight_object_store``
  fails the boot on — and **``open_stream()`` is seekable at the start**, what
  the replicate driver requires of the handle ``_write`` passes it.
"""

import os
import time
from pathlib import Path

import pytest
from co_core.pure.util.blobstore import BlobStore
from co_core_sync.drivers.blobstore import GcsBlobStore, LocalBlobStore
from google.api_core.exceptions import NotFound

from tests.storage.conftest import FINGERPRINT, FakeBucket, FakeClient


def _local_path(root: Path, fingerprint: str) -> Path:
    """Where the local backend puts a blob — spelled out, not asked of the store."""
    return root.resolve() / fingerprint[0:2] / fingerprint[2:4] / f"{fingerprint}.bin"


def test_the_object_store_uri_is_the_spelling_the_bus_carries(store):
    assert store.uri_for(FINGERPRINT) == f"gs://a-temp-bucket/blobs/{FINGERPRINT}.bin"


def test_the_object_store_returns_the_uri_it_would_derive(store):
    assert store.store(b"bytes", FINGERPRINT, "text/plain") == store.uri_for(FINGERPRINT)


def test_the_local_uri_is_the_spelling_the_bus_carries(tmp_path):
    store = LocalBlobStore(tmp_path, touch_on_rereference=True)
    expected = _local_path(tmp_path, FINGERPRINT).as_uri()

    assert store.store(b"bytes", FINGERPRINT, "text/plain") == expected
    assert store.uri_for(FINGERPRINT) == expected


def test_a_re_store_moves_the_object_store_retention_clock(store, bucket):
    store.store(b"bytes", FINGERPRINT, "text/plain")
    key = store.key_for(FINGERPRINT)
    stamped_at_create = bucket.custom_times[key]

    store.store(b"bytes", FINGERPRINT, "text/plain")

    assert stamped_at_create is not None, "the create itself must carry a customTime"
    assert bucket.patches == 1
    assert bucket.custom_times[key] >= stamped_at_create


def test_the_permanent_tier_default_never_moves_a_clock(bucket, client):
    """The hazard the worker's ``touch_on_rereference=True`` pin exists for.

    Left at the store's default, a temp-tier deployment would look healthy —
    every store succeeds — while no object ever acquired the ``customTime`` the
    lifecycle rule reaps on.
    """
    store = GcsBlobStore("a-temp-bucket", prefix="blobs", client=client)

    store.store(b"bytes", FINGERPRINT, "text/plain")
    store.store(b"bytes", FINGERPRINT, "text/plain")

    assert bucket.patches == 0
    assert bucket.custom_times[store.key_for(FINGERPRINT)] is None


def test_a_re_store_moves_the_local_retention_clock(tmp_path):
    store = LocalBlobStore(tmp_path, touch_on_rereference=True)
    store.store(b"bytes", FINGERPRINT, "text/plain")
    path = _local_path(tmp_path, FINGERPRINT)
    backdated = time.time() - 3600
    os.utime(path, (backdated, backdated))

    store.store(b"bytes", FINGERPRINT, "text/plain")

    assert path.stat().st_mtime > backdated + 1800


def test_every_create_is_checksummed(store, bucket):
    store.store(b"bytes", FINGERPRINT, "text/plain")

    assert bucket.checksums == ["crc32c"]


def test_a_missing_blob_is_a_file_not_found_on_both_backends(store, tmp_path):
    local = LocalBlobStore(tmp_path)

    for backend in (store, local):
        with pytest.raises(FileNotFoundError):
            backend.open(FINGERPRINT)
        with pytest.raises(FileNotFoundError):
            backend.open_stream(FINGERPRINT)


def test_both_backends_satisfy_the_shared_protocol(store, tmp_path):
    """Structural, not nominal — the seam is a ``Protocol`` on purpose.

    Asserted against both in the same breath, because what the loop depends on
    is that the two are *substitutable*: a member one backend grew and the other
    did not is a call site that works until an operator flips
    ``REPLICATOR_BLOB_BACKEND`` back.
    """
    members = BlobStore.__protocol_attrs__
    assert members  # a check over an empty set passes forever while proving nothing

    for name in members:
        assert callable(getattr(store, name)), name
        assert callable(getattr(LocalBlobStore(tmp_path), name)), name


def test_preflight_raises_when_the_bucket_is_not_there():
    """The CR #1 bug, kept pinned here because the boot depends on it.

    ``preflight_object_store`` fails the worker's boot on this raise. An
    existence check cannot detect a missing bucket — the SDK swallows the 404 —
    so a store whose preflight quietly became one would boot clean against a
    misspelled ``REPLICATOR_BLOB_BUCKET`` and announce ``blob_uri``s nobody can
    open.
    """
    bucket = FakeBucket("a-temp-bucket")
    store = GcsBlobStore("a-temp-bucket", prefix="blobs", client=FakeClient(bucket, missing=True))

    with pytest.raises(NotFound):
        store.preflight()


def test_preflight_is_a_one_object_listing_that_passes_on_an_empty_bucket(store, client):
    store.preflight()

    (listing,) = client.listings
    assert (listing["max_results"], listing["prefix"]) == (1, "blobs")


def test_open_stream_is_seekable_and_positioned_at_the_start(store, tmp_path):
    """What ``AsyncGcsDriver.create_if_absent`` requires of the handle ``_write`` hands it.

    The driver reads the local md5 only on the 412 path, after the failed create
    has moved the position, so the stream must seek — and it refuses one that
    cannot with a ``ValueError`` that closes the command as ``handler_error``.
    Positioned at the start, because a handle left at its end uploads nothing.
    """
    local = LocalBlobStore(tmp_path)

    for backend in (store, local):
        backend.store(b"bytes", FINGERPRINT, "application/pdf")
        with backend.open_stream(FINGERPRINT) as handle:
            assert handle.seekable()
            assert handle.tell() == 0
            assert handle.read() == b"bytes"
            handle.seek(0)
            assert handle.read(3) == b"byt"
