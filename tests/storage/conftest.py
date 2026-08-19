"""Fakes for the object-store backend, shared by every module that needs one (#7).

Hoisted here from `test_gcs.py` (CR #10). Three test modules now build a
`GcsBlobStore` — this package's unit tests, the replicate source guard, and the
boundaries charter's characterization pair — and cross-importing a fake from
whichever module happened to define it first meant a change made for one of them
could break the other two. The project's convention already says shared wiring
belongs in the package `conftest.py`.

**These fakes owe their fidelity to the SDK, not to the tests.** CR #1 and #2
were one bug in two places: `preflight` could not detect a missing bucket because
`Blob.exists()` swallows `NotFound`, and the test could not catch that because
the fake raised where the real client returns `False`. So each method here
mirrors what `google-cloud-storage` actually does, and where the real behaviour
is surprising it is commented rather than smoothed over.
"""

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed

from src.storage.gcs import GcsBlobStore

FINGERPRINT = "9f2a7c1e" + "0" * 56


class FakeBlob:
    """Just enough `google.cloud.storage.Blob` to answer this backend's questions."""

    def __init__(self, bucket, name):
        self._bucket = bucket
        self.name = name
        self.custom_time = None
        self.content_type = None
        self.patched = 0

    def exists(self):
        """Mirrors the SDK: **`False` for anything absent, never a raise.**

        The real `Blob.exists()` catches `NotFound` and returns `False` — which
        it does for a missing *bucket* just as readily as a missing object. That
        is exactly why `preflight` cannot be built on it (CR #1), and a fake that
        raised here is what let the original version look tested (CR #2).
        """
        return self.name in self._bucket.objects

    def upload_from_string(self, data, content_type=None, if_generation_match=None, timeout=None):
        if if_generation_match == 0 and self.name in self._bucket.objects:
            raise PreconditionFailed("object already exists")
        self._bucket.objects[self.name] = data
        self.content_type = content_type
        self._bucket.content_types[self.name] = content_type
        # Set at creation by the real SDK too: `custom_time` is object metadata
        # carried on the upload, not a second call.
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

    def blob(self, name):
        return FakeBlob(self, name)


class FakeClient:
    """A client over one bucket, with the listing `preflight` probes through."""

    def __init__(self, bucket=None, *, missing: bool = False):
        self._bucket = bucket if bucket is not None else FakeBucket("a-temp-bucket")
        # "this bucket is not there" — the state a misspelled
        # `REPLICATOR_BLOB_BUCKET` puts the worker in, and the one the listing
        # probe exists to report.
        self._missing = missing
        self.listings: list[dict] = []

    def bucket(self, name):
        assert name == self._bucket.name
        return self._bucket

    def list_blobs(self, bucket, max_results=None, prefix=None, timeout=None):
        """Lazy, like the real one — the request happens when it is iterated."""
        self.listings.append({"max_results": max_results, "prefix": prefix, "timeout": timeout})

        def _iter():
            if self._missing:
                raise NotFound(f"bucket {bucket.name} not found")
            yield from sorted(self._bucket.objects)

        return _iter()


@pytest.fixture
def bucket():
    return FakeBucket("a-temp-bucket")


@pytest.fixture
def client(bucket):
    return FakeClient(bucket)


@pytest.fixture
def store(bucket, client):
    return GcsBlobStore("a-temp-bucket", prefix="blobs", client=client)
