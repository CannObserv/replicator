"""Storage runs off the event-loop thread (#7).

The ``BlobStore`` seam is a synchronous ``Protocol`` and stays one. An async twin
would be a wide refactor across the handler, the replicate path and the sweeper
for no behavioural gain, and ``co_core_aio.gcs.AsyncGcsDriver`` reached the same
arrangement from the other side — ``google-cloud-storage`` ships no async client,
so its awaitable is ``asyncio.to_thread`` around the blocking SDK anyway.

What that costs is a rule the *call sites* have to keep: every ``BlobStore`` call
from a coroutine goes through ``asyncio.to_thread``. This module holds it, and
holds it by **observation** — a test that grepped the source for ``to_thread``
would pass on a call that awaits one and then calls the store directly two lines
down. These record the thread the store actually ran on.

The rule predates the object store and is not only about it. A GCS PUT is a
network round trip that would hold the loop for the length of a multi-megabyte
body, and ``LocalBlobStore._write_atomically`` ends in an ``fsync`` — a blocking
disk sync, on a VM shared with three other services. Both backends are wrapped,
so a `REPLICATOR_BLOB_BACKEND` flip cannot change whether the loop stalls.
"""

import threading

import pytest
from co_core.pure.util.gcs import GcsCreateOutcome

from src.core.config import get_settings
from src.storage.local import LocalBlobStore
from src.worker.handler import build_handler
from tests.worker.conftest import FakeFetcher, command
from tests.worker.test_replicate_writer import (
    FakeGcs,
    handler_for,
    result,
)
from tests.worker.test_replicate_writer import (
    command as replicate_command,
)

FINGERPRINT = "e" * 64
PUBLIC_URL = "https://storage.googleapis.com/example-replication-bucket/organizations/x/report.pdf"


class ThreadRecordingStore(LocalBlobStore):
    """A real store that remembers which thread each call arrived on.

    Subclassing the real backend rather than faking one: what is under test is
    the *call site*, so the store has to behave exactly as the one in production
    does or the surrounding path stops exercising anything.
    """

    def __init__(self, root):
        super().__init__(root)
        self.threads: dict[str, int] = {}

    def store(self, data, fingerprint, media_type):
        self.threads["store"] = threading.get_ident()
        return super().store(data, fingerprint, media_type)

    def exists(self, fingerprint):
        self.threads["exists"] = threading.get_ident()
        return super().exists(fingerprint)

    def open_stream(self, fingerprint):
        self.threads["open_stream"] = threading.get_ident()
        return super().open_stream(fingerprint)


@pytest.fixture
def store(tmp_path):
    return ThreadRecordingStore(tmp_path)


async def test_the_byte_path_stores_off_the_loop_thread(store, fake_redis):
    """``store`` blocks — on a PUT, or on the local backend's ``fsync``."""
    handler = build_handler(
        fetcher=FakeFetcher(),
        store=store,
        client=fake_redis,
        settings=get_settings(),
    )

    await handler(command())

    assert store.threads["store"] != threading.get_ident()


async def test_the_byte_path_checks_existence_off_the_loop_thread(store, fake_redis):
    """``exists`` is a network round trip on the object store, not a ``stat``.

    Cheap on a filesystem and easy to leave unwrapped for that reason — which is
    exactly how a rule that holds for the expensive call gets quietly broken for
    the one beside it.
    """
    handler = build_handler(
        fetcher=FakeFetcher(),
        store=store,
        client=fake_redis,
        settings=get_settings(),
    )

    await handler(command())

    assert store.threads["exists"] != threading.get_ident()


async def test_the_replicate_path_opens_the_blob_off_the_loop_thread(store):
    """``open_stream`` downloads the entire blob on the object-store backend.

    The one call in this repo where the difference is unmissable: on the local
    backend it is an ``open``, and on GCS it is a full download of up to
    ``REPLICATOR_MAX_BLOB_BYTES`` before the handle is usable.
    """
    blob_uri = store.store(b"artifact bytes", FINGERPRINT, "application/pdf")
    store.threads.clear()
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    await handler_for(store, writer)(replicate_command(blob_uri))

    assert store.threads["open_stream"] != threading.get_ident()


async def test_the_replicate_guard_resolves_the_source_off_the_loop_thread(store):
    """``locate_blob`` calls ``exists``, which is a round trip on the object store."""
    blob_uri = store.store(b"artifact bytes", FINGERPRINT, "application/pdf")
    store.threads.clear()
    writer = FakeGcs(result(GcsCreateOutcome.WROTE, public_url=PUBLIC_URL))

    await handler_for(store, writer)(replicate_command(blob_uri))

    assert store.threads["exists"] != threading.get_ident()


async def test_the_recording_store_would_notice_an_unwrapped_call(store, fake_redis):
    """The detector, tested — a comparison that can never fail proves nothing.

    Every assertion above is ``!=`` against the current thread, which is exactly
    the shape that passes forever if the recorded value is never written. So one
    call is made *directly*, on the loop thread, and asserted to be seen there:
    it fixes that these tests can tell the two situations apart.
    """
    store.exists(FINGERPRINT)

    assert store.threads["exists"] == threading.get_ident()
