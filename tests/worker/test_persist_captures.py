"""Persist routes by family (#189, archiver#380 D4): a capture is kept with captures.

Two families, each a temp store and the store its bytes are kept in: raw (the
temp bucket → the permanent bucket) and captures (the capture bucket's ``temp/``
→ its ``blobs/``). The family is the one whose store minted
``blob_uri``, and **neither may ever reach the other's kept store** — the
captures bucket is the only one Archiver may read, so a raw blob there is a
boundary breach, and a capture in the raw kept store is one it can never show.

Local stores stand in for all four, as in ``test_persist.py``: the routing is
store-agnostic, and the real-bucket row is ``test_persist_gcs.py``.
"""

import hashlib

import pytest
from co_core_sync.drivers.blobstore import LocalBlobStore

from src.core.errors import PermanentPersistError, PersistReason
from src.worker.persist import StoreFamily, build_persist_handler
from tests.worker.test_loop_spec import make_persist_command_model

DATA = b"\x89PNG\r\n\x1a\n a capture this command keeps"
FINGERPRINT = hashlib.sha256(DATA).hexdigest()


@pytest.fixture
def raw(tmp_path):
    return StoreFamily(
        temp=LocalBlobStore(tmp_path / "raw-temp", touch_on_rereference=True),
        kept=LocalBlobStore(tmp_path / "raw-kept"),
    )


@pytest.fixture
def captures(tmp_path):
    return StoreFamily(
        temp=LocalBlobStore(tmp_path / "captures-temp", touch_on_rereference=True),
        kept=LocalBlobStore(tmp_path / "captures-kept"),
    )


def handler(raw, captures, facts):
    async def complete(command, size_bytes):
        facts.append((command.command_id, size_bytes))

    return build_persist_handler(
        store=raw.temp, permanent=raw.kept, captures=captures, complete=complete
    )


def command(blob_uri):
    return make_persist_command_model(
        blob_uri=blob_uri, content_fingerprint=FINGERPRINT, media_type="image/png"
    )


async def test_a_capture_is_kept_with_captures_never_in_the_raw_kept_store(raw, captures):
    uri = captures.temp.store(DATA, FINGERPRINT, "image/png")
    facts = []

    await handler(raw, captures, facts)(command(uri))

    assert captures.kept.open(FINGERPRINT) == DATA
    assert not raw.kept.exists(FINGERPRINT), "a capture must never reach the raw kept store"
    assert facts == [("per-1", len(DATA))]


async def test_a_raw_blob_is_kept_in_the_raw_store_never_with_captures(raw, captures):
    uri = raw.temp.store(DATA, FINGERPRINT, "image/png")
    facts = []

    await handler(raw, captures, facts)(command(uri))

    assert raw.kept.open(FINGERPRINT) == DATA
    assert not captures.kept.exists(FINGERPRINT), "a raw blob must never reach the captures bucket"


async def test_a_capture_already_kept_is_a_no_op_named_by_its_kept_uri(raw, captures):
    captures.kept.store(DATA, FINGERPRINT, "image/png")
    facts = []

    await handler(raw, captures, facts)(command(captures.kept.uri_for(FINGERPRINT)))

    assert facts == [("per-1", len(DATA))]
    assert not raw.kept.exists(FINGERPRINT)


async def test_an_expired_capture_already_kept_with_captures_succeeds(raw, captures):
    """Gone from the temp tier, but kept: the *captures* kept store is the authority."""
    captures.kept.store(DATA, FINGERPRINT, "image/png")
    facts = []

    await handler(raw, captures, facts)(command(captures.temp.uri_for(FINGERPRINT)))

    assert facts == [("per-1", len(DATA))]
    assert not raw.kept.exists(FINGERPRINT)


async def test_an_expired_capture_kept_only_in_the_raw_store_is_still_expired(raw, captures):
    """The raw kept store is not asked on a capture's behalf — no crossing either way."""
    raw.kept.store(DATA, FINGERPRINT, "image/png")

    with pytest.raises(PermanentPersistError) as caught:
        await handler(raw, captures, [])(command(captures.temp.uri_for(FINGERPRINT)))

    assert caught.value.reason is PersistReason.BLOB_EXPIRED


async def test_a_capture_at_its_old_raw_location_is_kept_in_the_raw_store(raw, captures):
    """No migration (#189): a pre-split capture's URI is a raw-temp URI, so it is
    kept where Archiver's derivation looks for it, the permanent bucket."""
    uri = raw.temp.store(DATA, FINGERPRINT, "image/png")

    await handler(raw, captures, [])(command(uri))

    assert raw.kept.exists(FINGERPRINT)
    assert not captures.kept.exists(FINGERPRINT)


async def test_without_a_captures_family_a_captures_uri_is_not_ours(raw, captures):
    """A host configured with no capture bucket refuses it as it refuses any stranger."""
    uri = captures.temp.store(DATA, FINGERPRINT, "image/png")

    async def complete(command, size_bytes):
        raise AssertionError("no fact for a refusal")

    handle = build_persist_handler(store=raw.temp, permanent=raw.kept, complete=complete)
    with pytest.raises(PermanentPersistError) as caught:
        await handle(command(uri))

    assert caught.value.reason is PersistReason.INVALID_SOURCE
    assert not raw.kept.exists(FINGERPRINT)
