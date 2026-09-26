"""The ``content.persist`` handler (#114 step 6): copy a blob into the permanent store.

The digest is the address, so the handler's whole job is to prove the command
names bytes this host holds under that digest, and then store them there: T3a
on the ``blob_uri`` (the replicate guard, reused), agreement between the URI's
digest and ``content_fingerprint``, a read, a create-if-absent into the
permanent store — which re-hashes the bytes (cannobserv#492) — and the fact,
after the object exists.

Both stores are local here. Every guard runs identically over either backend,
and the real-bucket row is ``test_persist_gcs.py``.
"""

import hashlib

import pytest
from co_core.pure.util.blobstore import FingerprintMismatch
from co_core_sync.drivers.blobstore import LocalBlobStore
from google.api_core import exceptions as gexc

from src.core.errors import PermanentPersistError, PersistReason, TransientPersistError
from src.worker.persist import build_persist_handler
from tests.worker.test_loop_spec import make_persist_command_model

DATA = b"%PDF-1.4 the revision this command keeps\n"
FINGERPRINT = hashlib.sha256(DATA).hexdigest()


@pytest.fixture
def temp(tmp_path):
    return LocalBlobStore(tmp_path / "temp", touch_on_rereference=True)


@pytest.fixture
def permanent(tmp_path):
    """The permanent tier's shape: touch off."""
    return LocalBlobStore(tmp_path / "permanent")


class Persisted:
    """Collects the success facts, as (command_id, size) pairs, and checks ordering."""

    def __init__(self, permanent=None):
        self.facts = []
        self._permanent = permanent

    async def __call__(self, command, size_bytes):
        if self._permanent is not None:
            # Store, then publish: a fact naming absent bytes is unrepairable.
            assert self._permanent.exists(command.content_fingerprint)
        self.facts.append((command.command_id, size_bytes))


def command(blob_uri, **overrides):
    return make_persist_command_model(
        blob_uri=blob_uri,
        content_fingerprint=overrides.pop("content_fingerprint", FINGERPRINT),
        **overrides,
    )


async def test_a_temp_blob_is_copied_into_the_permanent_store(temp, permanent):
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")
    done = Persisted(permanent)

    await build_persist_handler(store=temp, permanent=permanent, complete=done)(command(uri))

    assert permanent.open(FINGERPRINT) == DATA
    assert done.facts == [("per-1", len(DATA))]


async def test_a_second_persist_is_a_no_op_that_still_emits(temp, permanent):
    """At-least-once: the redelivery finds the object, writes nothing, and re-emits,
    so an issuer that missed the first fact still hears."""
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")
    done = Persisted()
    handle = build_persist_handler(store=temp, permanent=permanent, complete=done)
    await handle(command(uri))
    stored = permanent.root / FINGERPRINT[:2] / FINGERPRINT[2:4] / f"{FINGERPRINT}.bin"
    before = stored.stat()

    await handle(command(uri, command_id="per-2"))

    after = stored.stat()
    assert done.facts == [("per-1", len(DATA)), ("per-2", len(DATA))]
    # Touch off: the permanent object has no retention clock to move.
    assert (after.st_mtime_ns, after.st_ino) == (before.st_mtime_ns, before.st_ino)


async def test_a_uri_naming_the_permanent_store_is_already_persisted(temp, permanent):
    """Nothing in the temp store, and a success: the bytes are where the command wants them."""
    uri = permanent.store(DATA, FINGERPRINT, "application/pdf")
    done = Persisted()

    await build_persist_handler(store=temp, permanent=permanent, complete=done)(command(uri))

    assert done.facts == [("per-1", len(DATA))]


@pytest.mark.parametrize(
    "blob_uri",
    [
        pytest.param("file:///etc/replicator/co-pypi-reader.json", id="a-real-secret"),
        # Same scheme as both stores here, but no store's root: a `gs://` stranger
        # would read as "the other backend" beside two local stores (#7).
        pytest.param(f"file:///srv/elsewhere/{FINGERPRINT}.bin", id="a-stranger-root"),
        pytest.param("not a uri", id="junk"),
    ],
)
async def test_a_uri_no_store_here_minted_is_invalid_source(temp, permanent, blob_uri):
    with pytest.raises(PermanentPersistError) as caught:
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(blob_uri)
        )

    assert caught.value.reason is PersistReason.INVALID_SOURCE


async def test_a_uri_naming_another_digest_is_invalid_source(temp, permanent):
    """co-core's rule: the two must agree, and the consumer decides (cannobserv#493)."""
    other = b"some other revision"
    uri = temp.store(other, hashlib.sha256(other).hexdigest(), "application/pdf")

    with pytest.raises(PermanentPersistError) as caught:
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(uri)
        )

    assert caught.value.reason is PersistReason.INVALID_SOURCE
    assert not permanent.exists(hashlib.sha256(other).hexdigest())


async def test_a_malformed_fingerprint_is_invalid_source(temp, permanent):
    """It decodes (no validator on the consumer class, #283) so it can be closed."""
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")

    with pytest.raises(PermanentPersistError) as caught:
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(uri, content_fingerprint="not-a-digest")
        )

    assert caught.value.reason is PersistReason.INVALID_SOURCE


@pytest.mark.parametrize(
    "content_fingerprint",
    [
        pytest.param(hashlib.sha256(b"another revision").hexdigest(), id="another-digest"),
        pytest.param("not-a-digest", id="malformed"),
    ],
)
async def test_a_disagreement_is_invalid_source_even_when_the_blob_is_gone(
    temp, permanent, content_fingerprint
):
    """The agreement is decided before existence (#114 CR 1).

    Checked after, the gone blob answered first with ``blob_expired``, whose remedy
    is a re-fetch — which cannot fix a command whose two digests disagree.
    """
    uri = temp.uri_for(FINGERPRINT)

    with pytest.raises(PermanentPersistError) as caught:
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(uri, content_fingerprint=content_fingerprint)
        )

    assert caught.value.reason is PersistReason.INVALID_SOURCE


async def test_a_blob_that_left_the_temp_tier_is_expired(temp, permanent):
    uri = temp.uri_for(FINGERPRINT)

    with pytest.raises(PermanentPersistError) as caught:
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(uri)
        )

    assert caught.value.reason is PersistReason.BLOB_EXPIRED


async def test_bytes_that_do_not_hash_to_their_name_are_refused_and_never_stored(
    temp, permanent, monkeypatch
):
    """A corrupted temp blob: the permanent store re-hashes on the create and refuses,
    so the one tier that cannot be undone never holds the wrong bytes."""
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")
    monkeypatch.setattr(temp, "open", lambda fingerprint: b"bit-rotted bytes")

    with pytest.raises(PermanentPersistError) as caught:
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(uri)
        )

    assert caught.value.reason is PersistReason.SOURCE_CORRUPT
    assert isinstance(caught.value.__cause__, FingerprintMismatch)
    assert not permanent.exists(FINGERPRINT)


class RefusingStore(LocalBlobStore):
    def __init__(self, root, error):
        super().__init__(root)
        self._error = error

    def store(self, data, fingerprint, media_type):
        raise self._error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(gexc.Forbidden("403 no storage.objects.create"), id="403"),
        pytest.param(gexc.NotFound("404 no such bucket"), id="404"),
    ],
)
async def test_a_terminal_store_refusal_is_store_refused(temp, tmp_path, error):
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")

    with pytest.raises(PermanentPersistError) as caught:
        await build_persist_handler(
            store=temp, permanent=RefusingStore(tmp_path / "p", error), complete=Persisted()
        )(command(uri))

    assert caught.value.reason is PersistReason.STORE_REFUSED


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(gexc.ServiceUnavailable("503"), id="503"),
        pytest.param(gexc.TooManyRequests("429"), id="429"),
        pytest.param(ConnectionError("reset"), id="no-status"),
    ],
)
async def test_a_transient_store_failure_stays_open(temp, tmp_path, error):
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")

    with pytest.raises(TransientPersistError):
        await build_persist_handler(
            store=temp, permanent=RefusingStore(tmp_path / "p", error), complete=Persisted()
        )(command(uri))


async def test_a_defect_in_the_store_reaches_the_ceiling(temp, tmp_path):
    """Not an outage: left unclassified so the delivery ceiling closes it (CR 7)."""
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")

    with pytest.raises(TypeError):
        await build_persist_handler(
            store=temp,
            permanent=RefusingStore(tmp_path / "p", TypeError("bug")),
            complete=Persisted(),
        )(command(uri))


class UnreachableTemp(LocalBlobStore):
    def __init__(self, root, error):
        super().__init__(root)
        self._error = error

    def exists(self, fingerprint):
        raise self._error


async def test_a_transient_failure_locating_the_source_stays_open(tmp_path, permanent):
    temp = UnreachableTemp(tmp_path / "t", gexc.ServiceUnavailable("503"))

    with pytest.raises(TransientPersistError):
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(temp.uri_for(FINGERPRINT))
        )


async def test_a_failed_success_publish_leaves_the_command_open(temp, permanent):
    """The fact re-raises, like replicate's: the redelivery re-runs a no-op and the
    fact gets another chance, where a swallowed failure would close it silently."""
    uri = temp.store(DATA, FINGERPRINT, "application/pdf")

    async def broken(command, size_bytes):
        raise ConnectionError("broker gone")

    with pytest.raises(ConnectionError):
        await build_persist_handler(store=temp, permanent=permanent, complete=broken)(command(uri))

    assert permanent.exists(FINGERPRINT)


# The source read, and the rest of the locate classification. These branches were
# written with the handler; the tests below pin them.


class UnreadableTemp(LocalBlobStore):
    """The existence check passes; the read then fails the given way."""

    def __init__(self, root, error):
        super().__init__(root, touch_on_rereference=True)
        self._error = error

    def open(self, fingerprint):
        raise self._error


async def _persist_from(temp, permanent):
    uri = LocalBlobStore.store(temp, DATA, FINGERPRINT, "application/pdf")
    await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(command(uri))


async def test_a_blob_swept_between_the_check_and_the_read_is_expired(tmp_path, permanent):
    """The retention sweep runs beside this loop; the window between the two is real."""
    with pytest.raises(PermanentPersistError) as caught:
        await _persist_from(UnreadableTemp(tmp_path / "t", FileNotFoundError("gone")), permanent)

    assert caught.value.reason is PersistReason.BLOB_EXPIRED


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(OSError(28, "No space left on device"), id="a-full-disk"),
        pytest.param(gexc.ServiceUnavailable("503"), id="503"),
    ],
)
async def test_a_transient_read_failure_stays_open(tmp_path, permanent, error):
    with pytest.raises(TransientPersistError):
        await _persist_from(UnreadableTemp(tmp_path / "t", error), permanent)


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(gexc.Forbidden("403 no storage.objects.get"), id="a-terminal-status"),
        pytest.param(TypeError("a defect"), id="a-defect"),
    ],
)
async def test_a_terminal_or_defective_read_is_left_to_the_ceiling(tmp_path, permanent, error):
    """No persist token says "this worker cannot read its own temp store"."""
    with pytest.raises(type(error)):
        await _persist_from(UnreadableTemp(tmp_path / "t", error), permanent)


async def test_an_expired_temp_blob_already_kept_is_a_success(temp, permanent):
    """The permanent store is the authority on whether the bytes are kept (#114 CR 2).

    A reaper re-issuing after the temp tier's seven days, for bytes an earlier
    command already persisted, was told ``blob_expired`` — that they are lost —
    and a re-fetch may no longer return them.
    """
    permanent.store(DATA, FINGERPRINT, "application/pdf")
    done = Persisted()

    await build_persist_handler(store=temp, permanent=permanent, complete=done)(
        command(temp.uri_for(FINGERPRINT))
    )

    assert done.facts == [("per-1", len(DATA))]


class UncheckablePermanent(LocalBlobStore):
    def __init__(self, root, error):
        super().__init__(root)
        self._error = error

    def exists(self, fingerprint):
        raise self._error


@pytest.mark.parametrize(
    ("error", "raised"),
    [
        pytest.param(gexc.ServiceUnavailable("503"), TransientPersistError, id="503"),
        pytest.param(TypeError("a defect"), TypeError, id="a-defect"),
    ],
)
async def test_a_failure_checking_the_permanent_store_is_classified(temp, tmp_path, error, raised):
    """The fallback's own round trip: an outage stays open, a defect reaches the ceiling."""
    permanent = UncheckablePermanent(tmp_path / "p", error)

    with pytest.raises(raised):
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(temp.uri_for(FINGERPRINT))
        )


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(gexc.Forbidden("403 no storage.objects.get"), id="a-terminal-status"),
        pytest.param(TypeError("a defect"), id="a-defect"),
    ],
)
async def test_a_terminal_or_defective_locate_failure_is_left_to_the_ceiling(
    tmp_path, permanent, error
):
    temp = UnreachableTemp(tmp_path / "t", error)

    with pytest.raises(type(error)):
        await build_persist_handler(store=temp, permanent=permanent, complete=Persisted())(
            command(temp.uri_for(FINGERPRINT))
        )
