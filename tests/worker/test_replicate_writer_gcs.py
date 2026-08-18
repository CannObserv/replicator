"""The T4 table against a real bucket — the coverage #38 was opened for.

`tests/worker/test_replicate_writer.py` asserts the four outcomes against a fake
driver, which means it asserts what we *believe* GCS does. The interesting
failures on this path are not ours: 412 semantics, what `ifGenerationMatch: 0`
compares, whether md5 is the thing that distinguishes "already there" from
"something else is there", whether a content type survives the round trip. Those
are properties of the provider, and a fake cannot be wrong about them in a way a
test would notice.

The write path was verified by hand once, against production, and deliberately
not committed — production's writer holds no `delete`, so every run was permanent
litter and a conflict fixture could not reset itself (#38). With
`co-gcs-test-replication` provisioned (#50) and the guards in place (#51), that
verification becomes a test.

**Three rows, not four.** `INDETERMINATE` is deliberately absent: a 412 whose
confirming read finds no object needs a delete landing between two calls, and
provoking that against real GCS is either flaky or requires a seam in the write
path that exists only for the test. It is covered against the fake, and the
reasoning for why the branch is what it is lives in that module's docstring.

**Every object this module writes is under one per-run prefix, and teardown
removes it.** The bucket's lifecycle rule is litter insurance with 24h+ latency,
not a reset — two runs in one afternoon must not collide by construction. The
reset is the test SA's `delete`, which is the whole reason the bucket exists.
"""

import hashlib
import uuid

import pytest
from co_core.pure.models.changes import ReplicationCompleteEvent
from co_core_aio.gcs import AsyncGcsDriver
from google.cloud import storage

from src.core.errors import PermanentReplicateError, ReplicateReason
from src.storage.local import LocalBlobStore
from src.worker.aliases import AliasBinding, AliasTable
from src.worker.replicate import build_replicate_handler
from tests.worker.conftest import now
from tests.worker.test_loop_spec import make_replicate_command_model

pytestmark = pytest.mark.gcs

MEDIA_TYPE = "application/pdf"
ARTIFACT = b"%PDF-1.4 the artifact these tests replicate\n"
DIFFERENT = b"%PDF-1.4 a different artifact at the same destination\n"


def fingerprint_of(data: bytes) -> str:
    """The real digest, so the store's URI and the bytes cannot disagree."""
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def run_prefix() -> str:
    """One prefix per test, so concurrent runs cannot collide.

    Not derived from the test name: two runs of the same test on two machines
    would share it, and the conflict row deliberately leaves an object in place
    for the duration of its own assertion.
    """
    return f"replicator-t4/{uuid.uuid4().hex[:12]}"


@pytest.fixture
def gcs(gcs_bucket):
    """A plain client for the assertions and the teardown.

    Separate from the driver under test on purpose: a test that verified a write
    through the same object that performed it would be asserting the driver
    against itself.
    """
    return storage.Client().bucket(gcs_bucket)


@pytest.fixture
def written(gcs, run_prefix):
    """Removes everything this test wrote, whether it passed or not — and checks.

    ``delete`` is the permission production's writer does not have, and the
    reason a separate bucket had to exist at all.

    The teardown asserts its own result rather than trusting it. A cleanup that
    silently misses an object leaves litter that the 1-day lifecycle rule
    eventually collects, so nothing would ever fail — and the next reader would
    inherit exactly the ambiguity #38 spent a comment thread resolving.
    """
    yield
    for blob in gcs.client.list_blobs(gcs, prefix=f"{run_prefix}/"):
        blob.delete()
    leftover = [blob.name for blob in gcs.client.list_blobs(gcs, prefix=f"{run_prefix}/")]
    assert leftover == [], f"teardown left objects behind: {leftover}"


@pytest.fixture
def driver(gcs_bucket):
    """The real ``AsyncGcsDriver``, against the test bucket.

    The autouse guard in ``tests/conftest.py`` has already refused every other
    bucket name before this constructor resolves a credential.
    """
    return AsyncGcsDriver(gcs_bucket)


@pytest.fixture
def store(tmp_path) -> LocalBlobStore:
    return LocalBlobStore(tmp_path)


class Completions:
    """Collects the facts the handler publishes, built as the real publisher does."""

    def __init__(self):
        self.facts = []

    async def __call__(self, command, public_url):
        self.facts.append(
            ReplicationCompleteEvent(
                occurred_at=now(),
                command_id=command.command_id,
                public_url=public_url,
                info_item_rep_spec_id=command.info_item_rep_spec_id,
                source_revision_id=command.source_revision_id,
                info_source_id=command.info_source_id,
            )
        )


@pytest.fixture
def replicate(store, driver, gcs_bucket):
    """The handler, wired to the real driver — the only thing that differs from
    the unit tests, and the whole point of this module."""
    done = Completions()
    handler = build_replicate_handler(
        store=store,
        aliases=AliasTable({"primary": AliasBinding(provider="gcs", bucket=gcs_bucket)}),
        writers={"primary": driver},
        complete=done,
    )
    return handler, done


@pytest.fixture
def command():
    def build(blob_uri, destination, **overrides):
        return make_replicate_command_model(blob_uri=blob_uri, destination=destination, **overrides)

    return build


async def test_an_absent_destination_is_written(
    replicate, store, command, gcs, run_prefix, written
):
    """T4 row one, end to end: the object lands, and the fact points at it."""
    handler, done = replicate
    uri = store.store(ARTIFACT, fingerprint_of(ARTIFACT), MEDIA_TYPE)
    destination = f"{run_prefix}/report.pdf"

    await handler(command(uri, destination))

    (fact,) = done.facts
    assert fact.public_url.endswith(destination)
    assert gcs.name in fact.public_url

    remote = gcs.get_blob(destination)
    assert remote is not None
    assert remote.download_as_bytes() == ARTIFACT
    # The round trip a fake cannot get wrong: the driver sets it, GCS stores it,
    # and a consumer of the public URL is served it.
    assert remote.content_type == MEDIA_TYPE


async def test_a_redelivery_onto_identical_bytes_is_a_no_op_that_still_emits(
    replicate, store, command, gcs, run_prefix, written
):
    """T4 row two, and the row that makes at-least-once delivery survivable.

    The second command must not fail, must re-emit the same `public_url` — an
    issuer that missed the first fact writes the same registry row — and must
    leave the object's **generation** untouched, which is the assertion that
    distinguishes a genuine no-op from a silent overwrite with identical bytes.
    """
    handler, done = replicate
    uri = store.store(ARTIFACT, fingerprint_of(ARTIFACT), MEDIA_TYPE)
    destination = f"{run_prefix}/report.pdf"

    await handler(command(uri, destination, command_id="rep-first"))
    first_generation = gcs.get_blob(destination).generation

    await handler(command(uri, destination, command_id="rep-redelivered"))

    first, second = done.facts
    assert second.public_url == first.public_url
    assert gcs.get_blob(destination).generation == first_generation


async def test_differing_bytes_at_the_same_destination_are_a_terminal_conflict(
    replicate, store, command, gcs, run_prefix, written
):
    """T4 row three: refused, and — the half that matters — nothing overwritten.

    This is the row that could not be tested against production at all. Proving
    it requires leaving the conflicting object in place for the duration of the
    assertion, and then removing it; production grants no `delete` to anyone who
    can write.
    """
    handler, done = replicate
    destination = f"{run_prefix}/report.pdf"
    original = store.store(ARTIFACT, fingerprint_of(ARTIFACT), MEDIA_TYPE)
    await handler(command(original, destination, command_id="rep-first"))

    other = store.store(DIFFERENT, fingerprint_of(DIFFERENT), MEDIA_TYPE)
    with pytest.raises(PermanentReplicateError) as caught:
        await handler(command(other, destination, command_id="rep-conflicting"))

    assert caught.value.reason is ReplicateReason.DESTINATION_CONFLICT
    assert gcs.get_blob(destination).download_as_bytes() == ARTIFACT
    # One fact, from the first command. A refusal is the loop's to report.
    assert [fact.command_id for fact in done.facts] == ["rep-first"]


async def test_a_conflict_fixture_can_reset_itself(gcs, run_prefix):
    """The property the other three rest on, and the one production cannot offer.

    Row three has to leave a conflicting object in place to assert against it. If
    that object could not then be removed, the row would pass once and fail every
    time after — which is exactly what production's grant does, and the whole
    argument for a second bucket (#38, #50). Asserted here rather than assumed,
    because a silent failure of it looks like a flaky test rather than a missing
    permission.
    """
    key = f"{run_prefix}/resettable.pdf"
    gcs.blob(key).upload_from_string(ARTIFACT, content_type=MEDIA_TYPE, if_generation_match=0)
    assert gcs.get_blob(key) is not None

    gcs.blob(key).delete()

    assert gcs.get_blob(key) is None
