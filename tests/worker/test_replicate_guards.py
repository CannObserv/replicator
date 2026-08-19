"""The two path guards on the replicate command (#29, contract T3 and T3a).

Replication is the first time a message value reaches a path **in both
directions**. The destination half was argued in #34's T3; the source half —
``blob_uri``, which the issuer supplies and the consumer must turn into local
bytes — is T3a, and it is the sharper of the two: this service writes to public,
undeletable archive.org items, so a read-side traversal publishes whatever it
reads, permanently.

Both guards are **allow-lists**, not deny-lists. The source is resolved from a
validated fingerprint through the store's own mapping, so a path never comes from
the message at all; the destination is checked against the alias root it must sit
under. That is the same inversion ``tests/test_boundaries.py``'s echo scan settled
on after three rounds of an incomplete deny-list: enumerate what is allowed, and
the shapes nobody thought of are refused for free.
"""

import io

import pytest

from src.core.errors import PermanentReplicateError, ReplicateReason
from src.storage.gcs import GcsBlobStore
from src.storage.local import LocalBlobStore
from src.worker.aliases import AliasBinding
from src.worker.replicate import locate_blob, validate_destination
from tests.storage.conftest import FakeBucket, FakeClient

FINGERPRINT = "a" * 64
GCS_ROOT = AliasBinding(provider="gcs", bucket="co-artifacts", prefix="reps")


@pytest.fixture
def store(tmp_path):
    return LocalBlobStore(tmp_path)


@pytest.fixture
def stored(store):
    """A blob actually on disk, and the URI the store minted for it."""
    return store.store(b"artifact bytes", FINGERPRINT, "application/pdf")


# --------------------------------------------------------------------------
# T3a — the source
# --------------------------------------------------------------------------


def test_a_uri_this_store_minted_locates_its_fingerprint(store, stored):
    assert locate_blob(stored, store=store) == FINGERPRINT


def test_locating_a_blob_reads_none_of_it(store, stored, monkeypatch):
    """CR #15: the guard answers "is this ours, and is it still here" — no bytes.

    Before the split it read the whole blob so the handler could log its length,
    then threw it away: a measured 5 MB off disk, synchronously, on the event
    loop, for a command that writes nothing. With two command loops now sharing
    that loop, the read stalled the *fetch* path as well.

    The writer does need the bytes, and takes them as a stream from
    ``BlobStore.open_stream`` rather than as a value — so the split holds and
    neither half materializes an artifact this service only means to pass on.
    """
    for method in ("open", "open_stream"):
        monkeypatch.setattr(
            LocalBlobStore, method, lambda self, fp: pytest.fail("the guard must not read the blob")
        )

    assert locate_blob(stored, store=store) == FINGERPRINT


@pytest.mark.parametrize(
    "blob_uri",
    [
        # The one that matters: a real, readable, secret file on this VM. An
        # implementation that parsed the URI and read the path would publish this
        # host's GCS reader key to a permanent public store.
        pytest.param("file:///etc/replicator/co-pypi-reader.json", id="a-real-secret"),
        pytest.param("file:///etc/passwd", id="a-classic"),
        pytest.param("file:///proc/self/environ", id="process-environment"),
        # Traversal dressed as a fingerprint.
        pytest.param(f"file:///var/lib/replicator/blobs/../../../etc/{FINGERPRINT}.bin", id="dots"),
        # A fingerprint-shaped name under a directory that is not ours.
        pytest.param(f"file:///tmp/{FINGERPRINT}.bin", id="right-name-wrong-root"),
        # Not a file URI at all.
        pytest.param(f"https://example.test/{FINGERPRINT}.bin", id="http"),
        # `gs://` used to belong on this list, and #7 moved it: a well-formed URI
        # from the *other* backend is now `blob_expired`, because after a backend
        # flip that is what it is — see
        # `test_a_uri_from_the_other_backend_is_expired_not_invalid`. What stays
        # here is the same scheme carrying something that was never a reference.
        pytest.param("gs://someone-elses-bucket/not-a-fingerprint.bin", id="object-store"),
        # Malformed and empty.
        pytest.param("", id="empty"),
        pytest.param("file://", id="no-path"),
        pytest.param("not a uri at all", id="garbage"),
        # Fingerprint-shaped but not a fingerprint.
        pytest.param(f"file:///blobs/{'A' * 64}.bin", id="uppercase-hex"),
        pytest.param(f"file:///blobs/{'z' * 64}.bin", id="not-hex"),
        pytest.param("file:///blobs/abc.bin", id="too-short"),
        pytest.param(f"file:///blobs/{'a' * 65}.bin", id="too-long"),
    ],
)
def test_a_uri_this_store_did_not_mint_is_refused(store, stored, blob_uri):
    """Refused as ``invalid_source``, before any byte is read.

    ``stored`` is in place so a passing test cannot be passing merely because the
    store is empty — the store *can* serve bytes here, and does not.
    """
    with pytest.raises(PermanentReplicateError) as caught:
        locate_blob(blob_uri, store=store)

    assert caught.value.reason is ReplicateReason.INVALID_SOURCE


def test_a_uri_that_will_not_even_parse_is_refused(store):
    """CR #17: ``urlsplit`` raises on some inputs rather than returning empties.

    A bracketed-IPv6 host with a bad port is the reachable case. Untested until
    now, which is the shape that reads as covered because the module sits at 95%.
    """
    with pytest.raises(PermanentReplicateError) as caught:
        locate_blob("file://[oops:::1]:notaport/x.bin", store=store)

    assert caught.value.reason is ReplicateReason.INVALID_SOURCE


def test_a_wellformed_uri_for_a_blob_that_is_gone_is_expired_not_invalid(store):
    """The distinguishable terminal reason #29 asks for.

    ``invalid_source`` and ``blob_expired`` must not collapse: one means the
    issuer's plumbing is wrong, the other means it was right and it waited too
    long. Only the second is fixed by fetching again.
    """
    minted = store.uri_for(FINGERPRINT)  # never stored, so the sweep has taken it

    with pytest.raises(PermanentReplicateError) as caught:
        locate_blob(minted, store=store)

    assert caught.value.reason is ReplicateReason.BLOB_EXPIRED


def test_the_resolver_never_touches_a_path_from_the_message(store, stored, monkeypatch):
    """The structural version of the traversal tests above.

    Those pin *outcomes* for the shapes we thought of. This pins the *mechanism*:
    the fingerprint is the only thing taken from the URI, and the path is rebuilt
    by the store. An implementation that read the message's path would have to
    open a file this test never authorised.
    """
    opened: list[str] = []
    monkeypatch.setattr(
        LocalBlobStore,
        "open_stream",
        lambda self, fingerprint: opened.append(fingerprint) or io.BytesIO(b""),
    )

    store.open_stream(locate_blob(stored, store=store))

    assert opened == [FINGERPRINT]


# --------------------------------------------------------------------------
# T3 — the destination
# --------------------------------------------------------------------------


def test_a_rendered_path_under_the_alias_root_is_accepted():
    assert validate_destination("2026/report.pdf", binding=GCS_ROOT) == "reps/2026/report.pdf"


def test_a_binding_with_no_prefix_roots_at_the_bucket():
    flat = AliasBinding(provider="gcs", bucket="b")

    assert validate_destination("2026/report.pdf", binding=flat) == "2026/report.pdf"


@pytest.mark.parametrize(
    "destination",
    [
        pytest.param("../escape.pdf", id="traversal"),
        pytest.param("a/../../escape.pdf", id="traversal-mid-path"),
        pytest.param("/absolute.pdf", id="leading-slash"),
        pytest.param("C:/windows.pdf", id="drive-qualifier"),
        pytest.param("back\\slash.pdf", id="backslash"),
        pytest.param("nul\x00byte.pdf", id="nul"),
        pytest.param("control\x01char.pdf", id="control-char"),
        pytest.param("double//segment.pdf", id="empty-segment"),
        pytest.param("trailing/", id="trailing-slash"),
        pytest.param("./relative.pdf", id="dot-segment"),
        pytest.param("", id="empty"),
        pytest.param("   ", id="whitespace-only"),
        # Percent-encoding is refused outright (CR #16). Under T3 the issuer
        # renders, so a rendered path has no business carrying escapes at all —
        # and decoding-then-checking silently *repaired* a mid-path %2F into a
        # separator, which is the "never repaired" promise broken by the very
        # decode the traversal check needed.
        pytest.param("%2e%2e/escape.pdf", id="encoded-traversal"),
        pytest.param("a/%2e%2e/%2e%2e/escape.pdf", id="encoded-traversal-mid-path"),
        pytest.param("%2fabsolute.pdf", id="encoded-slash"),
        pytest.param("a/b%2Fc.pdf", id="encoded-separator-mid-path"),
        pytest.param("sp%20ace.pdf", id="encoded-space"),
        pytest.param("%252e%252e/x.pdf", id="double-encoded"),
        pytest.param(" lead.pdf", id="leading-whitespace"),
        pytest.param("trail.pdf ", id="trailing-whitespace"),
        pytest.param("100%.pdf", id="bare-percent"),
        pytest.param("bad%zz.pdf", id="malformed-escape"),
        pytest.param("trailing%", id="trailing-percent"),
    ],
)
def test_a_destination_that_is_not_already_normalized_is_refused(destination):
    """Refused, never repaired. Same argument as refusing an unsendable header
    rather than stripping it: a change to the write the issuer cannot see is one
    it cannot account for, and under T4 the path *is* the idempotency key — a
    silently-normalized destination would make a redelivery target a different
    key and defeat the no-op that keeps a redelivery from destroying an artifact.
    """
    with pytest.raises(PermanentReplicateError) as caught:
        validate_destination(destination, binding=GCS_ROOT)

    assert caught.value.reason is ReplicateReason.INVALID_DESTINATION


def test_the_check_runs_on_the_rendered_string_not_on_a_template():
    """Under T3 the issuer renders, so a placeholder reaching here means the
    issuer skipped R1 — and a ``{`` in a key is a real object name, not a
    template, once it is written."""
    with pytest.raises(PermanentReplicateError) as caught:
        validate_destination("{info_item.slug}/report.pdf", binding=GCS_ROOT)

    assert caught.value.reason is ReplicateReason.INVALID_DESTINATION


def test_a_prefix_lookalike_cannot_escape_the_root():
    """``reps`` must not admit ``reps-other``. String-prefix containment checks
    fail exactly here, which is why the join is segment-wise."""
    sneaky = AliasBinding(provider="gcs", bucket="b", prefix="reps")

    assert validate_destination("2026/r.pdf", binding=sneaky).startswith("reps/")

    with pytest.raises(PermanentReplicateError) as caught:
        validate_destination("../reps-other/r.pdf", binding=sneaky)

    assert caught.value.reason is ReplicateReason.INVALID_DESTINATION


@pytest.mark.parametrize(
    "destination",
    [
        pytest.param("sp%20ace.pdf", id="valid-escape"),
        pytest.param("bad%zz.pdf", id="malformed-escape"),
        pytest.param("trailing%", id="trailing-percent"),
        pytest.param("100%.pdf", id="literal-percent"),
    ],
)
def test_every_percent_is_refused_by_the_same_rule(destination):
    """CR #22: the stated rule and the enforcing code must be the same rule.

    ``unquote`` only rewrites well-formed ``%XX``; a malformed or trailing
    ``%`` passes straight through it, so the first version refused those by the
    *character allow-list* while the docstring said "percent-encoding is refused
    outright". Both outcomes were right and the reasons disagreed, which is the
    kind of drift the next reader inherits. Now one check owns all of them, and
    the refusal says so.
    """
    with pytest.raises(PermanentReplicateError) as caught:
        validate_destination(destination, binding=GCS_ROOT)

    assert caught.value.reason is ReplicateReason.INVALID_DESTINATION
    assert "percent" in str(caught.value)


def test_a_long_disallowed_segment_is_bounded_in_the_refusal(binding=GCS_ROOT):
    """CR #24: the refusal message becomes the fact's ``detail`` and a dlq_reason.

    Both are places the logging bound was introduced to protect, and the segment
    was being embedded whole — so a multi-kilobyte destination reached the wire
    and the DLQ entry in full.
    """
    with pytest.raises(PermanentReplicateError) as caught:
        validate_destination("!" * 5000 + ".pdf", binding=binding)

    assert len(str(caught.value)) < 500


# --------------------------------------------------------------------------
# T3a — the source, with the object-store backend (#7)
# --------------------------------------------------------------------------


@pytest.fixture
def gcs_store():
    """A `GcsBlobStore` over a fake bucket — the guard never touches the network."""
    return GcsBlobStore(
        "a-temp-bucket", prefix="blobs", client=FakeClient(FakeBucket("a-temp-bucket"))
    )


@pytest.fixture
def gcs_stored(gcs_store):
    return gcs_store.store(b"artifact bytes", FINGERPRINT, "application/pdf")


def test_a_gs_uri_this_store_minted_locates_its_fingerprint(gcs_store, gcs_stored):
    assert gcs_stored.startswith("gs://")
    assert locate_blob(gcs_stored, store=gcs_store) == FINGERPRINT


@pytest.mark.parametrize(
    "blob_uri",
    [
        # A `file://` value that was never a blob reference. The well-formed
        # other-backend URI is deliberately absent — it is `blob_expired` now,
        # asserted below.
        pytest.param("file:///etc/replicator/co-pypi-reader.json", id="a-real-secret"),
        pytest.param(f"gs://someone-elses-bucket/blobs/{FINGERPRINT}.bin", id="wrong-bucket"),
        pytest.param(f"gs://a-temp-bucket/elsewhere/{FINGERPRINT}.bin", id="wrong-prefix"),
        pytest.param(f"gs://a-temp-bucket/blobs/{FINGERPRINT}", id="no-suffix"),
        pytest.param(f"gs://a-temp-bucket/blobs/{'A' * 64}.bin", id="uppercase-hex"),
        pytest.param(f"gs://a-temp-bucket/blobs/{'z' * 64}.bin", id="not-hex"),
        pytest.param("gs://a-temp-bucket/blobs/abc.bin", id="too-short"),
        pytest.param("gs://", id="no-path"),
        pytest.param("gs://a-temp-bucket", id="bucket-only"),
        # The traversal shapes, which mean nothing to a bucket and are refused
        # anyway — the guard never resolves the value, so there is no path here
        # to escape from and nothing to normalize.
        pytest.param(f"gs://a-temp-bucket/blobs/../../{FINGERPRINT}.bin", id="dots"),
        pytest.param("gs://a-temp-bucket/../etc/passwd", id="a-classic"),
    ],
)
def test_a_gs_uri_this_store_did_not_mint_is_refused(gcs_store, gcs_stored, blob_uri):
    with pytest.raises(PermanentReplicateError) as caught:
        locate_blob(blob_uri, store=gcs_store)

    assert caught.value.reason is ReplicateReason.INVALID_SOURCE


def test_a_gone_gs_blob_is_expired_not_invalid(gcs_store):
    """Same split as the filesystem backend, and the remedies are still opposite.

    `invalid_source` means re-fetching fixes nothing; `blob_expired` means the
    issuer named the right blob too late, which a fresh fetch does fix. An object
    store makes the second case *more* common rather than less — lifecycle
    deletion is asynchronous, so the horizon a consumer was told is a floor.
    """
    minted = gcs_store.uri_for(FINGERPRINT)

    with pytest.raises(PermanentReplicateError) as caught:
        locate_blob(minted, store=gcs_store)

    assert caught.value.reason is ReplicateReason.BLOB_EXPIRED


def test_the_guard_never_parses_the_message_value_into_a_key(gcs_store, gcs_stored, monkeypatch):
    """The property T3a is actually about, asserted rather than described.

    A refusal that happened to fire on every probe above would still be wrong if
    it worked by turning the URI into a key and inspecting it. What makes the
    guard sound is that the only string reaching the bucket is one the store
    derived from a validated fingerprint — so `key_for` must never see anything
    the message carried.
    """
    seen = []
    original = gcs_store.key_for
    monkeypatch.setattr(gcs_store, "key_for", lambda fp: seen.append(fp) or original(fp))

    locate_blob(gcs_stored, store=gcs_store)

    assert seen == [FINGERPRINT] * len(seen)
    assert seen


def test_a_uri_from_the_other_backend_is_expired_not_invalid(gcs_store):
    """The flip window, and the one consumer that cannot recover from getting it wrong.

    Phase C of #7 moves a live worker from one backend to the other. Commands
    naming the previous backend are still in the PEL when it restarts, and their
    blobs really are unreachable — but the *remedy* is a fresh fetch, which is
    what `blob_expired` says and what `invalid_source` denies. Archiver is the
    replicate issuer and has no fetch path of its own, so an issuer told
    "re-fetching fixes nothing" stops trying and that revision is never
    replicated (archiver#175).

    Deliberately narrow: only a URI that is well-formed for a *known* backend and
    ends in a valid fingerprint gets this reading. Everything else stays
    `invalid_source`, and neither reading reads a byte — both refuse before any
    credential is touched.
    """
    other_backend = LocalBlobStore("/var/lib/replicator/blobs").uri_for(FINGERPRINT)

    with pytest.raises(PermanentReplicateError) as caught:
        locate_blob(other_backend, store=gcs_store)

    assert caught.value.reason is ReplicateReason.BLOB_EXPIRED


def test_a_junk_path_under_the_other_scheme_is_still_invalid(gcs_store):
    """The narrowness above, asserted from the other side.

    `file:///etc/passwd` is not "a blob from the previous backend" — it is a
    value that was never a reference at all, and reporting it as expired would
    invite an issuer to keep re-fetching against it.
    """
    with pytest.raises(PermanentReplicateError) as caught:
        locate_blob("file:///etc/passwd", store=gcs_store)

    assert caught.value.reason is ReplicateReason.INVALID_SOURCE
