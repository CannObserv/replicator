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

import pytest

from src.core.errors import PermanentReplicateError, ReplicateReason
from src.storage.local import LocalBlobStore
from src.worker.aliases import AliasBinding
from src.worker.replicate import resolve_blob, validate_destination

FINGERPRINT = "a" * 64
GCS_ROOT = AliasBinding(alias="primary", provider="gcs", bucket="co-artifacts", prefix="reps")


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


def test_a_uri_this_store_minted_resolves_to_its_bytes(store, stored):
    assert resolve_blob(stored, store=store) == b"artifact bytes"


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
        pytest.param(f"gs://someone-elses-bucket/{FINGERPRINT}.bin", id="object-store"),
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
        resolve_blob(blob_uri, store=store)

    assert caught.value.reason is ReplicateReason.INVALID_SOURCE


def test_a_wellformed_uri_for_a_blob_that_is_gone_is_expired_not_invalid(store):
    """The distinguishable terminal reason #29 asks for.

    ``invalid_source`` and ``blob_expired`` must not collapse: one means the
    issuer's plumbing is wrong, the other means it was right and it waited too
    long. Only the second is fixed by fetching again.
    """
    minted = store.uri_for(FINGERPRINT)  # never stored, so the sweep has taken it

    with pytest.raises(PermanentReplicateError) as caught:
        resolve_blob(minted, store=store)

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
        LocalBlobStore, "open", lambda self, fingerprint: opened.append(fingerprint) or b""
    )

    resolve_blob(stored, store=store)

    assert opened == [FINGERPRINT]


# --------------------------------------------------------------------------
# T3 — the destination
# --------------------------------------------------------------------------


def test_a_rendered_path_under_the_alias_root_is_accepted():
    assert validate_destination("2026/report.pdf", binding=GCS_ROOT) == "reps/2026/report.pdf"


def test_a_binding_with_no_prefix_roots_at_the_bucket():
    flat = AliasBinding(alias="flat", provider="gcs", bucket="b")

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
        # Percent-encoded traversal: the guard decodes *before* checking, or this
        # is the one that gets through.
        pytest.param("%2e%2e/escape.pdf", id="encoded-traversal"),
        pytest.param("a/%2e%2e/%2e%2e/escape.pdf", id="encoded-traversal-mid-path"),
        pytest.param("%2fabsolute.pdf", id="encoded-slash"),
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
    sneaky = AliasBinding(alias="p", provider="gcs", bucket="b", prefix="reps")

    assert validate_destination("2026/r.pdf", binding=sneaky).startswith("reps/")

    with pytest.raises(PermanentReplicateError) as caught:
        validate_destination("../reps-other/r.pdf", binding=sneaky)

    assert caught.value.reason is ReplicateReason.INVALID_DESTINATION
