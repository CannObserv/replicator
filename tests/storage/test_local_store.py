"""The local store the worker builds: one more try when a sweep took its shard (#159)."""

import ast
import hashlib
import logging
from pathlib import Path

import pytest
from co_core_sync.drivers.blobstore import LocalBlobStore

from src.storage.local_store import SweepSafeLocalBlobStore

DATA = b"bytes"
FINGERPRINT = hashlib.sha256(DATA).hexdigest()
SRC = Path(__file__).resolve().parents[2] / "src"

# co-core 0.41's reporting twins (cannobserv#530), each keyed by the plain verb that
# dispatches to it.
TWINS = {"store": "store_reporting", "store_file": "store_file_reporting"}


def write(store, method: str, tmp_path: Path) -> str:
    """Store ``DATA`` through either write method, the copy one from a source file."""
    if method == "store":
        return store.store(DATA, FINGERPRINT, "text/plain")
    source = tmp_path / "source"
    source.write_bytes(DATA)
    return store.store_file(source, FINGERPRINT, "text/plain")


def lost_shard(tmp_path: Path) -> FileNotFoundError:
    """What the store raises when a sweep took its shard: a path under the root."""
    return FileNotFoundError(2, "No such file or directory", str(tmp_path / "blobs" / "ab" / "cd"))


def failing_first(monkeypatch, *errors: BaseException) -> list:
    """Make the shared store's write raise ``errors`` in turn, then succeed."""
    real = LocalBlobStore._publish
    calls: list = []

    def publish(self, fingerprint, write_bytes):
        calls.append(fingerprint)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]
        return real(self, fingerprint, write_bytes)

    monkeypatch.setattr(LocalBlobStore, "_publish", publish)
    return calls


@pytest.mark.parametrize("method", ["store", "store_file"])
def test_a_lost_shard_is_stored_again_once(monkeypatch, tmp_path, method):
    calls = failing_first(monkeypatch, lost_shard(tmp_path))
    store = SweepSafeLocalBlobStore(tmp_path / "blobs")

    uri = write(store, method, tmp_path)

    assert len(calls) == 2
    assert store.open(FINGERPRINT) == DATA
    assert uri == store.uri_for(FINGERPRINT)


@pytest.mark.parametrize("method", ["store", "store_file"])
def test_a_shard_lost_twice_is_the_callers_failure(monkeypatch, tmp_path, method):
    """One retry, not a loop: after a lost race every level is fresh, so a second loss is
    something else — and ``in_store`` classifies it as it would have anyway."""
    calls = failing_first(monkeypatch, lost_shard(tmp_path), lost_shard(tmp_path))

    with pytest.raises(FileNotFoundError):
        write(SweepSafeLocalBlobStore(tmp_path / "blobs"), method, tmp_path)

    assert len(calls) == 2


def test_any_other_failure_is_not_retried(monkeypatch, tmp_path):
    calls = failing_first(monkeypatch, PermissionError(13, "Permission denied", "ab"))

    with pytest.raises(PermissionError):
        SweepSafeLocalBlobStore(tmp_path / "blobs").store(DATA, FINGERPRINT, "text/plain")

    assert len(calls) == 1


def test_a_missing_source_file_is_not_taken_for_a_lost_shard(tmp_path):
    """Only a path under the root is the sweep's doing; anything else is raised as it was."""
    calls = []
    real = LocalBlobStore.store_file

    def counting(self, *args, **kwargs):
        calls.append(args)
        return real(self, *args, **kwargs)

    store = SweepSafeLocalBlobStore(tmp_path / "blobs")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(LocalBlobStore, "store_file", counting)
        with pytest.raises(FileNotFoundError):
            store.store_file(tmp_path / "absent", FINGERPRINT, "text/plain")

    assert len(calls) == 1


def test_the_retry_is_said_once(monkeypatch, tmp_path, caplog):
    """Rare and self-healing, but a rising count would mean a sweep is too eager."""
    failing_first(monkeypatch, lost_shard(tmp_path))

    with caplog.at_level(logging.INFO, logger="src.storage.local_store"):
        SweepSafeLocalBlobStore(tmp_path / "blobs").store(DATA, FINGERPRINT, "text/plain")

    (record,) = caplog.records
    assert record.content_fingerprint == FINGERPRINT
    assert "FileNotFoundError" in record.error


# --------------------------------------------------------------------------
# The reporting twins (co-core 0.41, cannobserv#530, #179)
#
# Since 0.41 a plain verb is its twin's ``.uri``: ``LocalBlobStore.store`` calls
# ``self.store_reporting``. The retry wraps the plain verbs, so it covers every
# call that enters through one, which is every call ``src/`` makes. A call made
# straight to a twin would skip it. #179 kept the overrides and guards the
# twins, rather than moving the retry onto them as co-core's docstring advises.
# The day ``src/`` needs a twin, move the retry onto the two twins and drop both
# plain-verb overrides. Keeping both nests the retry: up to 4 attempts per write.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("method", ["store", "store_file"])
def test_a_plain_verb_reaches_its_twin_inside_the_retry(monkeypatch, tmp_path, method):
    """The overrides survive 0.41's dispatch: a shard lost under the twin is still retried.

    Holds the 0.41 floor too: on 0.40 there is no twin to patch.
    """
    real = getattr(LocalBlobStore, TWINS[method])
    calls: list = []

    def twin(self, *args):
        calls.append(args)
        if len(calls) == 1:
            raise lost_shard(tmp_path)
        return real(self, *args)

    monkeypatch.setattr(LocalBlobStore, TWINS[method], twin)
    store = SweepSafeLocalBlobStore(tmp_path / "blobs")

    assert write(store, method, tmp_path) == store.uri_for(FINGERPRINT)
    assert len(calls) == 2


def test_the_retry_wraps_the_plain_verbs_and_no_twin():
    """Wrapping a twin as well would run the retry inside the retry."""
    overridden = set(vars(SweepSafeLocalBlobStore)) & (set(TWINS) | set(TWINS.values()))

    assert overridden == set(TWINS)


def twin_references(source: str) -> list[int]:
    """Lines naming a twin: an attribute, called or passed uncalled, or a ``getattr`` string.

    Every write site in ``src/`` hands ``asyncio.to_thread`` the bound method
    uncalled, so a call-only scan would miss the likeliest shape.
    """
    names = set(TWINS.values())
    return sorted(
        node.lineno
        for node in ast.walk(ast.parse(source))
        if (isinstance(node, ast.Attribute) and node.attr in names)
        or (isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in names)
    )


def test_the_twin_scan_sees_each_way_of_reaching_one():
    planted = (
        "store.store_reporting(data, fp, mt)\n"
        "asyncio.to_thread(store.store_file_reporting, path, fp, mt)\n"
        "getattr(store, 'store_reporting')\n"
        "store.store(data, fp, mt)\n"
    )

    assert twin_references(planted) == [1, 2, 3]


def test_nothing_in_src_reaches_a_reporting_twin():
    """``src/`` can't tell the local store from the object store, so no twin anywhere.

    Red means a twin call is coming: move ``SweepSafeLocalBlobStore``'s retry
    onto the twins and drop both plain-verb overrides, then retire this guard.
    """
    modules = sorted(SRC.rglob("*.py"))
    assert SRC / "worker" / "handler.py" in modules

    found = {
        str(path.relative_to(SRC)): lines
        for path in modules
        if (lines := twin_references(path.read_text()))
    }

    assert not found
