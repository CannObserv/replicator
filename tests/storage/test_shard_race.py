"""The sweep against a store writing into the same shard, interleaved on purpose (#159).

The race is one ``rmdir``: the sweep removes a shard directory the store has
checked or created but not yet put a temporary in, and the store's next step
fails with ``FileNotFoundError``. Left to chance it flaked
``test_run_dispatches_to_the_byte_path`` about one run in fifty, so each test
here forces the interleaving with a hook instead of looping for it.

Fetch and capture both write through the store ``_prepare_storage`` builds and
share the one sweep, so every case below is theirs alike — hence the names.
"""

import hashlib
import os
import time
from pathlib import Path
from types import SimpleNamespace

import co_core_sync.drivers.blobstore.local as shared_local
import pytest
from co_core_sync.drivers.blobstore import LocalBlobStore

from src.core.config import get_settings
from src.storage.sweeper import sweep
from src.worker.main import _prepare_storage

TTL = 600.0
TEMP_GRACE = 3600.0

DATA = b"fetched or captured"
FINGERPRINT = hashlib.sha256(DATA).hexdigest()


@pytest.fixture
def root(tmp_path):
    return tmp_path / "blobs"


@pytest.fixture
def worker_store(monkeypatch, root):
    """The store the worker builds over a local root — what fetch and capture write through."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(root))
    get_settings.cache_clear()
    store, _ = _prepare_storage(get_settings())
    return store


def leaf(root: Path) -> Path:
    return root / FINGERPRINT[0:2] / FINGERPRINT[2:4]


def age(path: Path, seconds: float) -> None:
    when = time.time() - seconds
    os.utime(path, (when, when))


def sweep_now(root: Path):
    return sweep(root, ttl_seconds=TTL, temp_grace_seconds=TEMP_GRACE)


def sweep_before_the_temporary(monkeypatch, root: Path) -> list:
    """Run one sweep inside the store: shard levels made, ``mkstemp`` not yet called."""
    real_mkstemp = shared_local.tempfile.mkstemp
    sweeps: list = []

    def mkstemp(*args, **kwargs):
        if not sweeps:
            sweeps.append(sweep_now(root))
        return real_mkstemp(*args, **kwargs)

    monkeypatch.setattr(shared_local, "tempfile", SimpleNamespace(mkstemp=mkstemp))
    return sweeps


def sweep_before_the_leaf(monkeypatch, root: Path) -> list:
    """Run one sweep after the store decided ``<ab>`` exists and before it makes ``<cd>``.

    Inside ``ensure_directory(<cd>)``, not before it: that call walks up from the
    leaf itself, so an ``<ab>`` removed any earlier is simply made again.
    """
    real_missing = shared_local._missing_levels
    sweeps: list = []

    def missing_levels(path):
        levels = real_missing(path)
        if path == leaf(root) and not sweeps:
            sweeps.append(sweep_now(root))
        return levels

    monkeypatch.setattr(shared_local, "_missing_levels", missing_levels)
    return sweeps


def test_a_sweep_leaves_a_shard_a_fetch_or_capture_store_just_made(monkeypatch, root):
    """The flake itself: a brand-new, still-empty shard is not the sweep's to remove.

    The plain shared store, so this holds on the sweep alone — no write-side retry.
    """
    sweeps = sweep_before_the_temporary(monkeypatch, root)

    uri = LocalBlobStore(root, touch_on_rereference=True).store(DATA, FINGERPRINT, "image/png")

    assert sweeps[0].shards_removed == 0
    assert Path(uri.removeprefix("file://")).read_bytes() == DATA


def test_a_fetch_or_capture_store_survives_a_sweep_removing_an_old_leaf_mid_write(
    monkeypatch, root, worker_store
):
    """An old, empty ``<cd>`` passes any grace — the store found it, then lost it."""
    leaf(root).mkdir(parents=True)
    age(leaf(root), TEMP_GRACE + 1)
    age(leaf(root).parent, TEMP_GRACE + 1)
    sweeps = sweep_before_the_temporary(monkeypatch, root)

    uri = worker_store.store(DATA, FINGERPRINT, "image/png")

    # The race really was lost once — and only ``<cd>`` went: its ``rmdir``
    # touched ``<ab>``, which the grace then kept.
    assert sweeps[0].shards_removed == 1
    assert leaf(root).parent.is_dir()
    assert Path(uri.removeprefix("file://")).read_bytes() == DATA


def test_a_fetch_or_capture_store_survives_a_sweep_removing_an_old_parent_mid_write(
    monkeypatch, root, worker_store
):
    """An old, empty ``<ab>`` goes between the store finding it and making ``<cd>`` inside."""
    leaf(root).parent.mkdir(parents=True)
    age(leaf(root).parent, TEMP_GRACE + 1)
    sweeps = sweep_before_the_leaf(monkeypatch, root)

    uri = worker_store.store(DATA, FINGERPRINT, "image/png")

    assert sweeps[0].shards_removed == 1
    assert Path(uri.removeprefix("file://")).read_bytes() == DATA
