"""Each handler gets its own store at startup (#189, archiver#380 D4).

Fetch writes the raw temp store; capture writes the captures temp store, and
never the raw one; persist gets both families; replicate is told the captures
stores are refused sources; the alias guard covers the capture bucket; and under
``local`` one sweep covers both roots into one usage figure.
"""

import asyncio
import hashlib
import json
import os

import pytest

import src.worker.main
from src.core.config import CAPTURE_KEPT_PREFIX, CAPTURE_TEMP_PREFIX, get_settings
from src.worker.main import run
from src.worker.persist import StoreFamily


@pytest.fixture(autouse=True)
def _boot(monkeypatch, tmp_path, fake_redis):
    monkeypatch.setenv("REPLICATOR_READ_BLOCK_MS", "50")
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)


class FakeRenderer:
    async def preflight(self) -> None:
        pass


class FakeGcsStore:
    """Records how each bucket store was built; preflight answers."""

    built: list

    def __init__(self, bucket, *, prefix="blobs", touch_on_rereference=False, **_options):
        self.bucket = bucket
        self.prefix = prefix
        self.touch = touch_on_rereference
        FakeGcsStore.built.append(self)

    def preflight(self):
        pass

    def uri_for(self, fingerprint):
        return f"gs://{self.bucket}/{self.prefix}/{fingerprint}.bin"


@pytest.fixture
def recorded(monkeypatch):
    """Every builder's kwargs, by name; the loops are stubbed and end the run."""
    seen: dict = {}

    def recording(name, real):
        def build(**kwargs):
            seen[name] = kwargs
            return real(**kwargs)

        return build

    for name in (
        "build_handler",
        "build_capture_handler",
        "build_persist_handler",
        "build_replicate_handler",
    ):
        monkeypatch.setattr(
            f"src.worker.main.{name}", recording(name, getattr(src.worker.main, name))
        )

    async def stub_run_loop(**kwargs):
        pass

    async def stub_run_sweeper(**kwargs):
        seen["run_sweeper"] = kwargs

    monkeypatch.setattr("src.worker.main.run_loop", stub_run_loop)
    monkeypatch.setattr("src.worker.main.run_sweeper", stub_run_sweeper)
    monkeypatch.setattr("src.worker.main.build_renderer", lambda settings, guard: FakeRenderer())
    return seen


@pytest.fixture
def gcs(monkeypatch):
    FakeGcsStore.built = []
    monkeypatch.setattr("src.worker.main.GcsBlobStore", FakeGcsStore)
    monkeypatch.setenv("REPLICATOR_BLOB_BACKEND", "gcs")
    monkeypatch.setenv("REPLICATOR_BLOB_BUCKET", "a-temp-bucket")
    monkeypatch.setenv("REPLICATOR_PERMANENT_BUCKET", "a-permanent-bucket")
    monkeypatch.setenv("REPLICATOR_CAPTURE_BUCKET", "a-captures-bucket")
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    monkeypatch.setenv("REPLICATOR_PERSIST_ENABLED", "true")
    get_settings.cache_clear()
    return FakeGcsStore


# --------------------------------------------------------------------------
# The object store: production's shape
# --------------------------------------------------------------------------


async def test_a_capture_lands_in_the_captures_temp_tier_never_the_raw_bucket(gcs, recorded):
    await run(asyncio.Event())

    capture_store = recorded["build_capture_handler"]["store"]
    assert (capture_store.bucket, capture_store.prefix) == ("a-captures-bucket", "temp")
    assert capture_store.touch is True, "the temp tier's lifecycle rule reads customTime"


async def test_a_fetch_never_lands_in_the_captures_bucket(gcs, recorded):
    """HTTP and `render="browser"` alike: both are the fetch handler's one store."""
    await run(asyncio.Event())

    fetch_store = recorded["build_handler"]["store"]
    assert fetch_store.bucket == "a-temp-bucket"
    assert all(
        store.bucket != "a-captures-bucket"
        for store in (fetch_store, recorded["build_persist_handler"]["permanent"])
    )


async def test_persist_gets_the_captures_family_kept_with_touch_off(gcs, recorded):
    await run(asyncio.Event())

    family = recorded["build_persist_handler"]["captures"]
    assert isinstance(family, StoreFamily)
    assert family.temp is recorded["build_capture_handler"]["store"]
    assert (family.kept.bucket, family.kept.prefix) == ("a-captures-bucket", CAPTURE_KEPT_PREFIX)
    assert family.kept.touch is False, "the kept tier's writer holds no update"


async def test_replicate_refuses_both_captures_stores(gcs, recorded):
    await run(asyncio.Event())

    family = recorded["build_persist_handler"]["captures"]
    assert tuple(recorded["build_replicate_handler"]["refused_stores"]) == tuple(family)


async def test_no_alias_may_bind_the_capture_bucket(gcs, recorded, monkeypatch):
    hosts = {}
    real = src.worker.main.load_alias_table

    def recording(path, *, host_stores):
        hosts["stores"] = host_stores
        return real(path, host_stores=host_stores)

    monkeypatch.setattr("src.worker.main.load_alias_table", recording)

    await run(asyncio.Event())

    assert "a-captures-bucket" in hosts["stores"]


async def test_the_boot_line_names_the_bucket_and_both_prefixes(gcs, recorded, capsys):
    await run(asyncio.Event())

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    (boot,) = [
        line for line in lines if line.get("message") == "storing captures in their own bucket"
    ]
    assert boot["capture_bucket"] == "a-captures-bucket"
    assert boot["capture_temp_prefix"] == CAPTURE_TEMP_PREFIX
    assert boot["capture_kept_prefix"] == CAPTURE_KEPT_PREFIX


async def test_no_capture_bucket_builds_no_captures_store(monkeypatch, recorded):
    FakeGcsStore.built = []
    monkeypatch.setattr("src.worker.main.GcsBlobStore", FakeGcsStore)
    monkeypatch.setenv("REPLICATOR_BLOB_BACKEND", "gcs")
    monkeypatch.setenv("REPLICATOR_BLOB_BUCKET", "a-temp-bucket")
    monkeypatch.delenv("REPLICATOR_CAPTURE_BUCKET", raising=False)
    monkeypatch.delenv("REPLICATOR_CAPTURE_ENABLED", raising=False)
    get_settings.cache_clear()

    await run(asyncio.Event())

    assert [store.bucket for store in FakeGcsStore.built] == ["a-temp-bucket"]
    assert tuple(recorded["build_replicate_handler"]["refused_stores"]) == ()


# --------------------------------------------------------------------------
# The local backend: its own root, one sweep, one usage figure
# --------------------------------------------------------------------------


async def test_under_local_captures_get_their_own_root(monkeypatch, recorded, tmp_path):
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()

    await run(asyncio.Event())

    blob_dir = (tmp_path / "blobs").resolve()
    assert recorded["build_handler"]["store"].root == blob_dir
    assert recorded["build_capture_handler"]["store"].root == blob_dir / "captures"
    assert (blob_dir / "captures").is_dir()


async def test_under_local_one_sweep_covers_both_roots_into_one_usage(
    monkeypatch, recorded, tmp_path
):
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()

    await run(asyncio.Event())

    blob_dir = (tmp_path / "blobs").resolve()
    assert tuple(recorded["run_sweeper"]["roots"]) == (blob_dir, blob_dir / "captures")
    assert recorded["run_sweeper"]["usage"] is recorded["build_capture_handler"]["usage"]


async def test_under_local_with_capture_off_no_captures_root_is_made(
    monkeypatch, recorded, tmp_path
):
    monkeypatch.delenv("REPLICATOR_CAPTURE_ENABLED", raising=False)
    get_settings.cache_clear()

    await run(asyncio.Event())

    assert not (tmp_path / "blobs" / "captures").exists()


async def test_under_local_a_re_capture_restarts_the_retention_clock(
    monkeypatch, recorded, tmp_path
):
    """The sweep reaps on "since last referenced", as for the raw root."""
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()
    await run(asyncio.Event())
    store = recorded["build_capture_handler"]["store"]
    data = b"\x89PNG a re-capture"
    fingerprint = hashlib.sha256(data).hexdigest()
    store.store(data, fingerprint, "image/png")
    path = store.root / fingerprint[0:2] / fingerprint[2:4] / f"{fingerprint}.bin"
    os.utime(path, (1, 1))

    store.store(data, fingerprint, "image/png")

    assert path.stat().st_mtime > 1


# --------------------------------------------------------------------------
# A captures store that cannot be used stops the boot, naming itself
# --------------------------------------------------------------------------


def _journal(capsys) -> list[dict]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


async def test_a_capture_bucket_that_does_not_answer_stops_the_boot(
    gcs, recorded, capsys, monkeypatch
):
    """As for the other two buckets: storing to an absent bucket is never meant."""

    def refuse(self):
        if self.bucket == "a-captures-bucket":
            raise PermissionError("403 storage.objects.list")

    monkeypatch.setattr(gcs, "preflight", refuse)

    with pytest.raises(PermissionError):
        await run(asyncio.Event())

    (line,) = [
        entry
        for entry in _journal(capsys)
        if entry.get("message") == "capture bucket is not usable"
    ]
    assert line["capture_bucket"] == "a-captures-bucket"
    assert "403" in line["error"]


async def test_a_capture_directory_that_cannot_be_made_stops_the_boot(
    monkeypatch, recorded, tmp_path, capsys
):
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()
    real = src.worker.main.ensure_directory

    def refuse_the_captures_root(path):
        if path.name == "captures":
            raise PermissionError(13, "Permission denied", str(path))
        return real(path)

    monkeypatch.setattr("src.worker.main.ensure_directory", refuse_the_captures_root)

    with pytest.raises(PermissionError):
        await run(asyncio.Event())

    (line,) = [
        entry
        for entry in _journal(capsys)
        if entry.get("message") == "capture directory is not usable"
    ]
    assert line["capture_dir"] == str((tmp_path / "blobs").resolve() / "captures")
    assert line["errno"] == 13
