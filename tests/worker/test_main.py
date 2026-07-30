"""Worker scaffold: the consumer is wired to the command stream and its group.

Assertions read the broker's own view (``xinfo_*``) rather than the consumer's
attributes — co-core keeps those private, and the observable Redis state is the
contract that actually matters for competing consumers and crash recovery.
"""

import errno
import json
import logging

import pytest
from co_core.pure.adapters.bus import streams

from src.core.config import get_settings
from src.worker.main import build_consumer, run


async def test_ensure_group_targets_the_command_stream(fake_redis):
    consumer = build_consumer(fake_redis, get_settings())
    await consumer.ensure_group(start_id="$")

    assert streams.CONTENT_FETCH == "content.fetch"
    assert await fake_redis.exists(streams.CONTENT_FETCH)


async def test_consumer_registers_under_the_configured_group(fake_redis, monkeypatch):
    monkeypatch.setenv("REPLICATOR_CONSUMER_GROUP", "replicator.fetch")
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator@test")

    consumer = build_consumer(fake_redis, get_settings())
    await consumer.ensure_group(start_id="$")
    # A read registers this consumer within the group, even with nothing pending.
    await consumer.read(count=1, block_ms=1)

    groups = await fake_redis.xinfo_groups(streams.CONTENT_FETCH)
    assert [g["name"] for g in groups] == [b"replicator.fetch"]

    consumers = await fake_redis.xinfo_consumers(streams.CONTENT_FETCH, "replicator.fetch")
    assert [c["name"] for c in consumers] == [b"replicator@test"]


async def test_ensure_group_is_idempotent(fake_redis):
    """A pre-existing group must not raise — every restart re-runs ensure_group."""
    consumer = build_consumer(fake_redis, get_settings())
    await consumer.ensure_group(start_id="$")
    await consumer.ensure_group(start_id="$")

    groups = await fake_redis.xinfo_groups(streams.CONTENT_FETCH)
    assert [g["name"] for g in groups] == [b"replicator.fetch"]


async def test_run_creates_the_group_and_closes_its_client(monkeypatch, fake_redis, tmp_path):
    """run() owns the client lifetime — the co-core driver never closes it."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    await run()

    groups = await fake_redis.xinfo_groups(streams.CONTENT_FETCH)
    assert [g["name"] for g in groups] == [b"replicator.fetch"]


async def test_run_creates_the_blob_dir(monkeypatch, fake_redis, tmp_path):
    """systemd's StateDirectory makes the parent only — the leaf is ours (CR #3)."""
    blob_dir = tmp_path / "state" / "blobs"
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    assert not blob_dir.exists()

    await run()

    assert blob_dir.is_dir()


async def test_run_tolerates_an_existing_blob_dir(monkeypatch, fake_redis, tmp_path):
    """Restarts must not fail on a directory the previous run created."""
    blob_dir = tmp_path / "blobs"
    blob_dir.mkdir()
    (blob_dir / "keep-me").write_bytes(b"prior content")
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    await run()

    assert (blob_dir / "keep-me").read_bytes() == b"prior content"


async def test_unusable_blob_dir_is_logged_then_raised(monkeypatch, fake_redis, tmp_path, capsys):
    """An unusable path must fail on boot, structurally — not as a bare traceback."""
    blocker = tmp_path / "not-a-dir"
    blocker.write_bytes(b"")
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blocker / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        with pytest.raises(OSError):
            await run()
        record = json.loads(
            next(
                ln
                for ln in capsys.readouterr().out.splitlines()
                if "blob directory is not usable" in ln
            )
        )
        assert record["level"] == "ERROR"
        assert record["blob_dir"] == str(blocker / "blobs")
        assert record["errno"] == errno.ENOTDIR
    finally:
        root.handlers, root.level = saved_handlers, saved_level


async def test_run_applies_the_configured_log_level(monkeypatch, fake_redis, tmp_path):
    """REPLICATOR_LOG_LEVEL reaches the root logger via run() (CR #1)."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setenv("REPLICATOR_LOG_LEVEL", "DEBUG")
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        await run()
        assert root.level == logging.DEBUG
    finally:
        root.handlers, root.level = saved_handlers, saved_level
