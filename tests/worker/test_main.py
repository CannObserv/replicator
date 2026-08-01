"""Worker scaffold: the consumer is wired to the command stream and its group.

Assertions read the broker's own view (``xinfo_*``) rather than the consumer's
attributes — co-core keeps those private, and the observable Redis state is the
contract that actually matters for competing consumers and crash recovery.
"""

import asyncio
import errno
import json
import logging
import os
import signal
import stat
import tempfile
from pathlib import Path

import pytest
from co_core.effects.fetch import FetchResult
from co_core.pure.adapters.bus import streams
from co_core.pure.util.hashing import sha256

from src.core.config import get_settings
from src.core.logging import configure_logging
from src.storage.local import LocalBlobStore
from src.worker.main import (
    build_consumer,
    install_signal_handlers,
    remove_signal_handlers,
    run,
    warn_if_unreachable,
)
from tests.worker.conftest import make_command


def _stopped() -> asyncio.Event:
    """A pre-set stop event: run() does its startup work, then returns.

    Scaffold assertions (group, blob dir, log level) are about what happens
    before the loop, so they must not depend on a message ever arriving.
    """
    stop = asyncio.Event()
    stop.set()
    return stop


async def test_ensure_group_targets_the_command_stream(fake_redis):
    consumer = build_consumer(fake_redis, get_settings())
    await consumer.ensure_group(start_id="$")

    assert streams.CONTENT_FETCH == "content.fetch"
    assert await fake_redis.exists(streams.CONTENT_FETCH)


async def test_the_command_stream_is_overridable(fake_redis):
    """A live-broker test must be able to consume from a scratch stream.

    ``content.fetch`` is the live command stream ``replicator.service`` is
    polling: a frame an integration run added there would be fetched for real.
    The default stays production's, so only a caller that asks can move.
    """
    topic = "replicator.itest.fetch"

    consumer = build_consumer(fake_redis, get_settings(), topic=topic)
    await consumer.ensure_group(start_id="$")

    assert await fake_redis.exists(topic)
    assert not await fake_redis.exists(streams.CONTENT_FETCH)


async def test_consumer_registers_under_the_configured_group(fake_redis, monkeypatch):
    """The configured name is the identity Redis records — asserted on delivery.

    A *delivered* message is what registers a consumer; an empty poll does not
    (GH #3). fakeredis registers on either, so triggering it with real bytes is
    what keeps this assertion true of the broker Replicator actually runs
    against. The negative half lives in ``tests/worker/test_main_integration.py``.
    """
    monkeypatch.setenv("REPLICATOR_CONSUMER_GROUP", "replicator.fetch")
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator@test")

    consumer = build_consumer(fake_redis, get_settings())
    await consumer.ensure_group(start_id="$")
    await fake_redis.xadd(streams.CONTENT_FETCH, make_command())
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

    await run(_stopped())

    groups = await fake_redis.xinfo_groups(streams.CONTENT_FETCH)
    assert [g["name"] for g in groups] == [b"replicator.fetch"]


async def test_run_creates_the_blob_dir(monkeypatch, fake_redis, tmp_path):
    """systemd's StateDirectory makes the parent only — the leaf is ours (CR #3)."""
    blob_dir = tmp_path / "state" / "blobs"
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    assert not blob_dir.exists()

    await run(_stopped())

    assert blob_dir.is_dir()


async def test_run_tolerates_an_existing_blob_dir(monkeypatch, fake_redis, tmp_path):
    """Restarts must not fail on a directory the previous run created."""
    blob_dir = tmp_path / "blobs"
    blob_dir.mkdir()
    (blob_dir / "keep-me").write_bytes(b"prior content")
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    await run(_stopped())

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
            await run(_stopped())
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
        await run(_stopped())
        assert root.level == logging.DEBUG
    finally:
        root.handlers, root.level = saved_handlers, saved_level


async def test_sigterm_sets_the_stop_event():
    """SIGTERM must reach the loop as a stop request, not a mid-message kill."""
    stop = asyncio.Event()
    install_signal_handlers(stop)
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(stop.wait(), timeout=5)
    finally:
        remove_signal_handlers()


async def test_run_exits_cleanly_on_sigterm(monkeypatch, fake_redis, tmp_path):
    """End to end: an unstopped run() installs the handler and returns on SIGTERM.

    Exactly one signal is sent, and only after run() is observed to have taken
    over the disposition. Polling for that is what makes this deterministic:
    asyncio's ``remove_signal_handler`` restores ``SIG_DFL`` on the way out, so a
    signal sent speculatively — before the handler is installed or after it is
    torn down — would terminate the whole test process rather than fail one test.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    def _guard(*_):
        raise AssertionError("SIGTERM arrived outside run()'s handler window")

    previous = signal.signal(signal.SIGTERM, _guard)

    task = asyncio.create_task(run())
    try:
        for _ in range(500):
            if signal.getsignal(signal.SIGTERM) is not _guard:
                break  # run() has installed its own handler
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("run() never installed a SIGTERM handler")

        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, timeout=5)
    finally:
        task.cancel()
        signal.signal(signal.SIGTERM, previous)


async def test_worker_ready_reports_the_outage_window(monkeypatch, fake_redis, tmp_path, capsys):
    """CR #22: the number the unit is sized against belongs in the journal."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        await run(_stopped())
        record = json.loads(
            next(ln for ln in capsys.readouterr().out.splitlines() if "worker ready" in ln)
        )
    finally:
        root.handlers, root.level = saved_handlers, saved_level

    assert record["worst_case_outage_seconds"] == get_settings().worst_case_outage_seconds


async def test_run_closes_the_fetch_driver(monkeypatch, fake_redis, tmp_path):
    """The driver owns an httpx client; leaking it leaks sockets across restarts.

    Symmetric with the Redis client: ``run()`` owns both for the worker's
    lifetime and releases both on the way out, including when the loop raised.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    closed = []

    class RecordingDriver:
        async def execute(self, effect):
            raise AssertionError("no fetch expected in this test")

        async def aclose(self):
            closed.append(True)

    monkeypatch.setattr("src.worker.main.AsyncFetchDriver", RecordingDriver)

    await run(_stopped())

    assert closed == [True]


async def test_run_dispatches_to_the_byte_path(monkeypatch, fake_redis, tmp_path):
    """The loop must be wired to the real handler, not the placeholder logger.

    Asserted end to end: a command on the stream produces a blob on disk and a
    fact on ``content.blobs``. Without this, every piece could pass its own unit
    tests while ``run()`` still dispatched to ``log_only_handler``.
    """
    blob_dir = tmp_path / "blobs"
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setenv("REPLICATOR_CONSUMER_START_ID", "0")
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    class StubDriver:
        async def execute(self, effect):
            return FetchResult(
                content=b"page bytes",
                status_code=200,
                headers={"content-type": "text/plain"},
                duration_ms=3,
                fetcher_used="http",
            )

        async def aclose(self):
            return None

    monkeypatch.setattr("src.worker.main.AsyncFetchDriver", StubDriver)
    await fake_redis.xadd(streams.CONTENT_FETCH, make_command("cmd-wired"))

    stop = asyncio.Event()

    async def stop_once_published():
        for _ in range(500):
            if await fake_redis.exists(streams.CONTENT_BLOBS):
                break
            await asyncio.sleep(0.01)
        stop.set()

    await asyncio.gather(run(stop), stop_once_published())

    fingerprint = sha256(b"page bytes")
    assert LocalBlobStore(blob_dir).open(fingerprint) == b"page bytes"
    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 1


async def test_an_unreachable_blob_dir_is_warned_about(monkeypatch, fake_redis, tmp_path, capsys):
    """The store leaves a pre-existing directory's mode alone — so say when it blocks readers.

    The failure this catches has no local symptom: blobs store, the fact
    publishes, and only the consuming service notices that blob_uri cannot be
    opened. A warning, not an error — a single-user deployment is legitimate.
    """
    blob_dir = tmp_path / "blobs"
    blob_dir.mkdir(mode=0o700)
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        await run(_stopped())
        record = json.loads(
            next(
                ln
                for ln in capsys.readouterr().out.splitlines()
                if "not traversable by other services" in ln
            )
        )
        assert record["level"] == "WARNING"
        assert record["blocked_at"][str(blob_dir)] == "0o700"
    finally:
        root.handlers, root.level = saved_handlers, saved_level


async def test_a_traversable_blob_dir_is_not_among_the_blocked_levels(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """Asserted per level, not on the warning's absence.

    Any test tree lives under pytest's own ``tmp_path`` root, which is 0700 by
    design — so the walk legitimately reports it, and "no warning at all" is not
    an assertion this environment can make. The deployed chain
    (``/var``, ``/var/lib``, systemd's ``StateDirectory``) is 0755 throughout.
    """
    blob_dir = tmp_path / "blobs"
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        await run(_stopped())
        record = json.loads(
            next(
                ln
                for ln in capsys.readouterr().out.splitlines()
                if "not traversable by other services" in ln
            )
        )
        assert str(blob_dir) not in record["blocked_at"]
    finally:
        root.handlers, root.level = saved_handlers, saved_level


async def test_a_restrictive_ancestor_is_warned_about(monkeypatch, fake_redis, tmp_path, capsys):
    """Traversal needs +x on the whole chain, so a fine leaf is not enough.

    The likeliest shape of the mistake: an operator pre-creates the parent
    restrictively and lets the worker create the leaf underneath, which comes
    out 0755 over 0700 and looks correct if only the leaf is inspected.
    """
    parent = tmp_path / "restricted"
    parent.mkdir(mode=0o700)
    blob_dir = parent / "blobs"
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        await run(_stopped())
        record = json.loads(
            next(
                ln
                for ln in capsys.readouterr().out.splitlines()
                if "not traversable by other services" in ln
            )
        )
        assert stat.S_IMODE(blob_dir.stat().st_mode) == 0o755  # the leaf itself is fine
        assert str(blob_dir) not in record["blocked_at"]  # ...and would pass a leaf-only check
        assert record["blocked_at"][str(parent)] == "0o700"
    finally:
        root.handlers, root.level = saved_handlers, saved_level


def test_an_unstatable_path_does_not_raise(monkeypatch, tmp_path, capsys):
    """A diagnostic that cannot complete must not be what kills startup.

    Exercised against the helper rather than through ``run()``: patching
    ``Path.stat`` reaches ``ensure_directory`` too, which would fail earlier at
    its own guard and never reach this one.
    """

    def denied(self, *args, **kwargs):
        raise PermissionError(errno.EACCES, "Permission denied", str(self))

    configure_logging("INFO")
    monkeypatch.setattr(Path, "stat", denied)

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        warn_if_unreachable(tmp_path / "blobs")  # must return, not raise

        record = json.loads(
            next(
                ln
                for ln in capsys.readouterr().out.splitlines()
                if "could not check whether the blob directory is reachable" in ln
            )
        )
        assert record["errno"] == errno.EACCES
    finally:
        root.handlers, root.level = saved_handlers, saved_level


def test_a_fully_traversable_chain_warns_about_nothing(capsys):
    """The quiet path, which needs a directory chain pytest cannot provide.

    ``tmp_path`` lives under pytest's own 0700 root, so no path it hands out has
    a clean chain. A directory directly under ``/tmp`` (1777) does, and matches
    the deployed shape: ``/var/lib/replicator/blobs`` is 0755 the whole way up.
    """
    blob_dir = Path(tempfile.mkdtemp(prefix="replicator-reachable-"))
    os.chmod(blob_dir, 0o755)
    configure_logging("INFO")

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        capsys.readouterr()  # discard configure_logging's own output
        warn_if_unreachable(blob_dir)

        assert "not traversable" not in capsys.readouterr().out
    finally:
        root.handlers, root.level = saved_handlers, saved_level
        blob_dir.rmdir()
