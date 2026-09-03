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
import time
from pathlib import Path

import pytest
from co_core.effects.fetch import FetchResult
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import FetchPolicyState
from co_core.pure.util.hashing import sha256

import src.worker.main
from src.core.config import get_settings
from src.core.logging import configure_logging
from src.storage.local import LocalBlobStore
from src.storage.sweeper import SweepResult
from src.worker.main import (
    build_consumer,
    consumer_name_for,
    install_signal_handlers,
    remove_signal_handlers,
    resolve_consumer_name,
    run,
    warn_if_unreachable,
)
from tests.worker.conftest import make_command, now


@pytest.fixture(autouse=True)
def _short_poll_window(monkeypatch):
    """Shrink the blocking-read window for every ``run()`` test in this module.

    fakeredis honours ``block`` on the groupless ``XREAD`` the policy reader uses
    (#19), while returning immediately from the consume loop's ``XREADGROUP`` —
    so on the fake, and only there, shutdown is held for a full poll window by
    the one task that is not what any of these tests are about. On a live broker
    both block concurrently and the worst case is the larger of the two, which is
    what ``TimeoutStopSec`` is already sized for.
    """
    monkeypatch.setenv("REPLICATOR_READ_BLOCK_MS", "50")
    get_settings.cache_clear()


# How long the end-to-end byte-path test waits for its fact to land. A liveness
# bound, not a latency assertion: generous enough that a loaded machine does not
# trip it, finite so a genuine hang still ends the run (CR round 1).
BYTE_PATH_DEADLINE_SECONDS = 30.0


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


def test_consumer_name_is_derived_from_the_group():
    """``replicator.fetch`` -> ``replicator-fetch-1`` (#77, archiver#156).

    Derived from the group rather than written per caller, so the next group
    consumer inherits the convention instead of copying a literal — and stable
    across restarts *and* VM moves, which is the whole point: a restart reuses
    its registration, so the orphan leak cannot recur and needs no periodic
    sweep and no shutdown hook that a ``SIGKILL`` would skip anyway.
    """
    assert resolve_consumer_name("replicator.fetch") == "replicator-fetch-1"
    assert resolve_consumer_name("replicator.replicate") == "replicator-replicate-1"


def test_one_precedence_rule_serves_every_caller(monkeypatch):
    """``consumer_name_for`` is the single site where an override beats the default.

    It was written out three times — in ``build_consumer`` and once per journal
    name — and the copies agreed only because their ``group`` arguments were kept
    in step by hand. A journal line naming a consumer the broker never registered
    sends an operator to ``XINFO`` after something that does not exist (CR round 1).
    """
    monkeypatch.delenv("REPLICATOR_CONSUMER_NAME", raising=False)
    monkeypatch.delenv("REPLICATOR_REPLICATE_CONSUMER_NAME", raising=False)
    settings = get_settings()

    assert consumer_name_for(settings, settings.consumer_group) == "replicator-fetch-1"
    assert (
        consumer_name_for(settings, settings.replicate_consumer_group) == "replicator-replicate-1"
    )


def test_an_override_moves_only_its_own_group(monkeypatch):
    """Overriding the fetch name must not rename the replicate consumer.

    The dev-worker invocation the docs *require* while the service runs sets the
    fetch override; when that also renamed the replicate consumer, the registration
    in ``replicator.replicate`` read ``replicator-fetch-…`` (CR round 1).
    """
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator-fetch-greg-dev")
    monkeypatch.delenv("REPLICATOR_REPLICATE_CONSUMER_NAME", raising=False)
    settings = get_settings()

    assert consumer_name_for(settings, settings.consumer_group) == "replicator-fetch-greg-dev"
    assert (
        consumer_name_for(settings, settings.replicate_consumer_group) == "replicator-replicate-1"
    )


async def test_the_override_reaches_the_registration_it_names(fake_redis, monkeypatch):
    """End to end at the broker: each override lands in its own group, and only it."""
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator-fetch-9")
    monkeypatch.setenv("REPLICATOR_REPLICATE_CONSUMER_NAME", "replicator-replicate-9")
    settings = get_settings()

    fetch = build_consumer(fake_redis, settings)
    replicate = build_consumer(
        fake_redis,
        settings,
        topic=streams.CONTENT_REPLICATE,
        group=settings.replicate_consumer_group,
    )
    for reader in (fetch, replicate):
        await reader.ensure_group(start_id="$")
        await reader.read(count=1, block_ms=1)

    registered = {
        topic: [c["name"] for c in await fake_redis.xinfo_consumers(topic, group)]
        for topic, group in (
            (streams.CONTENT_FETCH, settings.consumer_group),
            (streams.CONTENT_REPLICATE, settings.replicate_consumer_group),
        )
    }
    assert registered == {
        streams.CONTENT_FETCH: [b"replicator-fetch-9"],
        streams.CONTENT_REPLICATE: [b"replicator-replicate-9"],
    }


async def test_each_group_registers_under_its_own_derived_name(fake_redis, monkeypatch):
    """Two command streams, two groups, two names — with nothing configured.

    The bug this closes is that both loops took a single process-wide
    ``REPLICATOR_CONSUMER_NAME``, so the derivation has to be per-group or the
    two registrations collapse back onto one string.

    fakeredis registers a consumer on an empty poll where the live broker needs a
    delivery (GH #3) — relied on here deliberately, so the assertion is about the
    name ``build_consumer`` passed and not about decoding a payload for each of
    two different command schemas.
    """
    monkeypatch.delenv("REPLICATOR_CONSUMER_NAME", raising=False)
    get_settings.cache_clear()
    settings = get_settings()

    fetch = build_consumer(fake_redis, settings)
    replicate = build_consumer(
        fake_redis,
        settings,
        topic=streams.CONTENT_REPLICATE,
        group=settings.replicate_consumer_group,
    )
    for reader in (fetch, replicate):
        await reader.ensure_group(start_id="$")
        await reader.read(count=1, block_ms=1)

    registered = {
        topic: [c["name"] for c in await fake_redis.xinfo_consumers(topic, group)]
        for topic, group in (
            (streams.CONTENT_FETCH, settings.consumer_group),
            (streams.CONTENT_REPLICATE, settings.replicate_consumer_group),
        )
    }
    assert registered == {
        streams.CONTENT_FETCH: [b"replicator-fetch-1"],
        streams.CONTENT_REPLICATE: [b"replicator-replicate-1"],
    }


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


async def test_worker_ready_names_both_consumers(monkeypatch, fake_redis, tmp_path, capsys):
    """One name per group in the journal, because there is no longer just one.

    The line used to carry ``settings.consumer_name``, which is now the *override*
    and unset on every host — logging it would print ``null`` and leave an operator
    reading ``XINFO`` with nothing to match against (#77).
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.delenv("REPLICATOR_CONSUMER_NAME", raising=False)
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    get_settings.cache_clear()

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        await run(_stopped())
        record = json.loads(
            next(ln for ln in capsys.readouterr().out.splitlines() if "worker ready" in ln)
        )
    finally:
        root.handlers, root.level = saved_handlers, saved_level

    assert record["consumer"] == "replicator-fetch-1"
    assert record["replicate_consumer"] == "replicator-replicate-1"


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
    published = asyncio.Event()

    async def stop_once_published():
        """Stop as soon as the fact lands, and record *whether* it ever did.

        The bound is wall clock, so a loaded machine used to trip it silently: the
        old spelling set ``stop`` on timeout exactly as it did on success, and the
        test then died in ``LocalBlobStore.open`` with a bare ``FileNotFoundError``
        that reads as a byte-path regression rather than the timeout it was. Set
        generously — this is a liveness bound, not a latency assertion (CR round 1).
        """
        try:
            deadline = time.monotonic() + BYTE_PATH_DEADLINE_SECONDS
            while time.monotonic() < deadline:
                if await fake_redis.exists(streams.CONTENT_BLOBS):
                    published.set()
                    return
                await asyncio.sleep(0.01)
        finally:
            # In a ``finally`` so a timeout still releases ``run()``; leaving it
            # blocked would hang the gather rather than fail the assertion below.
            stop.set()

    await asyncio.gather(run(stop), stop_once_published())

    assert published.is_set(), (
        f"content.blobs never appeared within {BYTE_PATH_DEADLINE_SECONDS}s — the byte "
        "path did not complete. On a loaded machine that is this bound expiring, not a "
        "regression; check the machine before hunting in run()."
    )

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


async def test_run_sweeps_alongside_the_consume_loop(monkeypatch, fake_redis, tmp_path):
    """Retention is a second task in the same process, not a separate unit.

    Without this the sweeper could be complete, tested, and never actually run —
    a blob directory that grows forever while every unit test passes.
    """
    blob_dir = tmp_path / "blobs"
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    swept = []
    stop = asyncio.Event()

    def recording_sweep(root, **kwargs):
        swept.append(root)
        stop.set()
        return SweepResult()

    monkeypatch.setattr("src.worker.retention.sweep", recording_sweep)

    async with asyncio.timeout(5):
        await run(stop)

    assert swept == [blob_dir.resolve()]


async def test_the_sweeper_and_the_byte_path_share_one_measurement(
    monkeypatch, fake_redis, tmp_path
):
    """Two BlobUsage instances would make the ceiling permanently unreachable.

    The sweep measures the tree and the byte path adds to it; wired to separate
    objects both are individually correct and the guard never fires. Nothing
    else observes the difference until the disk is full.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    seen = {}
    real_build_handler = src.worker.main.build_handler

    def recording_build_handler(**kwargs):
        seen["handler"] = kwargs["usage"]
        return real_build_handler(**kwargs)

    async def recording_run_sweeper(**kwargs):
        seen["sweeper"] = kwargs["usage"]

    monkeypatch.setattr("src.worker.main.build_handler", recording_build_handler)
    monkeypatch.setattr("src.worker.main.run_sweeper", recording_run_sweeper)

    await run(_stopped())

    assert seen["handler"] is seen["sweeper"]


async def test_the_byte_path_and_the_policy_reader_share_one_map(monkeypatch, fake_redis, tmp_path):
    """Two ``FetchPolicyMap`` instances and the numbers never reach the pacer.

    The same failure shape as two ``BlobUsage`` instances: both halves stay
    individually correct, the worker logs policies being applied, and every host
    is still paced at the fallback with nothing observing the difference.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    seen = {}
    real_build_handler = src.worker.main.build_handler

    def recording_build_handler(**kwargs):
        seen["policy"] = kwargs["policy"]
        return real_build_handler(**kwargs)

    async def recording_run_policy_reader(*args, **kwargs):
        seen["reader"] = kwargs["policies"]

    monkeypatch.setattr("src.worker.main.build_handler", recording_build_handler)
    monkeypatch.setattr("src.worker.main.run_policy_reader", recording_run_policy_reader)

    await run(_stopped())

    # The handler is given a bound method, not the map, so the identity is
    # asserted through it — which is also what proves the byte path reads the
    # instance the reader writes to.
    assert seen["policy"].__self__ is seen["reader"]


async def test_the_policy_map_is_rebuilt_before_the_consume_loop_starts(
    monkeypatch, fake_redis, tmp_path
):
    """Started as a peer, the loop would fetch its opening commands against an
    empty map and pace every host at the fallback — safe only because the
    fallback is supposed to be the stricter number.

    Not ``_stopped()``: since CR #13 the replay honours the stop event, so a
    pre-set one correctly skips it. The stubbed consume loop returning is what
    ends the run here instead.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    await fake_redis.xadd(
        streams.CONTENT_FETCH_POLICY,
        to_wire(FetchPolicyState(occurred_at=now(), host="slow.test", min_interval_seconds=30.0)),
    )
    seen = {}

    async def recording_run_loop(**kwargs):
        seen["at_loop_start"] = policies["map"].interval_for("slow.test")

    real_map = src.worker.main.FetchPolicyMap
    policies = {}

    def recording_map(default):
        policies["map"] = real_map(default)
        return policies["map"]

    monkeypatch.setattr("src.worker.main.FetchPolicyMap", recording_map)
    monkeypatch.setattr("src.worker.main.run_loop", recording_run_loop)

    await run(asyncio.Event())

    assert seen["at_loop_start"] == 30.0


async def test_a_failing_consume_loop_stops_the_sweeper_and_propagates(
    monkeypatch, fake_redis, tmp_path
):
    """The loop re-raises so the unit restarts; a surviving task would hang that exit."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    returned = []

    async def failing_run_loop(**kwargs):
        raise RuntimeError("broker is gone")

    async def recording_run_sweeper(**kwargs):
        try:
            await kwargs["stop"].wait()
        finally:
            returned.append(True)

    monkeypatch.setattr("src.worker.main.run_loop", failing_run_loop)
    monkeypatch.setattr("src.worker.main.run_sweeper", recording_run_sweeper)

    async with asyncio.timeout(5):
        with pytest.raises(RuntimeError, match="broker is gone"):
            await run(asyncio.Event())

    assert returned == [True]


async def test_a_failing_sweeper_does_not_go_unnoticed(monkeypatch, fake_redis, tmp_path):
    """``run_sweeper`` absorbs a failed sweep; anything escaping it is a bug.

    Letting that task die quietly would leave the worker consuming normally with
    retention silently switched off — the exact state this issue exists to end.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    async def failing_run_sweeper(**kwargs):
        raise RuntimeError("sweeper bug")

    monkeypatch.setattr("src.worker.main.run_sweeper", failing_run_sweeper)

    async with asyncio.timeout(5):
        with pytest.raises(RuntimeError, match="sweeper bug"):
            await run(asyncio.Event())


async def test_cancelling_the_worker_does_not_strand_its_tasks(monkeypatch, fake_redis, tmp_path):
    """A cancel that skips the wind-down leaves "Task was destroyed" at exit.

    By then the cancellation that caused it is long gone, so the tasks are
    cancelled and awaited on the way out rather than dropped.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    started = asyncio.Event()
    finished = []

    async def recording_run_sweeper(**kwargs):
        started.set()
        try:
            await asyncio.Event().wait()  # never set: only cancellation ends this
        except asyncio.CancelledError:
            finished.append(True)
            raise

    monkeypatch.setattr("src.worker.main.run_sweeper", recording_run_sweeper)
    task = asyncio.create_task(run(asyncio.Event()))
    await asyncio.wait_for(started.wait(), 5)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished == [True]


async def test_a_sweeper_failing_during_shutdown_is_still_reported(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """The first exit explains the shutdown; the survivors' failures must not vanish.

    ``test_a_failing_sweeper_does_not_go_unnoticed`` covers the sweeper losing
    the race to fail. Losing it the other way — SIGTERM arrives, the loop returns
    cleanly, and the sweeper then raises on its way out — used to be swallowed
    whole by ``return_exceptions=True``.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    async def failing_on_the_way_out(**kwargs):
        await kwargs["stop"].wait()
        # Outlives the consume loop's own return, which is what puts this task
        # in `pending` rather than in the batch `asyncio.wait` returns.
        await asyncio.sleep(0.01)
        raise RuntimeError("sweeper bug during shutdown")

    monkeypatch.setattr("src.worker.main.run_sweeper", failing_on_the_way_out)
    configure_logging(logging.INFO)

    await run(_stopped())

    logged = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    errors = [record for record in logged if record["level"] == "ERROR"]
    assert any("sweeper bug during shutdown" in record["error"] for record in errors)


async def test_the_sweeper_is_given_an_absolute_root(monkeypatch, fake_redis, tmp_path):
    """The store resolves its root at construction; the sweep has no such guard.

    Handed the raw setting — which defaults to the relative ``blobs`` — a chdir
    would leave the two working on different directories, one writing where the
    other is not reaping.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", "blobs")
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    roots = []

    async def recording_run_sweeper(**kwargs):
        roots.append(kwargs["root"])

    monkeypatch.setattr("src.worker.main.run_sweeper", recording_run_sweeper)

    await run(_stopped())

    assert roots == [tmp_path.resolve() / "blobs"]


async def test_every_task_failure_is_reported_even_when_they_land_together(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """Both tasks can finish in one pass, and then neither is a "survivor".

    ``asyncio.wait`` returns every task that completed, not just the first, so a
    simultaneous pair leaves nothing pending — and the loser's failure had no
    path to the journal at all. Only one of the two can be raised; the other has
    to be logged or it is gone.

    Which one is raised is decided by argument order, not by timing: the consume
    loop is passed first and wins the tie. That is why this asserts on the loop's
    message rather than either — see ``_run_until_first_exit``.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    async def failing_run_loop(**kwargs):
        raise RuntimeError("loop failed")

    async def failing_run_sweeper(**kwargs):
        raise RuntimeError("sweeper failed")

    monkeypatch.setattr("src.worker.main.run_loop", failing_run_loop)
    monkeypatch.setattr("src.worker.main.run_sweeper", failing_run_sweeper)
    configure_logging(logging.INFO)

    with pytest.raises(RuntimeError, match="loop failed"):
        await run(asyncio.Event())

    logged = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert any("sweeper failed" in record.get("error", "") for record in logged)


async def test_a_failure_racing_an_external_cancel_is_reported(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """The cancel path discarded its gather too — the same silence, another door."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    started = asyncio.Event()

    async def failing_when_cancelled(**kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError("sweeper failed on the way down") from None

    monkeypatch.setattr("src.worker.main.run_sweeper", failing_when_cancelled)
    configure_logging(logging.INFO)
    task = asyncio.create_task(run(asyncio.Event()))
    await asyncio.wait_for(started.wait(), 5)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    logged = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert any("sweeper failed on the way down" in record.get("error", "") for record in logged)


def test_a_cancelled_task_is_not_reported_as_a_shutdown_failure(caplog):
    """Cancellation is shutdown working, not failing; only real errors are news.

    The one test here that calls a private function rather than driving ``run()``.
    Reaching this branch from outside means getting a task cancelled *and* having
    its CancelledError survive to the gather, which the wind-down path exists to
    prevent — so the observable route would be a fixture elaborate enough to test
    itself rather than the branch.
    """
    with caplog.at_level("ERROR", logger="src.worker.main"):
        src.worker.main._log_shutdown_failures(
            [None, asyncio.CancelledError(), RuntimeError("a real one")]
        )

    (record,) = caplog.records
    assert "a real one" in record.error


async def test_an_unusable_relative_blob_dir_is_named_absolutely(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """REPLICATOR_BLOB_DIR defaults to the relative ``blobs``.

    This one line is all an operator gets for a boot-blocking misconfiguration —
    it exists because an uncaught OSError would reach the journal as an
    unparseable traceback — so it has to say which directory.
    """
    monkeypatch.chdir(tmp_path)
    (tmp_path / "blobs").write_bytes(b"")  # a file where the directory belongs
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", "blobs")
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
        assert record["blob_dir"] == str(tmp_path.resolve() / "blobs")
    finally:
        root.handlers, root.level = saved_handlers, saved_level


# --------------------------------------------------------------------------
# Backend selection (#7)
# --------------------------------------------------------------------------


def _gcs_env(monkeypatch, bucket="a-temp-bucket"):
    monkeypatch.setenv("REPLICATOR_BLOB_BACKEND", "gcs")
    monkeypatch.setenv("REPLICATOR_BLOB_BUCKET", bucket)
    get_settings.cache_clear()


async def test_the_local_backend_is_what_a_bare_environment_gets(monkeypatch, fake_redis, tmp_path):
    """The compiled-in default, asserted at the wiring rather than in Settings.

    `tests/core/test_config.py` fixes that the *setting* defaults to `local`. This
    fixes that the worker built from it actually announces `file://` — the two
    can come apart in exactly one commit, and the symptom of them coming apart is
    a fetch storm in another repo (CannObserv/watcher#275).
    """
    monkeypatch.delenv("REPLICATOR_BLOB_BACKEND", raising=False)
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    get_settings.cache_clear()
    built = []
    monkeypatch.setattr(
        "src.worker.main.LocalBlobStore",
        lambda root: built.append(root) or LocalBlobStore(root),
    )

    await run(_stopped())

    assert built == [(tmp_path / "blobs").resolve()]


async def test_the_gcs_backend_builds_an_object_store(monkeypatch, fake_redis, tmp_path):
    _gcs_env(monkeypatch)
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    built = []

    def fake_store(bucket, *, prefix, timeout_seconds=None, client=None):
        built.append((bucket, prefix, timeout_seconds))
        return object()

    monkeypatch.setattr("src.worker.main.GcsBlobStore", fake_store)
    monkeypatch.setattr("src.worker.main.preflight_object_store", lambda store, settings: None)

    await run(_stopped())

    # The timeout is threaded from settings rather than left to the store's own
    # default (CR #8): it lands in the unit's shutdown budget, so the number the
    # operator configures has to be the number that runs.
    assert built == [("a-temp-bucket", "blobs", get_settings().blob_timeout_seconds)]


async def test_the_gcs_backend_does_not_create_a_blob_directory(monkeypatch, fake_redis, tmp_path):
    """Nothing local is involved, so nothing local should be provisioned.

    A directory created anyway would be harmless and misleading — an operator
    reading `/var/lib/replicator/blobs` on a host storing to a bucket would find
    an empty tree that looks like a broken store rather than an unused one.
    """
    _gcs_env(monkeypatch)
    blob_dir = tmp_path / "unwanted"
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(blob_dir))
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: object())
    monkeypatch.setattr("src.worker.main.preflight_object_store", lambda store, settings: None)

    await run(_stopped())

    assert not blob_dir.exists()


async def test_the_gcs_backend_runs_no_sweeper(monkeypatch, fake_redis, tmp_path):
    """Retention moves to the bucket's lifecycle rule, so this process stops reaping.

    Not an optimisation. A sweep over a bucket would have to LIST every object
    every cycle — O(n) Class A operations, forever — to re-derive a number the
    lifecycle rule already acts on. And it would be reaping objects on a clock
    this worker no longer owns, which is precisely the divergence the local
    sweep's "one resolved root" comment exists to prevent, one layer up.
    """
    _gcs_env(monkeypatch)
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: object())
    monkeypatch.setattr("src.worker.main.preflight_object_store", lambda store, settings: None)
    swept = []
    monkeypatch.setattr(
        "src.worker.retention.sweep",
        lambda root, **kwargs: swept.append(root) or SweepResult(),
    )

    await run(_stopped())

    assert swept == []


async def test_the_gcs_backend_says_the_ceiling_is_unenforced(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """A safety property is being given up, so it is said out loud at boot.

    `blob_max_total_bytes` bounds a shared disk, and a bucket is not one. Nothing
    replaces it on this backend — the per-blob `REPLICATOR_MAX_BLOB_BYTES` cap
    still holds and cost is watched by a budget alert, neither of which this
    process can enforce. An operator who reads the ceiling in the environment
    and assumes it applies is the failure this line exists to prevent.
    """
    _gcs_env(monkeypatch)
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: object())
    monkeypatch.setattr("src.worker.main.preflight_object_store", lambda store, settings: None)

    await run(_stopped())

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    stated = [line for line in lines if "REPLICATOR_BLOB_MAX_TOTAL_BYTES" in json.dumps(line)]
    assert stated, "the boot log must say the ceiling is not enforced on this backend"
    # Named by its variable rather than described, because the operator who needs
    # this line is the one reading that variable in `/etc/replicator/.env` and
    # concluding the disk-shaped protection it provides is still in force.
    assert "not enforced" in json.dumps(stated[0])


async def test_a_failing_preflight_stops_the_boot(monkeypatch, fake_redis, tmp_path):
    """The object-store twin of "blob directory is not usable".

    Same argument, same place in the sequence: a store that cannot be reached is
    a misconfiguration whose only other symptom is in another repo, so it fails
    at boot rather than at the first command. `warn_if_unreachable` could only
    warn because a single-user deployment legitimately has unreadable blobs;
    a bucket this worker cannot write to has no legitimate reading.
    """
    _gcs_env(monkeypatch)
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: object())

    def refuse(store, settings):
        raise OSError("no such bucket")

    monkeypatch.setattr("src.worker.main.preflight_object_store", refuse)

    with pytest.raises(OSError, match="no such bucket"):
        await run(_stopped())


async def test_the_local_backend_still_checks_traversal(monkeypatch, fake_redis, tmp_path):
    """`warn_if_unreachable` is not deleted — it is scoped to the backend it is about.

    Its question ("can another service traverse to these bytes") has no meaning
    against a bucket, where reachability is an IAM grant this process cannot
    inspect. Running it anyway would put a warning about directory modes in the
    journal of a worker that writes no directories.
    """
    monkeypatch.delenv("REPLICATOR_BLOB_BACKEND", raising=False)
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    checked = []
    monkeypatch.setattr("src.worker.main.warn_if_unreachable", lambda d: checked.append(d))

    await run(_stopped())

    assert checked == [(tmp_path / "blobs").resolve()]


async def test_the_gcs_backend_does_not_check_traversal(monkeypatch, fake_redis, tmp_path):
    _gcs_env(monkeypatch)
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: object())
    monkeypatch.setattr("src.worker.main.preflight_object_store", lambda store, settings: None)
    checked = []
    monkeypatch.setattr("src.worker.main.warn_if_unreachable", lambda d: checked.append(d))

    await run(_stopped())

    assert checked == []


async def test_a_preflight_failure_names_the_bucket_in_the_journal(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """The object-store twin of "blob directory is not usable", including its shape.

    Logged structurally before re-raising, for the reason the local branch does
    it: an uncaught exception puts the one line that matters into the journal as
    a bare traceback, unparseable by a pipeline expecting JSON, right before the
    unit flaps to its restart limit. The bucket is named because "not usable" is
    not an actionable line on a host that configures two of them.
    """
    _gcs_env(monkeypatch, bucket="a-temp-bucket")
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    class Refusing:
        def preflight(self):
            raise OSError("no such bucket")

    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: Refusing())

    with pytest.raises(OSError, match="no such bucket"):
        await run(_stopped())

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    named = [line for line in lines if "a-temp-bucket" in json.dumps(line)]
    assert named, "the boot failure must name the bucket it could not reach"
    assert any("not usable" in line.get("message", "") for line in named)


async def test_the_gcs_backend_hands_the_byte_path_no_ceiling(monkeypatch, fake_redis, tmp_path):
    """The boot log's claim, made true in the wiring rather than only in the journal.

    Saying "not enforced" while still passing the number would be the worse of
    the two failures: `BlobUsage` only ever falls when a sweep re-measures it,
    and there is no sweep here, so the estimate would climb to the ceiling and
    stay there — every fetch refused transiently, every command parked in the
    PEL waiting on a sweep that will never run.
    """
    _gcs_env(monkeypatch)
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: object())
    monkeypatch.setattr("src.worker.main.preflight_object_store", lambda store, settings: None)
    seen = {}
    real = src.worker.main.build_handler

    def recording(**kwargs):
        seen.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr("src.worker.main.build_handler", recording)

    await run(_stopped())

    assert seen["ceiling_bytes"] is None


async def test_the_local_backend_still_hands_over_its_ceiling(monkeypatch, fake_redis, tmp_path):
    monkeypatch.delenv("REPLICATOR_BLOB_BACKEND", raising=False)
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setenv("REPLICATOR_BLOB_MAX_TOTAL_BYTES", "4096")
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    seen = {}
    real = src.worker.main.build_handler

    def recording(**kwargs):
        seen.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr("src.worker.main.build_handler", recording)

    await run(_stopped())

    assert seen["ceiling_bytes"] == 4096


async def test_the_gcs_boot_log_states_the_horizon_it_will_publish(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """CR #11: the two halves of the TTL are configured in different places.

    `REPLICATOR_BLOB_TTL_SECONDS` sets what every `blob_available` announces; the
    bucket lifecycle rule sets what is actually reaped, and nothing keeps them in
    step. A rule shorter than the published horizon is the one way
    `blob_expires_at` can be wrong in the unsafe direction — a consumer holding a
    `blob_uri` the bucket has already deleted. Logging the number at boot does
    not enforce the pairing, but it puts both halves within reach of one grep.
    """
    _gcs_env(monkeypatch)
    monkeypatch.setenv("REPLICATOR_BLOB_TTL_SECONDS", "604800")
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.GcsBlobStore", lambda *a, **kw: object())
    monkeypatch.setattr("src.worker.main.preflight_object_store", lambda store, settings: None)

    await run(_stopped())

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    stated = [line for line in lines if line.get("message") == "storing blobs in an object store"]
    assert stated, "the object-store boot line is missing"
    assert stated[0]["blob_ttl_seconds"] == 604800.0
    assert "daysSinceCustomTime" in json.dumps(stated[0])
