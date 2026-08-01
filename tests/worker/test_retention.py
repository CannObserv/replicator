"""The in-worker sweep task: cadence, usage accounting, and its failure posture."""

import asyncio
import threading

import pytest

from src.core.config import get_settings
from src.storage.sweeper import BlobUsage, SweepResult
from src.worker.retention import run_sweeper

# Short enough that a test can watch several cycles, long enough not to spin.
INTERVAL = 0.01


@pytest.fixture
def retention_settings(monkeypatch):
    """Settings with a sweep cadence a test can outrun."""
    monkeypatch.setenv("REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS", str(INTERVAL))
    get_settings.cache_clear()
    return get_settings()


class RecordingSweep:
    """Stands in for the filesystem pass, counting calls and stopping the task."""

    def __init__(self, result: SweepResult | None = None, stop_after: int = 1) -> None:
        self.result = result if result is not None else SweepResult()
        self.calls = 0
        self._stop_after = stop_after
        self.stop = asyncio.Event()

    def __call__(self, root, **kwargs) -> SweepResult:
        self.calls += 1
        self.kwargs = kwargs
        if self.calls >= self._stop_after:
            self.stop.set()
        return self.result


async def drive(monkeypatch, sweeper, settings, usage=None, deadline: float = 5):
    """Run the task until the fake sweep says stop, under a test-only deadline."""
    monkeypatch.setattr("src.worker.retention.sweep", sweeper)
    async with asyncio.timeout(deadline):
        await run_sweeper(
            root=settings.blob_dir,
            settings=settings,
            usage=usage if usage is not None else BlobUsage(),
            stop=sweeper.stop,
        )


async def test_the_first_sweep_runs_before_the_first_interval_elapses(
    monkeypatch, retention_settings
):
    """A worker restarting after a long outage should reclaim on boot, not in 15 minutes."""
    sweeper = RecordingSweep()
    monkeypatch.setenv("REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS", "3600")
    get_settings.cache_clear()

    await drive(monkeypatch, sweeper, get_settings())

    assert sweeper.calls == 1


async def test_the_sweep_is_handed_the_configured_clocks(monkeypatch, retention_settings):
    sweeper = RecordingSweep()

    await drive(monkeypatch, sweeper, retention_settings)

    assert sweeper.kwargs == {
        "ttl_seconds": retention_settings.blob_ttl_seconds,
        "temp_grace_seconds": retention_settings.blob_temp_grace_seconds,
    }


async def test_it_keeps_sweeping_on_the_interval(monkeypatch, retention_settings):
    sweeper = RecordingSweep(stop_after=3)

    await drive(monkeypatch, sweeper, retention_settings)

    assert sweeper.calls == 3


async def test_the_measured_total_replaces_the_running_estimate(monkeypatch, retention_settings):
    """The ceiling reads this, so a sweep has to correct whatever the byte path guessed."""
    usage = BlobUsage()
    usage.add(10_000)
    sweeper = RecordingSweep(SweepResult(bytes_remaining=42))

    await drive(monkeypatch, sweeper, retention_settings, usage=usage)

    assert usage.total_bytes == 42


async def test_a_failing_sweep_does_not_stop_the_worker(monkeypatch, retention_settings):
    """Retention is not load-bearing for correctness; the consume path is.

    A tree the sweep cannot walk degrades to a tree that grows until the ceiling
    stops the byte path — which is the guard that exists for exactly this. Taking
    the worker down instead would trade a bounded degradation for an outage.
    """
    stop = asyncio.Event()
    calls = 0

    def failing_then_stopping(root, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("permission denied")
        stop.set()
        return SweepResult()

    monkeypatch.setattr("src.worker.retention.sweep", failing_then_stopping)
    async with asyncio.timeout(5):
        await run_sweeper(
            root=retention_settings.blob_dir,
            settings=retention_settings,
            usage=BlobUsage(),
            stop=stop,
        )

    assert calls == 2


async def test_a_failing_sweep_leaves_the_last_measurement_alone(monkeypatch, retention_settings):
    """A stale number is a better ceiling input than a zero that reads as "empty"."""
    usage = BlobUsage()
    usage.observe(5_000)
    stop = asyncio.Event()

    def failing(root, **kwargs):
        stop.set()
        raise OSError("permission denied")

    monkeypatch.setattr("src.worker.retention.sweep", failing)
    async with asyncio.timeout(5):
        await run_sweeper(
            root=retention_settings.blob_dir, settings=retention_settings, usage=usage, stop=stop
        )

    assert usage.total_bytes == 5_000


async def test_stopping_mid_interval_does_not_wait_it_out(monkeypatch, retention_settings):
    """SIGTERM lands while parked, and TimeoutStopSec is 60s against a 15-minute cadence."""
    monkeypatch.setenv("REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS", "3600")
    get_settings.cache_clear()
    settings = get_settings()
    stop = asyncio.Event()
    swept = asyncio.Event()

    def observing(root, **kwargs):
        swept.set()
        return SweepResult()

    monkeypatch.setattr("src.worker.retention.sweep", observing)
    task = asyncio.create_task(
        run_sweeper(root=settings.blob_dir, settings=settings, usage=BlobUsage(), stop=stop)
    )
    await asyncio.wait_for(swept.wait(), 5)

    stop.set()

    await asyncio.wait_for(task, 5)


async def test_a_sweep_that_did_nothing_is_not_logged(monkeypatch, retention_settings, caplog):
    """Every 15 minutes, forever — an idle tree must not fill the journal."""
    sweeper = RecordingSweep()

    with caplog.at_level("INFO", logger="src.worker.retention"):
        await drive(monkeypatch, sweeper, retention_settings)

    assert caplog.records == []


async def test_a_reap_names_each_population_it_touched(monkeypatch, retention_settings, caplog):
    """A rising temp count means SIGKILLs mid-store; it must not read as expiry."""
    sweeper = RecordingSweep(
        SweepResult(blobs_reaped=2, bytes_reclaimed=99, temps_reaped=1, shards_removed=3)
    )

    with caplog.at_level("INFO", logger="src.worker.retention"):
        await drive(monkeypatch, sweeper, retention_settings)

    record = caplog.records[0]
    assert (record.blobs_reaped, record.temps_reaped, record.shards_removed) == (2, 1, 3)
    assert record.bytes_reclaimed == 99


async def test_crossing_the_ceiling_is_reported(monkeypatch, retention_settings, caplog):
    """The byte path stops fetching at this point; the journal has to say why."""
    monkeypatch.setenv("REPLICATOR_BLOB_MAX_TOTAL_BYTES", "100")
    get_settings.cache_clear()
    sweeper = RecordingSweep(SweepResult(bytes_remaining=101))

    with caplog.at_level("WARNING", logger="src.worker.retention"):
        await drive(monkeypatch, sweeper, get_settings())

    assert "ceiling" in caplog.text


async def test_the_walk_does_not_run_on_the_event_loop_thread(monkeypatch, retention_settings):
    """A blocking walk on the loop thread makes retention a source of consume latency."""
    stop = asyncio.Event()
    swept_on = None

    def observing(root, **kwargs):
        nonlocal swept_on
        swept_on = threading.current_thread()
        stop.set()
        return SweepResult()

    monkeypatch.setattr("src.worker.retention.sweep", observing)
    async with asyncio.timeout(5):
        await run_sweeper(
            root=retention_settings.blob_dir,
            settings=retention_settings,
            usage=BlobUsage(),
            stop=stop,
        )

    assert swept_on is not threading.current_thread()
