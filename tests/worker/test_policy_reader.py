"""Reading ``content.fetch-policy``: replay at boot, tail after (#19).

The broker half. What a message *means* once decoded is
``tests/worker/test_policy.py``.

Against fakeredis, which is sound for what state a read leaves behind. It does
honour ``block`` on a groupless ``XREAD`` — unlike the ``XREADGROUP`` the consume
loop uses — so these run with a deliberately short poll window. What it cannot
attest to is the live broker's own behaviour on a real stream, which is
``tests/worker/test_policy_integration.py``.
"""

import asyncio

import pytest
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import FetchPolicyState

from src.core.config import get_settings
from src.worker.policy import (
    FetchPolicyMap,
    build_policy_reader,
    replay_policies,
    run_policy_reader,
)
from tests.worker.conftest import now

TOPIC = "content.fetch-policy"
DEFAULT = 1.0


async def publish(client, topic: str, host: str, min_interval_seconds: float = 30.0) -> None:
    """XADD a policy the way its producer would, through co-core's own envelope."""
    await client.xadd(
        topic,
        to_wire(
            FetchPolicyState(
                occurred_at=now(),
                host=host,
                min_interval_seconds=min_interval_seconds,
            )
        ),
    )


async def until(predicate, seconds: float = 2.0) -> None:
    """Wait for a background task to have applied something.

    Polled rather than awaited on an event: the thing being waited for is a
    change in the policy map, which is plain state with nothing to signal on.
    """
    async with asyncio.timeout(seconds):
        while not predicate():  # noqa: ASYNC110
            await asyncio.sleep(0.01)


@pytest.fixture
def policies() -> FetchPolicyMap:
    return FetchPolicyMap(DEFAULT)


@pytest.fixture
def fast_settings(worker_env, monkeypatch):
    """Settings with a poll window short enough to test against.

    The tail's shutdown latency is bounded by ``REPLICATOR_READ_BLOCK_MS``, the
    same quantity the consume loop is bounded by and the same one
    ``TimeoutStopSec`` is already sized for — the two block concurrently, so the
    worst case is the max of them rather than the sum. Shrinking it here keeps
    these tests from spending a real poll window each.
    """
    monkeypatch.setenv("REPLICATOR_READ_BLOCK_MS", "50")
    get_settings.cache_clear()
    return get_settings()


async def test_replay_reads_from_the_beginning_of_the_stream(fake_redis, policies):
    """The whole point of the driver: a booting worker must see the messages
    published before it existed. Reading from ``$`` would return nothing, and an
    empty map is indistinguishable from a working one."""
    await publish(fake_redis, TOPIC, "slow.test", 30.0)
    await publish(fake_redis, TOPIC, "fast.test", 0.5)

    await replay_policies(build_policy_reader(fake_redis, topic=TOPIC), policies)

    assert policies.interval_for("slow.test") == 30.0
    assert policies.interval_for("fast.test") == 0.5


async def test_replay_of_an_empty_stream_leaves_an_empty_map(fake_redis, policies):
    """No stream yet is not an error — the producer may not have started."""
    await replay_policies(build_policy_reader(fake_redis, topic=TOPIC), policies)

    assert policies.tracked_hosts == 0


async def test_replay_reports_what_it_rebuilt(fake_redis, policies, caplog):
    """The gauge that makes the empty case visible rather than inferred."""
    await publish(fake_redis, TOPIC, "slow.test")

    with caplog.at_level("INFO"):
        await replay_policies(build_policy_reader(fake_redis, topic=TOPIC), policies)

    record = next(r for r in caplog.records if r.message == "fetch policy replay complete")
    assert record.tracked_hosts == 1


async def test_replay_leaves_the_cursor_where_the_tail_picks_up(fake_redis, policies):
    """One shared cursor across replay and read, so there is no boundary a caller
    can land on the wrong side of — and nothing already applied is applied twice."""
    reader = build_policy_reader(fake_redis, topic=TOPIC)
    await publish(fake_redis, TOPIC, "slow.test", 30.0)
    await replay_policies(reader, policies)

    await publish(fake_redis, TOPIC, "later.test", 12.0)
    for message in await reader.read(count=1):
        policies.apply(message.payload)

    assert policies.interval_for("later.test") == 12.0


async def test_the_tail_applies_a_policy_published_while_it_runs(
    fake_redis, policies, fast_settings
):
    reader = build_policy_reader(fake_redis, topic=TOPIC)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_policy_reader(reader, policies=policies, settings=fast_settings, stop=stop)
    )

    await publish(fake_redis, TOPIC, "slow.test", 30.0)
    try:
        await until(lambda: policies.interval_for("slow.test") == 30.0)
    finally:
        stop.set()
        await task


async def test_the_tail_returns_when_the_stop_event_is_set(fake_redis, policies, fast_settings):
    """It rides the same stop event as the consume loop, so a SIGTERM is not held
    behind a poll window."""
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_policy_reader(
            build_policy_reader(fake_redis, topic=TOPIC),
            policies=policies,
            settings=fast_settings,
            stop=stop,
        )
    )
    await asyncio.sleep(0.05)

    stop.set()

    async with asyncio.timeout(2.0):
        await task


async def test_a_malformed_frame_does_not_block_the_ones_behind_it(
    fake_redis, policies, fast_settings
):
    """With no group there is no ack to move past a poison frame, so the cursor
    has to be forced — otherwise the next read redelivers it and raises forever,
    and every policy published after it is unreachable."""
    await fake_redis.xadd(TOPIC, {"not": "a frame"})
    await publish(fake_redis, TOPIC, "slow.test", 30.0)
    reader = build_policy_reader(fake_redis, topic=TOPIC)

    await replay_policies(reader, policies)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_policy_reader(reader, policies=policies, settings=fast_settings, stop=stop)
    )
    try:
        await until(lambda: policies.interval_for("slow.test") == 30.0)
    finally:
        stop.set()
        await task


async def test_a_malformed_frame_is_reported(fake_redis, policies, caplog):
    await fake_redis.xadd(TOPIC, {"not": "a frame"})

    with caplog.at_level("WARNING"):
        await replay_policies(build_policy_reader(fake_redis, topic=TOPIC), policies)

    assert "skipping a malformed frame" in caplog.text


class BrokenReader:
    """A reader whose broker is gone. Counts reads so backoff is observable."""

    def __init__(self) -> None:
        self.reads = 0

    async def read(self, *, count: int, block_ms: int | None = None):
        self.reads += 1
        raise ConnectionError("broker is gone")

    async def replay(self, *, count: int = 100):
        raise ConnectionError("broker is gone")

    def seek(self, message_id: str) -> None:  # pragma: no cover - never reached
        raise AssertionError("nothing to seek past")


async def test_a_failed_replay_does_not_stop_the_boot(policies, caplog):
    """Failing here would turn a policy-stream hiccup into a total fetch outage.

    Absorbing it is safe because the cursor only advances over messages that
    decoded: the tail resumes from the same place and drains the rest.
    """
    with caplog.at_level("ERROR"):
        await replay_policies(BrokenReader(), policies)

    assert "could not replay the fetch policy stream" in caplog.text


async def test_a_failed_read_backs_off_instead_of_taking_the_worker_down(
    policies, worker_env, monkeypatch
):
    """Politeness is not load-bearing for correctness, and the last-known map
    survives in memory across the outage. A broker that is genuinely gone still
    surfaces — through the consume loop, which has the delivery obligations."""
    monkeypatch.setenv("REPLICATOR_ERROR_BACKOFF_BASE_SECONDS", "0.01")
    monkeypatch.setenv("REPLICATOR_ERROR_BACKOFF_MAX_SECONDS", "0.01")
    get_settings.cache_clear()
    reader = BrokenReader()
    stop = asyncio.Event()

    task = asyncio.create_task(
        run_policy_reader(reader, policies=policies, settings=get_settings(), stop=stop)
    )
    try:
        await until(lambda: reader.reads >= 3)
    finally:
        stop.set()
        await task

    assert not task.cancelled()


async def test_an_outage_leaves_the_last_known_policies_in_place(policies, fast_settings):
    """The map is state, not a cache of the last read."""
    reader = BrokenReader()
    policies._intervals["slow.test"] = 30.0
    stop = asyncio.Event()

    task = asyncio.create_task(
        run_policy_reader(reader, policies=policies, settings=fast_settings, stop=stop)
    )
    await until(lambda: reader.reads >= 1)
    stop.set()
    await task

    assert policies.interval_for("slow.test") == 30.0


async def test_a_malformed_frame_arriving_mid_tail_is_skipped(fake_redis, policies, fast_settings):
    """The poison branch on the steady-state path, not just the boot one.

    A producer that starts emitting bad frames after the worker booted would
    otherwise wedge the tail permanently: no group means no ack, so the same
    frame is redelivered and raises on every poll while every policy behind it
    goes unread.
    """
    reader = build_policy_reader(fake_redis, topic=TOPIC)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_policy_reader(reader, policies=policies, settings=fast_settings, stop=stop)
    )
    try:
        await fake_redis.xadd(TOPIC, {"not": "a frame"})
        await publish(fake_redis, TOPIC, "behind.test", 30.0)
        await until(lambda: policies.interval_for("behind.test") == 30.0)
    finally:
        stop.set()
        await task
