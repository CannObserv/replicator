"""Crash recovery: reclaiming what a dead worker left in the pending list.

``claim_stale`` restarts at ``0-0`` on every call, so these tests pin the two
properties that follow from it — a poison entry must not jam the pass, and the
pass must give up rather than spin when the PEL is pathological.

And the one that follows from running recovery first (#98): first must not mean
only. A reclaim owes the stream a turn, so a handler slower than
``claim_min_idle_ms`` cannot stop the group reading.
"""

import asyncio

import pytest
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import ContentFetchCommand
from redis.exceptions import ConnectionError as RedisConnectionError

from src.core.errors import TransientError
from src.worker.loop import MAX_POISON_SKIPS, Outcome, PollCadence, claim_once, poll_once
from tests.worker.conftest import GROUP, TOPIC, drive_loop, make_command, process_one

# How many times the starvation test lets its slow command fail before calling
# the run off. Far past the one reclaim a fixed loop spends before its read, so
# reaching it is the bug and not a tight bound.
SLOW_ATTEMPTS_BEFORE_GIVING_UP = 6


async def test_a_message_from_a_dead_consumer_is_reclaimed_and_processed(
    fake_redis, consumer, settings
):
    """AC: crash recovery — an abandoned PEL entry comes back via claim_stale."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-orphan"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=1)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    seen: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)

    messages = await poll_once(fake_redis, consumer, eager, group=GROUP)
    assert await process_one(fake_redis, consumer, eager, messages[0], handler) is Outcome.ACKED

    assert seen == ["cmd-orphan"]
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_a_poison_pel_entry_does_not_jam_recovery(fake_redis, consumer, settings):
    """claim_stale restarts at 0-0 every call, so a poison entry would block it."""
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-behind-poison"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=2)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})

    messages = await poll_once(fake_redis, consumer, eager, group=GROUP)

    assert len(messages) == 1
    command = messages[0].payload
    assert isinstance(command, ContentFetchCommand)
    assert command.command_id == "cmd-behind-poison"
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_recovery_gives_up_after_the_poison_skip_bound(fake_redis, consumer, settings):
    """CR #3: a pathological PEL must not starve the read path within one tick."""
    for _ in range(MAX_POISON_SKIPS + 2):
        await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=100)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})

    reclaimed = await claim_once(fake_redis, consumer, eager, group=GROUP)

    assert reclaimed == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == MAX_POISON_SKIPS
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 2  # the bound stopped the pass; the rest wait for the next


async def test_a_handler_slower_than_the_idle_window_does_not_stop_the_group_reading(
    fake_redis, consumer, settings
):
    """#98: recovery first must not become recovery only.

    XAUTOCLAIM restarts an entry's idle clock when it claims it, so a handler that
    runs past ``claim_min_idle_ms`` and fails transiently hands back an entry that
    is already reclaimable — and a loop that always reclaims before it reads never
    reads again. ``claim_min_idle_ms=0`` is that ratio with the sleeping taken
    out: every entry is reclaimable the moment its handler returns.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-slow"))
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-behind"))
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    stop = asyncio.Event()
    seen: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)
        if command.command_id == "cmd-behind" or len(seen) >= SLOW_ATTEMPTS_BEFORE_GIVING_UP:
            stop.set()
        if command.command_id == "cmd-slow":
            raise TransientError("provider stalled")

    await drive_loop(fake_redis, consumer, eager, handler, stop, deadline=1.0)

    # Read, reclaim, then the stream's turn: one reclaim between reads, not all of them.
    assert seen == ["cmd-slow", "cmd-slow", "cmd-behind"]


async def test_after_a_reclaim_the_stream_gets_the_next_turn(fake_redis, consumer, settings):
    """#98: the PEL and the stream take turns, and the PEL still goes first.

    Nothing is acked between polls, so both entries stay reclaimable throughout —
    the turn order is the only thing choosing between them.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-orphan"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=1)
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-new"))
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    cadence = PollCadence()
    polled: list[str] = []

    for _ in range(3):
        (message,) = await poll_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)
        assert isinstance(message.payload, ContentFetchCommand)
        polled.append(message.payload.command_id)

    assert polled == ["cmd-orphan", "cmd-new", "cmd-orphan"]


async def test_the_streams_turn_does_not_hold_up_recovery(
    fake_redis, consumer, settings, monkeypatch
):
    """An empty stream answers its turn at once, and the reclaim runs in the same poll.

    The turn is a look, not a wait: blocking for ``read_block_ms`` there would add
    the read window to every retry on an idle stream. fakeredis ignores ``block``,
    so the argument is the only place that difference shows.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-orphan"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=1)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    cadence = PollCadence()
    blocks: list[int | None] = []
    real_read = consumer.read

    async def recording_read(*, count, block_ms):
        blocks.append(block_ms)
        return await real_read(count=count, block_ms=block_ms)

    monkeypatch.setattr(consumer, "read", recording_read)
    await poll_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)

    (message,) = await poll_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)

    assert isinstance(message.payload, ContentFetchCommand)
    assert message.payload.command_id == "cmd-orphan"
    assert blocks == [None]


async def test_a_refused_read_does_not_spend_the_streams_turn(
    fake_redis, consumer, settings, monkeypatch
):
    """A broker that refused the look has not given the stream its turn.

    The refusal is a cycle failure, ``run_loop``'s to back off from; what must
    survive it is the debt, or an outage that lands on the turn would hand the
    next cycle straight back to recovery.
    """

    async def dead_read(**kwargs):
        raise RedisConnectionError("broker went away")

    monkeypatch.setattr(consumer, "read", dead_read)
    cadence = PollCadence(stream_owed=True)

    with pytest.raises(RedisConnectionError):
        await poll_once(fake_redis, consumer, settings, group=GROUP, cadence=cadence)

    assert cadence.stream_owed


async def test_a_poison_frame_on_the_streams_turn_still_lets_the_reclaim_run(
    fake_redis, consumer, settings
):
    """The turn routes a bad frame away like any read, then recovery proceeds."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-orphan"))
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=1)
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    cadence = PollCadence(stream_owed=True)

    (message,) = await poll_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)

    assert isinstance(message.payload, ContentFetchCommand)
    assert message.payload.command_id == "cmd-orphan"
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
    assert cadence.stream_owed  # the reclaim owes the next turn in its own right
