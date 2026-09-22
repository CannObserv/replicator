"""Crash recovery: reclaiming what a dead worker left in the pending list.

A claim that raises on a poison entry returns nothing else, so these tests pin
the two properties that follow from it — a poison entry must not jam the pass,
and the pass must give up rather than spin when the PEL is pathological.

And the one that follows from running recovery first (#98): first must not mean
only. A reclaim owes the stream a turn, so a handler slower than
``claim_min_idle_ms`` cannot stop the group reading.

And its sequel on the PEL's side (#102): recovery walks the pending list rather
than restarting at ``0-0``, so the oldest slow failure cannot take every turn.
"""

import asyncio

import pytest
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import ContentFetchCommand
from redis.exceptions import ConnectionError as RedisConnectionError

from src.core.errors import TransientError
from src.worker.loop import (
    MAX_POISON_SKIPS,
    Outcome,
    PollCadence,
    _after,
    claim_once,
    poll_once,
)
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
    """A poison entry at the head would block every claim from ``0-0``."""
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

    # Reclaim, read, reclaim. The second reclaim is ``cmd-new`` because recovery
    # walks the PEL from where it left off (#102), and at a zero window the entry
    # the stream's turn just delivered is already reclaimable.
    assert polled == ["cmd-orphan", "cmd-new", "cmd-new"]


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


async def pend(fake_redis, *command_ids: str) -> list[str]:
    """Leave each command pending on a dead consumer, oldest first; return their ids."""
    ids = [await fake_redis.xadd(TOPIC, make_command(command_id=cid)) for cid in command_ids]
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=len(ids))
    return [entry_id.decode() for entry_id in ids]


def command_ids(messages) -> list[str]:
    return [message.payload.command_id for message in messages]


async def test_recovery_walks_the_pending_list_rather_than_restarting_at_its_head(
    fake_redis, consumer, settings
):
    """#102: every reclaimable entry gets a turn, in order, then the walk wraps.

    With the window at zero every entry is reclaimable at every claim, which is
    the steady state of several slow transient failures: ``XAUTOCLAIM`` restarts
    the idle clock of whatever it claims, so from ``0-0`` the oldest wins every
    turn. Three entries rather than two, because skipping only the entry claimed
    last — the third shape #102 weighed — alternates the first two and passes this
    with two.
    """
    await pend(fake_redis, "cmd-a", "cmd-b", "cmd-c")
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    cadence = PollCadence()

    claimed = []
    for _ in range(4):
        claimed += await claim_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)

    assert command_ids(claimed) == ["cmd-a", "cmd-b", "cmd-c", "cmd-a"]


async def test_the_walk_resumes_where_it_left_off_after_the_pending_list_changes(
    fake_redis, consumer, settings
):
    """The cursor is a position, not an index: an entry acked behind it costs nothing."""
    await pend(fake_redis, "cmd-a", "cmd-b", "cmd-c")
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    cadence = PollCadence()

    (first,) = await claim_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)
    await consumer.ack(first.message_id)
    rest = []
    for _ in range(3):
        rest += await claim_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)

    assert command_ids(rest) == ["cmd-b", "cmd-c", "cmd-b"]


async def test_an_empty_pass_leaves_the_cursor_at_the_head(fake_redis, consumer, settings):
    """Nothing reclaimable anywhere: one wrapped claim, then the next pass starts fresh."""
    ids = await pend(fake_redis, "cmd-a")
    patient = settings.model_copy(update={"claim_min_idle_ms": 60_000})
    cadence = PollCadence(reclaim_from=ids[0])

    assert await claim_once(fake_redis, consumer, patient, group=GROUP, cadence=cadence) == []
    assert cadence.reclaim_from == "0-0"


async def test_a_poison_entry_moves_the_walk_past_itself(fake_redis, consumer, settings):
    """Dead-lettered on the way, and the walk continues behind it rather than restarting."""
    await pend(fake_redis, "cmd-a")
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=1)
    await pend(fake_redis, "cmd-c")
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    cadence = PollCadence()

    claimed = []
    for _ in range(3):
        claimed += await claim_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence)

    assert command_ids(claimed) == ["cmd-a", "cmd-c", "cmd-a"]
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_a_pass_stopped_by_the_poison_bound_resumes_behind_the_last_skipped(
    fake_redis, consumer, settings
):
    """The bound pauses the walk rather than rewinding it to frames already routed."""
    poison = [
        (await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "x"})).decode()
        for _ in range(MAX_POISON_SKIPS + 1)
    ]
    await fake_redis.xreadgroup(GROUP, "replicator@dead-worker", {TOPIC: ">"}, count=100)
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    cadence = PollCadence()

    assert await claim_once(fake_redis, consumer, eager, group=GROUP, cadence=cadence) == []

    assert cadence.reclaim_from == _after(poison[MAX_POISON_SKIPS - 1])


@pytest.mark.parametrize(
    ("entry_id", "after"),
    [
        ("1700000000000-0", "1700000000000-1"),
        ("1700000000000-41", "1700000000000-42"),
        # The sequence is a u64; its successor is the next millisecond's first.
        (f"1700000000000-{2**64 - 1}", "1700000000001-0"),
        # The largest id a stream can hold has no successor, so the walk wraps.
        (f"{2**64 - 1}-{2**64 - 1}", "0-0"),
    ],
)
def test_the_cursor_starts_just_past_the_entry_claimed(entry_id, after):
    """co-core discards ``XAUTOCLAIM``'s own cursor, so the next start is computed."""
    assert _after(entry_id) == after


async def test_among_slow_failing_entries_a_younger_one_is_still_retried(
    fake_redis, consumer, settings
):
    """#102's reproduction: ``cmd-b`` would succeed on its second attempt, if it got one.

    Read ``cmd-a``, reclaim it, read ``cmd-b`` on the stream's turn — and then
    recovery's turn goes to ``cmd-b``, not back to the entry that always wins from
    ``0-0``. Before the fix ``cmd-b`` was delivered once and never again.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-a"))
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-b"))
    eager = settings.model_copy(update={"claim_min_idle_ms": 0})
    stop = asyncio.Event()
    seen: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)
        if len(seen) >= SLOW_ATTEMPTS_BEFORE_GIVING_UP:
            stop.set()
        if command.command_id == "cmd-b" and seen.count("cmd-b") > 1:
            stop.set()
            return
        raise TransientError("provider stalled")

    await drive_loop(fake_redis, consumer, eager, handler, stop, deadline=1.0)

    assert seen == ["cmd-a", "cmd-a", "cmd-b", "cmd-b"]
