"""The consume path against the real broker — what fakeredis cannot settle.

The fake is sound for consumer-group *mechanics*, and the default suite leans on
it for every outcome ``process_message`` can produce. What it diverges on is
*lifecycle* and *timing*: it registers a consumer on an empty ``XREADGROUP``
(GH #3, covered in ``test_main_integration.py``) and it ignores ``block``
entirely. So the properties here are the ones whose truth is a claim about *when*
Redis acts, or about a reply shape the fake never produces:

1. ``claim_stale`` against a real PEL — the reason for the Redis >= 7.0 floor.
   ``XAUTOCLAIM``'s three-element reply, with the deleted-ids element added in
   7.0, is unpacked inside co-core; below that floor this raises. No test had
   ever run it against a server that implements it.
2. The blocking read. ``REPLICATOR_READ_BLOCK_MS`` bounds worst-case shutdown
   latency and the unit's ``TimeoutStopSec`` is sized against it — a relationship
   asserted only by arithmetic in a comment until now.
3. ``times_delivered`` on a reclaim. The delivery ceiling is a bound in *time*
   rather than in retries precisely because the counter advances only when a
   message is reclaimed.
4. The DLQ round-trip, including the ``XRANGE``-by-id re-read for a frame that
   failed to decode and the synthesized-fields fallback when that entry is gone.

Everything runs on ``replicator.itest.*`` scratch streams. The ``real_redis``
fixture refuses db 0 outright — the database that carries the live
``content.fetch`` the running service consumes — but that guard is the backstop,
not the plan.
"""

import asyncio
import time
from collections.abc import AsyncGenerator

import pytest
from co_core.pure.adapters.bus.exceptions import BusMessageAnomaly
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.util.hashing import sha256

from scripts.seed_fetch import last_id, publish, watch_for_facts
from src.core.config import get_settings
from src.storage.local import LocalBlobStore
from src.worker.handler import build_handler
from src.worker.loop import (
    DEDUPE_KEY_PREFIX,
    Outcome,
    claim_once,
    dead_letter_anomaly,
    poll_once,
    run_loop,
)
from src.worker.main import build_consumer
from tests.worker.conftest import BODY, FakeFetcher, make_command

pytestmark = pytest.mark.integration

GROUP = "replicator.itest"
CONSUMER = "replicator@itest"

# Short enough that a reclaim test is not a wall-clock tax, long enough that the
# read preceding it does not fall inside the window by accident. Production runs
# 60s (REPLICATOR_CLAIM_MIN_IDLE_MS); the property under test is unaffected by
# the size of the window, only by its existence.
CLAIM_MIN_IDLE_MS = 100

# The window a read waits when nothing arrives. Deliberately small: the point is
# that it blocks at all, and every assertion below is a lower bound. It also
# bounds how long the end-to-end loop takes to notice its stop event.
READ_BLOCK_MS = 300

# The end-to-end run writes a real replicator:cmd:* dedupe key. Teardown deletes
# it; this is the backstop for a run that dies before teardown, and it is short
# because nothing on the scratch database has any reason to be deduped later.
DEDUPE_TTL_SECONDS = 60

# How long the end-to-end test waits for its fact. Generous against a busy shared
# VM; the loop normally closes in well under a second.
FACT_TIMEOUT_SECONDS = 15


@pytest.fixture
def itest_settings():
    """Production settings with the identity and the timers a scratch run needs.

    ``model_copy`` rather than monkeypatched env, following the recovery tests:
    the override is per-test data, not configuration the worker would ever read.
    """
    return get_settings().model_copy(
        update={
            "consumer_group": GROUP,
            "consumer_name": CONSUMER,
            "claim_min_idle_ms": CLAIM_MIN_IDLE_MS,
            "read_block_ms": READ_BLOCK_MS,
        }
    )


@pytest.fixture
async def itest_consumer(real_redis, scratch_topic, itest_settings):
    """The production wiring, pointed at a scratch stream.

    ``build_consumer`` rather than a bare ``AsyncBusConsumer`` so the topic seam
    the integration suite depends on is itself exercised against a real broker.
    ``start_id="0"`` so a frame added before the group exists is still delivered
    — ordering is a hazard these tests have no reason to carry.
    """
    consumer = build_consumer(real_redis, itest_settings, topic=scratch_topic)
    await consumer.ensure_group(start_id="0")
    return consumer


@pytest.fixture
async def scratch_blobs_topic(real_redis, scratch_topic) -> AsyncGenerator[str]:
    """Where the end-to-end run's facts land, deleted afterwards.

    Derived from ``scratch_topic`` so one uuid identifies the whole run, and kept
    off the real ``content.blobs``: a fact written there would announce a blob
    under ``tmp_path`` that is gone before anything could open it.
    """
    topic = f"{scratch_topic}.blobs"
    try:
        yield topic
    finally:
        await real_redis.delete(topic)


@pytest.fixture
async def dedupe_keys(real_redis) -> AsyncGenerator[list[str]]:
    """Collects the ``replicator:cmd:*`` keys a test creates, and deletes them.

    They are the one thing an integration run leaves outside the
    ``replicator.itest.*`` namespace — the prefix is a constant in
    ``src.worker.loop`` — so the session sweeper cannot reach them and the run
    has to clean up after itself.
    """
    keys: list[str] = []
    try:
        yield keys
    finally:
        if keys:
            await real_redis.delete(*keys)


async def times_delivered(client, topic: str, message_id: str) -> int:
    """The broker's own delivery counter for one pending entry."""
    entries = await client.xpending_range(topic, GROUP, min=message_id, max=message_id, count=1)
    return int(entries[0]["times_delivered"])


async def keyspace(client) -> set[str]:
    """Every key on the scratch database, as an operator would see it."""
    return {key.decode() for key in await client.keys("*")}


async def seed_and_consume(
    real_redis, scratch_topic, blobs_topic, dedupe_keys, settings, tmp_path, url
):
    """Run one command all the way through the loop; return the facts it produced.

    The wiring is ``run()``'s, minus the process-level concerns ``run()`` owns
    (signals, client lifetime, creating the blob directory): ``ensure_group``,
    then ``run_loop`` over the real handler. The command comes from the seed
    harness and the fact is read back by the harness's own ``--watch`` machinery,
    so a passing run exercises the operator tool as well as the worker.
    """
    consumer = build_consumer(real_redis, settings, topic=scratch_topic)
    await consumer.ensure_group(start_id="0")
    handler = build_handler(
        fetcher=FakeFetcher(),
        store=LocalBlobStore(tmp_path),
        client=real_redis,
        settings=settings,
        blobs_topic=blobs_topic,
    )
    stop = asyncio.Event()
    loop = asyncio.create_task(
        run_loop(
            client=real_redis,
            consumer=consumer,
            group=GROUP,
            settings=settings,
            handler=handler,
            stop=stop,
        )
    )

    start_id = await last_id(real_redis, blobs_topic)
    (seeded,) = await publish(real_redis, scratch_topic, [url])
    dedupe_keys.append(f"{DEDUPE_KEY_PREFIX}{seeded.command_id}")
    try:
        return await watch_for_facts(
            real_redis,
            blobs_topic,
            start_id,
            {seeded.command_id},
            timeout_seconds=FACT_TIMEOUT_SECONDS,
        )
    finally:
        # In a finally, and awaited: a watch that timed out must not leave a loop
        # task consuming streams the fixtures are about to delete underneath it.
        stop.set()
        await asyncio.wait_for(loop, timeout=5)


async def test_a_message_left_pending_is_reclaimed(
    real_redis, scratch_topic, itest_consumer, itest_settings
):
    """Crash recovery, against a PEL the broker actually keeps.

    This is the path the Redis >= 7.0 floor exists for: co-core unpacks
    ``XAUTOCLAIM``'s three-element reply, and the third element does not exist
    below 7.0. ``scripts/check_redis_floor.sh`` guards it as an ExecStartPre;
    this is the test that would have caught a floor violation regardless.
    """
    await real_redis.xadd(scratch_topic, make_command("cmd-orphan"))
    (delivered,) = await itest_consumer.read(count=1, block_ms=READ_BLOCK_MS)
    # Deliberately not acked — exactly what a worker killed mid-handler leaves.
    await asyncio.sleep(CLAIM_MIN_IDLE_MS / 1000)

    reclaimed = await claim_once(real_redis, itest_consumer, itest_settings, group=GROUP)

    assert [message.message_id for message in reclaimed] == [delivered.message_id]


async def test_a_message_younger_than_the_idle_window_is_left_alone(
    real_redis, scratch_topic, itest_consumer, itest_settings
):
    """The window is what keeps a reclaim from stealing live work.

    Without it, ``claim_once`` running ahead of every read would pull back
    messages another worker is still handling — at-least-once turned into
    duplicate-always.
    """
    await real_redis.xadd(scratch_topic, make_command("cmd-in-flight"))
    await itest_consumer.read(count=1, block_ms=READ_BLOCK_MS)

    assert await claim_once(real_redis, itest_consumer, itest_settings, group=GROUP) == []


async def test_a_reclaim_advances_the_delivery_counter(
    real_redis, scratch_topic, itest_consumer, itest_settings
):
    """XPENDING's counter is the retry accounting — no side counter to reconcile.

    It advances on a reclaim and only on a reclaim, which is what makes
    ``REPLICATOR_MAX_DELIVERY_ATTEMPTS`` a ceiling in time (attempts x
    ``claim_min_idle_ms``) rather than in retries.
    """
    await real_redis.xadd(scratch_topic, make_command("cmd-counted"))
    (delivered,) = await itest_consumer.read(count=1, block_ms=READ_BLOCK_MS)
    assert await times_delivered(real_redis, scratch_topic, delivered.message_id) == 1

    await asyncio.sleep(CLAIM_MIN_IDLE_MS / 1000)
    await claim_once(real_redis, itest_consumer, itest_settings, group=GROUP)

    assert await times_delivered(real_redis, scratch_topic, delivered.message_id) == 2


async def test_a_read_with_nothing_to_read_blocks_for_its_window(itest_consumer):
    """fakeredis returns immediately; the real server waits, and shutdown latency
    is bounded by exactly this.

    A lower bound only. An upper one would be a bet on scheduler and broker
    latency on a shared VM, which is how a suite acquires a test that fails on
    Tuesdays.
    """
    started = time.monotonic()

    assert await itest_consumer.read(count=1, block_ms=READ_BLOCK_MS) == []

    assert time.monotonic() - started >= READ_BLOCK_MS / 1000 * 0.8


async def test_a_blocking_read_wakes_as_soon_as_a_message_arrives(
    real_redis, scratch_topic, itest_consumer
):
    """A long block is not a long wait: delivery wakes the reader immediately.

    This is why a 5s ``REPLICATOR_READ_BLOCK_MS`` costs nothing under load and
    only shapes the idle case.
    """
    started = time.monotonic()
    reader = asyncio.create_task(itest_consumer.read(count=1, block_ms=5_000))
    await asyncio.sleep(0.1)
    await real_redis.xadd(scratch_topic, make_command("cmd-wake"))

    messages = await reader

    elapsed = time.monotonic() - started
    assert len(messages) == 1
    # Past the sleep, so it genuinely blocked; far short of the window, so it
    # woke on the delivery rather than on the timeout.
    assert 0.1 <= elapsed < 2.0


async def test_a_frame_that_will_not_decode_round_trips_to_the_dlq(
    real_redis, scratch_topic, itest_consumer, itest_settings
):
    """``from_wire`` raises from inside ``read``, so the DLQ copy is a re-read.

    The anomaly carries ``topic`` and ``message_id`` only — no field map — and
    ``XADD`` rejects an empty one, so the frame is fetched back by id. Nothing
    about that survives a broker that reports ids differently than the fake.
    """
    await real_redis.xadd(scratch_topic, {"event_type": "content_fetch", "payload": "not json"})

    assert await poll_once(real_redis, itest_consumer, itest_settings, group=GROUP) == []

    ((_id, entry),) = await real_redis.xrange(dlq_name(scratch_topic))
    assert entry[b"payload"] == b"not json"
    assert entry[b"dlq_reason"] == b"frame failed to decode"
    assert (await real_redis.xpending(scratch_topic, GROUP))["pending"] == 0


async def test_a_poison_entry_whose_frame_is_gone_still_leaves_the_pel(
    real_redis, scratch_topic, itest_consumer
):
    """Trimmed between delivery and dead-lettering: the fallback path, live.

    A pending entry whose stream entry no longer exists is the shape that would
    otherwise wedge recovery forever — ``claim_stale`` restarts at ``0-0``, so a
    frame it cannot decode and cannot copy would be re-raised on every pass.
    """
    await real_redis.xadd(scratch_topic, {"event_type": "content_fetch", "payload": "{"})
    with pytest.raises(BusMessageAnomaly) as excinfo:
        await itest_consumer.read(count=1, block_ms=READ_BLOCK_MS)
    await real_redis.xdel(scratch_topic, excinfo.value.message_id)

    outcome = await dead_letter_anomaly(real_redis, itest_consumer, excinfo.value)

    assert outcome is Outcome.DEAD_LETTERED
    ((_id, entry),) = await real_redis.xrange(dlq_name(scratch_topic))
    assert entry[b"original_message_id"] == excinfo.value.message_id.encode()
    assert (await real_redis.xpending(scratch_topic, GROUP))["pending"] == 0


async def test_a_seeded_command_stores_a_blob_and_publishes_the_fact(
    real_redis, scratch_topic, scratch_blobs_topic, dedupe_keys, itest_settings, tmp_path
):
    """The MVP's core claim, demonstrated rather than asserted.

    ``content.fetch`` -> fetch -> fingerprint -> temp-store -> ``blob_available``,
    driven end to end against the Archiver-operated broker. Only the fetch is
    faked — this exists to prove the bus and storage behaviour, and a live origin
    would add flakiness rather than signal.
    """
    settings = itest_settings.model_copy(
        update={"blob_dir": tmp_path, "dedupe_ttl_seconds": DEDUPE_TTL_SECONDS}
    )

    facts = await seed_and_consume(
        real_redis,
        scratch_topic,
        scratch_blobs_topic,
        dedupe_keys,
        settings,
        tmp_path,
        "https://example.test/e2e",
    )

    (fact,) = facts
    assert fact.content_fingerprint == sha256(BODY)
    assert fact.url == "https://example.test/e2e"
    assert fact.size_bytes == len(BODY)
    # The bytes are at the URI the fact announced, not merely somewhere on disk.
    assert (
        fact.blob_uri
        == f"file://{tmp_path}/{sha256(BODY)[:2]}/{sha256(BODY)[2:4]}/{sha256(BODY)}.bin"
    )
    assert LocalBlobStore(tmp_path).open(fact.content_fingerprint) == BODY
    # Exactly one fact, and the command is off the PEL — the loop acked it.
    assert await real_redis.xlen(scratch_blobs_topic) == 1
    assert (await real_redis.xpending(scratch_topic, GROUP))["pending"] == 0


async def test_an_end_to_end_run_only_creates_predictable_keys(
    real_redis, scratch_topic, scratch_blobs_topic, dedupe_keys, itest_settings, tmp_path
):
    """The scratch-key promise, asserted rather than eyeballed.

    This suite shares a broker with whatever else uses the scratch database, and
    what makes it safe to run is that everything it creates is named predictably
    and cleaned up. Two namespaces, and only two: the ``replicator.itest.*``
    streams the fixtures own, and the ``replicator:cmd:*`` dedupe keys the loop
    itself writes — those carry a constant prefix from ``src.worker.loop``, out
    of the stream sweeper's reach, so ``dedupe_keys`` deletes them instead.

    A key outside both namespaces is a housekeeping bug: it outlives the run, and
    on a shared database nobody can tell whose it was.
    """
    settings = itest_settings.model_copy(
        update={"blob_dir": tmp_path, "dedupe_ttl_seconds": DEDUPE_TTL_SECONDS}
    )
    before = await keyspace(real_redis)

    await seed_and_consume(
        real_redis,
        scratch_topic,
        scratch_blobs_topic,
        dedupe_keys,
        settings,
        tmp_path,
        "https://example.test/keyspace",
    )

    created = await keyspace(real_redis) - before
    # Not merely "nothing unexpected": a run that created nothing at all would
    # satisfy a subset check while proving the loop never ran.
    assert created
    assert not [
        key
        for key in created
        if not (key.startswith("replicator.itest.") or key.startswith(DEDUPE_KEY_PREFIX))
    ]
