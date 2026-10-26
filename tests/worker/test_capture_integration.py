"""The capture loop against a real broker (#138): group, delivery, facts, ack, dedupe.

``run()``'s capture wiring minus the browser: ``CAPTURE_SPEC`` over ``run_loop``, the
real handler and both publishers, on scratch streams. The render is faked (the fake
from ``test_capture.py``); what a real browser does is ``-m browser``'s. What this
proves is the part fakeredis is weakest on: the frames ``seed_capture`` writes and
the facts the reporters write survive a real round trip, and the group created at
``0`` delivers a command that was on the stream before it.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest
from co_core.pure.adapters.bus.envelope import from_wire, to_wire
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import CaptureFailedEvent, CaptureStoredEvent
from co_core.pure.util.hashing import sha256
from co_core_sync.drivers.blobstore import LocalBlobStore

from scripts import seed_capture
from src.core.config import get_settings
from src.storage.sweeper import BlobUsage
from src.worker.capture import build_capture_handler
from src.worker.capture_reporter import build_capture_reporter, build_captured_publisher
from src.worker.loop import CAPTURE_SPEC, run_loop
from src.worker.main import build_consumer
from src.worker.pacing import HostPacer
from tests.worker.conftest import capture_command
from tests.worker.test_capture import PNG, FakeRenderer

pytestmark = pytest.mark.integration

GROUP = "replicator.itest.capture"
CONSUMER = "replicator@itest"
READ_BLOCK_MS = 300
DEDUPE_TTL_SECONDS = 60
FACT_TIMEOUT_SECONDS = 15


@pytest.fixture
async def artifacts_topic(real_redis, scratch_topic) -> AsyncGenerator[str]:
    """The scratch stand-in for ``content.artifacts``, deleted afterwards."""
    topic = f"{scratch_topic}.artifacts"
    try:
        yield topic
    finally:
        await real_redis.delete(topic)


async def _facts(client, topic: str) -> list:
    """Every fact on ``topic``, once the first has arrived (a blocking read, not a poll)."""
    await client.xread({topic: "0-0"}, count=1, block=FACT_TIMEOUT_SECONDS * 1000)
    return [
        from_wire(
            {k.decode(): v.decode() for k, v in fields.items()},
            topic=topic,
            message_id=entry_id.decode(),
        ).payload
        for entry_id, fields in await client.xrange(topic)
    ]


async def _consume(real_redis, scratch_topic, artifacts_topic, tmp_path, seed):
    """Seed first, then the group at ``0`` (as ``run()`` creates it), then the loop."""
    settings = get_settings().model_copy(
        update={
            "capture_consumer_group": GROUP,
            "consumer_name": CONSUMER,
            "read_block_ms": READ_BLOCK_MS,
            "dedupe_ttl_seconds": DEDUPE_TTL_SECONDS,
            "blob_dir": tmp_path,
        }
    )
    command = await seed()
    consumer = build_consumer(real_redis, settings, topic=scratch_topic, group=GROUP)
    await consumer.ensure_group(start_id="0")
    store = LocalBlobStore(tmp_path, touch_on_rereference=True)
    stop = asyncio.Event()
    loop = asyncio.create_task(
        run_loop(
            client=real_redis,
            consumer=consumer,
            group=GROUP,
            settings=settings,
            handler=build_capture_handler(
                renderer=FakeRenderer(),
                store=store,
                settings=settings,
                pacer=HostPacer(0.0),
                usage=BlobUsage(),
                ceiling_bytes=None,
                complete=build_captured_publisher(
                    client=real_redis, artifacts_topic=artifacts_topic
                ),
                stop=stop,
            ),
            reporter=build_capture_reporter(client=real_redis, artifacts_topic=artifacts_topic),
            spec=CAPTURE_SPEC,
            stop=stop,
        )
    )
    try:
        facts = await _facts(real_redis, artifacts_topic)
    finally:
        stop.set()
        await asyncio.wait_for(loop, timeout=5)
    return command, facts, store


async def test_a_seeded_capture_is_stored_and_announced_on_artifacts(
    real_redis, scratch_topic, artifacts_topic, tmp_path
):
    async def seed():
        args = seed_capture.build_parser().parse_args(
            ["--redis-url", "redis://unused/15", "--topic", scratch_topic, "--full-page",
             "--wait", "networkidle", "https://example.test/page"]
        )  # fmt: skip
        command = seed_capture.build_command(args)
        await seed_capture.publish(real_redis, scratch_topic, command)
        return command

    dedupe_key = None
    try:
        command, facts, store = await _consume(
            real_redis, scratch_topic, artifacts_topic, tmp_path, seed
        )
        dedupe_key = CAPTURE_SPEC.dedupe_key(command.command_id)

        (fact,) = facts
        assert isinstance(fact, CaptureStoredEvent)
        assert fact.command_id == command.command_id
        assert fact.url == "https://example.test/page"
        assert fact.info_source_id == seed_capture.SEED_INFO_SOURCE_ID
        assert fact.content_fingerprint == sha256(PNG)
        assert fact.media_type == "image/png"
        assert (fact.image_width, fact.image_height) == (1280, 2400)
        assert fact.blob_uri == store.uri_for(sha256(PNG))
        assert fact.blob_expires_at > fact.captured_at
        assert store.open(sha256(PNG)) == PNG
        # Acked, and deduped under the capture segment.
        assert (await real_redis.xpending(scratch_topic, GROUP))["pending"] == 0
        assert await real_redis.exists(dedupe_key)
    finally:
        if dedupe_key is not None:
            await real_redis.delete(dedupe_key)


async def test_a_refused_capture_publishes_capture_failed_and_dead_letters(
    real_redis, scratch_topic, artifacts_topic, tmp_path
):
    """A zero viewport, which the strict ``*Emit`` would refuse, so written raw."""

    async def seed():
        command = capture_command("itest-cap-bad", width=0)
        await real_redis.xadd(scratch_topic, to_wire(command))
        return command

    command, facts, _ = await _consume(real_redis, scratch_topic, artifacts_topic, tmp_path, seed)

    (fact,) = facts
    assert isinstance(fact, CaptureFailedEvent)
    assert fact.command_id == "itest-cap-bad"
    assert fact.reason == "invalid_request_options"
    assert fact.terminal is True
    assert await real_redis.xlen(dlq_name(scratch_topic)) == 1
    assert (await real_redis.xpending(scratch_topic, GROUP))["pending"] == 0
    assert not await real_redis.exists(CAPTURE_SPEC.dedupe_key(command.command_id))
