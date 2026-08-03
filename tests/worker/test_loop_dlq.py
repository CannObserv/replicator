"""Dead-lettering: everything the consume path cannot process, and why.

Five routes reach ``<topic>.dlq`` — a frame that will not decode, one whose
entry has been trimmed, a payload of the wrong type, an unrecognized
``schema_version``, and a handler reporting a permanent failure.
"""

import asyncio
import json
from datetime import UTC, datetime

from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.adapters.bus.exceptions import BusMessageAnomaly
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import BlobAvailableEvent, ContentFetchCommand

from src.core.errors import FailureReason, PermanentFetchError
from src.worker.loop import Outcome, dead_letter_anomaly, poll_once, process_message
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    collected_reports,
    drive_loop,
    make_command,
    process_one,
    unreachable_handler,
)


async def test_a_malformed_frame_is_dead_lettered_and_acked(fake_redis, consumer, settings):
    """AC: poison goes to content.fetch.dlq, the original is acked, the loop lives."""
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})

    messages = await poll_once(fake_redis, consumer, settings, group=GROUP)

    assert messages == []
    dlq = await fake_redis.xrange(dlq_name(TOPIC))
    assert len(dlq) == 1
    assert dlq[0][1][b"payload"] == b"not json"
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_an_unknown_event_type_is_dead_lettered(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, {"event_type": "who_knows", "payload": "{}"})

    assert await poll_once(fake_redis, consumer, settings, group=GROUP) == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_a_deleted_poison_entry_still_dead_letters(fake_redis, consumer, settings):
    """XADD rejects an empty field map, so a trimmed entry needs synthesized fields."""
    message_id = await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "{"})
    await fake_redis.xreadgroup(GROUP, settings.consumer_name, {TOPIC: ">"}, count=1)
    await fake_redis.xdel(TOPIC, message_id)  # entry gone; still pending

    await dead_letter_anomaly(
        fake_redis,
        consumer,
        BusMessageAnomaly("boom", topic=TOPIC, message_id=message_id.decode()),
    )

    dlq = await fake_redis.xrange(dlq_name(TOPIC))
    assert len(dlq) == 1
    assert dlq[0][1][b"original_message_id"] == message_id
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_a_foreign_payload_type_is_dead_lettered(fake_redis, consumer, settings):
    """from_wire's dispatch table is global — a fact on the command stream decodes."""
    await fake_redis.xadd(
        TOPIC,
        to_wire(
            BlobAvailableEvent(
                occurred_at=datetime.now(UTC),
                content_fingerprint="f" * 64,
                blob_uri="file:///blobs/f",
                size_bytes=1,
                media_type="text/html",
                url="https://example.test/a",
            )
        ),
    )

    messages = await poll_once(fake_redis, consumer, settings, group=GROUP)
    outcome = await process_one(fake_redis, consumer, settings, messages[0], unreachable_handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_an_unknown_schema_version_is_dead_lettered(fake_redis, consumer, settings):
    """Branch on schema_version before destructuring — an unknown one is not ours."""
    fields = make_command(command_id="cmd-future")
    payload = json.loads(fields["payload"])
    payload["schema_version"] = 2
    fields["payload"] = json.dumps(payload)
    fields["schema_version"] = "2"
    await fake_redis.xadd(TOPIC, fields)

    messages = await poll_once(fake_redis, consumer, settings, group=GROUP)
    outcome = await process_one(fake_redis, consumer, settings, messages[0], unreachable_handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_dead_lettered_frames_carry_their_reason(fake_redis, consumer, settings):
    """CR #4: the DLQ is the triage surface — the reason belongs in the entry."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-doomed"))

    async def handler(command: ContentFetchCommand) -> None:
        raise PermanentFetchError(
            "this url will never be fetchable", reason=FailureReason.NOT_FETCHABLE
        )

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_message(
        message,
        client=fake_redis,
        consumer=consumer,
        group=GROUP,
        settings=settings,
        handler=handler,
        reporter=collected_reports(),
    )

    entry = (await fake_redis.xrange(dlq_name(TOPIC)))[0][1]
    assert entry[b"dlq_reason"] == b"handler reported a permanent failure"
    assert entry[b"dlq_original_id"] == message.message_id.encode()
    assert entry[b"payload"]  # the original frame is preserved alongside


async def test_the_loop_keeps_running_past_a_poison_frame(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "not json"})
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-after-poison"))
    stop = asyncio.Event()
    seen: list[str] = []

    async def handler(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)
        stop.set()

    await drive_loop(fake_redis, consumer, settings, handler, stop)

    assert seen == ["cmd-after-poison"]
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
