"""``capture_stored`` / ``capture_failed`` on ``content.artifacts`` (#138).

Beside the replicate and persist outcomes, so the issuer's one group there sees
every outcome of its commands (co-core's ``CaptureStoredEvent``). Published through
the ``*Emit`` twins, which refuse a digest that is not bare hex and an image with
no pixels.
"""

import asyncio
import hashlib
from datetime import UTC, datetime

import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire, to_wire
from co_core.pure.models.changes import CaptureFailedEvent, CaptureStoredEvent
from co_core_sync.drivers.blobstore import LocalBlobStore
from redis.exceptions import OutOfMemoryError, ResponseError

from src.core.config import get_settings
from src.core.errors import CaptureReason
from src.storage.sweeper import BlobUsage
from src.worker.capture import Captured, build_capture_handler
from src.worker.capture_reporter import build_capture_reporter, build_captured_publisher
from src.worker.loop import CAPTURE_SPEC, CaptureFailureReport, Outcome, poll_once
from src.worker.pacing import HostPacer
from tests.worker.conftest import GROUP, INFO_SOURCE_ID, TOPIC, URL, capture_command, process_one
from tests.worker.test_capture import FakeRenderer

ARTIFACTS = "replicator.test.artifacts"
DIGEST = hashlib.sha256(b"an image").hexdigest()
AT = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


async def facts_on(client, topic):
    return [
        from_wire(
            {k.decode(): v.decode() for k, v in fields.items()},
            topic=topic,
            message_id=message_id.decode(),
        ).payload
        for message_id, fields in await client.xrange(topic)
    ]


def a_report(**overrides) -> CaptureFailureReport:
    fields = {
        "command_id": "cap-1",
        "url": URL,
        "info_source_id": INFO_SOURCE_ID,
        "reason": CaptureReason.HTTP_STATUS,
        "status_code": 404,
    }
    return CaptureFailureReport(**{**fields, **overrides})


def a_capture(**overrides) -> Captured:
    fields = {
        "content_fingerprint": DIGEST,
        "blob_uri": f"file:///var/lib/replicator/blobs/{DIGEST}.bin",
        "media_type": "image/png",
        "size_bytes": 1234,
        "image_width": 1280,
        "image_height": 2400,
        "final_url": "https://example.test/landed",
        "captured_at": AT,
        "blob_expires_at": AT,
    }
    return Captured(**{**fields, **overrides})


async def test_a_report_becomes_a_terminal_capture_failed_fact(fake_redis):
    report = build_capture_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await report(a_report(attempts=2, detail="404"))

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert isinstance(fact, CaptureFailedEvent)
    assert (fact.command_id, fact.url, fact.info_source_id) == ("cap-1", URL, INFO_SOURCE_ID)
    assert (fact.reason, fact.status_code, fact.attempts, fact.detail) == (
        "http_status",
        404,
        2,
        "404",
    )
    assert fact.terminal is True


async def test_a_failed_failure_publish_is_swallowed(fake_redis, monkeypatch, caplog):
    """The dead-letter is already the durable record, as for the other reporters."""

    async def refuse(*args, **kwargs):
        raise ResponseError("NOGROUP")

    report = build_capture_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)
    monkeypatch.setattr(fake_redis, "xadd", refuse)

    with caplog.at_level("ERROR", logger="src.worker.capture_reporter"):
        await report(a_report())

    (record,) = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.command_id == "cap-1"


async def test_the_default_topic_is_content_artifacts(fake_redis):
    await build_capture_reporter(client=fake_redis)(a_report())
    await build_captured_publisher(client=fake_redis)(capture_command(), a_capture())

    assert await fake_redis.xlen(streams.CONTENT_ARTIFACTS) == 2
    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 0


async def test_the_success_fact_names_the_image_in_a_persists_spelling(fake_redis):
    publish = build_captured_publisher(client=fake_redis, artifacts_topic=ARTIFACTS)

    await publish(capture_command("cap-ok"), a_capture())

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert isinstance(fact, CaptureStoredEvent)
    assert (fact.command_id, fact.url, fact.info_source_id) == ("cap-ok", URL, INFO_SOURCE_ID)
    assert (fact.content_fingerprint, fact.media_type, fact.size_bytes) == (
        DIGEST,
        "image/png",
        1234,
    )
    assert fact.blob_uri.endswith(f"{DIGEST}.bin")
    assert (fact.image_width, fact.image_height) == (1280, 2400)
    assert fact.final_url == "https://example.test/landed"
    assert fact.captured_at == AT
    assert fact.blob_expires_at == AT


async def test_a_failed_success_publish_is_raised(fake_redis, monkeypatch):
    """Raised, the redelivery re-renders and the fact gets another chance."""

    async def oom(*args, **kwargs):
        raise OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'.")

    publish = build_captured_publisher(client=fake_redis, artifacts_topic=ARTIFACTS)
    monkeypatch.setattr(fake_redis, "xadd", oom)

    with pytest.raises(OutOfMemoryError):
        await publish(capture_command(), a_capture())


def _handler(fake_redis, tmp_path, renderer):
    return build_capture_handler(
        renderer=renderer,
        store=LocalBlobStore(tmp_path / "blobs"),
        settings=get_settings(),
        pacer=HostPacer(0.0),
        usage=BlobUsage(),
        ceiling_bytes=None,
        complete=build_captured_publisher(client=fake_redis, artifacts_topic=ARTIFACTS),
        stop=asyncio.Event(),
    )


async def test_the_loop_closes_an_invalid_capture_with_a_real_fact(
    fake_redis, consumer, settings, tmp_path
):
    """End to end: a frame on the command stream, a terminal fact on ``content.artifacts``."""
    renderer = FakeRenderer()
    reporter = build_capture_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await fake_redis.xadd(TOPIC, to_wire(capture_command("cap-e2e", width=0)))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis,
        consumer,
        settings,
        message,
        _handler(fake_redis, tmp_path, renderer),
        reporter=reporter,
        spec=CAPTURE_SPEC,
    )

    assert outcome is Outcome.DEAD_LETTERED
    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert (fact.command_id, fact.reason, fact.terminal) == (
        "cap-e2e",
        "invalid_request_options",
        True,
    )
    assert renderer.requests == []


async def test_the_loop_acks_a_stored_capture_after_its_fact(
    fake_redis, consumer, settings, tmp_path
):
    await fake_redis.xadd(TOPIC, to_wire(capture_command("cap-ok")))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis,
        consumer,
        settings,
        message,
        _handler(fake_redis, tmp_path, FakeRenderer()),
        spec=CAPTURE_SPEC,
    )

    assert outcome is Outcome.ACKED
    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert isinstance(fact, CaptureStoredEvent)
    assert await fake_redis.exists(CAPTURE_SPEC.dedupe_key("cap-ok"))
