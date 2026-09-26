"""``blob_persisted`` / ``persist_failed`` on ``content.artifacts`` (#114 step 6).

The persist pair rides the replicate pair's stream, so Archiver's one group there
sees every outcome of both commands (cannobserv#493). Published through the
``*Emit`` twins: the success twin refuses a digest that is not bare hex, and the
failure twin echoes one verbatim, because a malformed digest is what
``invalid_source`` refuses.
"""

import hashlib

import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire, to_wire
from co_core.pure.models.changes import BlobPersistedEvent, PersistFailedEvent
from co_core_sync.drivers.blobstore import LocalBlobStore
from redis.exceptions import OutOfMemoryError, ResponseError

from src.core.errors import PersistReason
from src.worker.loop import PERSIST_SPEC, Outcome, PersistFailureReport, poll_once
from src.worker.persist import build_persist_handler
from src.worker.persist_reporter import build_persist_reporter, build_persisted_publisher
from tests.worker.conftest import GROUP, TOPIC, process_one
from tests.worker.test_loop_spec import make_persist_command_model

ARTIFACTS = "replicator.test.artifacts"
DIGEST = hashlib.sha256(b"kept bytes").hexdigest()


async def facts_on(client, topic):
    out = []
    for message_id, fields in await client.xrange(topic):
        out.append(
            from_wire(
                {k.decode(): v.decode() for k, v in fields.items()},
                topic=topic,
                message_id=message_id.decode(),
            ).payload
        )
    return out


def a_report(**overrides) -> PersistFailureReport:
    fields = {
        "command_id": "per-1",
        "content_fingerprint": DIGEST,
        "reason": PersistReason.BLOB_EXPIRED,
    }
    return PersistFailureReport(**{**fields, **overrides})


async def test_a_report_becomes_a_terminal_persist_failed_fact(fake_redis):
    report = build_persist_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await report(a_report(detail="swept"))

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert isinstance(fact, PersistFailedEvent)
    assert (fact.command_id, fact.content_fingerprint, fact.reason, fact.detail) == (
        "per-1",
        DIGEST,
        "blob_expired",
        "swept",
    )
    # Every fact Replicator emits closes its command; a transient failure publishes nothing.
    assert fact.terminal is True


async def test_a_malformed_digest_is_echoed_verbatim(fake_redis):
    """The value ``invalid_source`` refused, so the issuer can find its own command."""
    report = build_persist_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await report(a_report(content_fingerprint="NOT-A-DIGEST", reason=PersistReason.INVALID_SOURCE))

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert fact.content_fingerprint == "NOT-A-DIGEST"


async def test_a_failed_failure_publish_is_swallowed(fake_redis, monkeypatch, caplog):
    """The dead-letter is already the durable record, as for the other two reporters."""

    async def refuse(*args, **kwargs):
        raise ResponseError("NOGROUP")

    report = build_persist_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)
    monkeypatch.setattr(fake_redis, "xadd", refuse)

    with caplog.at_level("ERROR", logger="src.worker.persist_reporter"):
        await report(a_report())

    (record,) = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.command_id == "per-1"


async def test_the_default_topic_is_content_artifacts(fake_redis):
    await build_persist_reporter(client=fake_redis)(a_report())
    await build_persisted_publisher(client=fake_redis)(
        make_persist_command_model(content_fingerprint=DIGEST), 10
    )

    assert await fake_redis.xlen(streams.CONTENT_ARTIFACTS) == 2
    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 0


async def test_the_success_fact_carries_the_digest_and_size_and_no_url(fake_redis):
    publish = build_persisted_publisher(client=fake_redis, artifacts_topic=ARTIFACTS)

    await publish(make_persist_command_model(command_id="per-ok", content_fingerprint=DIGEST), 10)

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert isinstance(fact, BlobPersistedEvent)
    assert (fact.command_id, fact.content_fingerprint, fact.size_bytes) == ("per-ok", DIGEST, 10)
    assert not hasattr(fact, "public_url")


async def test_a_failed_success_publish_is_raised(fake_redis, monkeypatch):
    """Swallowed, the command would close with its bytes kept and no fact; raised, the
    redelivery re-runs a no-op and the fact gets another chance."""

    async def oom(*args, **kwargs):
        raise OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'.")

    publish = build_persisted_publisher(client=fake_redis, artifacts_topic=ARTIFACTS)
    monkeypatch.setattr(fake_redis, "xadd", oom)

    with pytest.raises(OutOfMemoryError):
        await publish(make_persist_command_model(content_fingerprint=DIGEST), 10)


async def test_the_loop_closes_a_persist_command_with_a_real_fact(
    fake_redis, consumer, settings, tmp_path
):
    """End to end: a frame on the command stream, a terminal fact on ``content.artifacts``."""
    handler = build_persist_handler(
        store=LocalBlobStore(tmp_path / "temp", touch_on_rereference=True),
        permanent=LocalBlobStore(tmp_path / "permanent"),
        complete=build_persisted_publisher(client=fake_redis, artifacts_topic=ARTIFACTS),
    )
    reporter = build_persist_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)
    frame = make_persist_command_model(command_id="per-e2e", blob_uri="file:///etc/passwd")

    await fake_redis.xadd(TOPIC, to_wire(frame))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, handler, reporter=reporter, spec=PERSIST_SPEC
    )

    assert outcome is Outcome.DEAD_LETTERED
    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert (fact.command_id, fact.reason, fact.terminal) == ("per-e2e", "invalid_source", True)


async def test_the_loop_acks_a_persisted_command_after_its_fact(
    fake_redis, consumer, settings, tmp_path
):
    temp = LocalBlobStore(tmp_path / "temp", touch_on_rereference=True)
    uri = temp.store(b"kept bytes", DIGEST, "application/pdf")
    handler = build_persist_handler(
        store=temp,
        permanent=LocalBlobStore(tmp_path / "permanent"),
        complete=build_persisted_publisher(client=fake_redis, artifacts_topic=ARTIFACTS),
    )

    await fake_redis.xadd(
        TOPIC, to_wire(make_persist_command_model(blob_uri=uri, content_fingerprint=DIGEST))
    )
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler, spec=PERSIST_SPEC)

    assert outcome is Outcome.ACKED
    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert isinstance(fact, BlobPersistedEvent)
