"""Which closing paths announce themselves, and which are still silent (#9).

Before this, every row of the contract's failure taxonomy that dead-lettered was
issuer-visible as **nothing** — the DLQ is a stream no issuer reads. Each test
here pins one row of that table to a ``fetch_failed`` fact, and the last two pin
the rows that deliberately stay silent because there is no ``command_id`` to
correlate on.
"""

import asyncio
import json

from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.adapters.bus.exceptions import BusMessageAnomaly
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import (
    BlobAvailableEvent,
    ContentFetchCommand,
    FetchFailedEvent,
    SourceRevisionCapturedEvent,
)

from src.core.errors import FailureReason, PermanentFetchError
from src.worker.loop import Outcome, dead_letter_anomaly, poll_once, process_message
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    collected_reports,
    decoded_facts,
    make_command,
    now,
    process_one,
    unreachable_handler,
)


async def failing_handler(command: ContentFetchCommand) -> None:
    raise PermanentFetchError(
        f"{command.url} returned HTTP 404", reason=FailureReason.HTTP_STATUS, status_code=404
    )


async def test_a_permanent_handler_failure_is_announced(fake_redis, consumer, settings):
    """The taxonomy's largest silent row: a 4xx now closes the issuer's entry."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-404", url="https://example.test/x"))
    reports = collected_reports()

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, failing_handler, reporter=reports
    )

    assert outcome is Outcome.DEAD_LETTERED
    (report,) = reports.reports
    assert report.command_id == "cmd-404"
    assert report.url == "https://example.test/x"
    assert report.reason is FailureReason.HTTP_STATUS
    assert report.status_code == 404


async def test_the_handlers_reason_is_the_one_reported(fake_redis, consumer, settings):
    """Three permanent conditions share one exception type — the loop must not guess.

    Left to the loop, ``too_large`` and ``not_fetchable`` would both have to be
    recovered by reading ``str(exc)``.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-big"))
    reports = collected_reports()

    async def too_large(command: ContentFetchCommand) -> None:
        raise PermanentFetchError("over the ceiling", reason=FailureReason.TOO_LARGE)

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_one(fake_redis, consumer, settings, message, too_large, reporter=reports)

    (report,) = reports.reports
    assert report.reason is FailureReason.TOO_LARGE
    assert report.status_code is None


async def test_an_unsupported_schema_version_is_announced(fake_redis, consumer, settings):
    """Reported, but only after branching on the version — never by destructuring first."""
    fields = make_command(command_id="cmd-future")
    payload = json.loads(fields["payload"])
    payload["schema_version"] = 2
    fields["payload"] = json.dumps(payload)
    fields["schema_version"] = "2"
    await fake_redis.xadd(TOPIC, fields)
    reports = collected_reports()

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_one(
        fake_redis, consumer, settings, message, unreachable_handler, reporter=reports
    )

    (report,) = reports.reports
    assert report.command_id == "cmd-future"
    assert report.reason is FailureReason.UNSUPPORTED_SCHEMA_VERSION


async def test_the_delivery_ceiling_reports_how_many_attempts_it_took(
    fake_redis, consumer, settings, monkeypatch
):
    """The one report whose ``attempts`` is meaningful — it is why the command closed."""
    monkeypatch.setattr(settings, "max_delivery_attempts", 1)
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-buggy"))
    reports = collected_reports()

    async def buggy(command: ContentFetchCommand) -> None:
        raise ValueError("a handler bug")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, buggy, reporter=reports)

    assert outcome is Outcome.DEAD_LETTERED
    (report,) = reports.reports
    assert report.reason is FailureReason.HANDLER_ERROR
    assert report.attempts == 1
    assert report.detail is not None
    assert "ValueError" in report.detail


async def test_a_retry_short_of_the_ceiling_announces_nothing(fake_redis, consumer, settings):
    """Nothing is closed yet, so nothing is reported (#9 §3 — no non-terminal facts).

    The cost of deferring §3, pinned so it is a decision rather than an oversight.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-retrying"))
    reports = collected_reports()

    async def buggy(command: ContentFetchCommand) -> None:
        raise ValueError("a handler bug")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, buggy, reporter=reports)

    assert outcome is Outcome.RETRY
    assert reports.reports == []


async def test_a_transient_failure_announces_nothing(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-slow"))
    reports = collected_reports()

    async def transient(command: ContentFetchCommand) -> None:
        raise ConnectionError("origin down")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, transient, reporter=reports
    )

    assert outcome is Outcome.RETRY
    assert reports.reports == []


async def test_a_deduped_command_announces_nothing(fake_redis, consumer, settings):
    """A duplicate is not a failure — the first delivery already produced the fact."""
    await fake_redis.set("replicator:cmd:cmd-dupe", "seen")
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-dupe"))
    reports = collected_reports()

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, unreachable_handler, reporter=reports
    )

    assert outcome is Outcome.DEDUPED
    assert reports.reports == []


async def test_a_foreign_payload_is_never_announced_even_when_it_echoes_a_command_id(
    fake_redis, consumer, settings
):
    """CR #1: a ``command_id`` inside a foreign payload is not *our* command.

    ``BlobAvailableEvent.command_id`` names a command that **succeeded** — that
    is why a blob exists for it. Announcing ``fetch_failed(terminal=True)``
    against it would tell the issuer that a command it already closed with good
    bytes will never produce any: a wrong correlation applied silently, which is
    the failure class MUST-1 / MUST-3 / MUST-5 all exist to prevent. Emitting
    nothing costs a reaper timeout; emitting this corrupts issuer state.
    """
    await fake_redis.xadd(
        TOPIC,
        to_wire(
            BlobAvailableEvent(
                occurred_at=now(),
                content_fingerprint="f" * 64,
                blob_uri="file:///blobs/f",
                size_bytes=1,
                media_type="text/html",
                url="https://example.test/a",
                command_id="cmd-that-actually-succeeded",
            )
        ),
    )
    reports = collected_reports()

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, unreachable_handler, reporter=reports
    )

    assert outcome is Outcome.DEAD_LETTERED
    assert reports.reports == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_a_command_with_a_blank_command_id_is_not_announced(
    fake_redis, consumer, settings, caplog
):
    """CR #3: a fact with no correlator closes nothing — and says so in the journal.

    The guard lives at ``_close``, not at one call site, so it holds for every
    report path there is and every one added later. Silence here would otherwise
    be indistinguishable from an ordinary dead-letter.
    """
    fields = make_command(command_id="cmd-blank")
    payload = json.loads(fields["payload"])
    payload["command_id"] = ""
    payload["schema_version"] = 2  # any closing path will do; this one needs no handler
    fields["payload"] = json.dumps(payload)
    await fake_redis.xadd(TOPIC, fields)
    reports = collected_reports()

    with caplog.at_level("WARNING", logger="src.worker.loop"):
        message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
        outcome = await process_one(
            fake_redis, consumer, settings, message, unreachable_handler, reporter=reports
        )

    assert outcome is Outcome.DEAD_LETTERED
    assert reports.reports == []
    assert any("no command_id" in record.message for record in caplog.records)
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_a_foreign_payload_without_a_command_id_stays_silent(fake_redis, consumer, settings):
    """The correction to #9's scope: this row cannot be announced, only dead-lettered.

    ``FetchFailedEvent.command_id`` is required and is the entire point of the
    event. A payload that carries none — most of the union — has nothing to
    correlate a fact against, so the issuer's reaper stays the mechanism here.
    """
    await fake_redis.xadd(
        TOPIC,
        to_wire(
            SourceRevisionCapturedEvent(
                occurred_at=now(),
                source_revision_id="rev-1",
                info_source_id="src-1",
                content_fingerprint="a" * 64,
                bindings=[],
            )
        ),
    )
    reports = collected_reports()

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, unreachable_handler, reporter=reports
    )

    assert outcome is Outcome.DEAD_LETTERED
    assert reports.reports == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_an_undecodable_frame_stays_silent(fake_redis, consumer, settings):
    """No payload at all, so no ``command_id`` — DLQ-only, by construction."""
    message_id = await fake_redis.xadd(TOPIC, {"event_type": "content_fetch", "payload": "{"})
    await fake_redis.xreadgroup(GROUP, settings.consumer_name, {TOPIC: ">"}, count=1)

    await dead_letter_anomaly(
        fake_redis,
        consumer,
        BusMessageAnomaly("boom", topic=TOPIC, message_id=message_id.decode()),
    )

    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 0
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_the_fact_is_published_before_the_ack(fake_redis, consumer, settings):
    """Same reasoning as store-then-publish, one step further along.

    ``dead_letter`` acks inside itself, so a fact published afterwards is lost
    outright on a crash in between. Published first, the crash costs a *duplicate*
    fact on redelivery — which MUST-4 already requires issuers to tolerate.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-order"))
    seen: list[str] = []

    async def reporter(report) -> None:
        pending = await fake_redis.xpending(TOPIC, GROUP)
        seen.append(f"reported(pending={pending['pending']})")

    original_dead_letter = consumer.dead_letter

    async def watched_dead_letter(*args, **kwargs):
        seen.append("dead_lettered")
        return await original_dead_letter(*args, **kwargs)

    consumer.dead_letter = watched_dead_letter

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_one(fake_redis, consumer, settings, message, failing_handler, reporter=reporter)

    assert seen == ["reported(pending=1)", "dead_lettered"]


async def test_a_reporter_that_raises_cannot_strand_the_message(fake_redis, consumer, settings):
    """Belt and braces: the reporter swallows its own publish failures, but the
    loop must not depend on that. A raising reporter here would leave the message
    unacked and re-deliver a command that is never going to succeed.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-noisy"))

    async def exploding_reporter(report) -> None:
        raise RuntimeError("reporter is broken")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, failing_handler, reporter=exploding_reporter
    )

    assert outcome is Outcome.DEAD_LETTERED
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_the_loop_publishes_a_real_fact_end_to_end(fake_redis, consumer, settings):
    """Wired to the real reporter, not a spy: the seam must actually fit together."""
    from src.worker.reporter import build_failure_reporter

    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-e2e", url="https://example.test/z"))
    stop = asyncio.Event()

    async def handler(command: ContentFetchCommand) -> None:
        stop.set()
        await failing_handler(command)

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await process_message(
        message,
        client=fake_redis,
        consumer=consumer,
        group=GROUP,
        handler=handler,
        settings=settings,
        reporter=build_failure_reporter(client=fake_redis),
    )

    (fact,) = await decoded_facts(fake_redis, streams.CONTENT_BLOBS)
    assert isinstance(fact, FetchFailedEvent)
    assert fact.command_id == "cmd-e2e"
    assert fact.url == "https://example.test/z"
    assert fact.reason == "http_status"
    assert fact.status_code == 404
    assert fact.terminal is True
    # Both surfaces, not one: #9 §4 — the fact is the issuer's, the DLQ is the
    # operator's, and this adds a signal rather than replacing one.
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
