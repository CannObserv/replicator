"""The failure fact path: what a closed-without-a-blob command announces.

The other half of ``test_handler.py``. Both publish to ``content.blobs``; this
one is what an issuer reads to close a pending entry with a reason instead of
waiting out a reaper (#9, co-core cannobserv#270).
"""

import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.models.changes import FetchFailedEvent
from redis.exceptions import ResponseError

from src.core.errors import FailureReason
from src.worker.loop import FailureReport
from src.worker.reporter import build_failure_reporter
from tests.worker.conftest import decoded_facts

REPORT = FailureReport(
    command_id="cmd-1",
    url="https://example.test/a",
    reason=FailureReason.HTTP_STATUS,
    status_code=404,
)


@pytest.fixture
def reporter(fake_redis):
    def build(blobs_topic: str | None = None):
        return build_failure_reporter(
            client=fake_redis,
            **({} if blobs_topic is None else {"blobs_topic": blobs_topic}),
        )

    return build


async def test_a_report_becomes_a_fetch_failed_fact(reporter, fake_redis):
    await reporter()(REPORT)

    (fact,) = await decoded_facts(fake_redis, streams.CONTENT_BLOBS)
    assert isinstance(fact, FetchFailedEvent)
    assert fact.command_id == "cmd-1"
    assert fact.url == "https://example.test/a"
    assert fact.reason == "http_status"
    assert fact.status_code == 404


async def test_every_fact_replicator_emits_today_is_terminal(reporter, fake_redis):
    """A report *is* a closure — the loop only builds one when it stops retrying.

    ``terminal=False`` is the deferred half of #9 §3 (a 429 retrying at the
    reclaim cadence stays invisible while it retries); until a consumer asks for
    it, hardcoding True here keeps the untestable branch from existing.
    """
    await reporter()(REPORT)

    (fact,) = await decoded_facts(fake_redis, streams.CONTENT_BLOBS)
    assert fact.terminal is True


async def test_the_reason_lands_on_the_wire_as_its_token(reporter, fake_redis):
    """StrEnum, so the JSON carries ``too_large`` — not ``FailureReason.TOO_LARGE``.

    Watcher branches on this string. A repr leaking into it would be a wire break
    that no local assertion on the enum itself would catch.
    """
    await reporter()(FailureReport(command_id="c", url="u", reason=FailureReason.TOO_LARGE))

    entry = (await fake_redis.xrange(streams.CONTENT_BLOBS))[0][1]
    assert b'"reason":"too_large"' in entry[b"payload"]


async def test_the_envelope_key_is_per_emission_not_per_command(reporter, fake_redis):
    """co-core keys fetch_failed on ``command_id:occurred_at`` (cannobserv#270).

    Two facts for one command — the redelivery duplicate MUST-4 warns about —
    must not collapse under a consumer's dedup-on-key, or the terminal one is
    the one it drops.
    """
    await reporter()(REPORT)
    await reporter()(REPORT)

    keys = [fields[b"key"] for _, fields in await fake_redis.xrange(streams.CONTENT_BLOBS)]
    assert all(key.startswith(b"cmd-1:") for key in keys)
    assert len(set(keys)) == 2


async def test_absent_context_is_omitted_rather_than_guessed(reporter, fake_redis):
    """No HTTP exchange, no status; not on the ceiling path, no attempt count."""
    await reporter()(FailureReport(command_id="c", url="u", reason=FailureReason.NOT_FETCHABLE))

    (fact,) = await decoded_facts(fake_redis, streams.CONTENT_BLOBS)
    assert fact.status_code is None
    assert fact.attempts is None


async def test_the_ceiling_path_reports_how_many_attempts_it_took(reporter, fake_redis):
    await reporter()(
        FailureReport(
            command_id="c", url="u", reason=FailureReason.HANDLER_ERROR, attempts=5, detail="boom"
        )
    )

    (fact,) = await decoded_facts(fake_redis, streams.CONTENT_BLOBS)
    assert fact.attempts == 5
    assert fact.detail == "boom"


async def test_facts_can_be_pointed_at_a_scratch_stream(reporter, fake_redis):
    """Same reason ``build_handler`` takes a topic: a live-broker test needs one."""
    topic = "replicator.itest.blobs"

    await reporter(blobs_topic=topic)(REPORT)

    assert len(await decoded_facts(fake_redis, topic)) == 1
    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 0


async def test_a_failed_publish_is_swallowed_so_the_dead_letter_still_happens(
    reporter, fake_redis, monkeypatch, caplog
):
    """Deliberately asymmetric with the byte path's ``_publish``, which re-raises.

    There, re-raising is what stops an orphan blob being left with no fact. Here
    the DLQ entry is already the durable record, and raising would turn a clean
    dead-letter into an *unclassified* failure that burns the delivery ceiling
    and reaches the same DLQ minutes later — strictly worse, and it would strand
    the message in the PEL in between.
    """

    async def refuse(*args, **kwargs):
        raise ResponseError("NOGROUP")

    monkeypatch.setattr(fake_redis, "xadd", refuse)

    with caplog.at_level("ERROR", logger="src.worker.reporter"):
        await reporter()(REPORT)  # must not raise

    (record,) = [r for r in caplog.records if r.levelname == "ERROR"]
    assert "failed to publish fetch_failed" in record.message
    # The command_id is the whole value of the line: it is what an operator
    # correlates against the DLQ entry that did still get written.
    assert record.command_id == "cmd-1"
