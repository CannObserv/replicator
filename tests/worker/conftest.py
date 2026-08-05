"""Helpers for driving the consumer loop, against the fake broker and the real one.

Messages are built through co-core's own ``to_wire`` rather than a hand-written
field map, so a producer-side envelope change breaks these tests instead of
silently drifting from what the archiver actually publishes.

``scratch_topic`` is the live-broker half: every ``@pytest.mark.integration``
test in this package works on a stream key it alone owns.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import pytest
from co_core.effects.fetch import FetchResult
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire, to_wire
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import BlobAvailableEvent, ContentFetchCommand

from src.core.config import get_settings
from src.storage.local import LocalBlobStore
from src.storage.sweeper import BlobUsage
from src.worker.handler import build_handler
from src.worker.loop import FailureReport, process_message, run_loop
from src.worker.main import build_consumer

TOPIC = streams.CONTENT_FETCH
GROUP = "replicator.fetch"

# The bytes every byte-path test fetches. Shared so the handler's unit tests and
# the live end-to-end test agree on what a successful fetch returns.
BODY = b"<html>hello</html>"

# The URL the byte-path tests ask for. Distinct from any landing URL a
# FetchResult reports, so a handler echoing the request in place of the redirect
# target is visible rather than tautological.
URL = "https://example.test/a"


def fetch_result(
    content: bytes = BODY,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
    final_url: str | None = None,
) -> FetchResult:
    """A ``FetchResult`` as the co-core driver would return it.

    ``final_url`` defaults to ``None`` — the "driver did not report a landing
    URL" case (cannobserv#279), which is the one a handler could paper over by
    echoing the requested URL. Tests that care about the passthrough set it.
    """
    return FetchResult(
        content=content,
        status_code=status_code,
        headers={"content-type": "text/html"} if headers is None else headers,
        duration_ms=12,
        fetcher_used="http",
        final_url=final_url,
    )


class FakeFetcher:
    """Stands in for ``AsyncFetchDriver``, recording what it was asked for.

    The fetch stays faked even in the live-broker tests: they exist to prove the
    bus and storage behaviour against a real Redis, and a network dependency
    would make them flaky for no added signal.
    """

    def __init__(self, result: FetchResult | None = None, error: Exception | None = None) -> None:
        self._result = result if result is not None else fetch_result()
        self._error = error
        self.urls: list[str] = []

    async def execute(self, effect) -> FetchResult:
        self.urls.append(effect.url)
        if self._error is not None:
            raise self._error
        return self._result


def now() -> datetime:
    """A tz-aware UTC stamp — the only kind co-core's payloads accept.

    ``occurred_at`` is an ``AwareDatetime`` on every model since cannobserv#273;
    a naive value is rejected fail-loud rather than assumed to be UTC.
    """
    return datetime.now(UTC)


async def decoded_facts(client, topic: str) -> list:
    """Every payload on a fact stream, decoded the way a consumer would.

    Untyped on purpose: ``content.blobs`` carries **both** outcomes of a command
    since #9 (``blob_available`` and ``fetch_failed``), so a shared helper cannot
    assert one model. Callers that expect a single type ``isinstance``-check it
    themselves — ``from_wire``'s dispatch table is global and will decode any
    known event type off any topic.
    """
    payloads = []
    for message_id, fields in await client.xrange(topic):
        payloads.append(
            from_wire(
                {k.decode(): v.decode() for k, v in fields.items()},
                topic=topic,
                message_id=message_id.decode(),
            ).payload
        )
    return payloads


def command(command_id: str = "cmd-1", url: str = URL) -> ContentFetchCommand:
    """A decoded ``content.fetch`` command, as the handler receives it."""
    return ContentFetchCommand(occurred_at=datetime.now(UTC), command_id=command_id, url=url)


async def published_facts(client, topic: str = streams.CONTENT_BLOBS) -> list[BlobAvailableEvent]:
    """Decode the fact stream the way a downstream consumer would.

    The ``isinstance`` check is the assertion, not a type-checker appeasement:
    ``from_wire``'s dispatch table is global, so it happily decodes any known
    event type off any topic. A handler that published the wrong model to
    ``content.blobs`` would otherwise sail through every assertion below.
    """
    facts = []
    entries = await client.xrange(topic)
    for message_id, fields in entries:
        payload = from_wire(
            {k.decode(): v.decode() for k, v in fields.items()},
            topic=topic,
            message_id=message_id.decode(),
        ).payload
        assert isinstance(payload, BlobAvailableEvent)
        facts.append(payload)
    return facts


@pytest.fixture
def handler(fake_redis, tmp_path):
    """The real handler over a real store and publisher, with the fetch faked."""

    def build(fetcher=None, blobs_topic: str | None = None, usage: BlobUsage | None = None):
        return build_handler(
            fetcher=fetcher or FakeFetcher(),
            store=LocalBlobStore(tmp_path),
            client=fake_redis,
            settings=get_settings(),
            usage=usage,
            **({} if blobs_topic is None else {"blobs_topic": blobs_topic}),
        )

    return build


def make_command(command_id: str = "cmd-1", url: str = URL) -> dict[str, str]:
    """A well-formed ``content.fetch`` wire frame."""
    return to_wire(
        ContentFetchCommand(
            occurred_at=datetime.now(UTC),
            command_id=command_id,
            url=url,
        )
    )


@pytest.fixture
async def scratch_topic(real_redis) -> AsyncGenerator[str]:
    """A per-test stream key on the scratch database, deleted afterwards.

    The uuid keeps concurrent runs (and a run that died before teardown) from
    colliding on a group whose PEL would otherwise leak into the next test.

    The DLQ goes with it. ``dead_letter`` XADDs to ``<topic>.dlq``, so a test
    that dead-letters anything creates a second key — one the session sweeper
    would eventually expire, but which has no reason to outlive the test that
    made it.
    """
    topic = f"replicator.itest.{uuid.uuid4().hex}"
    try:
        yield topic
    finally:
        await real_redis.delete(topic, dlq_name(topic))


@pytest.fixture
def worker_env(monkeypatch):
    """Pin the group and consumer name so assertions can name them literally."""
    monkeypatch.setenv("REPLICATOR_CONSUMER_GROUP", GROUP)
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator@test")
    get_settings.cache_clear()


@pytest.fixture
async def consumer(fake_redis, worker_env):
    """An ``AsyncBusConsumer`` on ``content.fetch`` with its group created."""
    consumer = build_consumer(fake_redis, get_settings())
    await consumer.ensure_group(start_id="0")
    return consumer


@pytest.fixture
def settings(worker_env):
    """Settings built from the same env the ``consumer`` fixture reads."""
    return get_settings()


async def unreachable_handler(command: ContentFetchCommand) -> None:
    """A handler the test asserts is never called."""
    raise AssertionError(f"handler must not run (got {command.command_id})")


class collected_reports:
    """A ``FailureReporter`` that records instead of publishing.

    Lower-case because it reads as a factory at the call site
    (``reports = collected_reports()``); the reporter seam is a callable, so the
    spy is one too. ``test_reporter.py`` covers what a real one puts on the wire.
    """

    def __init__(self) -> None:
        self.reports: list[FailureReport] = []

    async def __call__(self, report: FailureReport) -> None:
        self.reports.append(report)


async def process_one(fake_redis, consumer, settings, message, handler, reporter=None):
    """``process_message`` with the fixture wiring filled in.

    ``reporter`` defaults to a discarding one so the loop tests that predate #9
    stay about what they were about. Tests that assert on the fact pass a spy.
    """
    return await process_message(
        message,
        client=fake_redis,
        consumer=consumer,
        group=GROUP,
        handler=handler,
        settings=settings,
        reporter=reporter if reporter is not None else collected_reports(),
    )


async def drive_loop(
    fake_redis, consumer, settings, handler, stop, deadline: float = 5, reporter=None
):
    """Run the loop to completion under a deadline, with the fixture wiring.

    The deadline is a test guard, not a feature of the loop: fakeredis does not
    honour ``block``, so a loop that failed to notice ``stop`` would spin rather
    than hang, and the suite would sit at pytest's 300s timeout instead.
    """
    async with asyncio.timeout(deadline):
        await run_loop(
            client=fake_redis,
            consumer=consumer,
            group=GROUP,
            settings=settings,
            handler=handler,
            stop=stop,
            reporter=reporter if reporter is not None else collected_reports(),
        )


async def noop_handler(command: ContentFetchCommand) -> None:
    """A handler that succeeds without doing anything.

    Loop tests are about what the loop does with a handler's outcome, not about
    the byte path — that has its own tests in ``test_handler.py``.
    """
