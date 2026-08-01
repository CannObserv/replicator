"""The byte path behind the loop's handler seam: fetch, fingerprint, store, publish."""

from datetime import UTC, datetime

import httpx
import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire
from co_core.pure.models.changes import BlobAvailableEvent, ContentFetchCommand
from co_core.pure.util.hashing import sha256

from src.core.config import get_settings
from src.core.errors import PermanentFetchError, TransientFetchError
from src.storage.local import LocalBlobStore
from src.worker.handler import build_handler
from tests.worker.conftest import BODY, FakeFetcher, fetch_result

URL = "https://example.test/a"


def command(command_id: str = "cmd-1", url: str = URL) -> ContentFetchCommand:
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

    def build(fetcher=None, blobs_topic: str | None = None):
        return build_handler(
            fetcher=fetcher or FakeFetcher(),
            store=LocalBlobStore(tmp_path),
            client=fake_redis,
            settings=get_settings(),
            **({} if blobs_topic is None else {"blobs_topic": blobs_topic}),
        )

    return build


async def test_the_command_url_is_the_url_fetched(handler):
    """The service's defining contract: fetch what you were told to fetch.

    Deliberately not the module default — asserting against ``URL`` would pass
    for a handler that ignored the command and fetched a constant.
    """
    fetcher = FakeFetcher()

    await handler(fetcher)(command(url="https://elsewhere.test/z"))

    assert fetcher.urls == ["https://elsewhere.test/z"]


async def test_a_successful_fetch_stores_the_bytes_under_their_fingerprint(handler, tmp_path):
    await handler()(command())

    fingerprint = sha256(BODY)
    assert LocalBlobStore(tmp_path).open(fingerprint) == BODY


async def test_a_successful_fetch_publishes_blob_available(handler, fake_redis, tmp_path):
    await handler()(command("cmd-7"))

    (fact,) = await published_facts(fake_redis)
    assert fact.content_fingerprint == sha256(BODY)
    assert (
        fact.blob_uri
        == f"file://{tmp_path}/{sha256(BODY)[0:2]}/{sha256(BODY)[2:4]}/{sha256(BODY)}.bin"
    )
    assert fact.size_bytes == len(BODY)
    assert fact.media_type == "text/html"
    assert fact.url == URL
    assert fact.command_id == "cmd-7"


async def test_the_fact_stream_defaults_to_content_blobs(handler, fake_redis):
    """The override below must not be able to move production off the real stream."""
    await handler()(command())

    assert len(await published_facts(fake_redis)) == 1


async def test_the_fact_stream_is_overridable(handler, fake_redis):
    """A live-broker test must be able to keep its facts on a scratch stream.

    ``content.blobs`` is cluster infrastructure: an integration run that wrote
    there would leave a real-looking fact on the shared broker, pointing at a
    blob under ``tmp_path`` that is gone by the time anything reads it.
    """
    topic = "replicator.itest.blobs"

    await handler(blobs_topic=topic)(command())

    assert len(await published_facts(fake_redis, topic)) == 1
    assert await published_facts(fake_redis) == []


@pytest.mark.parametrize("status_code", [400, 404, 410, 451])
async def test_a_client_error_is_permanent(handler, status_code):
    """Re-fetching a 404 gets another 404 — retrying it is only a slower DLQ."""
    with pytest.raises(PermanentFetchError):
        await handler(FakeFetcher(fetch_result(status_code=status_code)))(command())


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
async def test_a_server_error_or_backpressure_is_transient(handler, status_code):
    """The origin may well answer on the next reclaim; do not burn the command."""
    with pytest.raises(TransientFetchError):
        await handler(FakeFetcher(fetch_result(status_code=status_code)))(command())


async def test_a_body_less_redirect_is_permanent(handler):
    """A 304 passes ``is_success`` but carries no body — storing it would store nothing."""
    with pytest.raises(PermanentFetchError):
        await handler(FakeFetcher(fetch_result(content=b"", status_code=304)))(command())


async def test_a_failed_fetch_stores_nothing_and_publishes_nothing(handler, fake_redis, tmp_path):
    with pytest.raises(PermanentFetchError):
        await handler(FakeFetcher(fetch_result(status_code=404)))(command())

    assert await published_facts(fake_redis) == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("text/html; charset=utf-8", "text/html"),
        ("TEXT/HTML", "text/html"),
        ("text/html;charset=utf-8", "text/html"),
        ("  application/pdf  ", "application/pdf"),
        ("", "application/octet-stream"),
        ("   ", "application/octet-stream"),
    ],
)
async def test_media_type_is_normalized(handler, fake_redis, header, expected):
    """The charset parameter describes the bytes' encoding, not their type.

    Downstream groups facts by media type, so ``text/html`` and
    ``text/html; charset=utf-8`` must not read as two different kinds of thing.
    """
    fetcher = FakeFetcher(fetch_result(headers={"content-type": header}))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.media_type == expected


async def test_a_response_without_a_content_type_falls_back(handler, fake_redis):
    await handler(FakeFetcher(fetch_result(headers={})))(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.media_type == "application/octet-stream"


async def test_a_canonically_cased_content_type_header_is_still_found(handler, fake_redis):
    """Lowercase keys are httpx's habit, not a contract.

    ``FetchResult.headers`` is typed as a plain ``Mapping[str, str]``.
    """
    fetcher = FakeFetcher(fetch_result(headers={"Content-Type": "application/pdf"}))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.media_type == "application/pdf"


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("timed out"),
        httpx.RemoteProtocolError("server disconnected"),
    ],
)
async def test_a_transport_failure_is_transient(handler, error):
    """httpx's errors are disjoint from the builtins the loop already classifies.

    Left unmapped they would fall through to the loop's unclassified branch and
    burn the delivery ceiling, so a two-minute origin outage could dead-letter a
    perfectly good command.
    """
    with pytest.raises(TransientFetchError):
        await handler(FakeFetcher(error=error))(command())


@pytest.mark.parametrize(
    "error",
    [
        httpx.UnsupportedProtocol("no scheme"),
        httpx.InvalidURL("not a url"),
    ],
)
async def test_an_unusable_url_is_permanent(handler, error):
    """A malformed URL does not become well-formed on the next reclaim."""
    with pytest.raises(PermanentFetchError):
        await handler(FakeFetcher(error=error))(command())


async def test_a_body_larger_than_the_cap_is_permanent(handler, monkeypatch):
    """A blob that will not fit is the origin's answer, not a transient condition."""
    monkeypatch.setenv("REPLICATOR_MAX_BLOB_BYTES", "8")
    get_settings.cache_clear()

    with pytest.raises(PermanentFetchError):
        await handler(FakeFetcher(fetch_result(content=b"x" * 9)))(command())


async def test_a_body_at_the_cap_is_stored(handler, fake_redis, monkeypatch):
    monkeypatch.setenv("REPLICATOR_MAX_BLOB_BYTES", "8")
    get_settings.cache_clear()

    await handler(FakeFetcher(fetch_result(content=b"x" * 8)))(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.size_bytes == 8


async def test_the_blob_is_stored_before_the_fact_is_published(
    handler, fake_redis, tmp_path, monkeypatch
):
    """Store first, announce second — never the reverse.

    A crash between the two must not leave a ``blob_available`` pointing at
    bytes that are not there: a consumer would read the fact, fail to open the
    blob, and have no way to ask for it again. The opposite gap — stored bytes
    with no fact — is repaired for free, because the message stays unacked and
    the reclaim re-runs a handler that content-addressed storage makes a no-op.
    """

    async def failing_xadd(*args, **kwargs):
        raise ConnectionError("broker went away mid-publish")

    monkeypatch.setattr(fake_redis, "xadd", failing_xadd)

    with pytest.raises(ConnectionError):
        await handler()(command())

    assert LocalBlobStore(tmp_path).exists(sha256(BODY))


async def test_a_rerun_of_the_same_command_republishes_the_same_fingerprint(handler, fake_redis):
    """At-least-once delivery re-runs handlers; the fact must stay identical."""
    await handler()(command("cmd-1"))
    await handler()(command("cmd-1"))

    first, second = await published_facts(fake_redis)
    assert first.content_fingerprint == second.content_fingerprint
    assert first.blob_uri == second.blob_uri


async def test_a_successful_fetch_is_logged_with_what_it_cost(handler, caplog):
    """``duration_ms`` is otherwise discarded — the journal is where it lands."""
    with caplog.at_level("INFO", logger="src.worker.handler"):
        await handler()(command("cmd-9"))

    (record,) = [r for r in caplog.records if r.name == "src.worker.handler"]
    assert record.command_id == "cmd-9"
    assert record.content_fingerprint == sha256(BODY)
    assert record.size_bytes == len(BODY)
    assert record.duration_ms == 12
