"""The byte path behind the loop's handler seam: fetch, fingerprint, store, publish."""

import httpx
import pytest
from co_core.pure.util.hashing import sha256
from redis.exceptions import ResponseError

from src.core.config import get_settings
from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.storage.local import LocalBlobStore
from src.storage.sweeper import BlobUsage
from tests.worker.conftest import (
    BODY,
    INFO_SOURCE_ID,
    URL,
    FakeFetcher,
    command,
    fetch_result,
    published_facts,
)


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


async def test_the_domain_key_is_echoed_verbatim_onto_the_success_fact(handler, fake_redis):
    """#28: copy ``info_source_id`` across, interpret nothing.

    Verbatim is the whole requirement — Replicator holds no domain state, so the
    value is opaque here and the only way to get it wrong is to transform it.
    Asserted against a value the handler could not have derived from anything
    else on the command, so a site reading ``command_id`` by mistake fails
    instead of coincidentally matching.
    """
    await handler()(command("cmd-7", info_source_id="isrc-elsewhere"))

    (fact,) = await published_facts(fake_redis)
    assert fact.info_source_id == "isrc-elsewhere"
    assert fact.command_id == "cmd-7"


async def test_an_opaque_domain_key_is_not_normalized(handler, fake_redis):
    """Whatever the issuer sent is what the fact carries, shape included.

    Replicator has no schema for this value and must not acquire one: trimming,
    lower-casing, or rejecting an odd-looking id would all be *interpretation*,
    and the charter's rule is that the mechanics layer never reads domain meaning
    (``docs/contracts/replicator-boundaries.md``). co-core sets no ``min_length``,
    so even the empty string is the issuer's business, not the fetcher's.
    """
    await handler()(command(info_source_id="  Odd/Id:v2  "))

    (fact,) = await published_facts(fake_redis)
    assert fact.info_source_id == "  Odd/Id:v2  "


async def test_the_default_command_carries_the_shared_domain_key(handler, fake_redis):
    """Guards the fixture itself: a default of ``""`` would make the echo tests
    above pass against a handler that hardcoded a blank."""
    await handler()(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.info_source_id == INFO_SOURCE_ID


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
    with pytest.raises(PermanentFetchError) as caught:
        await handler(FakeFetcher(fetch_result(status_code=status_code)))(command())

    # The status travels as a field, not only inside the message: it is the one
    # datum a 4xx fact carries that the reason token alone cannot express, and
    # Watcher's per-domain backoff branches on it (#9).
    assert caught.value.reason is FailureReason.HTTP_STATUS
    assert caught.value.status_code == status_code


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
async def test_a_server_error_or_backpressure_is_transient(handler, status_code):
    """The origin may well answer on the next reclaim; do not burn the command."""
    with pytest.raises(TransientFetchError):
        await handler(FakeFetcher(fetch_result(status_code=status_code)))(command())


async def test_a_body_less_redirect_is_permanent(handler):
    """A 304 passes ``is_success`` but carries no body — storing it would store nothing."""
    with pytest.raises(PermanentFetchError) as caught:
        await handler(FakeFetcher(fetch_result(content=b"", status_code=304)))(command())

    assert caught.value.reason is FailureReason.HTTP_STATUS
    assert caught.value.status_code == 304


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
    with pytest.raises(PermanentFetchError) as caught:
        await handler(FakeFetcher(error=error))(command())

    assert caught.value.reason is FailureReason.NOT_FETCHABLE
    # No HTTP exchange happened, so there is no status to report.
    assert caught.value.status_code is None


async def test_a_body_larger_than_the_cap_is_permanent(handler, monkeypatch):
    """A blob that will not fit is the origin's answer, not a transient condition."""
    monkeypatch.setenv("REPLICATOR_MAX_BLOB_BYTES", "8")
    get_settings.cache_clear()

    with pytest.raises(PermanentFetchError) as caught:
        await handler(FakeFetcher(fetch_result(content=b"x" * 9)))(command())

    assert caught.value.reason is FailureReason.TOO_LARGE


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


async def test_a_tree_over_its_ceiling_refuses_the_fetch(handler, monkeypatch):
    """Backpressure, not reaping: the bytes a consumer was promised stay put.

    Transient, so the delivery ceiling is untouched and the command waits on the
    bus for the reclaim that follows a sweep freeing space.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_MAX_TOTAL_BYTES", "1000")
    get_settings.cache_clear()
    usage = BlobUsage()
    usage.observe(1_000)
    fetcher = FakeFetcher()

    with pytest.raises(TransientFetchError):
        await handler(fetcher, usage=usage)(command())

    assert fetcher.urls == []


async def test_a_tree_under_its_ceiling_fetches_normally(handler, fake_redis, monkeypatch):
    """Asserted on the fact, not on the usage total.

    A handler that stored the bytes and never announced them would leave usage
    over the ceiling just the same, so counting bytes proves the wrong half.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_MAX_TOTAL_BYTES", "1000")
    get_settings.cache_clear()
    usage = BlobUsage()
    usage.observe(999)

    await handler(usage=usage)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.content_fingerprint == sha256(BODY)


async def test_stored_bytes_are_accounted_for_before_the_next_sweep(handler):
    """A burst can cross the ceiling long before the tree is walked again."""
    usage = BlobUsage()

    await handler(usage=usage)(command())

    assert usage.total_bytes == len(BODY)


async def test_re_storing_the_same_bytes_is_not_counted_twice(handler):
    """Content-addressed storage makes the second store a no-op; usage must agree."""
    usage = BlobUsage()
    build = handler(usage=usage)

    await build(command("cmd-1"))
    await build(command("cmd-2"))

    assert usage.total_bytes == len(BODY)


async def test_a_blob_whose_fact_never_published_is_named_in_the_journal(
    handler, fake_redis, monkeypatch, caplog
):
    """The orphan signal, taken where it is exact rather than reconstructed later.

    Store-then-publish means a publish failure leaves bytes on disk with no fact
    and no command_id pointing at them — invisible to the bus and to any operator
    query starting from content.blobs. Recording the fingerprint here costs
    nothing and avoids making a delete decision depend on another service's
    stream retention.
    """

    async def failing_xadd(*args, **kwargs):
        raise ResponseError("WRONGTYPE")

    monkeypatch.setattr(fake_redis, "xadd", failing_xadd)

    with caplog.at_level("ERROR", logger="src.worker.handler"), pytest.raises(ResponseError):
        await handler()(command("cmd-orphan"))

    (record,) = [r for r in caplog.records if r.levelname == "ERROR"]
    assert record.content_fingerprint == sha256(BODY)
    assert record.command_id == "cmd-orphan"


async def test_a_failed_publish_still_reaches_the_loop_unchanged(handler, fake_redis, monkeypatch):
    """The loop classifies failures; the handler only records what it saw.

    Swallowing or re-wrapping this would change the message's fate — a
    ResponseError walks the delivery ceiling to the DLQ, which is the intended
    outcome for a publish that is not going to start working.
    """

    async def failing_xadd(*args, **kwargs):
        raise ResponseError("WRONGTYPE")

    monkeypatch.setattr(fake_redis, "xadd", failing_xadd)

    with pytest.raises(ResponseError):
        await handler()(command())
