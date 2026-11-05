"""The browser fetch path, through the real handler with a fake loader (#63).

What a ``render="browser"`` command produces on the wire, row by row of the design
doc's outcome table (``docs/plans/2026-11-05-browser-fetch-design.md``). The loader is
faked; ``tests/worker/test_browser_real.py`` (``-m browser``) runs the pinned shell.

The handler is the real one, so a status reaches ``report_rate_limited`` and
``_raise_for_status`` exactly as an HTTP status does: the browser fetcher's whole job is
a ``FetchResult`` the rest of the byte path cannot tell from the driver's.
"""

import asyncio

import pytest

from src.core.errors import (
    CompletedWithoutBlobError,
    FailureReason,
    PermanentFetchError,
    TransientFetchError,
)
from src.worker.browser import (
    BrowserKilledError,
    DestinationRefusedError,
    DocumentTooLargeError,
    Loaded,
    LoadRequest,
    NavigationFailedError,
    NavigationImpossibleError,
    NotADocumentError,
    RenderDeadlineError,
    WaitTimeoutError,
)
from src.worker.browser_fetch import BrowserFetcher
from src.worker.pacing import HostPacer
from tests.worker.conftest import URL, FakeFetcher, command, published_facts

DOCUMENT = "<!DOCTYPE html><html><body>rendered é</body></html>".encode()
LANDED = "https://example.org/landed"
UA = {"user-agent": "watcher/0.1.0"}


class FakeLoader:
    def __init__(self, loaded: Loaded | None = None, error: Exception | None = None) -> None:
        self.loaded = loaded or Loaded(
            document=DOCUMENT,
            media_type="text/html",
            status=200,
            retry_after=None,
            final_url=LANDED,
        )
        self.error = error
        self.requests: list[LoadRequest] = []
        self.hang = False

    async def load(self, request: LoadRequest) -> Loaded:
        self.requests.append(request)
        if self.hang:
            await asyncio.sleep(3600)
        if self.error is not None:
            raise self.error
        return self.loaded


def status(code: int, retry_after: str | None = None) -> Loaded:
    return Loaded(document=b"", media_type=None, status=code, retry_after=retry_after,
                  final_url=LANDED)  # fmt: skip


@pytest.fixture
def browser_handler(handler):
    def build(loader: FakeLoader, **kwargs):
        fetcher = BrowserFetcher(loader, default_wait_seconds=30, max_bytes=1024)
        return handler(FakeFetcher(), browser_fetcher=fetcher, **kwargs)

    return build


# --------------------------------------------------------------------------- the request


async def test_the_load_carries_the_url_the_ua_and_the_default_bound(browser_handler):
    loader = FakeLoader()

    await browser_handler(loader)(command(render="browser", headers=UA))

    assert loader.requests == [
        LoadRequest(url=URL, user_agent="watcher/0.1.0", max_wait_seconds=30, max_bytes=1024)
    ]


async def test_timeout_seconds_bounds_each_wait_and_no_ua_is_chromiums_own(browser_handler):
    loader = FakeLoader()

    await browser_handler(loader)(command(render="browser", timeout_seconds=12.5))

    (request,) = loader.requests
    assert (request.max_wait_seconds, request.user_agent) == (12.5, None)


# --------------------------------------------------------------------------- success


async def test_the_fact_describes_the_stored_document_not_the_origin(browser_handler, fake_redis):
    """UTF-8 bytes, so the charset the fact names is UTF-8's; no validators (#602 decision 3)."""
    await browser_handler(FakeLoader())(command(render="browser"))

    (fact,) = await published_facts(fake_redis)
    assert fact.size_bytes == len(DOCUMENT)
    assert fact.media_type == "text/html"
    assert fact.content_type_raw == "text/html; charset=utf-8"
    assert fact.etag is None and fact.last_modified is None
    assert fact.final_url == LANDED
    assert fact.status_code == 200


async def test_xhtml_keeps_its_own_media_type(browser_handler, fake_redis):
    loader = FakeLoader()
    loader.loaded = Loaded(DOCUMENT, "application/xhtml+xml", 200, None, LANDED)

    await browser_handler(loader)(command(render="browser"))

    (fact,) = await published_facts(fake_redis)
    assert fact.media_type == "application/xhtml+xml"


async def test_a_document_over_the_blob_ceiling_after_the_measure_is_too_large(
    browser_handler, monkeypatch
):
    """The in-page measure is the cheap exit; ``_raise_for_size`` on the bytes is the check."""
    monkeypatch.setenv("REPLICATOR_MAX_BLOB_BYTES", str(len(DOCUMENT) - 1))
    with pytest.raises(PermanentFetchError) as refused:
        await browser_handler(FakeLoader())(command(render="browser"))

    assert refused.value.reason is FailureReason.TOO_LARGE


@pytest.mark.parametrize(
    "url",
    ["file:///etc/", "file:///home/exedev/x.html", "chrome://version", "data:text/html,hi",
     "javascript:alert(1)", "https:///nohost", "ftp://example.test/a"],
)  # fmt: skip
async def test_a_url_a_browser_must_not_open_is_not_fetchable_and_never_loaded(
    browser_handler, url
):
    """CR 1: the SOCKS guard never sees ``file:``, and Chromium serves a local file as
    ``text/html`` (measured), so the scheme is checked before anything launches."""
    loader = FakeLoader()

    with pytest.raises(PermanentFetchError) as refused:
        await browser_handler(loader)(command(url=url, render="browser"))

    assert refused.value.reason is FailureReason.NOT_FETCHABLE
    assert loader.requests == []


# --------------------------------------------------------------------------- statuses


@pytest.mark.parametrize("code", [500, 503, 408, 429])
async def test_a_retryable_status_is_transient(browser_handler, code):
    with pytest.raises(TransientFetchError):
        await browser_handler(FakeLoader(status(code)))(command(render="browser"))


@pytest.mark.parametrize("code", [404, 410, 301])
async def test_any_other_non_2xx_is_http_status_with_its_code(browser_handler, code):
    with pytest.raises(PermanentFetchError) as refused:
        await browser_handler(FakeLoader(status(code)))(command(render="browser"))

    assert refused.value.reason is FailureReason.HTTP_STATUS
    assert refused.value.status_code == code


async def test_a_429s_retry_after_reaches_the_shared_pacer(browser_handler, clock):
    pacer = HostPacer(1.0, clock=clock)

    with pytest.raises(TransientFetchError):
        await browser_handler(FakeLoader(status(429, "45")), pacer=pacer)(command(render="browser"))

    assert pacer.wait_seconds(URL) == pytest.approx(45.0)


async def test_a_503_without_retry_after_still_escalates(browser_handler, clock):
    pacer = HostPacer(1.0, clock=clock)

    with pytest.raises(TransientFetchError):
        await browser_handler(FakeLoader(status(503)), pacer=pacer)(command(render="browser"))

    assert pacer.wait_seconds(URL) > 1.0


async def test_a_304_status_would_still_complete_without_a_blob(browser_handler):
    """Unreachable from Chromium (it aborts an uncached 304), but the arm is the shared one."""
    with pytest.raises(CompletedWithoutBlobError):
        await browser_handler(FakeLoader(status(304)))(command(render="browser"))


# --------------------------------------------------------------------------- render errors


async def test_a_refused_destination_closes_with_the_guards_own_refusal(browser_handler):
    refusal = PermanentFetchError("10.0.0.7 is private", reason=FailureReason.DESTINATION_REFUSED)

    with pytest.raises(PermanentFetchError) as refused:
        await browser_handler(FakeLoader(error=DestinationRefusedError(refusal)))(
            command(render="browser")
        )

    assert refused.value is refusal


async def test_no_response_is_transient(browser_handler):
    with pytest.raises(TransientFetchError, match="ERR_CONNECTION_REFUSED"):
        await browser_handler(
            FakeLoader(error=NavigationFailedError("net::ERR_CONNECTION_REFUSED"))
        )(command(render="browser"))


@pytest.mark.parametrize(
    "error",
    [
        NavigationImpossibleError("net::ERR_UNSAFE_PORT"),
        NotADocumentError("the response is application/json, not an HTML document"),
        NotADocumentError("the response is a download, not a document"),
    ],
)
async def test_a_url_the_browser_cannot_store_is_not_fetchable(browser_handler, error):
    """Operator call 2: the issuer's remedy is ``render="http"``."""
    with pytest.raises(PermanentFetchError) as refused:
        await browser_handler(FakeLoader(error=error))(command(render="browser"))

    assert refused.value.reason is FailureReason.NOT_FETCHABLE
    assert refused.value.status_code is None


async def test_a_document_measured_over_the_ceiling_is_too_large(browser_handler):
    with pytest.raises(PermanentFetchError) as refused:
        await browser_handler(FakeLoader(error=DocumentTooLargeError(2048, 1024)))(
            command(render="browser")
        )

    assert refused.value.reason is FailureReason.TOO_LARGE


@pytest.mark.parametrize(
    "error",
    [WaitTimeoutError("load", 30), BrowserKilledError("group-killed"), RuntimeError("other")],
)
async def test_an_unmet_load_or_a_dead_browser_is_left_unclassified(browser_handler, error):
    """Operator call 4: neither transient (forever) nor a guessed permanent reason."""
    with pytest.raises(type(error)):
        await browser_handler(FakeLoader(error=error))(command(render="browser"))


async def test_the_deadline_under_browser_is_unclassified_not_transient(
    browser_handler, monkeypatch
):
    """HTTP's expiry is transient; a render's re-spends a whole render every reclaim."""
    monkeypatch.setenv("REPLICATOR_MAX_FETCH_SECONDS", "0.05")
    monkeypatch.setenv("REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS", "0.05")
    loader = FakeLoader()
    loader.hang = True

    with pytest.raises(RenderDeadlineError, match="0.05"):
        await browser_handler(loader)(command(render="browser"))
