"""The ``content.capture`` handler (#138): validate, pace, render, store, publish.

Against a fake ``Renderer``: what the browser does is ``test_browser.py``'s and
``test_browser_real.py``'s. These pin what the handler makes of it, in the issuer
contract's vocabulary.
"""

import asyncio
import struct
import zlib
from datetime import UTC, datetime, timedelta

import pytest
from co_core.pure.util.hashing import sha256
from co_core_sync.drivers.blobstore import LocalBlobStore

from src.core.config import get_settings
from src.core.errors import (
    CaptureReason,
    FailureReason,
    PermanentCaptureError,
    PermanentFetchError,
    TransientError,
)
from src.storage.sweeper import BlobUsage
from src.worker.browser import (
    BrowserKilledError,
    DestinationRefusedError,
    InvalidSelectorError,
    NavigationFailedError,
    NavigationStatusError,
    PageTooTallError,
    Rendered,
    RenderRequest,
    RuntimeUnavailableError,
    Wait,
    WaitTimeoutError,
)
from src.worker.capture import (
    MAX_CAPTURE_HEIGHT,
    MAX_VIEWPORT,
    Captured,
    RenderDeadlineError,
    build_capture_handler,
    render_request,
)
from src.worker.pacing import HostPacer
from tests.worker.conftest import INFO_SOURCE_ID, URL, capture_command

CAPTURED_AT = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def _png(width: int, height: int) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        body = kind + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body))

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IEND", b"")


PNG = _png(1280, 2400)


class FakeRenderer:
    def __init__(self, outcome: Rendered | BaseException | None = None) -> None:
        self.outcome = outcome or Rendered(
            png=PNG,
            image_width=1280,
            image_height=2400,
            final_url="https://example.test/landed",
            status=200,
            captured_at=CAPTURED_AT,
        )
        self.requests: list[RenderRequest] = []
        self.hang = False

    async def render(self, request: RenderRequest) -> Rendered:
        self.requests.append(request)
        if self.hang:
            await asyncio.sleep(3600)
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome


@pytest.fixture
def store(tmp_path):
    return LocalBlobStore(tmp_path / "blobs")


@pytest.fixture
def published():
    return []


def _handler(store, published, renderer, *, pacer=None, usage=None, ceiling=None, **settings_env):
    settings = get_settings()

    async def complete(command, captured):
        published.append((command, captured))

    return build_capture_handler(
        renderer=renderer,
        store=store,
        settings=settings,
        pacer=pacer or HostPacer(0.0),
        usage=usage or BlobUsage(),
        ceiling_bytes=ceiling,
        complete=complete,
        stop=asyncio.Event(),
    )


# --------------------------------------------------------------------------- success


async def test_a_capture_is_stored_and_its_fact_describes_the_image(store, published):
    renderer = FakeRenderer()
    handle = _handler(store, published, renderer)
    before = datetime.now(UTC)

    await handle(capture_command(full_page=True, wait={"kind": "selector", "selector": "#main"}))

    (request,) = renderer.requests
    assert request == RenderRequest(
        url=URL,
        width=1280,
        height=800,
        full_page=True,
        wait=Wait(kind="selector", selector="#main", seconds=None),
        max_wait_seconds=30.0,
        max_height=MAX_CAPTURE_HEIGHT,
    )
    ((command, captured),) = published
    assert command.command_id == "cap-1"
    assert command.info_source_id == INFO_SOURCE_ID
    assert isinstance(captured, Captured)
    assert captured.blob_uri == store.uri_for(captured.content_fingerprint)
    assert store.open(captured.content_fingerprint) == PNG
    assert captured.media_type == "image/png"
    assert captured.size_bytes == len(PNG)
    assert (captured.image_width, captured.image_height) == (1280, 2400)
    assert captured.final_url == "https://example.test/landed"
    assert captured.captured_at == CAPTURED_AT
    ttl = timedelta(seconds=get_settings().blob_ttl_seconds)
    assert before + ttl <= captured.blob_expires_at <= datetime.now(UTC) + ttl


async def test_the_bytes_are_stored_before_the_fact_is_published(store, published):
    """Store, then publish: a fact naming absent bytes is unrepairable."""
    seen = []

    async def complete(command, captured):
        seen.append(store.exists(captured.content_fingerprint))

    handle = build_capture_handler(
        renderer=FakeRenderer(),
        store=store,
        settings=get_settings(),
        pacer=HostPacer(0.0),
        usage=BlobUsage(),
        ceiling_bytes=None,
        complete=complete,
        stop=asyncio.Event(),
    )

    await handle(capture_command())

    assert seen == [True]


async def test_a_new_capture_counts_against_the_disk_ceiling_once(store, published):
    usage = BlobUsage()
    handle = _handler(store, published, FakeRenderer(), usage=usage, ceiling=10**9)

    await handle(capture_command("a"))
    await handle(capture_command("b"))

    assert usage.total_bytes == len(PNG)


async def test_over_the_disk_ceiling_nothing_renders(store, published):
    usage = BlobUsage()
    usage.add(100)
    renderer = FakeRenderer()
    handle = _handler(store, published, renderer, usage=usage, ceiling=50)

    with pytest.raises(TransientError):
        await handle(capture_command())
    assert renderer.requests == []


# --------------------------------------------------------------------------- validation


@pytest.mark.parametrize(
    ("width", "height"),
    [(0, 800), (1280, 0), (-1, 800), (MAX_VIEWPORT + 1, 800), (1280, MAX_VIEWPORT + 1)],
)
def test_an_unusable_viewport_is_refused(width, height):
    with pytest.raises(PermanentCaptureError) as caught:
        render_request(capture_command(width=width, height=height), get_settings())
    assert caught.value.reason == CaptureReason.INVALID_REQUEST_OPTIONS
    assert "viewport" in str(caught.value)


def test_the_largest_viewport_is_allowed():
    request = render_request(
        capture_command(width=MAX_VIEWPORT, height=MAX_VIEWPORT), get_settings()
    )
    assert (request.width, request.height) == (MAX_VIEWPORT, MAX_VIEWPORT)


@pytest.mark.parametrize(
    "wait",
    [
        {"kind": "screenshot-now"},
        {"kind": ""},
        {"kind": "selector"},
        {"kind": "selector", "selector": "   "},
        {"kind": "delay"},
        {"kind": "delay", "seconds": 0},
        {"kind": "delay", "seconds": -1},
        {"kind": "delay", "seconds": float("nan")},
        {"kind": "delay", "seconds": float("inf")},
        {"kind": "delay", "seconds": 30.5},
    ],
)
def test_an_unusable_wait_is_refused(wait):
    with pytest.raises(PermanentCaptureError) as caught:
        render_request(capture_command(wait=wait), get_settings())
    assert caught.value.reason == CaptureReason.INVALID_REQUEST_OPTIONS


@pytest.mark.parametrize(
    ("wait", "expected"),
    [
        (None, None),
        ({"kind": "networkidle"}, Wait("networkidle")),
        ({"kind": "networkidle", "seconds": 5}, Wait("networkidle")),
        ({"kind": "delay", "seconds": 30}, Wait("delay", seconds=30.0)),
        ({"kind": "selector", "selector": " #a "}, Wait("selector", selector=" #a ")),
    ],
)
def test_a_usable_wait_reaches_the_renderer(wait, expected):
    assert render_request(capture_command(wait=wait), get_settings()).wait == expected


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.test/a",
        "file:///etc/passwd",
        "data:text/html,hi",
        "javascript:alert(1)",
        "chrome://settings",
        "https:///nohost",
        "http://[::1/",
        "https://example.test:99999/",
        "https://exa mple.test/",
        "https://" + "a" * 64 + ".test/",
    ],
)
def test_a_url_a_browser_should_not_be_pointed_at_is_not_fetchable(url):
    with pytest.raises(PermanentCaptureError) as caught:
        render_request(capture_command(url=url), get_settings())
    assert caught.value.reason == CaptureReason.NOT_FETCHABLE


@pytest.mark.parametrize("url", ["http://example.test/", "HTTPS://Example.TEST/a?b=c#d"])
def test_http_and_https_are_fetchable(url):
    assert render_request(capture_command(url=url), get_settings()).url == url


async def test_an_invalid_command_never_reaches_the_browser(store, published):
    renderer = FakeRenderer()
    handle = _handler(store, published, renderer)

    with pytest.raises(PermanentCaptureError):
        await handle(capture_command(width=0))
    assert renderer.requests == []
    assert published == []


# --------------------------------------------------------------------------- outcomes


async def _fails_with(store, published, exc, **kwargs):
    handle = _handler(store, published, FakeRenderer(exc), **kwargs)
    with pytest.raises(BaseException) as caught:
        await handle(capture_command())
    assert published == []
    return caught.value


async def test_a_refused_destination_is_terminal(store, published):
    refusal = PermanentFetchError("10.0.0.7 is private", reason=FailureReason.DESTINATION_REFUSED)
    exc = await _fails_with(store, published, DestinationRefusedError(refusal))

    assert isinstance(exc, PermanentCaptureError)
    assert exc.reason == CaptureReason.DESTINATION_REFUSED
    assert "10.0.0.7" in str(exc)


@pytest.mark.parametrize("status", [404, 410, 403, 301])
async def test_a_terminal_navigation_status_is_http_status(store, published, status):
    exc = await _fails_with(store, published, NavigationStatusError(status))

    assert isinstance(exc, PermanentCaptureError)
    assert exc.reason == CaptureReason.HTTP_STATUS
    assert exc.status_code == status


@pytest.mark.parametrize("status", [500, 502, 504, 408, 429, 503])
async def test_a_retryable_navigation_status_retries(store, published, status):
    exc = await _fails_with(store, published, NavigationStatusError(status))

    assert isinstance(exc, TransientError)


async def test_a_429_escalates_the_hosts_pacing(store, published):
    """The same pacer instance fetch uses, so the origin's request is honoured by both."""
    pacer = HostPacer(0.0)
    await _fails_with(store, published, NavigationStatusError(429, retry_after="30"), pacer=pacer)

    assert pacer.wait_seconds(URL) == pytest.approx(30, abs=1)


async def test_no_response_to_the_navigation_retries(store, published):
    exc = await _fails_with(store, published, NavigationFailedError("net::ERR_CONNECTION_REFUSED"))

    assert isinstance(exc, TransientError)


async def test_an_unmet_wait_is_a_terminal_wait_timeout(store, published):
    exc = await _fails_with(store, published, WaitTimeoutError("selector", 30))

    assert isinstance(exc, PermanentCaptureError)
    assert exc.reason == CaptureReason.WAIT_TIMEOUT
    assert "selector" in str(exc)


async def test_a_selector_the_browser_cannot_parse_is_invalid_options(store, published):
    exc = await _fails_with(store, published, InvalidSelectorError("while parsing css selector"))

    assert isinstance(exc, PermanentCaptureError)
    assert exc.reason == CaptureReason.INVALID_REQUEST_OPTIONS


async def test_an_over_tall_page_is_too_large(store, published):
    exc = await _fails_with(store, published, PageTooTallError(40000, MAX_CAPTURE_HEIGHT))

    assert isinstance(exc, PermanentCaptureError)
    assert exc.reason == CaptureReason.TOO_LARGE


async def test_an_image_over_the_blob_ceiling_is_too_large_and_not_stored(
    store, published, monkeypatch
):
    monkeypatch.setenv("REPLICATOR_MAX_BLOB_BYTES", str(len(PNG) - 1))
    get_settings.cache_clear()

    exc = await _fails_with(store, published, None)

    assert isinstance(exc, PermanentCaptureError)
    assert exc.reason == CaptureReason.TOO_LARGE
    assert not store.exists(sha256(PNG))


@pytest.mark.parametrize(
    "exc",
    [
        BrowserKilledError("group-killed at the cap"),
        RuntimeUnavailableError("the shell is missing"),
        RuntimeError("Target page, context or browser has been closed"),
    ],
)
async def test_a_dead_browser_is_left_for_the_delivery_ceiling(store, published, exc):
    """Unclassified on purpose: a page that kills the browser once usually does again,
    and the transient exemption would retry it forever."""
    raised = await _fails_with(store, published, exc)

    assert raised is exc
    assert not isinstance(raised, (TransientError, PermanentCaptureError))


async def test_the_whole_render_is_bounded_and_overrunning_it_is_not_transient(
    store, published, monkeypatch
):
    """A builtin ``TimeoutError`` would be transient and retried forever: the deadline
    is its own, unclassified error, so the delivery ceiling closes it."""
    monkeypatch.setenv("REPLICATOR_MAX_FETCH_SECONDS", "0.05")
    monkeypatch.setenv("REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS", "0.05")
    get_settings.cache_clear()
    renderer = FakeRenderer()
    renderer.hang = True
    handle = _handler(store, published, renderer)

    with pytest.raises(RenderDeadlineError, match="0.05"):
        await handle(capture_command())
    assert not issubclass(RenderDeadlineError, (TimeoutError, TransientError))


async def test_a_long_politeness_wait_parks_without_rendering(store, published):
    pacer = HostPacer(600.0)
    pacer.record(URL)
    renderer = FakeRenderer()
    handle = _handler(store, published, renderer, pacer=pacer)

    with pytest.raises(TransientError):
        await handle(capture_command())
    assert renderer.requests == []
