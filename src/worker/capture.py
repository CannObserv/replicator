"""The ``content.capture`` handler: render a page, store the image (#138).

Fetch's shape with a browser in place of the HTTP driver:

1. **Validate** before anything launches: the URL is ``http(s)`` with a host
   (``not_fetchable``), the viewport and the wait are usable
   (``invalid_request_options``).
2. **The disk ceiling and the host's politeness**, fetch's own checks against
   the same ``BlobUsage`` and **the same** ``HostPacer`` instance: a capture is an
   origin request, and an origin's tolerance is one number however it is asked.
3. **Render** inside one ``REPLICATOR_MAX_FETCH_SECONDS`` deadline, so the stop
   budget gains no term. The renderer says what happened to the page; this module
   says what that means on the wire (``docs/contracts/content-capture-issuer-contract.md``).
4. **Store, then publish** ``capture_stored``: a fact naming absent bytes would be
   unrepairable by the issuer.

No ``content.blobs`` fact and no change fingerprint: a render varies with fonts,
ads and clocks, so its digest is an address and nothing more.
"""

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from co_core.pure.models.changes import ContentCaptureCommand
from co_core.pure.util.blobstore import BlobStore
from co_core.pure.util.hashing import sha256

from src.core.config import Settings
from src.core.errors import CaptureReason, PermanentCaptureError, TransientCaptureError
from src.core.logging import get_logger
from src.storage.sweeper import BlobUsage
from src.worker.browser import (
    DestinationRefusedError,
    InvalidSelectorError,
    NavigationFailedError,
    NavigationStatusError,
    PageTooTallError,
    PageTooWideError,
    Rendered,
    Renderer,
    RenderRequest,
    Wait,
    WaitTimeoutError,
)
from src.worker.handler import in_store, pace, raise_for_ceiling, report_rate_limited
from src.worker.pacing import HostPacer

logger = get_logger(__name__)

MEDIA_TYPE = "image/png"
# A viewport side, in CSS pixels. Past it the screenshot alone is tens of MB.
MAX_VIEWPORT = 4096
# A full page's height, in pixels. Over it the capture is refused, never clipped:
# a clipped image could not be told from a whole one.
MAX_CAPTURE_HEIGHT = 32768
WAIT_KINDS = frozenset({"networkidle", "selector", "delay"})
# Fetch's retryable statuses (handler.py's ``_raise_for_status``): every 5xx, 408, 429.
_RETRYABLE_STATUSES = frozenset({408, 429})
_HTTP_SCHEMES = frozenset({"http", "https"})

type CaptureHandler = Callable[[ContentCaptureCommand], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class Captured:
    """What ``capture_stored`` says beyond the command it echoes."""

    content_fingerprint: str
    blob_uri: str
    media_type: str
    size_bytes: int
    image_width: int
    image_height: int
    final_url: str
    captured_at: datetime
    blob_expires_at: datetime


type CapturedPublisher = Callable[[ContentCaptureCommand, Captured], Awaitable[None]]


class RenderDeadlineError(RuntimeError):
    """The whole render overran ``REPLICATOR_MAX_FETCH_SECONDS``.

    Not a ``TimeoutError``, which the loop counts transient and would retry
    forever: left unclassified, so the delivery ceiling closes it ``handler_error``.
    """


def build_capture_handler(
    *,
    renderer: Renderer,
    store: BlobStore,
    settings: Settings,
    pacer: HostPacer,
    usage: BlobUsage,
    ceiling_bytes: int | None,
    complete: CapturedPublisher,
    stop: asyncio.Event,
    park_above_seconds: float | None = None,
) -> CaptureHandler:
    """Wire the capture path. ``pacer`` and ``usage`` are fetch's instances, shared.

    ``ceiling_bytes`` is ``None`` under the object-store backend, as for fetch.
    """
    if park_above_seconds is None:
        park_above_seconds = settings.read_block_ms / 1000

    async def handle(command: ContentCaptureCommand) -> None:
        request = render_request(command, settings)
        raise_for_ceiling(usage, ceiling_bytes)
        paced_seconds = await pace(pacer, command, stop=stop, park_above_seconds=park_above_seconds)
        started = time.monotonic()
        rendered = await _render(renderer, request, command, pacer, settings.max_fetch_seconds)
        render_ms = round((time.monotonic() - started) * 1000, 1)
        if len(rendered.png) > settings.max_blob_bytes:
            raise PermanentCaptureError(
                f"the image is {len(rendered.png)} bytes, over the "
                f"{settings.max_blob_bytes}-byte ceiling",
                reason=CaptureReason.TOO_LARGE,
            )
        fingerprint = sha256(rendered.png)
        is_new = not await in_store(asyncio.to_thread(store.exists, fingerprint))
        # Before the store, as for blob_available: the announced horizon errs early.
        stored_at = datetime.now(UTC)
        blob_uri = await in_store(
            asyncio.to_thread(store.store, rendered.png, fingerprint, MEDIA_TYPE)
        )
        if is_new and ceiling_bytes is not None:
            usage.add(len(rendered.png))
        captured = Captured(
            content_fingerprint=fingerprint,
            blob_uri=blob_uri,
            media_type=MEDIA_TYPE,
            size_bytes=len(rendered.png),
            image_width=rendered.image_width,
            image_height=rendered.image_height,
            final_url=rendered.final_url,
            captured_at=rendered.captured_at,
            blob_expires_at=stored_at + timedelta(seconds=settings.blob_ttl_seconds),
        )
        await complete(command, captured)
        logger.info(
            "stored a capture and published capture_stored",
            extra={
                "command_id": command.command_id,
                "content_fingerprint": fingerprint,
                "size_bytes": captured.size_bytes,
                "image_width": captured.image_width,
                "image_height": captured.image_height,
                "full_page": command.full_page,
                "wait": None if request.wait is None else request.wait.kind,
                "duration_ms": render_ms,
                "paced_seconds": paced_seconds,
            },
        )

    return handle


def render_request(command: ContentCaptureCommand, settings: Settings) -> RenderRequest:
    """The command as the renderer takes it, or the refusal that closes it."""
    _check_url(command.url)
    viewport = command.viewport
    for side, value in (("width", viewport.width), ("height", viewport.height)):
        if not 1 <= value <= MAX_VIEWPORT:
            raise _invalid(f"viewport {side} {value} is outside 1..{MAX_VIEWPORT}")
    return RenderRequest(
        url=command.url,
        width=viewport.width,
        height=viewport.height,
        full_page=command.full_page,
        wait=_wait(command, settings.capture_max_wait_seconds),
        max_wait_seconds=settings.capture_max_wait_seconds,
        max_height=MAX_CAPTURE_HEIGHT,
        max_width=MAX_VIEWPORT,
    )


def _check_url(url: str) -> None:
    """``http(s)`` with a host a browser can ask a proxy for; nothing else is fetchable.

    The guard decides *where* the host is; this decides only that there is one. A
    browser told ``file:`` or ``chrome:`` would never ask the SOCKS guard at all.
    """
    try:
        parts = urlsplit(url)
        parts.port  # noqa: B018 - raises on an out-of-range or non-numeric port
        host = parts.hostname
        if parts.scheme.lower() not in _HTTP_SCHEMES or not host:
            raise ValueError("not an http(s) URL with a host")
        if any(c.isspace() for c in url):
            raise ValueError("whitespace in the URL")
        host.encode("idna")
    except (ValueError, UnicodeError) as exc:
        raise PermanentCaptureError(
            f"{url!r} is not a URL a browser may be pointed at: {exc}",
            reason=CaptureReason.NOT_FETCHABLE,
        ) from exc


def _wait(command: ContentCaptureCommand, bound: float) -> Wait | None:
    wait = command.wait
    if wait is None:
        return None
    if wait.kind not in WAIT_KINDS:
        raise _invalid(f"wait kind {wait.kind!r} is not one of {sorted(WAIT_KINDS)}")
    if wait.kind == "selector":
        if wait.selector is None or not wait.selector.strip():
            raise _invalid("a selector wait needs a selector")
        return Wait("selector", selector=wait.selector)
    if wait.kind == "delay":
        seconds = wait.seconds
        if seconds is None or not math.isfinite(seconds) or seconds <= 0:
            raise _invalid("a delay wait needs a finite number of seconds over zero")
        if seconds > bound:
            raise _invalid(f"a {seconds:g}s delay is over the {bound:g}s capture ceiling")
        return Wait("delay", seconds=float(seconds))
    return Wait("networkidle")


def _invalid(detail: str) -> PermanentCaptureError:
    return PermanentCaptureError(detail, reason=CaptureReason.INVALID_REQUEST_OPTIONS)


async def _render(
    renderer: Renderer,
    request: RenderRequest,
    command: ContentCaptureCommand,
    pacer: HostPacer,
    deadline: float,
) -> Rendered:
    """The render, in the contract's vocabulary. A dead browser stays unclassified."""
    timeout = asyncio.timeout(deadline)
    try:
        async with timeout:
            return await renderer.render(request)
    except TimeoutError as exc:
        if timeout.expired():
            raise RenderDeadlineError(
                f"the render overran REPLICATOR_MAX_FETCH_SECONDS ({deadline:g}s)"
            ) from exc
        raise
    except DestinationRefusedError as exc:
        raise PermanentCaptureError(str(exc), reason=CaptureReason.DESTINATION_REFUSED) from exc
    except NavigationFailedError as exc:
        raise TransientCaptureError(f"{command.url}: {exc}") from exc
    except NavigationStatusError as exc:
        headers = {} if exc.retry_after is None else {"retry-after": exc.retry_after}
        report_rate_limited(pacer, command, exc.status, headers, now=datetime.now(UTC))
        detail = f"{command.url} answered HTTP {exc.status}"
        if exc.status >= 500 or exc.status in _RETRYABLE_STATUSES:
            raise TransientCaptureError(detail) from exc
        raise PermanentCaptureError(
            detail, reason=CaptureReason.HTTP_STATUS, status_code=exc.status
        ) from exc
    except WaitTimeoutError as exc:
        raise PermanentCaptureError(str(exc), reason=CaptureReason.WAIT_TIMEOUT) from exc
    except InvalidSelectorError as exc:
        raise _invalid(f"the selector is not CSS the browser accepts: {exc}") from exc
    except (PageTooTallError, PageTooWideError) as exc:
        raise PermanentCaptureError(str(exc), reason=CaptureReason.TOO_LARGE) from exc
