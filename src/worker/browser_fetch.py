"""The browser fetcher: a ``render="browser"`` command's ``Fetcher`` (#63).

A second implementation of ``handler.Fetcher`` over the renderer's ``load``. Its job is a
``FetchResult`` the rest of the byte path cannot tell from the HTTP driver's: the
pacer's escalation, the status classifier, the size ceiling, fingerprint, store and
``blob_available`` all run unchanged. Design: ``docs/plans/2026-11-05-browser-fetch-design.md``.

**The result describes the stored document, not the origin's response.** The bytes are
the DOM at the load event, re-serialized as UTF-8, so:

- ``content-type`` is ``document.contentType`` with ``charset=utf-8``. The origin's
  charset, and its ``<meta>`` (which the serialization keeps), may say otherwise;
- there is never an ``etag`` or ``last-modified``. They describe bytes that were not
  stored, and validators are refused under ``browser`` (cannobserv#602 decision 3);
- ``retry-after`` rides along on a non-2xx, for the pacer.

**This module raises the fetch vocabulary itself**, as ``capture._render`` does for
capture: ``_fetch``'s httpx arms never see a browser failure (#173's note). What it does
not classify is unclassified on purpose (operator call 4): an unmet ``load`` or a dead
browser rides the delivery ceiling to ``handler_error``, rather than re-spending a whole
render on the serial fetch path every reclaim, forever.
"""

import time

from co_core.effects.fetch import FetchContent, FetchResult

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.worker.browser import (
    DestinationRefusedError,
    DocumentLoader,
    DocumentTooLargeError,
    LoadRequest,
    NavigationFailedError,
    NavigationImpossibleError,
    NotADocumentError,
)

FETCHER_USED = "browser"


class BrowserFetcher:
    """``Fetcher`` over a :class:`~src.worker.browser.DocumentLoader`."""

    def __init__(self, loader: DocumentLoader, *, default_wait_seconds: float, max_bytes: int):
        self._loader = loader
        self._default_wait = default_wait_seconds
        self._max_bytes = max_bytes

    async def execute(self, effect: FetchContent) -> FetchResult:
        """Load ``effect.url``. ``effect.headers`` holds at most ``user-agent`` here.

        The handler refused anything else before this was called, so the one header
        becomes the browser's user agent, on every request the page makes.
        ``effect.timeout``, the issuer's per-operation bound, bounds ``commit`` and
        ``load`` each; the handler's deadline still bounds the whole.
        """
        request = LoadRequest(
            url=effect.url,
            user_agent=(effect.headers or {}).get("user-agent"),
            max_wait_seconds=self._default_wait if effect.timeout is None else effect.timeout,
            max_bytes=self._max_bytes,
        )
        started = time.monotonic()
        try:
            loaded = await self._loader.load(request)
        except DestinationRefusedError as exc:
            # The guard's own refusal, reason and message intact: the URL, a redirect
            # hop, or any request the page made.
            raise exc.refusal from exc
        except NavigationFailedError as exc:
            raise TransientFetchError(f"{effect.url} failed to load: {exc}") from exc
        except (NavigationImpossibleError, NotADocumentError) as exc:
            raise PermanentFetchError(
                f"{effect.url} is not fetchable with render='browser': {exc}",
                reason=FailureReason.NOT_FETCHABLE,
            ) from exc
        except DocumentTooLargeError as exc:
            raise PermanentFetchError(
                f"{effect.url}: {exc}", reason=FailureReason.TOO_LARGE
            ) from exc
        headers: dict[str, str] = {}
        if loaded.media_type is not None:
            headers["content-type"] = f"{loaded.media_type}; charset=utf-8"
        if loaded.retry_after is not None:
            headers["retry-after"] = loaded.retry_after
        return FetchResult(
            content=loaded.document,
            status_code=loaded.status,
            headers=headers,
            # Includes any wait for the render lock: the journal's number is how long
            # this command held the serial path, which is what an operator reads it for.
            duration_ms=round((time.monotonic() - started) * 1000),
            fetcher_used=FETCHER_USED,
            final_url=loaded.final_url,
        )
