"""The byte path: fetch -> fingerprint -> temp-store -> ``blob_available``.

Fills the seam ``src.worker.loop`` dispatches to. The loop owns a message's fate;
this module's only vocabulary for influencing it is raising —
``PermanentFetchError`` to dead-letter now, ``TransientFetchError`` to leave the
message pending for the next reclaim.
"""

from datetime import UTC, datetime
from typing import Protocol

import httpx
from co_core.effects.bus import BusPublish
from co_core.effects.fetch import FetchContent, FetchResult
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import BlobAvailableEvent, ContentFetchCommand
from co_core.pure.util.hashing import sha256
from co_core_aio.bus import AsyncBusPublisher
from redis.asyncio import Redis

from src.core.config import Settings
from src.core.errors import PermanentFetchError, TransientFetchError
from src.core.logging import get_logger
from src.storage.base import BlobStore
from src.worker.loop import Handler

logger = get_logger(__name__)

# What a response with no usable Content-Type is stored and announced as.
DEFAULT_MEDIA_TYPE = "application/octet-stream"

# Non-2xx statuses worth another attempt. 5xx is the origin failing rather than
# refusing; 429 and 408 are it asking for later explicitly. Everything else
# non-2xx is the origin's settled answer — re-fetching a 404 gets another 404,
# so retrying only postpones the dead-letter while holding a PEL slot.
#
# The retry cadence is REPLICATOR_CLAIM_MIN_IDLE_MS, so this is deliberately not
# a tight loop against a struggling origin: raising leaves the message pending
# and the next reclaim brings it back a minute later.
_RETRYABLE_STATUSES = frozenset({408, 429})


class Fetcher(Protocol):
    """The fetch seam — ``co_core_aio.fetch.AsyncFetchDriver`` in production."""

    async def execute(self, effect: FetchContent) -> FetchResult: ...


def build_handler(
    *,
    fetcher: Fetcher,
    store: BlobStore,
    client: Redis,
    settings: Settings,
) -> Handler:
    """Wire the byte path into a handler the loop can dispatch to."""
    publisher = AsyncBusPublisher(client)

    async def handle(command: ContentFetchCommand) -> None:
        result = await _fetch(fetcher, command)
        _raise_for_status(result, command)
        _raise_for_size(result, command, settings.max_blob_bytes)
        fingerprint = sha256(result.content)
        media_type = _media_type(result)
        blob_uri = store.store(result.content, fingerprint, media_type)
        await publisher.execute(
            BusPublish(
                streams.CONTENT_BLOBS,
                to_wire(
                    BlobAvailableEvent(
                        occurred_at=datetime.now(UTC),
                        content_fingerprint=fingerprint,
                        blob_uri=blob_uri,
                        size_bytes=len(result.content),
                        media_type=media_type,
                        url=command.url,
                        command_id=command.command_id,
                    )
                ),
            )
        )
        logger.info(
            "stored a blob and published blob_available",
            extra={
                "command_id": command.command_id,
                "content_fingerprint": fingerprint,
                "size_bytes": len(result.content),
                "media_type": media_type,
                # The one number nothing else keeps: the fact carries size but
                # not how long the origin took to give it up.
                "duration_ms": result.duration_ms,
            },
        )

    return handle


async def _fetch(fetcher: Fetcher, command: ContentFetchCommand) -> FetchResult:
    """Fetch the command's URL, mapping httpx's failures into the loop's vocabulary.

    httpx's exception hierarchy is disjoint from the builtin ``ConnectionError`` /
    ``TimeoutError`` the loop already treats as transient, so an unmapped
    ``ConnectError`` would land in the loop's *unclassified* branch and consume
    the delivery ceiling — an origin down for a few reclaims could dead-letter a
    perfectly good command.

    ``InvalidURL`` and ``UnsupportedProtocol`` are the exceptions: a URL that is
    not a URL will not become one on the next reclaim.
    """
    try:
        return await fetcher.execute(FetchContent(command.url))
    except (httpx.UnsupportedProtocol, httpx.InvalidURL) as exc:
        raise PermanentFetchError(f"{command.url} is not fetchable: {exc}") from exc
    except httpx.HTTPError as exc:
        raise TransientFetchError(f"{command.url} failed to fetch: {exc}") from exc


def _raise_for_size(result: FetchResult, command: ContentFetchCommand, maximum: int) -> None:
    """Refuse a body too large to keep, before it reaches the blob directory."""
    if len(result.content) <= maximum:
        return
    raise PermanentFetchError(
        f"{command.url} returned {len(result.content)} bytes, over the {maximum}-byte ceiling"
    )


def _raise_for_status(result: FetchResult, command: ContentFetchCommand) -> None:
    """Turn a captured non-2xx status into the loop's failure vocabulary.

    ``is_2xx`` and not ``is_success`` is the body-presence predicate: a 304 Not
    Modified reports ``is_success`` while carrying an empty body, and it reaches
    us on the *default* path — httpx only follows the redirects that carry a
    ``Location``. Storing that would content-address the empty string.

    The co-core fetch driver captures non-2xx into the result rather than
    raising, so this is the only place a status becomes an outcome.
    """
    if result.is_2xx:
        return
    detail = f"{command.url} returned HTTP {result.status_code}"
    if result.status_code >= 500 or result.status_code in _RETRYABLE_STATUSES:
        raise TransientFetchError(detail)
    raise PermanentFetchError(detail)


def _media_type(result: FetchResult) -> str:
    """The response's media type, normalized, or the generic fallback.

    The ``charset`` parameter describes the bytes' *encoding*, not their type, so
    it is dropped: a consumer grouping facts by media type must not see
    ``text/html`` and ``text/html; charset=utf-8`` as two different kinds of
    thing. Case is normalized for the same reason — RFC 9110 makes the type and
    subtype case-insensitive, and origins are inconsistent about it.

    An absent, blank, or parameter-only header falls back rather than
    propagating an empty ``media_type`` onto the fact.

    The lookup is case-folded rather than trusting ``content-type`` to arrive
    lowercased. httpx does normalize, but ``FetchResult.headers`` is typed as a
    plain ``Mapping[str, str]`` with no such guarantee, and the failure mode of
    assuming it — every response silently typed ``application/octet-stream`` —
    is quiet enough to survive a long time.
    """
    headers = {name.lower(): value for name, value in result.headers.items()}
    header = headers.get("content-type", "")
    return header.split(";")[0].strip().lower() or DEFAULT_MEDIA_TYPE
