"""``capture_stored`` and ``capture_failed`` on ``content.artifacts`` (#138).

The persist reporter's shape. Published through co-core's ``*Emit`` twins, so a
digest that is not bare hex, or an image with no pixels, is refused here rather
than decoded downstream.
"""

from datetime import UTC, datetime

from co_core.effects.bus import BusPublish
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import (
    CaptureFailedEmit,
    CaptureStoredEmit,
    ContentCaptureCommand,
)
from co_core_aio.bus import AsyncBusPublisher
from redis.asyncio import Redis

from src.core.logging import get_logger
from src.worker.capture import Captured, CapturedPublisher
from src.worker.loop import CaptureFailureReport, FailureReporter

logger = get_logger(__name__)


def build_capture_reporter(
    *,
    client: Redis,
    artifacts_topic: str = streams.CONTENT_ARTIFACTS,
) -> FailureReporter[CaptureFailureReport]:
    """Publish ``capture_failed`` as the loop closes a command.

    ``artifacts_topic`` is a defaulted argument, not a setting: only a live-broker
    test moves it.
    """
    publisher = AsyncBusPublisher(client)

    async def report(failure: CaptureFailureReport) -> None:
        event = CaptureFailedEmit(
            # Half the envelope key, so a re-run failure is a second fact.
            occurred_at=datetime.now(UTC),
            command_id=failure.command_id,
            url=failure.url,
            # Copied across, never read (#28).
            info_source_id=failure.info_source_id,
            reason=failure.reason,
            # The loop builds a report only where it has stopped retrying.
            terminal=True,
            status_code=failure.status_code,
            attempts=failure.attempts,
            detail=failure.detail,
        )
        try:
            await publisher.execute(BusPublish(artifacts_topic, to_wire(event)))
        except Exception as exc:
            logger.error(
                "failed to publish capture_failed — this command closes silently",
                extra={
                    "command_id": failure.command_id,
                    "reason": failure.reason,
                    "error": f"{type(exc).__name__}: {exc}",
                    "detail": "the dead-letter still happens; the issuer's reaper is the backstop",
                },
            )
            return
        logger.info(
            "published capture_failed",
            extra={"command_id": failure.command_id, "reason": failure.reason},
        )

    return report


def build_captured_publisher(
    *,
    client: Redis,
    artifacts_topic: str = streams.CONTENT_ARTIFACTS,
) -> CapturedPublisher:
    """Publish ``capture_stored``. Raised on failure: the redelivery re-renders."""
    publisher = AsyncBusPublisher(client)

    async def publish(command: ContentCaptureCommand, captured: Captured) -> None:
        event = CaptureStoredEmit(
            occurred_at=datetime.now(UTC),
            command_id=command.command_id,
            url=command.url,
            # Copied across, never read (#28).
            info_source_id=command.info_source_id,
            content_fingerprint=captured.content_fingerprint,
            blob_uri=captured.blob_uri,
            media_type=captured.media_type,
            size_bytes=captured.size_bytes,
            image_width=captured.image_width,
            image_height=captured.image_height,
            final_url=captured.final_url,
            captured_at=captured.captured_at,
            blob_expires_at=captured.blob_expires_at,
        )
        await publisher.execute(BusPublish(artifacts_topic, to_wire(event)))
        logger.info(
            "published capture_stored",
            extra={
                "command_id": event.command_id,
                "content_fingerprint": event.content_fingerprint,
            },
        )

    return publish
