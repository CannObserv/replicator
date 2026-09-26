"""The persist facts: ``blob_persisted`` and ``persist_failed`` on ``content.artifacts``.

The persist counterpart of ``src.worker.replicate_reporter``, on the same stream,
so the issuer's one group there sees every outcome of both commands
(cannobserv#493). Built through co-core's ``*Emit`` twins: the success twin
refuses a digest that is not bare hex; the failure twin echoes whatever the
command carried, because a malformed digest is exactly what ``invalid_source``
refuses and the issuer still needs it to find its own command.

**The asymmetry the other reporters keep.** A failed *failure* publish is
swallowed: the dead-letter entry is already the durable record. A failed
*success* publish re-raises: the command stays pending, the redelivery re-runs a
no-op store, and the fact gets another chance rather than the command closing
with its bytes kept and no one told.
"""

from datetime import UTC, datetime

from co_core.effects.bus import BusPublish
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import BlobPersistedEmit, ContentPersistCommand, PersistFailedEmit
from co_core_aio.bus import AsyncBusPublisher
from redis.asyncio import Redis

from src.core.logging import get_logger
from src.worker.loop import FailureReporter, PersistFailureReport
from src.worker.persist import PersistedPublisher

logger = get_logger(__name__)


def build_persist_reporter(
    *,
    client: Redis,
    artifacts_topic: str = streams.CONTENT_ARTIFACTS,
) -> FailureReporter[PersistFailureReport]:
    """Publish ``persist_failed`` as the loop closes a command.

    ``artifacts_topic`` is a defaulted argument, not a setting, for the reason the
    replicate reporter's is: only a live-broker test moves it.
    """
    publisher = AsyncBusPublisher(client)

    async def report(failure: PersistFailureReport) -> None:
        event = PersistFailedEmit(
            # Half the envelope key, so a re-run failure is a second fact.
            occurred_at=datetime.now(UTC),
            command_id=failure.command_id,
            content_fingerprint=failure.content_fingerprint,
            reason=failure.reason,
            # The loop builds a report only where it has stopped retrying; a
            # transient failure publishes nothing and stays pending.
            terminal=True,
            detail=failure.detail,
        )
        try:
            await publisher.execute(BusPublish(artifacts_topic, to_wire(event)))
        except Exception as exc:
            logger.error(
                "failed to publish persist_failed — this command closes silently",
                extra={
                    "command_id": failure.command_id,
                    "reason": failure.reason,
                    "error": f"{type(exc).__name__}: {exc}",
                    "detail": "the dead-letter still happens; the issuer's reaper is the backstop",
                },
            )
            return
        logger.info(
            "published persist_failed",
            extra={"command_id": failure.command_id, "reason": failure.reason},
        )

    return report


def build_persisted_publisher(
    *,
    client: Redis,
    artifacts_topic: str = streams.CONTENT_ARTIFACTS,
) -> PersistedPublisher:
    """Publish ``blob_persisted``: the digest and the stored size, and no URL.

    The digest is the address; a reader derives the location with co-core's
    ``gcs_uri(bucket, digest)`` from the bucket it is configured with.
    """
    publisher = AsyncBusPublisher(client)

    async def publish(command: ContentPersistCommand, size_bytes: int) -> None:
        event = BlobPersistedEmit(
            occurred_at=datetime.now(UTC),
            command_id=command.command_id,
            content_fingerprint=command.content_fingerprint,
            size_bytes=size_bytes,
        )
        await publisher.execute(BusPublish(artifacts_topic, to_wire(event)))
        logger.info(
            "published blob_persisted",
            extra={
                "command_id": event.command_id,
                "content_fingerprint": event.content_fingerprint,
            },
        )

    return publish
