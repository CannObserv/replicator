"""The replicate failure fact: ``replication_failed`` on ``content.artifacts``.

The mirror of ``src.worker.reporter``, and separate from it for the reason that
module is separate from ``handler``: nothing here touches bytes. It fills the
same ``FailureReporter`` seam the loop dispatches to, with its own report type,
because ``ReplicationFailedEvent`` and ``FetchFailedEvent`` agree on four fields
and disagree on the rest — there is no ``status_code`` here and no ``url``, and
there is an ``info_item_rep_spec_id`` the fetch fact has never heard of.

**A different stream, and that is the point.** ``content.artifacts`` carries both
replicate outcomes exactly as ``content.blobs`` carries both fetch outcomes, so
an issuer watching one consumer group sees success and failure for the commands
it sent. The envelope key is ``command_id:occurred_at`` on both, which is
load-bearing here rather than incidental: T4's no-op row re-emits a *success*
for an artifact already written, and a bare ``command_id`` would collapse that
re-emission and collide a non-terminal failure with the success that follows it.

**A failed publish is swallowed**, the same asymmetry ``reporter`` documents: the
dead-letter entry is already the durable record, and raising would convert a
clean dead-letter into an unclassified handler error that burns the delivery
ceiling and lands in the same DLQ minutes later.
"""

from datetime import UTC, datetime

from co_core.effects.bus import BusPublish
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import (
    ContentReplicateCommand,
    ReplicationCompleteEvent,
    ReplicationFailedEvent,
)
from co_core_aio.bus import AsyncBusPublisher
from redis.asyncio import Redis

from src.core.logging import get_logger
from src.worker.loop import FailureReporter, ReplicateFailureReport
from src.worker.replicate import CompletePublisher

logger = get_logger(__name__)


def build_replicate_reporter(
    *,
    client: Redis,
    artifacts_topic: str = streams.CONTENT_ARTIFACTS,
) -> FailureReporter[ReplicateFailureReport]:
    """Wire fact publishing into a reporter the loop can call as it closes a command.

    ``artifacts_topic`` is a defaulted argument rather than a setting, for the
    reason ``content.blobs`` is: the only caller that moves it is a live-broker
    test, and a ``replication_failed`` written to the real stream during a test
    would tell an issuer that a command it is waiting on has failed.
    """
    publisher = AsyncBusPublisher(client)

    async def report(failure: ReplicateFailureReport) -> None:
        event = ReplicationFailedEvent(
            # Stamped here, not carried: it is half the envelope key, so a
            # redelivery that re-runs the same failure publishes a
            # *distinguishable* second fact rather than one a consumer's
            # dedup-on-key would collapse. MUST-4 requires issuers to tolerate it.
            occurred_at=datetime.now(UTC),
            command_id=failure.command_id,
            # Copied across, never read (#28, #29).
            info_item_rep_spec_id=failure.info_item_rep_spec_id,
            source_revision_id=failure.source_revision_id,
            info_source_id=failure.info_source_id,
            reason=failure.reason,
            # Every fact Replicator emits closes its command — the loop builds a
            # report only where it has stopped retrying, so there is no path here
            # that could produce a False (CR #28, #34).
            #
            # The outcomes that are *not* terminal — a provider 5xx, an
            # unresolved conditional create — raise ``TransientReplicateError``,
            # which is exempt from the delivery ceiling: the loop logs, returns
            # RETRY and leaves the entry pending, publishing **nothing**. So an
            # issuer's absence of a fact means "still trying", and MUST-6's
            # reaper is what bounds that. Emitting a `terminal=false` fact
            # instead is a contract change, not an implementation detail.
            terminal=True,
            attempts=failure.attempts,
            detail=failure.detail,
        )
        try:
            await publisher.execute(BusPublish(artifacts_topic, to_wire(event)))
        except Exception as exc:
            logger.error(
                "failed to publish replication_failed — this command closes silently",
                extra={
                    "command_id": failure.command_id,
                    "reason": failure.reason,
                    "error": f"{type(exc).__name__}: {exc}",
                    "detail": "the dead-letter still happens; the issuer's reaper is the backstop",
                },
            )
            return
        logger.info(
            "published replication_failed",
            extra={
                "command_id": failure.command_id,
                "reason": failure.reason,
                "attempts": failure.attempts,
            },
        )

    return report


def build_completion_publisher(
    *,
    client: Redis,
    artifacts_topic: str = streams.CONTENT_ARTIFACTS,
) -> CompletePublisher:
    """Publish ``replication_complete`` — the other outcome, on the same stream.

    Separate from the failure reporter because the loop never calls this one: the
    handler publishes its own success, the way the byte path publishes
    ``blob_available``, and the loop's seam sees failures only.

    **A failed publish re-raises here**, the opposite of the failure reporter's
    swallow — and the asymmetry is the byte path's, for the byte path's reason.
    There, raising is what stops an orphan blob with no fact; here it stops an
    orphan *artifact*: a permanent object in a bucket that no registry row points
    at, which nothing downstream can discover and no reaper can clean up, because
    ``objectCreator`` cannot delete it either. Raising leaves the command in the
    PEL, and the redelivery re-runs a write that T4 makes a safe no-op — the
    fact gets another chance and the artifact is never orphaned.
    """
    publisher = AsyncBusPublisher(client)

    async def publish(command: ContentReplicateCommand, public_url: str) -> None:
        event = ReplicationCompleteEvent(
            occurred_at=datetime.now(UTC),
            command_id=command.command_id,
            public_url=public_url,
            # Copied across, never read (#28, #29). Built here rather than in the
            # handler so that module stays off the echo allowlist entirely.
            info_item_rep_spec_id=command.info_item_rep_spec_id,
            source_revision_id=command.source_revision_id,
            info_source_id=command.info_source_id,
        )
        await publisher.execute(BusPublish(artifacts_topic, to_wire(event)))
        logger.info(
            "published replication_complete",
            extra={"command_id": event.command_id, "public_url": event.public_url},
        )

    return publish
