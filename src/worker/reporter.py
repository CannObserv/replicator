"""The closing fact path: ``fetch_failed`` for a command that closes without a blob.

Fills the ``FailureReporter`` seam ``src.worker.loop`` dispatches to, and is the
mirror of ``src.worker.handler``: both publish to ``content.blobs``, because an
issuer wants **one** consumer group seeing both outcomes of its commands
(co-core cannobserv#270, shipped in v0.7.2). Separate module rather than a second
function in ``handler``, because nothing here touches the byte path — there are
no bytes, which is the whole point.

The loop decides *whether* a command is closed and *why*; this module knows only
how to say so on the wire. That split is why ``terminal`` is not a parameter: the
loop builds a report only where it has stopped retrying (#9 §3 — non-terminal
facts are deferred).

**Not every fact this publishes is a failure (#17).** ``not_modified`` is a
successful conditional GET, carried on ``fetch_failed`` because the event's real
meaning is "this command will not produce a blob" and ``terminal`` is the field
consumers branch on. So the *count* of facts on this path stopped being a
failure signal — ``reason != "not_modified"`` is the one to alert on — and the
journal line is split accordingly below.

**A failed publish is swallowed here**, deliberately asymmetric with
``handler._publish``, which re-raises. There, raising is what prevents an orphan
blob nothing references. On a *failed* close the dead-letter entry is already the
durable record, and raising would convert a clean dead-letter into an
*unclassified* handler error that burns the delivery ceiling and lands in the
same DLQ minutes later, stranding the message in the PEL in between. On the
``not_modified`` close there is no dead-letter entry at all (#17), so the swallow
is doing more work: raising there would leave a command whose origin has already
answered pending until the next reclaim, to be re-asked. Either way the issuer's
reaper (contract MUST-6) is the backstop for the fact that did not make it —
which is why ``_close_without_dlq`` catches this a second time rather than
trusting the swallow.
"""

from datetime import UTC, datetime

from co_core.effects.bus import BusPublish
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import FetchFailedEvent
from co_core_aio.bus import AsyncBusPublisher
from redis.asyncio import Redis

from src.core.errors import FailureReason
from src.core.logging import get_logger
from src.worker.loop import FailureReport, FailureReporter

logger = get_logger(__name__)


def build_failure_reporter(
    *,
    client: Redis,
    blobs_topic: str = streams.CONTENT_BLOBS,
) -> FailureReporter:
    """Wire fact publishing into a reporter the loop can call as it closes a command.

    ``blobs_topic`` is a defaulted argument rather than a setting, for the same
    reason ``build_handler``'s is: the only caller that moves it is a live-broker
    test, and no deployment wants a different stream. A ``fetch_failed`` written
    to the real ``content.blobs`` during a test would be a genuine announcement
    that a command an issuer is waiting on has failed.
    """
    publisher = AsyncBusPublisher(client)

    async def report(failure: FailureReport) -> None:
        event = FetchFailedEvent(
            # Stamped here, not carried on the report: it is also half the
            # envelope key (``command_id:occurred_at``), so a redelivery that
            # re-runs the same failure publishes a *distinguishable* second fact
            # rather than one a consumer's dedup-on-key would collapse. That
            # duplicate is what contract MUST-4 requires issuers to tolerate.
            occurred_at=datetime.now(UTC),
            command_id=failure.command_id,
            url=failure.url,
            # Copied across, never read (#28). Replicator holds no domain state
            # and this does not change that: the value is opaque here, and the
            # only thing that could go wrong with it is transforming it.
            info_source_id=failure.info_source_id,
            reason=failure.reason,
            # Every fact Replicator emits today closes its command; see the
            # module docstring and FailureReport.
            terminal=True,
            status_code=failure.status_code,
            attempts=failure.attempts,
            detail=failure.detail,
        )
        try:
            await publisher.execute(BusPublish(blobs_topic, to_wire(event)))
        except Exception as exc:
            logger.error(
                "failed to publish fetch_failed — this command closes silently",
                extra={
                    "command_id": failure.command_id,
                    "reason": str(failure.reason),
                    "error": f"{type(exc).__name__}: {exc}",
                    # Says what the issuer is left with, so the line reads as a
                    # regression to MUST-6's old behaviour rather than as noise.
                    # Hedged on the dead-letter since #17: a ``not_modified``
                    # close writes no DLQ entry, so on that path this line and
                    # the reaper are the only records there are.
                    "detail": (
                        "the command still closes, and the dead-letter still happens unless this "
                        "was a not_modified close; the issuer's reaper is the backstop"
                    ),
                },
            )
            return
        # Two messages for one event type, and the split is operational rather
        # than cosmetic (#17). ``not_modified`` becomes the commonest line in the
        # journal once issuers replay validators, and log-based alerting reads
        # "fetch_failed" as an error — so the routine case must not carry the
        # token in its message. Branching on ``reason`` is safe *here* in a way it
        # would not be in the loop: this decides what a line says, not what
        # happens to a message, so renaming the token costs a log message and
        # nothing else.
        message = (
            "published a completed-without-blob outcome"
            if failure.reason is FailureReason.NOT_MODIFIED
            else "published fetch_failed"
        )
        logger.info(
            message,
            extra={
                "command_id": failure.command_id,
                "reason": str(failure.reason),
                "status_code": failure.status_code,
                "attempts": failure.attempts,
            },
        )

    return report
