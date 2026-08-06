"""The ``content.fetch-policy`` consumer: per-host politeness numbers (#19).

`docs/contracts/replicator-boundaries.md` splits politeness into mechanism and
policy: Replicator enforces a per-host rate because it is the only process that
can see an origin's tolerance across commands, and the issuer decides the
numbers because they are operator policy. #12 shipped the mechanism with a
single env default standing in for the numbers; this module is the other half —
Watcher publishes a `FetchPolicyState` per host, every worker replays the stream
from `0-0` at boot and tails it thereafter, and the map that results is what
`HostPacer` consults.

Two pieces, split because only one of them needs a broker:

* ``FetchPolicyMap`` — what a message *means*. Last-write-wins per host, with a
  tombstone that falls back rather than unlocking, and every rule that can be
  got wrong silently.
* ``run_policy_reader`` — the poll loop, a peer of the consume loop and the
  retention sweep.

**No consumer group.** Every worker needs every message, and a group on a
broadcast stream accumulates a PEL nothing drains. That also means there is no
``ack`` and no DLQ here: a frame that cannot be decoded is skipped past, not
routed anywhere, because there is no correlation obligation to discharge — a
policy message has nothing to close and no fact to publish back.

The state is one of the three shapes the charter permits: derived from the bus,
bounded by the producer's host count, and rebuilt by replay on every boot.
"""

import asyncio
from datetime import datetime
from typing import Protocol

from co_core.pure.adapters.bus.envelope import BusMessage
from co_core.pure.adapters.bus.exceptions import BusMessageAnomaly
from co_core.pure.models.changes import ChangeEventPayload, FetchPolicyState
from co_core_aio.bus import AsyncBusTailReader

from src.core.config import Settings
from src.core.logging import get_logger
from src.worker.loop import (
    IDLE_SLEEP_SECONDS,
    SUPPORTED_SCHEMA_VERSION,
    error_backoff_seconds,
    park,
)

logger = get_logger(__name__)

# Entries are read one at a time, deliberately, and the acceptance criteria in
# #19 allow it as the documented choice.
#
# ``AsyncBusTailReader`` advances its cursor only on a fully-decoded batch, so a
# poison frame at position k discards the k-1 good messages ahead of it and
# redelivers them. Recovering from that at ``count > 1`` means re-reading at
# ``count=1`` to drain the prefix and land on the poison *before* seeking past
# it, because ``seek`` only moves forward and would otherwise skip the prefix
# for good. Reading at ``count=1`` throughout deletes that sequence rather than
# implementing it: this stream carries one message per host per republish, so
# the extra round-trips are noise against a correctness argument that is easy to
# get right today and easy to break on the next edit.
READ_COUNT = 1


class PolicyReader(Protocol):
    """The read seam — ``AsyncBusTailReader`` in production.

    Declared here for the same reason ``handler.py`` declares ``Fetcher``: the
    poll loop's contract with the broker is three calls, and stating them lets a
    test stand in an outage or a fixed sequence without a live client.
    """

    async def read(self, *, count: int, block_ms: int | None = None) -> list[BusMessage]: ...

    async def replay(self, *, count: int = 100) -> list[BusMessage]: ...

    def seek(self, message_id: str) -> None: ...


class FetchPolicyMap:
    """Host -> minimum spacing, as published on ``content.fetch-policy``.

    Deliberately **not** pruned, unlike ``HostPacer``'s last-request map. That
    one is consumer-derived and bounded by what this worker happens to fetch;
    this one is bounded by what the producer publishes, and dropping an entry to
    honour a local limit would silently loosen a host's spacing — the exact
    failure the stream exists to remove. If it ever needs a bound, the bound
    belongs on the producer.
    """

    def __init__(self, default_interval_seconds: float) -> None:
        self._default = default_interval_seconds
        self._intervals: dict[str, float] = {}
        # Last applied stamp per host, live values and tombstones alike. Arrival
        # order is not publication order: the producer periodically republishes
        # its whole set, and a republish assembled from a snapshot taken before a
        # change that already shipped would otherwise revert it.
        self._applied_at: dict[str, datetime] = {}

    @property
    def tracked_hosts(self) -> int:
        """How many hosts carry an explicit policy.

        Logged after the boot replay, because an empty map and a working one are
        otherwise indistinguishable from outside — which is the failure mode the
        replay-from-``0-0`` rule exists to prevent, restated as a log line.
        """
        return len(self._intervals)

    def interval_for(self, host: str) -> float | None:
        """This host's published spacing, or ``None`` when it has no policy.

        ``None`` rather than the default: resolving the fallback is the pacer's
        job, and answering with the default here would make "no policy" and "a
        policy that happens to equal the default" the same answer at the one
        place worth telling them apart.
        """
        return self._intervals.get(host)

    def apply(self, payload: ChangeEventPayload) -> None:
        """Fold one decoded message into the map.

        Every guard that a reader could reasonably leave out lives here rather
        than in the poll loop, so all of them are testable without a broker.
        """
        # from_wire's event_type -> model table is global, so a fact XADDed to
        # this stream decodes cleanly into the wrong model rather than raising.
        # There is no anomaly to recover from and nothing to dead-letter — the
        # only defence is to check the type before destructuring.
        if not isinstance(payload, FetchPolicyState):
            logger.warning(
                "ignoring a message on content.fetch-policy that is not a fetch policy",
                extra={"event_type": payload.event_type},
            )
            return
        # Before destructuring, the same rule the command path follows: a future
        # version that moved `host` would otherwise key the map on nothing.
        if payload.schema_version != SUPPORTED_SCHEMA_VERSION:
            logger.warning(
                "ignoring a fetch policy with an unsupported schema_version",
                extra={"schema_version": payload.schema_version},
            )
            return
        if not self._is_current(payload):
            return
        self._applied_at[payload.host] = payload.occurred_at
        # revoked first. `min_interval_seconds` is None on a tombstone by
        # design — a consumer that reached for it here would store a None and
        # hand it to the pacer as though it were a number.
        if payload.revoked:
            self._revoke(payload.host)
            return
        self._store(payload.host, payload.min_interval_seconds)

    def _is_current(self, payload: FetchPolicyState) -> bool:
        """Whether this message is at least as new as the one already applied.

        ``>=`` rather than ``>``: a producer is free to stamp an entire full-set
        republish with one ``occurred_at``, and a strict comparison would apply
        the first host and drop every other one.

        Tombstones are guarded exactly as live values are. Exempting them would
        make the single message that *erases* state the single message that
        ignores ordering.
        """
        applied_at = self._applied_at.get(payload.host)
        if applied_at is None or payload.occurred_at >= applied_at:
            return True
        logger.info(
            "ignoring a fetch policy older than the one already applied",
            extra={
                "host": payload.host,
                "occurred_at": payload.occurred_at.isoformat(),
                "applied_at": applied_at.isoformat(),
            },
        )
        return False

    def _revoke(self, host: str) -> None:
        """Drop the host: "no explicit policy", **not** "no limit".

        It resolves to ``None`` from here on, so the pacer falls back to the
        conservative env default. Whether that fallback is stricter than the
        value it replaces is not knowable at this moment — which is why the
        contract fixes it from the consumer side and ``_store`` reports every
        policy that turns out to be stricter than it.
        """
        self._intervals.pop(host, None)
        logger.info(
            "revoked a host fetch policy — falling back to the default",
            extra={"host": host, "default_interval_seconds": self._default},
        )

    def _store(self, host: str, min_interval_seconds: float | None) -> None:
        if min_interval_seconds is None:  # pragma: no cover - the model forbids it
            # Belt and braces: FetchPolicyState's own validator rejects a live
            # policy with no interval, so this is unreachable through from_wire.
            logger.warning("ignoring a live fetch policy with no interval", extra={"host": host})
            return
        self._intervals[host] = min_interval_seconds
        logger.info(
            "applied a host fetch policy",
            extra={
                "host": host,
                "min_interval_seconds": min_interval_seconds,
                # Both numbers on one line so "policy never arrived" and "policy
                # says exactly the default" are distinguishable from outside —
                # the failure the Watcher cutover would hit silently otherwise.
                "default_interval_seconds": self._default,
            },
        )
        if min_interval_seconds > self._default:
            # The enforceable half of "the fallback must be at least as strict as
            # anything the producer publishes". Nothing can be asserted at
            # startup: a published interval has no upper bound, so there is no
            # value to compare the default against, and hardcoding the issuer's
            # own backoff ceiling would import the constant this stream exists to
            # avoid importing. What *is* knowable is the moment a real policy
            # turns out to be stricter than the fallback that would replace it on
            # revocation or staleness — and that is the number to raise.
            logger.warning(
                "a published fetch policy is stricter than the fallback default",
                extra={
                    "host": host,
                    "min_interval_seconds": min_interval_seconds,
                    "default_interval_seconds": self._default,
                    "detail": (
                        "revoking this host, or a replay that misses it, would pace it "
                        "more loosely than its owner asked — raise "
                        "REPLICATOR_MIN_HOST_INTERVAL_SECONDS"
                    ),
                },
            )


def build_policy_reader(client, *, topic: str) -> AsyncBusTailReader:
    """Wire a groupless reader for the policy stream.

    ``topic`` is a defaulted argument at the call site rather than a setting, for
    the same reason ``content.fetch`` is: the only caller that moves it is a
    live-broker test working on a scratch stream, and configuring it would put
    the production stream an operator's typo away.
    """
    return AsyncBusTailReader(client, topic=topic)


async def replay_policies(reader: PolicyReader, policies: FetchPolicyMap) -> None:
    """Rebuild the map from the stream, before the worker fetches anything.

    Runs synchronously at boot rather than as the first pass of the tail task:
    started as a peer of the consume loop, a worker would fetch its first
    commands against an empty map and pace every host at the fallback. That is
    safe only because the fallback is meant to be the stricter number, which is
    exactly the assumption not worth spending on startup ordering.

    A failure is absorbed. The cursor advances only over messages that decoded,
    so the tail resumes from the same place and drains the rest — a failed
    replay repairs itself, while failing the boot would turn a policy-stream
    hiccup into a total fetch outage.
    """
    try:
        for message in await reader.replay(count=READ_COUNT):
            policies.apply(message.payload)
    except BusMessageAnomaly as exc:
        # A poison frame at boot. Skip it and leave the rest to the tail loop,
        # which re-enters the same recovery on the next one.
        _skip_poison(reader, exc)
    except Exception as exc:
        logger.error(
            "could not replay the fetch policy stream — starting with what arrived",
            extra={"error": f"{type(exc).__name__}: {exc}"},
        )
    logger.info(
        "fetch policy replay complete",
        extra={"tracked_hosts": policies.tracked_hosts},
    )


async def run_policy_reader(
    reader: PolicyReader,
    *,
    policies: FetchPolicyMap,
    settings: Settings,
    stop: asyncio.Event,
) -> None:
    """Tail the policy stream until ``stop`` is set, folding messages into ``policies``.

    A peer of the consume loop and the retention sweep, and like the sweep it
    **absorbs its own failures**: politeness is not load-bearing for correctness,
    the last-known map stays in memory across an outage, and taking the worker
    down over a stream it only reads would trade a bounded degradation for one.
    A broker that is genuinely gone still surfaces — through the consume loop,
    which has the delivery obligations and the consecutive-failure ceiling.

    Backoff reuses ``REPLICATOR_ERROR_BACKOFF_*`` rather than adding a knob: it
    is the same quantity, a poll cycle that raised.
    """
    consecutive_failures = 0
    while not stop.is_set():
        try:
            messages = await reader.read(count=READ_COUNT, block_ms=settings.read_block_ms)
        except BusMessageAnomaly as exc:
            _skip_poison(reader, exc)
            continue
        # No CancelledError branch: it is a BaseException, so shutdown propagates
        # through the clause below untouched — the same reason process_message
        # can catch broadly without swallowing a cancel.
        except Exception as exc:
            consecutive_failures += 1
            logger.warning(
                "fetch policy poll cycle failed",
                extra={
                    "error": f"{type(exc).__name__}: {exc}",
                    "consecutive_failures": consecutive_failures,
                },
            )
            await park(
                stop,
                error_backoff_seconds(
                    consecutive_failures,
                    base=settings.error_backoff_base_seconds,
                    maximum=settings.error_backoff_max_seconds,
                ),
            )
            continue
        consecutive_failures = 0
        for message in messages:
            policies.apply(message.payload)
        if not messages:
            # Insurance against a client that does not honour `block`, exactly as
            # the consume loop's idle tick is: fakeredis returns from a blocking
            # read immediately, which would busy-spin this loop in tests.
            await park(stop, IDLE_SLEEP_SECONDS)


def _skip_poison(reader: PolicyReader, exc: BusMessageAnomaly) -> None:
    """Advance past a frame that will never decode.

    With no group there is no ``ack`` to move past one, so the cursor has to be
    forced or the next read redelivers the same frame and raises forever. Safe to
    seek straight to it only because reads are ``count=1``: there is no
    well-formed prefix behind it to skip.
    """
    logger.warning(
        "skipping a malformed frame on the fetch policy stream",
        extra={"message_id": exc.message_id, "error": str(exc)},
    )
    reader.seek(exc.message_id)
