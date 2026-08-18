"""The command consume path: poll -> dispatch -> ack.

Split from ``main`` so each outcome is unit-testable without driving the loop:
``process_message`` decides the fate of one message, ``poll_once`` sources them,
and ``run_loop`` owns only cadence and shutdown.

The work itself lives behind the ``Handler`` seam — for ``content.fetch`` that is
the byte path in ``src.worker.handler`` (fetch, fingerprint, temp-store,
``blob_available``). This module stays ignorant of what a handler does, and
decides only what its success or failure means.

**Three fates, not two (#17).** Transient ⇒ retry, no fact; permanent ⇒ fact,
then dead-letter; and *completed without bytes* ⇒ fact, then ack, and no DLQ
entry at all. The third is selected by the exception's **type**, exactly as the
other two are — see ``CompletedWithoutBlobError`` for why branching on the
``reason`` token instead would make a wire string load-bearing here.

**One loop, N command streams (#29).** Everything here is the same decision for
every command stream: read one at a time, refuse a foreign payload, branch on
``schema_version`` before destructuring, dedupe on ``command_id``, ack after the
handler, retry the transient and dead-letter the deterministic, and publish the
fact before whatever closes the message. What differs per stream — which payload type
is its command, what its dedupe keys are namespaced under, what to call it in the
journal, and how to build its failure report — is injected as a ``CommandSpec``.
So ``content.replicate`` is another ``run_loop`` with another spec, not a second
copy of this file with the four differences edited in and the other nine
invariants free to drift.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from co_core.effects.bus import BusMessage
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.exceptions import BusMessageAnomaly
from co_core.pure.models.changes import ContentFetchCommand, ContentReplicateCommand
from co_core_aio.bus import AsyncBusConsumer
from redis.asyncio import Redis
from redis.exceptions import BusyLoadingError, OutOfMemoryError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.core.config import Settings
from src.core.errors import CompletedWithoutBlobError, FailureReason, PermanentError, TransientError
from src.core.logging import get_logger

logger = get_logger(__name__)

# The only command schema this worker understands. co-core's model validates
# any integer here (schema_version is a plain int, not a Literal), so an
# unrecognized version is ours to catch — branch before destructuring.
SUPPORTED_SCHEMA_VERSION = 1

# Namespace for the command dedupe keys. Redis, not an in-memory set: the point
# is to survive the restart that redelivery follows.
#
# **Per stream, not per worker.** Each ``CommandSpec`` appends its own segment,
# so a ``content.replicate`` command can never dedupe against a ``content.fetch``
# one (#29). Issuer-assigned ids make a collision unlikely rather than
# impossible, and the failure it would cause is the worst shape available: the
# second command acks having done nothing, silently, which is MUST-1's failure
# mode reached from a direction MUST-1 does not describe — the same hazard the
# blank-``command_id`` guard below already refuses.
DEDUPE_KEY_PREFIX = "replicator:cmd:"

# Handler failures that are *transient* (broker or origin down / slow / loading)
# and must retry indefinitely — EXEMPT from the delivery ceiling, so a long-but-
# genuine outage can never silently drop a valid command. Mirrors archiver's
# publisher tuple; redis-py's error types are disjoint from the builtins, so
# both are listed.
#
# NOTE (co-core coupling): this gate assumes co-core-aio propagates the
# underlying redis exception types *unwrapped* (its current behavior). If it
# ever wraps them in its own error type, a real outage would fall through to the
# ceiling and the cliff reopens — this tuple must then track that wrapper.
_TRANSIENT_ERRORS: tuple[type[Exception], ...] = (
    TransientError,
    ConnectionError,  # builtin
    TimeoutError,  # builtin (asyncio.TimeoutError since 3.11)
    RedisConnectionError,
    RedisTimeoutError,
    BusyLoadingError,
    # A capped broker refusing a write (#20, archiver#129). Listed explicitly
    # because ``OutOfMemoryError`` subclasses ``ResponseError`` — the type that
    # otherwise means "this command will never work" — and only this one member of
    # that family is someone else's incident rather than our bug. The command is
    # valid, so it must be exempt from the delivery ceiling: the PEL is already the
    # durable record of intent, ``claim_stale`` re-runs it, and content-addressed
    # storage makes the re-run a no-op.
    #
    # Not cosmetic in either direction. A *flapping* cap classified as
    # unclassified burns ``times_delivered`` — which only ever advances — without
    # ever completing the DLQ write, so after the incident any later unclassified
    # failure dead-letters immediately with none of the grace the ceiling exists to
    # give. And on the *clearing* edge, a command refused at attempt >= the ceiling
    # whose DLQ write then succeeds as memory frees is closed with
    # ``fetch_failed(handler_error)`` for bytes that stored fine and are sitting on
    # disk — store-then-publish means the OOM lands after the write.
    OutOfMemoryError,
)

# Bound on consecutive poison frames stepped over before a reader pauses.
#
# Here: claim_stale restarts at 0-0 on every call, so each poison entry must be
# routed away before the next claim can reach a good message, and the bound keeps
# a pathological PEL from starving the read path within a single tick.
#
# Also imported by src/worker/policy.py, whose groupless reader skips by forcing
# the cursor rather than by dead-lettering (#19). Same shape of hazard — skipping
# is cheap enough per frame to hide that an unbounded run of them is a hot loop
# against the broker — so it is one constant rather than two that drift apart.
MAX_POISON_SKIPS = 10

# The consume-path handler seam. Raising signals failure; the loop — not the
# handler — decides whether that means retry or dead-letter.
type Handler[C: Command] = Callable[[C], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class FailureReport:
    """What the loop knows about a command it is closing without a blob (#9).

    Built only at the moments ``process_message`` gives up, so a report **is** a
    closure — which is why there is no ``terminal`` field to set. Replicator
    emits no non-terminal fact today (#9 §3, deferred: ``content.blobs`` is
    broadcast and nothing trims it, so a transient fact per reclaim during an
    origin outage is unbounded growth on a stream nobody prunes). The reporter
    stamps ``terminal=True`` accordingly.

    ``reason`` comes from the handler where the handler knows it
    (``PermanentFetchError.reason``) and from the loop where only the loop does —
    an unrecognized ``schema_version``, a foreign payload, the delivery ceiling.

    ``info_source_id`` is carried, never consulted (#28). It is required rather
    than defaulted because co-core requires it on the fact: a report built
    without one could not be published at all, so a default here would only move
    the failure from this constructor to the reporter's.
    """

    command_id: str
    url: str
    info_source_id: str
    reason: FailureReason
    status_code: int | None = None
    attempts: int | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class ReplicateFailureReport:
    """What the loop knows about a replicate command it is closing (#29).

    Defined here beside ``FailureReport`` rather than with its publisher, for the
    reason that one is: the loop builds these and the reporter consumes them, so
    the dependency runs one way and ``src.worker.replicate_reporter`` imports
    from this module without a cycle.

    **No ``status_code``.** ``ReplicationFailedEvent`` models none — no documented
    refusal reports a provider's HTTP status as its own outcome, since
    ``destination_conflict`` consumes the 412 rather than reporting it. The loop
    passes one anyway, because fetch has them, and the spec's builder drops it.

    ``info_item_rep_spec_id`` and ``source_revision_id`` are carried and never
    consulted, exactly as ``info_source_id`` is. They are required rather than
    defaulted because co-core requires them on the fact.
    """

    command_id: str
    info_item_rep_spec_id: str
    source_revision_id: str
    info_source_id: str
    reason: str
    attempts: int | None = None
    detail: str | None = None


class Command(Protocol):
    """The only two fields the loop reads off a command, whatever stream it came from.

    Everything else — a URL, a destination, a blob reference — is the handler's
    business. Keeping this pair as the whole of the loop's knowledge is what
    makes "which stream is this?" a property of the injected ``CommandSpec``
    rather than a branch in here.
    """

    @property
    def command_id(self) -> str: ...

    @property
    def schema_version(self) -> int: ...


class Report(Protocol):
    """The one thing the loop requires of any command stream's failure report.

    ``_close`` refuses a report with no correlator, and that guard has to hold
    for every stream — so ``command_id`` is the whole of the shared shape.
    Everything else a fact carries (``url`` and ``status_code`` for fetch,
    ``info_item_rep_spec_id`` and ``source_revision_id`` for replicate) belongs
    to that stream's own report type and its own reporter.

    Deliberately **not** one dataclass with every stream's fields optional: the
    replicate fact models no ``status_code`` at all, so a shared superset would
    put a field on the wire path that one of its two producers can never fill,
    and "which of these are meaningful here" would be knowledge held nowhere.

    ``reason`` rides along because the correlator guard logs it, and a refusal
    the journal cannot explain is the failure that guard exists to make visible.
    Typed ``str`` rather than ``FailureReason``: co-core types the field that way
    on both facts precisely so each producer owns its own token vocabulary, and
    ``FailureReason`` is a ``StrEnum``, so the fetch report satisfies this as it
    stands.
    """

    @property
    def command_id(self) -> str: ...

    @property
    def reason(self) -> str: ...


# The failure-fact seam, parallel to ``Handler`` and injected the same way. This
# module stays ignorant of ``content.blobs`` and of how a fact is published —
# ``src.worker.reporter`` owns that, exactly as ``src.worker.handler`` owns the
# byte path. The alternative (publishing inline here) would thread a topic
# through poll_once / claim_once / dead_letter_anomaly and cost a live-broker
# test its scratch stream.
type FailureReporter[R: Report] = Callable[[R], Awaitable[None]]


class ReportBuilder[C: Command, R: Report](Protocol):
    """How one command stream turns a command plus a cause into its own report.

    The loop knows *that* it is closing a command and *why*; it does not know
    what that stream's failure fact is shaped like. Passing the builder in is
    what lets ``process_message`` close a command on a stream whose fact it has
    never heard of — and keeps the fetch report's ``url`` from becoming an
    ``Optional`` that replicate would always leave empty.

    ``reason`` is ``str``, not ``FailureReason`` (CR #5). The token vocabulary is
    **producer-owned per stream** — that is the whole premise this seam rests on,
    and co-core types the field ``str`` on both facts for exactly that reason.
    ``FailureReason`` holds fetch's tokens plus the two the *loop* owns
    (``unsupported_schema_version``, ``handler_error``), which every stream emits;
    replicate's (``alias_unknown``, ``invalid_destination``, …) live with
    replicate. Annotating this ``FailureReason`` would have forced the second
    stream either to widen the fetch enum or to fight the type. ``StrEnum``
    members satisfy ``str``, so the loop's own two pass unchanged.
    """

    def __call__(
        self,
        command: C,
        *,
        reason: str,
        status_code: int | None = None,
        attempts: int | None = None,
        detail: str | None = None,
    ) -> R: ...


@dataclass(frozen=True, slots=True)
class CommandSpec[C: Command, R: Report]:
    """Everything the loop needs to know about one command stream.

    Bundled into one object rather than threaded as four parameters because they
    have to agree with each other: a spec whose ``command_type`` and
    ``build_report`` disagreed would decode a command and then fail to describe
    it, at the exact moment it is trying to report a failure.

    ``label`` reaches the journal only. ``dedupe_segment`` reaches Redis, so it
    is the one field where a collision is a correctness bug rather than a
    confusing log line — see ``DEDUPE_KEY_PREFIX``. Two specs sharing a segment
    would dedupe each other's commands, so ``test_loop_spec.py`` asserts they are
    distinct; nothing in the type system can (CR #8).

    ``describe`` names a frame in the journal when the correlator cannot. It is
    per stream because what identifies a command is: fetch has a URL, replicate
    has a destination, and the loop knows neither (CR #1).
    """

    command_type: type[C]
    label: str
    dedupe_segment: str
    build_report: ReportBuilder[C, R]
    describe: Callable[[C], dict[str, object]]

    def dedupe_key(self, command_id: str) -> str:
        """The Redis key under which this stream remembers a handled command."""
        return f"{DEDUPE_KEY_PREFIX}{self.dedupe_segment}:{command_id}"


FETCH_SPEC: CommandSpec[ContentFetchCommand, FailureReport] = CommandSpec(
    command_type=ContentFetchCommand,
    label=streams.CONTENT_FETCH,
    dedupe_segment="fetch",
    build_report=lambda command, **cause: FailureReport(
        command_id=command.command_id,
        url=command.url,
        # Copied across, never read (#28).
        info_source_id=command.info_source_id,
        **cause,
    ),
    describe=lambda command: {"url": command.url},
)

# The second command stream (#29), beside FETCH_SPEC so the uniqueness assertion
# in ``test_loop_spec.py`` sees both.
REPLICATE_SPEC: CommandSpec[ContentReplicateCommand, ReplicateFailureReport] = CommandSpec(
    command_type=ContentReplicateCommand,
    label=streams.CONTENT_REPLICATE,
    dedupe_segment="replicate",
    # ``status_code`` is bound and discarded: ReplicationFailedEvent models none.
    # The loop passes every cause it has and each stream decides what its own
    # fact can say.
    build_report=lambda command, *, status_code=None, **cause: ReplicateFailureReport(
        command_id=command.command_id,
        # Copied across, never read (#28, #29).
        info_item_rep_spec_id=command.info_item_rep_spec_id,
        source_revision_id=command.source_revision_id,
        info_source_id=command.info_source_id,
        **cause,
    ),
    # A replicate command has no URL; what names it is where it was going.
    describe=lambda command: {"destination": command.destination},
)


# How long an idle poll parks before looking again. On a live broker the
# blocking XREADGROUP is the real wait and this adds ~50ms per idle tick; it is
# kept as insurance against a client that does not honour `block` — fakeredis
# returns from a blocking read immediately, which would otherwise busy-spin the
# loop in tests. Waiting on the stop event rather than sleeping keeps shutdown
# prompt.
IDLE_SLEEP_SECONDS = 0.05

# Outage handling is operator-tunable and lives in Settings
# (REPLICATOR_ERROR_BACKOFF_BASE_SECONDS / _MAX_SECONDS,
# REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES) — these two are shape, not policy:
# the exponent cap keeps 2**shift from overflowing before the max is applied,
# and the log interval keeps a long outage from flooding the journal at one
# line per retry. Mirrors archiver's drain-loop backoff (archiver#107, CR #13).
ERROR_BACKOFF_MAX_SHIFT = 5
ERROR_LOG_EVERY = 15


def _report_or_none[C: Command, R: Report](
    spec: CommandSpec[C, R],
    command: C,
    *,
    reason: str,
    status_code: int | None = None,
    attempts: int | None = None,
    detail: str | None = None,
) -> R | None:
    """Build this stream's failure report, degrading to no-fact if the spec is broken.

    Unguarded, a raising ``build_report`` is the worst failure this module has
    (CR #2). It is called from inside ``_handle_unclassified`` too — which is
    where the delivery ceiling lives — so the raise escapes ``process_message``
    before anything is dead-lettered, leaves the entry in the PEL, and comes
    straight back via ``claim_stale``. The ceiling can never fire, because the
    ceiling is the code that is failing. One programming error in one spec
    becomes a permanent jam on that stream with no DLQ entry to triage from, and
    `run_loop` can only back off and eventually exit into the same jam.

    Degrading is the right trade rather than a defensive reflex: the frame still
    dead-letters, which is the outcome that unblocks the stream, and a command
    closing without a fact is a condition contract MUST-6 already requires
    issuers to keep a reaper for. The alternative — a correct-looking guard that
    re-raises — buys nothing the caller could act on.

    The cause is spelled out rather than forwarded as ``**kwargs`` so this
    signature and ``ReportBuilder``'s are checkable against each other. A
    ``**kwargs`` passthrough type-checks as ``object`` and would have hidden
    exactly the kind of mismatch this function exists to survive.
    """
    # ``Exception``, never ``BaseException`` — the same line the handler ``try``
    # in ``process_message`` holds (CR #11). ``asyncio.CancelledError`` is a
    # ``BaseException``, so shutdown propagates through here untouched; widening
    # this would swallow a SIGTERM mid-dead-letter and hang the wind-down inside
    # a function whose whole purpose is to never be the thing that blocks.
    try:
        return spec.build_report(
            command, reason=reason, status_code=status_code, attempts=attempts, detail=detail
        )
    except Exception as exc:
        logger.error(
            "could not build a failure report",
            extra={
                "stream": spec.label,
                "error": f"{type(exc).__name__}: {exc}",
                "detail": "the frame still dead-letters; the issuer's reaper is the backstop",
            },
            exc_info=exc,
        )
        return None


class Outcome(StrEnum):
    """What ``process_message`` did with one message.

    ``COMPLETED_WITHOUT_BLOB`` is the third fate (#17): a fact is published, the
    message is acked, and nothing reaches ``<topic>.dlq``.

    **Nothing aggregates these today** — ``run_loop`` discards what
    ``process_message`` returns, so the operator-visible distinction is the
    journal line each path emits, and these members are the loop's own vocabulary
    for its tests (CR #10). Kept distinct from ``ACKED`` anyway, and not as
    bookkeeping: once issuers replay validators a 304 becomes the *common* answer,
    so a counter built on these later would report "commands that produced bytes"
    as the sum of two very different things — wrong in the direction that looks
    healthy. Folding them now would quietly foreclose that.
    """

    ACKED = "acked"
    DEDUPED = "deduped"
    DEAD_LETTERED = "dead_lettered"
    RETRY = "retry"
    COMPLETED_WITHOUT_BLOB = "completed_without_blob"


async def process_message[C: Command, R: Report](
    message: BusMessage,
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    group: str,
    handler: Handler[C],
    settings: Settings,
    reporter: FailureReporter[R],
    spec: CommandSpec[C, R],
) -> Outcome:
    """Dispatch one decoded message and decide its fate.

    Acks only after the handler returns — an unacked message stays in the PEL and
    comes back via ``claim_stale``, which is what makes delivery at-least-once.

    ``group`` is passed rather than re-read from ``settings`` so the PEL this
    consults is provably the one ``consumer`` acks against: co-core keeps the
    consumer's group private, and two independent reads of the same setting can
    drift, in which case the ceiling would query an unrelated PEL and silently
    never trip.

    ``reporter`` is required rather than optional: every path that closes a
    command without a blob must announce it, and a default would make "no fact"
    the outcome of forgetting to wire one — the exact silence #9 exists to end.

    ``spec`` carries the only things here that differ per command stream: which
    payload type is this stream's command, what its dedupe keys are namespaced
    under, what to call it in the journal, and how to build its failure report.
    Everything else in this function — the ordering, the guards, and which
    outcome each one produces — is the same decision for every stream, which is
    why there is one of these rather than two loops (#29).
    """
    command = message.payload
    # from_wire's event_type -> model table is global, so a fact XADDed to the
    # command stream decodes cleanly into the wrong type rather than raising.
    if not isinstance(command, spec.command_type):
        return await _close(
            consumer,
            message.message_id,
            dict(message.fields),
            reason=f"payload is not a {spec.label} command",
            detail={"event_type": command.event_type},
            label=spec.label,
            reporter=reporter,
            # DLQ-only, never announced (CR #1). Most of the payload union has no
            # command_id at all, and the members that do carry *somebody else's* —
            # BlobAvailableEvent's names a command that succeeded, which is why
            # there is a blob for it. A fact keyed on that id would tell an issuer
            # its good bytes will never arrive: a wrong correlation applied
            # silently, strictly worse than the reaper timeout that silence costs.
            report=None,
        )
    if command.schema_version != SUPPORTED_SCHEMA_VERSION:
        return await _close(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="unsupported schema_version",
            detail={"command_id": command.command_id, "schema_version": command.schema_version},
            label=spec.label,
            reporter=reporter,
            # Reading the correlator fields off a version this worker does not
            # support is the destructuring the contract warns issuers about —
            # done knowingly, and pushed into ``spec.build_report`` so each
            # stream names its own v1 baseline. For fetch that is command_id,
            # url and info_source_id; a fact naming none of them could not close
            # anything. Every one is *required* on its command, so a frame that
            # decoded at all has them, and a frame missing one never reaches this
            # branch, having failed ``from_wire`` outright. If a future version
            # moves them, this is where it breaks — and ``_close`` refuses a
            # report with no correlator rather than publishing an empty one.
            report=_report_or_none(
                spec,
                command,
                reason=FailureReason.UNSUPPORTED_SCHEMA_VERSION,
                detail=f"schema_version={command.schema_version}",
            ),
        )
    # A command with no correlator is malformed, and is refused before anything
    # can use it (CR #6). Left to run it would do the work, publish a success fact
    # no issuer can match, and take the empty-id dedupe key — under which every
    # *later* blank-id command is a silent no-op, MUST-1's failure mode reached
    # from a direction MUST-1 does not describe.
    #
    # After the schema_version branch, not before: reading command_id is
    # destructuring, and the contract's rule is to branch on the version first.
    # That leaves a v2-with-blank-id frame reaching a report site with nothing to
    # key on, which is what the correlator guard in ``_close`` is the backstop for.
    #
    # No fact, for the same reason there is no dedupe key: nothing to name it by.
    if not command.command_id:
        return await _close(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="command_id is blank",
            # The one dead-letter with nothing to correlate on, which is what
            # makes the stream's own identifier load-bearing here (CR #1). Not
            # ``spec.label`` — ``_dead_letter`` already puts that on the line,
            # and repeating it would displace the identifier rather than add one.
            detail=spec.describe(command),
            label=spec.label,
            reporter=reporter,
            report=None,
        )

    dedupe_key = spec.dedupe_key(command.command_id)
    if await client.exists(dedupe_key):
        await consumer.ack(message.message_id)
        logger.info(
            "skipped an already-handled command",
            extra={"command_id": command.command_id, "message_id": message.message_id},
        )
        return Outcome.DEDUPED

    # asyncio.CancelledError is a BaseException, so shutdown propagates through
    # these handlers untouched — it is not a message failure.
    try:
        await handler(command)
    except CompletedWithoutBlobError as exc:
        # The third fate (#17): the command is *done*, it just produced no bytes.
        # Caught before the permanent arm only for readability — the two types are
        # siblings, so neither can shadow the other, which is the whole reason
        # this is a type rather than a branch on ``exc.reason``.
        return await _close_without_dlq(
            consumer,
            message.message_id,
            client=client,
            dedupe_key=dedupe_key,
            dedupe_ttl_seconds=settings.dedupe_ttl_seconds,
            label=spec.label,
            reporter=reporter,
            report=_report_or_none(
                spec,
                command,
                reason=exc.reason,
                status_code=exc.status_code,
                detail=str(exc),
            ),
        )
    except PermanentError as exc:
        return await _close(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="handler reported a permanent failure",
            detail={"command_id": command.command_id, "error": str(exc)},
            label=spec.label,
            reporter=reporter,
            # The handler classified this, not the loop: several unrelated
            # conditions (a 4xx, an unfetchable scheme, an oversized body) raise
            # one exception type, and recovering which from str(exc) would be a
            # wire contract resting on a message format.
            report=_report_or_none(
                spec,
                command,
                reason=exc.reason,
                status_code=exc.status_code,
                detail=str(exc),
            ),
        )
    except _TRANSIENT_ERRORS as exc:
        logger.warning(
            "transient failure — leaving the message pending for redelivery",
            extra={
                "command_id": command.command_id,
                "message_id": message.message_id,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        return Outcome.RETRY
    except Exception as exc:
        return await _handle_unclassified(
            message,
            client=client,
            consumer=consumer,
            group=group,
            settings=settings,
            command=command,
            exc=exc,
            reporter=reporter,
            spec=spec,
        )

    # Written *after* the handler, deliberately. Marking first would turn a crash
    # between the mark and a completed handle into permanent loss: redelivery
    # would short-circuit to ack having never done the work. This ordering can
    # only re-run a handler that already succeeded, which content-addressed
    # storage absorbs — the key is a cheap short-circuit, not the correctness
    # mechanism.
    await client.set(dedupe_key, message.message_id, nx=True, ex=settings.dedupe_ttl_seconds)
    await consumer.ack(message.message_id)
    return Outcome.ACKED


async def _handle_unclassified[C: Command, R: Report](
    message: BusMessage,
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    group: str,
    settings: Settings,
    command: C,
    exc: Exception,
    reporter: FailureReporter[R],
    spec: CommandSpec[C, R],
) -> Outcome:
    """Retry a failure the loop could not classify, up to the delivery ceiling.

    Defense in depth, not the primary DLQ route: a handler bug must not discard
    a valid command on its first failure, but it must not spin forever either.

    The only path that reports ``attempts``, because it is the only one where the
    number is *why* the command closed rather than incidental.
    """
    attempts = await _delivery_count(client, message, group=group)
    error = f"{type(exc).__name__}: {exc}"
    if attempts >= settings.max_delivery_attempts:
        return await _close(
            consumer,
            message.message_id,
            dict(message.fields),
            reason="unclassified failure hit the delivery ceiling",
            detail={"command_id": command.command_id, "attempts": attempts, "error": error},
            label=spec.label,
            reporter=reporter,
            report=_report_or_none(
                spec,
                command,
                reason=FailureReason.HANDLER_ERROR,
                attempts=attempts,
                detail=error,
            ),
        )
    logger.warning(
        "unclassified failure — leaving the message pending for redelivery",
        extra={
            "command_id": command.command_id,
            "message_id": message.message_id,
            "attempts": attempts,
            "error": error,
        },
        exc_info=exc,
    )
    return Outcome.RETRY


async def _delivery_count(client: Redis, message: BusMessage, *, group: str) -> int:
    """How many times this entry has been delivered, per the broker's own PEL.

    XPENDING is the source of truth — no side counter to keep, expire, or
    reconcile. Note it only advances on a claim_stale reclaim (a ``>`` read never
    redelivers), which is what makes the ceiling a bound in time.
    """
    entries = await client.xpending_range(
        message.topic, group, min=message.message_id, max=message.message_id, count=1
    )
    if not entries:
        # Nothing to count against: the entry left the PEL underneath us, or the
        # group is not the one it was delivered to. Treat it as a first attempt
        # (retry rather than discard) but say so — a silent 1 here would mean a
        # ceiling that can never trip.
        logger.warning(
            "no pending entry for this message — cannot read its delivery count",
            extra={"message_id": message.message_id, "topic": message.topic, "group": group},
        )
        return 1
    return int(entries[0]["times_delivered"])


async def _close[R: Report](
    consumer: AsyncBusConsumer,
    message_id: str,
    fields: dict[str, str],
    *,
    label: str,
    reason: str,
    detail: dict[str, object] | None = None,
    reporter: FailureReporter[R],
    report: R | None,
) -> Outcome:
    """Announce the failure, then dead-letter it. Fact **before** ack, always.

    Both parameters are required so the ordering cannot be got wrong one call
    site at a time: ``dead_letter`` acks inside itself, so a fact published after
    it is lost outright if the process dies in between, while a fact published
    before it costs at worst a duplicate on redelivery — which contract MUST-4
    already requires issuers to tolerate. Same reasoning as store-then-publish on
    the byte path, one step further along.

    A ``report`` of ``None`` means the row has no fact to announce at all — a
    frame whose payload is not a command, and (via ``dead_letter_anomaly``) one
    that did not decode. Those stay DLQ-only, and contract MUST-6 keeps the
    issuer's reaper as the backstop for them.

    The correlator guard and the swallowed reporter failure live in
    ``_announce``, shared with the one closing path that does *not* dead-letter.
    """
    await _announce(reporter, report, message_id=message_id)
    return await _dead_letter(
        consumer, message_id, fields, label=label, reason=reason, detail=detail
    )


async def _close_without_dlq[R: Report](
    consumer: AsyncBusConsumer,
    message_id: str,
    *,
    client: Redis,
    dedupe_key: str,
    dedupe_ttl_seconds: int,
    label: str,
    reporter: FailureReporter[R],
    report: R | None,
) -> Outcome:
    """Announce a command that completed with no bytes, then ack it. No DLQ (#17).

    **The ordering inverts here, and nothing structural enforces it.** ``_close``
    gets fact-before-ack right for free — ``dead_letter`` acks inside itself, so
    there is only one call to make. This path acks explicitly, which means the
    two statements could be written in either order and only one is correct: a
    fact published after the ack is lost outright if the process dies in between,
    and unlike the dead-letter path there is no DLQ entry left behind to repair
    from. Publish, then key, then ack — and ``_close``'s reasoning about why the
    duplicate on redelivery is the cheap side of that trade (contract MUST-4)
    applies here unchanged.

    **This is the one close that writes the dedupe key**, and it is the reason
    the key is not simply ``_close``'s job too. The other closes *discard* a
    command; this one completes it. Without the key a reclaim after a crash
    re-asks an origin that has just said nothing changed — the politeness cost of
    a request Replicator already knows the answer to. Written before the ack for
    the same reason the fact is, and ``nx=True`` so a redelivery cannot extend a
    window the first delivery opened.

    **Nothing reaches ``<topic>.dlq``.** A successful conditional GET is not
    operator-actionable, and wherever conditional GET is in use it is the
    *common* outcome, so copying each one there would fill the operator's surface
    with routine successes and devalue it — the same argument the fact stream now
    carries as its own cost (see ``FailureReason.NOT_MODIFIED``).

    A ``report`` of ``None`` means this stream's builder is broken; the command
    still closes, and MUST-6's reaper is the backstop, exactly as on the
    dead-letter path.
    """
    await _announce(reporter, report, message_id=message_id)
    await client.set(dedupe_key, message_id, nx=True, ex=dedupe_ttl_seconds)
    await consumer.ack(message_id)
    logger.info(
        "closed a command that completed without a blob",
        extra={
            "stream": label,
            "message_id": message_id,
            "command_id": None if report is None else report.command_id,
            "reason": None if report is None else str(report.reason),
        },
    )
    return Outcome.COMPLETED_WITHOUT_BLOB


async def _announce[R: Report](
    reporter: FailureReporter[R], report: R | None, *, message_id: str
) -> None:
    """Publish one closing fact, and never be the reason a message is stranded.

    Extracted from ``_close`` so the second closing path (``_close_without_dlq``)
    cannot drift from it: the correlator guard and the swallow are what make a
    fact optional and safe, and a copy of them would be one refactor away from
    being only half a copy.

    **A correlator-less report is refused here rather than at the call sites.**
    ``Report.command_id`` is the one field every stream's fact requires and *is*
    the event — ``FetchFailedEvent``'s is the shipped example — so a fact naming
    no command closes nothing and only adds noise to a broadcast stream.
    Enforced at this one choke point so the invariant holds for every report path
    there is and every one added later; it is logged because otherwise a
    malformed command would be indistinguishable in the journal from an ordinary
    close.

    The reporter's own failures are swallowed inside it, but they are caught here
    as well: a reporter that raised would abandon whatever its caller does next —
    the dead-letter, or the ack — and leave the command in the PEL to be
    redelivered forever.
    """
    if report is None:
        return
    if not report.command_id:
        logger.warning(
            "cannot announce this failure — the command carries no command_id",
            extra={"message_id": message_id, "reason": str(report.reason)},
        )
        return
    try:
        await reporter(report)
    except Exception as exc:
        logger.error(
            "failure reporter raised — closing the command anyway",
            extra={
                "command_id": report.command_id,
                "error": f"{type(exc).__name__}: {exc}",
            },
            exc_info=exc,
        )


# Field names ``logging`` will not let an ``extra`` dict carry: every attribute a
# ``LogRecord`` already owns, plus the two the formatter adds later. Derived from
# an actual record rather than hardcoded, so it tracks the interpreter — 3.12
# added ``taskName``, and a list written from memory would have missed it.
_RESERVED_LOG_FIELDS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message",
    "asctime",
}


def _loggable(detail: dict[str, object]) -> tuple[dict[str, object], list[str]]:
    """Split ``detail`` into what a log ``extra`` can carry and what it cannot.

    ``logging.makeRecord`` raises ``KeyError`` on a key a ``LogRecord`` already
    owns, so a ``detail`` naming ``module`` or ``name`` would raise *inside* the
    dead-letter — after the DLQ write, before the log line — leaving a frame that
    is neither acked nor announced. That is CR #2's permanent jam reached through
    a different door, and ``CommandSpec.describe`` is the seam that opens it: it
    is the one place a stream author chooses arbitrary field names (CR #14).

    Filtered rather than renamed, and the dropped names are returned so the
    caller can put them on the record — a silently vanishing diagnostic is what
    CR #1 was about, and this must not reintroduce it one field at a time.
    """
    safe = {key: value for key, value in detail.items() if key not in _RESERVED_LOG_FIELDS}
    dropped = sorted(key for key in detail if key in _RESERVED_LOG_FIELDS)
    return safe, dropped


async def _dead_letter(
    consumer: AsyncBusConsumer,
    message_id: str,
    fields: dict[str, str],
    *,
    label: str,
    reason: str,
    detail: dict[str, object] | None = None,
) -> Outcome:
    """Copy a frame to ``<topic>.dlq``, ack the original, and say why.

    The reason travels *in the entry*, not only in the log line: the DLQ is the
    operator's triage surface, and correlating a stream entry back to a journal
    timestamp to learn which of the five routes sent it there is avoidable work.
    Consumers use ``extra="ignore"`` models, so the added envelope keys cannot
    break a replay tool that re-reads the original ``payload``.

    ``label`` says which stream, and is a parameter because ``AsyncBusConsumer``
    keeps its topic private — so with two command streams sharing this function,
    a hardcoded name would mislabel half the dead-letters in the journal, and
    the operator's first triage question is exactly which stream jammed.
    """
    dlq_id = await consumer.dead_letter(
        message_id,
        {**fields, "dlq_reason": reason, "dlq_original_id": message_id},
    )
    safe, dropped = _loggable(detail or {})
    logger.warning(
        "dead-lettered a frame",
        extra={
            "stream": label,
            "reason": reason,
            "message_id": message_id,
            "dlq_id": dlq_id,
            **safe,
            **({"dropped_detail_keys": dropped} if dropped else {}),
        },
    )
    return Outcome.DEAD_LETTERED


async def dead_letter_anomaly(
    client: Redis,
    consumer: AsyncBusConsumer,
    exc: BusMessageAnomaly,
) -> Outcome:
    """Route a frame that failed to decode at all.

    ``from_wire`` raises from inside ``read``/``claim_stale``, so there is no
    ``BusMessage`` and no field map — the anomaly carries only ``topic`` and
    ``message_id``. ``dead_letter`` XADDs the fields it is given and ``XADD``
    rejects an empty map, so the raw frame is re-read by id; a trimmed or
    ``XDEL``-ed entry (still pending, no longer in the stream) falls back to a
    synthesized record so the message can never get stuck in the PEL.
    """
    raw = await client.xrange(exc.topic, min=exc.message_id, max=exc.message_id)
    if raw:
        fields = {_as_str(k): _as_str(v) for k, v in raw[0][1].items()}
    else:
        fields = {
            "error": str(exc),
            "original_message_id": exc.message_id,
            "original_topic": exc.topic,
        }
    return await _dead_letter(
        consumer,
        exc.message_id,
        fields,
        # The anomaly carries the topic even though the consumer does not expose
        # it, so this route needs no CommandSpec — which is what lets poll_once
        # and claim_once stay stream-agnostic.
        label=exc.topic,
        reason="frame failed to decode",
        detail={"anomaly": type(exc).__name__, "recovered_fields": bool(raw)},
    )


def _as_str(value: bytes | str) -> str:
    """Decode a raw Redis field (the client is not in decode_responses mode)."""
    return value.decode() if isinstance(value, bytes) else value


async def claim_once(
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
    *,
    group: str,
) -> list[BusMessage]:
    """Reclaim one message abandoned by a crashed (or transiently failing) worker.

    ``count=1`` for the batch-poison reason plus a sharper one: XAUTOCLAIM
    transfers ownership and resets the idle clock on every entry it returns
    *before* co-core decodes them, so one poison frame in a batch of ten would
    strand nine good messages with their timers restarted.

    A poison entry would otherwise jam recovery permanently — ``claim_stale``
    restarts at ``0-0`` on every call, so the same bad frame is re-claimed and
    re-raised forever. Routing it to the DLQ and re-claiming is what lets the
    entries behind it through.
    """
    for _ in range(MAX_POISON_SKIPS):
        try:
            return await consumer.claim_stale(
                min_idle_ms=settings.claim_min_idle_ms, count=1, start_id="0-0"
            )
        except BusMessageAnomaly as exc:
            await dead_letter_anomaly(client, consumer, exc)
    logger.warning(
        "recovery pass hit the poison-skip bound",
        extra={"skipped": MAX_POISON_SKIPS, "group": group},
    )
    return []


async def poll_once(
    client: Redis,
    consumer: AsyncBusConsumer,
    settings: Settings,
    *,
    group: str,
) -> list[BusMessage]:
    """Source the next message, dead-lettering anything that will not decode.

    ``count=1`` throughout: ``read`` decodes with the fail-loud ``from_wire``, so
    a poison frame in a ``count>1`` batch raises before the well-formed messages
    in that batch are returned — reading one at a time keeps a single bad frame
    from swallowing good ones. An empty list means "nothing to do this tick",
    whether the stream was idle or a poison frame was just routed away.

    Recovery comes first: work already delivered to a worker that died (or that
    failed transiently here) is older than anything unread, and leaving it while
    new messages flow would let it age indefinitely.
    """
    reclaimed = await claim_once(client, consumer, settings, group=group)
    if reclaimed:
        return reclaimed
    try:
        return await consumer.read(count=1, block_ms=settings.read_block_ms)
    except BusMessageAnomaly as exc:
        await dead_letter_anomaly(client, consumer, exc)
        return []


async def process_batch[C: Command, R: Report](
    messages: list[BusMessage],
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    group: str,
    settings: Settings,
    handler: Handler[C],
    reporter: FailureReporter[R],
    spec: CommandSpec[C, R],
    stop: asyncio.Event,
) -> None:
    """Dispatch a polled batch, stopping between messages once asked to.

    The stop check is between messages, never inside one: a SIGTERM arriving
    mid-handler finishes that message and acks it, so ``systemctl restart`` does
    not strand work in the pending entries list. Polling is ``count=1`` today, so
    the loop body runs once — the check is here so that raising the count later
    cannot silently extend shutdown latency.
    """
    for message in messages:
        await process_message(
            message,
            client=client,
            consumer=consumer,
            group=group,
            handler=handler,
            settings=settings,
            reporter=reporter,
            spec=spec,
        )
        if stop.is_set():
            break


async def run_loop[C: Command, R: Report](
    *,
    client: Redis,
    consumer: AsyncBusConsumer,
    group: str,
    settings: Settings,
    handler: Handler[C],
    reporter: FailureReporter[R],
    spec: CommandSpec[C, R],
    stop: asyncio.Event,
) -> None:
    """Poll and dispatch until ``stop`` is set.

    A failing *message* is decided by ``process_message``; a failing *cycle* —
    the broker refusing a read, an ack, or a DLQ write — is this loop's problem.
    It backs off and retries rather than propagating, because the alternative is
    the process dying on every Redis restart and leaning on systemd to bring it
    back through the full ExecStartPre chain. ``asyncio.CancelledError`` is a
    ``BaseException`` and still propagates: shutdown is not a cycle failure.
    """
    consecutive_failures = 0
    while not stop.is_set():
        # Bound before the try: the read below is only valid by the except
        # branch always continuing, and a future branch that falls through
        # would hit UnboundLocalError on a path that runs only during an outage.
        messages: list[BusMessage] = []
        try:
            messages = await poll_once(client, consumer, settings, group=group)
            await process_batch(
                messages,
                client=client,
                consumer=consumer,
                group=group,
                settings=settings,
                handler=handler,
                reporter=reporter,
                spec=spec,
                stop=stop,
            )
        except Exception as exc:
            consecutive_failures += 1
            if consecutive_failures >= settings.max_consecutive_cycle_failures:
                logger.error(
                    "poll cycle has failed continuously — exiting so the unit restarts",
                    extra={
                        "consecutive_failures": consecutive_failures,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    exc_info=exc,
                )
                raise
            if consecutive_failures == 1 or consecutive_failures % ERROR_LOG_EVERY == 0:
                logger.error(
                    "poll cycle failed — backing off",
                    extra={
                        "consecutive_failures": consecutive_failures,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    exc_info=exc,
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

        if consecutive_failures:
            logger.info("poll cycle recovered", extra={"after_failures": consecutive_failures})
            consecutive_failures = 0
        if not messages:
            await park(stop, IDLE_SLEEP_SECONDS)


def error_backoff_seconds(consecutive_failures: int, *, base: float, maximum: float) -> float:
    """Exponential backoff (``base * 2**(n-1)``) capped at ``maximum``.

    The exponent is clamped so the intermediate cannot overflow before the cap
    is applied — a loop that has been failing for hours must not compute
    ``2**10000``.
    """
    if consecutive_failures <= 1:
        return base
    shift = min(consecutive_failures - 1, ERROR_BACKOFF_MAX_SHIFT)
    return min(base * 2**shift, maximum)


async def park(stop: asyncio.Event, seconds: float) -> None:
    """Wait ``seconds``, returning early once ``stop`` is set.

    Shared with the retention task, which parks on a cadence measured in minutes
    and must not hold up a SIGTERM for one.
    """
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except TimeoutError:
        pass
