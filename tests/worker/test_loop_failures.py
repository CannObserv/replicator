"""Failure classification: which handler failures retry, and which dead-letter.

Three tiers — permanent (DLQ now), transient (retry forever, exempt from the
ceiling), and unclassified (retry against the ceiling). The ceiling itself is
read from XPENDING rather than a side counter, so the tests that cover it also
cover what happens when that row is gone — and, since #103, when the read itself
is refused.
"""

import asyncio

import pytest
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import ContentFetchCommand
from co_core_sync.drivers.blobstore import LocalBlobStore
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoPermissionError, OutOfMemoryError, ResponseError

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.storage.sweeper import BlobUsage
from src.worker.handler import build_handler
from src.worker.loop import FETCH_SPEC, Outcome, _delivery_count, poll_once
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    FakeFetcher,
    collected_reports,
    drive_loop,
    make_command,
    process_one,
)


async def test_a_transient_failure_leaves_the_message_pending(fake_redis, consumer, settings):
    """AC: transient => retry. Redelivery is claim_stale's job, so do not ack."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-transient"))

    async def handler(command: ContentFetchCommand) -> None:
        raise TransientFetchError("broker or origin is having a moment")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    assert not await fake_redis.exists(FETCH_SPEC.dedupe_key("cmd-transient"))


async def test_a_redis_connection_error_counts_as_transient(fake_redis, consumer, settings):
    """redis-py's error types are disjoint from the builtins — both are listed."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-redis-down"))

    async def handler(command: ContentFetchCommand) -> None:
        raise RedisConnectionError("connection refused")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]

    assert await process_one(fake_redis, consumer, settings, message, handler) is Outcome.RETRY


async def test_a_flapping_memory_cap_never_burns_the_delivery_counter(
    fake_redis, consumer, settings, monkeypatch
):
    """#20: a capped broker's OOM is transient, so the ceiling is never consulted.

    ``OutOfMemoryError`` subclasses ``ResponseError``, so before #20 it fell to
    ``_handle_unclassified``. ``times_delivered`` only ever advances, so an
    incident that clears would leave the command with its 5-attempt grace already
    spent — the *next* unrelated handler bug dead-letters on its first failure.

    ``_delivery_count`` is patched to a landmine rather than asserted on after the
    fact: the counter is the broker's, so "was not consulted" is the only
    observable form of "was not spent".
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-oom"))

    async def landmine(*args, **kwargs):
        raise AssertionError("a transient failure must not read the delivery counter")

    monkeypatch.setattr("src.worker.loop._delivery_count", landmine)

    async def handler(command: ContentFetchCommand) -> None:
        raise OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'.")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    assert not await fake_redis.exists(FETCH_SPEC.dedupe_key("cmd-oom"))


async def test_an_oom_at_the_ceiling_does_not_close_a_command_whose_bytes_stored(
    fake_redis, consumer, settings
):
    """#20, the clearing edge: no ``fetch_failed`` for a blob sitting on disk.

    Store-then-publish means an OOM on the ``blob_available`` XADD happens with
    the bytes already stored. Classified as unclassified at attempt >= the
    ceiling, the DLQ write succeeds the moment memory frees and the issuer is
    told its content will never arrive — about content that exists.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-oom-at-ceiling"))
    strict = settings.model_copy(update={"max_delivery_attempts": 1})
    reports = collected_reports()

    async def handler(command: ContentFetchCommand) -> None:
        raise OutOfMemoryError("OOM command not allowed when used memory > 'maxmemory'.")

    message = (await poll_once(fake_redis, consumer, strict, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, strict, message, handler, reporter=reports)

    assert outcome is Outcome.RETRY
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    assert reports.reports == []


async def test_an_acl_denial_retries_rather_than_dead_lettering(
    fake_redis, consumer, settings, monkeypatch
):
    """#82: a broker-side grant is somebody else's incident, not this command's fault.

    ``NoPermissionError`` is a ``ResponseError`` subclass, so it reaches
    ``_handle_unclassified`` exactly as ``OutOfMemoryError`` did before #20 — and
    the consequence is worse than a burnt counter. A mistyped rule at
    CannObserv/broker#2's ACL cutover would retry to the ceiling and then close a
    *valid* command with a terminal ``fetch_failed(handler_error)``, telling the
    issuer its bytes are never coming about a fault an operator fixes with one
    ``ACL SETUSER``. Bytes already stored become orphans no fact references.

    The landmine is ``test_a_flapping_memory_cap_never_burns_the_delivery_counter``'s,
    for its reason: the counter belongs to the broker, so "was not consulted" is
    the only observable form of "was not spent".

    Archiver classified ``NOPERM`` transient in CannObserv/archiver#193 Phase 1;
    watcher's loops classify by nothing and back off already. Replicator's loop
    classifies by type, which is why it is the participant that needed this.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-noperm"))

    async def landmine(*args, **kwargs):
        raise AssertionError("an ACL denial must not read the delivery counter")

    monkeypatch.setattr("src.worker.loop._delivery_count", landmine)
    reports = collected_reports()

    async def handler(command: ContentFetchCommand) -> None:
        raise NoPermissionError(
            "this user has no permissions to access one of the keys used as arguments"
        )

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler, reporter=reports)

    assert outcome is Outcome.RETRY
    assert (await fake_redis.xpending(TOPIC, GROUP))["pending"] == 1
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    # No fact either: a non-terminal failure closes nothing, so the issuer's
    # reaper (MUST-6) is what bounds the wait rather than a wrong terminal fact.
    assert reports.reports == []
    assert not await fake_redis.exists(FETCH_SPEC.dedupe_key("cmd-noperm"))


async def test_a_permanent_failure_is_dead_lettered(fake_redis, consumer, settings):
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-permanent"))

    async def handler(command: ContentFetchCommand) -> None:
        raise PermanentFetchError(
            "this url will never be fetchable", reason=FailureReason.NOT_FETCHABLE
        )

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 0


async def test_an_unclassified_failure_retries_below_the_ceiling(fake_redis, consumer, settings):
    """A handler bug must not discard a valid command on its first failure."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-bug"))

    async def handler(command: ContentFetchCommand) -> None:
        raise AttributeError("NoneType has no attribute 'content'")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0


async def test_an_unclassified_failure_dead_letters_at_the_ceiling(fake_redis, consumer, settings):
    """The ceiling is read from XPENDING's delivery counter, not a side counter."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-persistent-bug"))
    strict = settings.model_copy(update={"max_delivery_attempts": 1})

    async def handler(command: ContentFetchCommand) -> None:
        raise AttributeError("still broken")

    message = (await poll_once(fake_redis, consumer, strict, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, strict, message, handler)

    assert outcome is Outcome.DEAD_LETTERED
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 1


async def test_transient_failures_are_exempt_from_the_ceiling(fake_redis, consumer, settings):
    """A long outage must never drop a valid command (archiver#107, CR #2)."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-outage"))
    strict = settings.model_copy(update={"max_delivery_attempts": 1})

    async def handler(command: ContentFetchCommand) -> None:
        raise TransientFetchError("still down")

    message = (await poll_once(fake_redis, consumer, strict, group=GROUP))[0]

    assert await process_one(fake_redis, consumer, strict, message, handler) is Outcome.RETRY
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0


async def test_cancellation_is_never_classified(fake_redis, consumer, settings):
    """Shutdown is not a message failure — CancelledError must propagate."""
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-cancelled"))

    async def handler(command: ContentFetchCommand) -> None:
        raise asyncio.CancelledError

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    with pytest.raises(asyncio.CancelledError):
        await process_one(fake_redis, consumer, settings, message, handler)

    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    pending = await fake_redis.xpending(TOPIC, GROUP)
    assert pending["pending"] == 1


async def test_a_missing_pending_row_is_logged_not_silently_retried(
    fake_redis, consumer, settings, caplog
):
    """CR #5: no PEL row means the ceiling cannot be read — say so.

    The entry leaving the PEL underneath us is the reachable case; a *wrong*
    group is not, since Redis answers XPENDING for an unknown group with NOGROUP
    rather than an empty result.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-no-row"))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await consumer.ack(message.message_id)  # gone from the PEL

    with caplog.at_level("WARNING"):
        attempts = await _delivery_count(fake_redis, message, group=GROUP)

    assert attempts == 1
    assert "no pending entry" in caplog.text


async def test_the_missing_pending_row_warning_is_undamped(fake_redis, consumer, settings, caplog):
    """CR #16: one warning per occurrence, unlike run_loop's every-Nth logging.

    Safe today because retries are gated by claim_min_idle_ms, so occurrences
    are minutes apart. Pinned so that dropping that gate — a much faster reclaim
    cadence — shows up here as a failing test rather than as journal flood.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-no-row-twice"))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    await consumer.ack(message.message_id)

    with caplog.at_level("WARNING"):
        await _delivery_count(fake_redis, message, group=GROUP)
        await _delivery_count(fake_redis, message, group=GROUP)
        await _delivery_count(fake_redis, message, group=GROUP)

    warnings = [r for r in caplog.records if "no pending entry" in r.getMessage()]
    assert len(warnings) == 3


# The ways the count's own read has been refused or lost. The first is #103's:
# the live broker denied this credential XPENDING until broker#39.
UNREADABLE_COUNT_ERRORS = [
    pytest.param(
        NoPermissionError("this user has no permissions to run the 'xpending' command"),
        id="acl-denied",
    ),
    pytest.param(RedisConnectionError("connection reset by peer"), id="broker-gone"),
    pytest.param(ResponseError("NOGROUP No such key or consumer group"), id="refused"),
]


@pytest.mark.parametrize("count_error", UNREADABLE_COUNT_ERRORS)
async def test_an_unreadable_delivery_count_retries_the_message_rather_than_failing_the_cycle(
    fake_redis, consumer, settings, monkeypatch, caplog, count_error
):
    """#103: the count failing must not escape the message it was counting.

    ``_delivery_count`` runs inside ``process_message``'s ``except Exception`` arm,
    after the handler has failed, so an error there escaped as a *cycle* failure:
    ``run_loop`` blamed the broker, backed off, and the entry came back to fail the
    same way forever, with the handler's own error surviving only as chained
    context. Retried rather than dead-lettered, for #82's reason: an unreadable
    count says nothing about the command, and a wrong terminal fact is the one
    outcome nothing downstream can repair.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-uncountable"))

    async def unreadable(*args, **kwargs):
        raise count_error

    monkeypatch.setattr(fake_redis, "xpending_range", unreadable)
    reports = collected_reports()

    async def handler(command: ContentFetchCommand) -> None:
        raise AttributeError("NoneType has no attribute 'content'")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    with caplog.at_level("ERROR", logger="src.worker.loop"):
        outcome = await process_one(
            fake_redis, consumer, settings, message, handler, reporter=reports
        )

    assert outcome is Outcome.RETRY
    assert (await fake_redis.xpending(TOPIC, GROUP))["pending"] == 1
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0
    assert reports.reports == []
    # One line that names both failures, the handler's first: that is the bug an
    # operator is looking for, and the count is why it is not closing.
    (record,) = [r for r in caplog.records if "delivery count" in r.getMessage()]
    assert record.levelname == "ERROR"
    assert record.error == "AttributeError: NoneType has no attribute 'content'"
    assert record.count_error == f"{type(count_error).__name__}: {count_error}"
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], AttributeError)


async def test_an_unreadable_delivery_count_never_fails_a_poll_cycle(
    fake_redis, consumer, settings, monkeypatch, caplog
):
    """#103's loud variant: a handler failing everything must not exit the worker.

    Before the fix every cycle that reached the ceiling's read failed, so a deploy
    regression failing every command climbed ``REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES``
    and exited into a systemd restart loop. Now each cycle completes, and the run
    ends on the attempt budget rather than on an exit.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-every-time"))
    eager = settings.model_copy(
        update={"claim_min_idle_ms": 0, "max_consecutive_cycle_failures": 2}
    )

    async def denied(*args, **kwargs):
        raise NoPermissionError("this user has no permissions to run the 'xpending' command")

    monkeypatch.setattr(fake_redis, "xpending_range", denied)
    stop = asyncio.Event()
    attempts = 0

    async def handler(command: ContentFetchCommand) -> None:
        nonlocal attempts
        attempts += 1
        if attempts >= 4:
            stop.set()
        raise AttributeError("still broken")

    with caplog.at_level("ERROR", logger="src.worker.loop"):
        await drive_loop(fake_redis, consumer, eager, handler, stop, deadline=2.0)

    assert attempts == 4
    assert not [r for r in caplog.records if r.getMessage().startswith("poll cycle")]


async def test_a_command_blocked_by_the_blob_ceiling_stays_pending(fake_redis, consumer, settings):
    """Backpressure only works if the command survives to be retried.

    The byte path refuses to fetch once the blob tree is over its ceiling. That
    refusal is transient by design — dead-lettering instead would discard work
    over a condition a later sweep clears, and the DLQ would fill with perfectly
    good commands during any period of disk pressure.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-over-ceiling"))
    store = LocalBlobStore(settings.blob_dir)
    usage = BlobUsage()
    usage.observe(settings.blob_max_total_bytes)
    fetcher = FakeFetcher()
    handler = build_handler(
        fetcher=fetcher, store=store, client=fake_redis, settings=settings, usage=usage
    )

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    assert fetcher.urls == []
    assert await fake_redis.xlen(dlq_name(TOPIC)) == 0


async def test_the_retry_warning_reports_how_long_the_handler_held_the_loop(
    fake_redis, consumer, settings, caplog
):
    """The hold that can actually breach a threshold is a slow *failure* (CR 2).

    ``duration_ms`` on the replicate success line (#96) answers "how long does a
    handler that worked take". It is the transient arm that has no bound: the
    entry stays pending, the classes here are exempt from the delivery ceiling,
    and a 120-second write timeout that keeps timing out holds this loop for two
    minutes at a time, forever. Timed at this seam rather than in either handler
    so both command streams report it from one place, and so the number is the
    loop's own window rather than a handler's account of itself.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-slow-transient"))

    async def handler(command: ContentFetchCommand) -> None:
        await asyncio.sleep(0.05)
        raise TransientFetchError("the provider accepted the connection and then stalled")

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    with caplog.at_level("WARNING", logger="src.worker.loop"):
        outcome = await process_one(fake_redis, consumer, settings, message, handler)

    assert outcome is Outcome.RETRY
    (record,) = [r for r in caplog.records if r.message.startswith("transient failure")]
    assert 50 <= record.duration_ms < 5_000
