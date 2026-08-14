"""Bus consumer entry point — the primary Replicator process.

Consumes ``content.fetch`` commands from the Redis change bus. See
``docs/plans/2026-06-25-replicator-mvp-design.md``.

This module is wiring: client lifetime, consumer group, signal handling. The
consume path itself lives in ``src.worker.loop``, and the fetch → fingerprint →
temp-store → ``blob_available`` work behind that module's handler seam lives in
``src.worker.handler``.

Bus clients are **injection-only** — the co-core driver never opens or closes the
``redis.asyncio.Redis`` client, so this module owns one for the worker lifetime
and closes it on the way out.
"""

import asyncio
import signal
import stat
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

from co_core.pure.adapters.bus import streams
from co_core_aio.bus import AsyncBusConsumer
from co_core_aio.fetch import AsyncFetchDriver
from redis.asyncio import Redis

from src.core.config import Settings, get_settings
from src.core.logging import configure_logging, get_logger
from src.storage.local import LocalBlobStore, ensure_directory
from src.storage.sweeper import BlobUsage
from src.worker.aliases import load_alias_table
from src.worker.handler import build_handler
from src.worker.loop import FETCH_SPEC, REPLICATE_SPEC, run_loop
from src.worker.policy import (
    FetchPolicyMap,
    build_policy_reader,
    replay_policies,
    run_policy_reader,
)
from src.worker.replicate import build_replicate_handler
from src.worker.replicate_reporter import build_replicate_reporter
from src.worker.reporter import build_failure_reporter
from src.worker.retention import run_sweeper

logger = get_logger(__name__)

# Signals that mean "stop taking new work". SIGINT is included so an interactive
# Ctrl-C drains the same way systemd's SIGTERM does.
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def warn_if_unreachable(blob_dir: Path) -> None:
    """Say so when the blob root cannot be traversed by another service.

    A pre-existing directory keeps whatever mode its operator gave it — the
    store deliberately does not widen what it did not create. The cost of that
    choice is a misconfiguration with no local symptom: blobs store fine, the
    fact publishes fine, and the failure appears in *another repo* as a
    ``blob_uri`` nothing can open. This is the one place it can be noticed.

    Every level is checked, not just the leaf. Traversal needs ``+x`` on the
    whole chain, and the likeliest shape of the mistake is precisely a split
    one: an operator pre-creates ``/var/lib/replicator`` restrictively and lets
    the worker create ``blobs/`` underneath, giving 0700 over 0755 and a leaf
    whose own mode looks fine.

    A warning rather than a failure: a single-user deployment where nothing else
    reads the blobs is legitimate, and the operator may mean it. For the same
    reason the check is total — a diagnostic that cannot complete must never be
    the thing that stops the boot, so an unstatable ancestor is reported and
    swallowed rather than raised.
    """
    try:
        blocked = _unreachable_levels(blob_dir)
    except OSError as exc:
        logger.warning(
            "could not check whether the blob directory is reachable",
            extra={"blob_dir": str(blob_dir), "errno": exc.errno},
        )
        return
    if not blocked:
        return
    logger.warning(
        "blob directory is not traversable by other services",
        extra={
            "blob_dir": str(blob_dir),
            # Every blocking level, because fixing only the innermost leaves the
            # blob just as unreachable as before.
            "blocked_at": {str(level): oct(mode) for level, mode in blocked},
            "detail": "blob_uri will be announced on content.blobs but cannot be opened",
        },
    )


def _unreachable_levels(blob_dir: Path) -> list[tuple[Path, int]]:
    """Every level from ``blob_dir`` up that denies traversal, with its mode."""
    resolved = blob_dir.resolve()
    chain = (resolved, *resolved.parents)
    levels = [(level, stat.S_IMODE(level.stat().st_mode)) for level in chain]
    # Group- or other-executable is what lets a reader traverse; without either,
    # only this process's own user can reach anything below.
    return [(level, mode) for level, mode in levels if not mode & 0o011]


def build_consumer(
    client: Redis,
    settings: Settings,
    *,
    topic: str = streams.CONTENT_FETCH,
    group: str | None = None,
) -> AsyncBusConsumer:
    """Wire an ``AsyncBusConsumer`` for the ``content.fetch`` command stream.

    ``content.fetch`` carries command semantics, so there is exactly one group
    cluster-wide (``replicator.fetch``) whose members compete for messages —
    unlike a fact stream, where each consuming service gets its own group.

    ``topic`` is a defaulted argument rather than a setting: the only caller that
    moves it is a live-broker test, which must consume from a scratch stream
    because a frame added to the real ``content.fetch`` would be fetched for real
    by the running service. Configuration would make the production stream an
    operator's typo away, and there is no deployment that wants a different one.

    ``group`` defaults to the fetch group and is passed explicitly by the
    replicate loop (#29). One group per *stream*, never one shared across both:
    ``claim_stale`` walks a group's PEL, so a shared name would let recovery on
    one stream reach into the other's pending entries.
    """
    return AsyncBusConsumer(
        client,
        topic=topic,
        group=group or settings.consumer_group,
        consumer=settings.consumer_name,
    )


def install_signal_handlers(stop: asyncio.Event) -> None:
    """Route SIGTERM/SIGINT to ``stop`` instead of killing the loop mid-message.

    Setting an event rather than cancelling means the in-flight message finishes
    and acks; a cancelled handler would leave it in the PEL for a stale-claim
    round-trip that a clean restart has no reason to need.
    """
    loop = asyncio.get_running_loop()
    for sig in _STOP_SIGNALS:
        loop.add_signal_handler(sig, stop.set)


def remove_signal_handlers() -> None:
    """Restore default signal disposition (mirrors ``install_signal_handlers``)."""
    loop = asyncio.get_running_loop()
    for sig in _STOP_SIGNALS:
        loop.remove_signal_handler(sig)


async def _run_until_first_exit(
    *coroutines: Coroutine[Any, Any, None], stop: asyncio.Event
) -> None:
    """Run the worker's tasks together; the first to finish winds down the rest.

    The consume loop and the retention sweep are peers here rather than a task
    and a background chore. Either finishing means the worker is done: a clean
    return is SIGTERM having set ``stop``, and a raise is a failure the unit
    should restart through — the loop re-raises only after
    ``REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES``, and ``run_sweeper`` absorbs a
    failed sweep, so anything escaping either is worth an exit.

    The wind-down is bounded by the slowest in-flight step — a handler, or a tree
    walk that ``asyncio.to_thread`` puts beyond cancellation anyway — which is
    what ``TimeoutStopSec`` covers.

    Exactly one failure can be raised, and every other one is logged. The raised
    one comes from a task that ended the wait, because that is the failure
    explaining why the worker is going down; a shutdown-ordering error raised
    over it would bury the cause. When several end it together the **first
    argument wins**, deliberately — the consume loop is passed first, and its
    failure is the more explanatory of the two — so reordering the arguments
    changes which error the unit reports. But nothing may be *dropped*:
    ``asyncio.wait`` returns every task that completed in the same pass, not
    just the first, so a simultaneous pair leaves no survivor to log and the
    loser's traceback would reach the journal only as asyncio's detached
    "Task exception was never retrieved", if at all.
    """
    tasks = [asyncio.create_task(coroutine) for coroutine in coroutines]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except BaseException:
        # Shutdown cancelled the wait itself. The tasks are still ours to tidy
        # up: leaving them pending surfaces as "Task was destroyed but it is
        # pending" at interpreter exit, with the real cause already gone.
        #
        # A second cancellation arriving during the gather below would propagate
        # from it and replace the cause being re-raised. Accepted: systemd sends
        # one SIGTERM and then SIGKILL, and SIGKILL is not deliverable as a
        # Python exception at all, so there is no second cancel to guard against.
        for task in tasks:
            task.cancel()
        _log_shutdown_failures(await asyncio.gather(*tasks, return_exceptions=True))
        raise
    stop.set()
    # Asked to stop rather than cancelled, then awaited: a cancelled consume loop
    # would abandon a message mid-handler for a stale-claim round-trip a clean
    # restart has no reason to need.
    results = await asyncio.gather(*tasks, return_exceptions=True)
    failure = _exit_failure(tasks, results, among=done)
    _log_shutdown_failures([result for result in results if result is not failure])
    if failure is not None:
        raise failure


def _exit_failure(
    tasks: list[asyncio.Task[None]],
    results: list[BaseException | None],
    *,
    among: set[asyncio.Task[None]],
) -> BaseException | None:
    """The failure to raise: the first one from a task that ended the wait.

    "First" is by argument order, not by completion time — see
    ``_run_until_first_exit``. Ties are common: both tasks watch one stop event.
    """
    for task, result in zip(tasks, results, strict=True):
        if task in among and isinstance(result, BaseException):
            return result
    return None


def _log_shutdown_failures(results: list[BaseException | None]) -> None:
    """Report every failure that is not the one being raised.

    ``CancelledError`` is skipped: that is shutdown working, not failing.
    """
    for result in results:
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            logger.error(
                "a worker task failed while shutting down",
                extra={"error": f"{type(result).__name__}: {result}"},
                exc_info=result,
            )


async def run(
    stop: asyncio.Event | None = None,
    *,
    policy_topic: str = streams.CONTENT_FETCH_POLICY,
    replicate_topic: str = streams.CONTENT_REPLICATE,
) -> None:
    """Connect to the bus, ensure the consumer group, and consume until stopped.

    ``stop`` is injectable so tests drive the loop without signals; left unset,
    the process owns its own event and wires SIGTERM/SIGINT to it.

    ``policy_topic`` and ``replicate_topic`` are defaulted arguments for the same
    reason ``content.fetch`` and ``content.blobs`` are: the only caller that moves
    them is a live-broker test working on a scratch stream.
    """
    settings = get_settings()
    configure_logging(settings.log_level)

    # systemd's StateDirectory= creates the parent only, so the leaf is ours to
    # make. Doing it at startup rather than at first write means a
    # misconfigured path fails loudly on boot, not mid-fetch. The failure is
    # logged structurally before re-raising: an uncaught OSError would put the
    # one line that matters into the journal as a bare traceback, unparseable by
    # a pipeline expecting JSON, right before the unit flaps to its restart
    # limit.
    #
    # ensure_directory rather than a bare mkdir so a directory this process
    # creates is left readable by the service that reads the blobs, while one
    # that already exists keeps whatever mode its operator gave it.
    # Resolved first, and passed to everything that touches the tree. The store
    # resolves internally so a later chdir cannot move where blobs land; the
    # sweep has no such protection of its own, and the two reaping and writing
    # different directories is the kind of divergence nothing reports. Resolving
    # before the check rather than after also means the failure below names an
    # absolute path — REPLICATOR_BLOB_DIR defaults to the relative `blobs`, and
    # "blobs is not usable" is not an actionable line.
    blob_dir = settings.blob_dir.resolve()
    try:
        ensure_directory(blob_dir)
    except OSError as exc:
        logger.error(
            "blob directory is not usable",
            extra={"blob_dir": str(blob_dir), "errno": exc.errno},
        )
        raise
    warn_if_unreachable(blob_dir)

    owns_signals = stop is None
    client = Redis.from_url(settings.redis_url)
    # Constructed inside the try, like the signal handlers: anything opened
    # between here and the try would leak the Redis client if it raised.
    fetcher: AsyncFetchDriver | None = None
    try:
        # One driver for the worker's lifetime, not one per message: it wraps an
        # httpx.AsyncClient whose connection pool is the point, and a per-message
        # driver would open and discard a pool per fetch. Closed in the same
        # finally as the Redis client — both are ours because we opened them.
        fetcher = AsyncFetchDriver()
        # Installed inside the try so the handlers are always removed again —
        # outside it, a failure between install and the try would leak global
        # signal state (harmless for a dying process, not for an in-process test).
        if stop is None:
            stop = asyncio.Event()
            install_signal_handlers(stop)

        consumer = build_consumer(client, settings)
        # The second command stream (#29). Its own group, its own PEL, its own
        # dedupe namespace — everything a competing-consumer command stream needs
        # not to interfere with the first one.
        replicate_consumer = build_consumer(
            client, settings, topic=replicate_topic, group=settings.replicate_consumer_group
        )
        # Host state, read once at boot: which destinations an operator
        # provisioned here. Unset means nothing is provisioned and every
        # replicate command is refused, which is the current state of every host
        # and the safe default (contract T5).
        aliases = load_alias_table(settings.replication_aliases_file)
        # Default start_id="$" reads only messages added after group creation.
        # The MVP seed harness controls when commands appear, so a backlog drain
        # ("0") is not needed; REPLICATOR_CONSUMER_START_ID flips it once a live
        # issuer exists — see the setting for the XGROUP SETID caveat.
        await consumer.ensure_group(start_id=settings.consumer_start_id)
        await replicate_consumer.ensure_group(start_id=settings.consumer_start_id)
        logger.info(
            "worker ready",
            extra={
                "group": settings.consumer_group,
                "consumer": settings.consumer_name,
                "build": settings.build_id,
                # How long this worker will absorb a broker outage before
                # exiting. In the journal at every boot because the unit's
                # StartLimitIntervalSec is sized against it, and a config change
                # that widens it would otherwise be invisible.
                "worst_case_outage_seconds": settings.worst_case_outage_seconds,
                # What this host will accept a replicate command for. Empty is
                # the expected value today and says so plainly, rather than
                # leaving an operator to infer it from a stream of refusals.
                "replication_aliases": list(aliases.provisioned),
            },
        )
        # One instance, deliberately shared: the sweep measures the tree and the
        # byte path adds to it between sweeps. Wired to two objects both halves
        # would be individually correct and the ceiling would never fire, with
        # nothing observing the difference until the disk was full.
        usage = BlobUsage()
        # One store for both command loops, for the reason `usage` is one object:
        # wired twice, each half would be individually correct and any state a
        # backend later holds — a client pool, #7's object-store handle — would
        # silently be two (CR #18).
        store = LocalBlobStore(blob_dir)
        # Rebuilt from the stream *before* the consume loop starts, not as the
        # first pass of the tail task (#19). Started as a peer, the loop would
        # fetch its opening commands against an empty map and pace every host at
        # the fallback — safe only because the fallback is supposed to be the
        # stricter number, which is the one assumption not worth spending on
        # startup ordering. `ensure_group` above already makes a blocking broker
        # call at boot, so this adds a round trip, not a new failure mode.
        policies = FetchPolicyMap(settings.min_host_interval_seconds)
        policy_reader = build_policy_reader(client, topic=policy_topic)
        await replay_policies(policy_reader, policies, stop=stop)
        await _run_until_first_exit(
            run_loop(
                client=client,
                consumer=consumer,
                # The same value build_consumer used — threaded explicitly so the
                # PEL the ceiling reads is provably the one the consumer acks against.
                group=settings.consumer_group,
                settings=settings,
                handler=build_handler(
                    fetcher=fetcher,
                    store=store,
                    client=client,
                    settings=settings,
                    usage=usage,
                    # Where the per-host numbers come from (#19). A bound method
                    # rather than the map, so the byte path never learns there is
                    # a stream behind it — and the *same* map the reader below
                    # writes to, for the reason `usage` is one instance: two
                    # would both be individually correct and the policies would
                    # never reach the pacer.
                    policy=policies.interval_for,
                    # The same stop event the loop and the sweeper ride, so a
                    # handler waiting out a politeness window does not hold a
                    # SIGTERM for it (#12).
                    stop=stop,
                ),
                # The other outcome of a command, on the same stream: an issuer
                # closes a pending entry off one consumer group either way (#9,
                # co-core cannobserv#270).
                reporter=build_failure_reporter(client=client),
                # Which command stream this loop is: the payload type it accepts,
                # its dedupe namespace, its journal label, and how it builds a
                # failure report. A second loop over content.replicate is another
                # run_loop with another spec, not another module (#29).
                spec=FETCH_SPEC,
                stop=stop,
            ),
            # The second command loop: same machinery, different spec (#29). It
            # writes nothing yet — no provider is enabled on any host — so every
            # command is refused with an accurate reason and a real fact, which
            # is what lets Archiver build against it before the writers land.
            run_loop(
                client=client,
                consumer=replicate_consumer,
                group=settings.replicate_consumer_group,
                settings=settings,
                handler=build_replicate_handler(store=store, aliases=aliases),
                reporter=build_replicate_reporter(client=client),
                spec=REPLICATE_SPEC,
                stop=stop,
            ),
            run_sweeper(root=blob_dir, settings=settings, usage=usage, stop=stop),
            # Passed last: `_exit_failure` picks the raised failure by argument
            # order, and this task absorbs its own errors anyway, so it should
            # never be the one explaining an exit.
            run_policy_reader(policy_reader, policies=policies, settings=settings, stop=stop),
            stop=stop,
        )
        logger.info("worker stopped", extra={"consumer": settings.consumer_name})
    finally:
        if owns_signals:
            remove_signal_handlers()
        if fetcher is not None:
            await fetcher.aclose()
        await client.aclose()


def main() -> None:
    """Console entry point."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
