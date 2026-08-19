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
from co_core_aio.gcs import AsyncGcsDriver
from redis.asyncio import Redis

from src.core.config import Settings, get_settings
from src.core.logging import configure_logging, get_logger
from src.storage.base import BlobStore
from src.storage.gcs import GcsBlobStore
from src.storage.local import LocalBlobStore, ensure_directory
from src.storage.sweeper import BlobUsage
from src.worker.aliases import AliasTable, load_alias_table
from src.worker.checkout import checkout_refusal
from src.worker.handler import build_handler
from src.worker.loop import FETCH_SPEC, REPLICATE_SPEC, run_loop
from src.worker.policy import (
    FetchPolicyMap,
    build_policy_reader,
    replay_policies,
    run_policy_reader,
)
from src.worker.replicate import build_replicate_handler
from src.worker.replicate_reporter import build_completion_publisher, build_replicate_reporter
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


def preflight_object_store(store: GcsBlobStore, settings: Settings) -> None:
    """Fail the boot if the temp bucket is not there to be written to (#7).

    The object-store counterpart of ``ensure_directory`` — asked at startup for
    the same reason: a storage misconfiguration whose only other symptom is a
    ``blob_uri`` nobody can open in *another repo* has to fail here, loudly,
    rather than at the first command.

    Deliberately stricter than ``warn_if_unreachable``, which only warns. That
    check tolerates its own failure because a single-user deployment where
    nothing else reads the blobs is a legitimate configuration an operator may
    have meant. A bucket this worker cannot reach has no such reading — there is
    no deployment in which storing to an absent bucket is what somebody wanted.

    What ``GcsBlobStore.preflight`` actually proves is narrower than "usable":
    the credentials resolve, the bucket resolves, and this identity may read it.
    Write access is left to the first ``store`` and to the grant — see that
    method for why a probe object would be a bad trade.

    **What it does not check is consumer read access**, and that is the honest
    limit of it. The coupling #7 removes was a filesystem one and what replaces
    it is an IAM grant on the consumer's service account — auditable, and
    host-independent, but not visible from here. ``warn_if_unreachable`` could at
    least walk the ancestors; this cannot ask Google whether Watcher may read.
    The grant is verified where it is made (docs/DEPLOYMENT.md), not at this
    boot.
    """
    try:
        store.preflight()
    except Exception as exc:
        logger.error(
            "temp blob bucket is not usable",
            extra={
                "blob_bucket": settings.blob_bucket,
                "blob_prefix": settings.blob_prefix,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        raise


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


def build_writers(aliases: AliasTable) -> dict[str, AsyncGcsDriver]:
    """One provider writer per provisioned binding, keyed **by alias** (#29).

    By alias and not by provider (CR #26): ``AsyncGcsDriver`` takes a bucket in
    its constructor and never sees another, so a driver *is* a bucket and the key
    has to be whatever selects one. Keyed by provider, two ``gcs`` bindings
    collapsed onto a single entry — the surviving driver served both aliases, so
    a command could land in a bucket its binding never named, outside the T3 root
    the destination guard had just checked. The loser was also dropped on the
    floor with its HTTP session open, because shutdown iterates this dict.

    **A binding that cannot be built is skipped, not raised** (CR #29).
    ``storage.Client()`` resolves ADC in the constructor, so an expired key file
    or a revoked SA raises here — and ``load_alias_table`` promises in as many
    words that a replicate misconfiguration must not take down a worker whose
    actual job is ``content.fetch``. Skipping keeps that true: the alias has no
    writer, so the handler refuses it ``provider_disabled``, which is both
    accurate and the reason whose remedy is the operator act that fixes it. Per
    binding rather than all-or-nothing, the same shape ``load_alias_table`` uses
    for one unusable entry in a readable table.

    **And a checkout that is not main's code builds no writer at all** (#52).
    ``replicator.service`` asks that question as an ``ExecStartPre``; a dev worker
    started with ``uv run python -m src.worker.main`` runs no ``ExecStartPre`` and
    inherits, per AGENTS.md's own shell snippet, the production ADC and the
    production alias table. Refused the same way an unbuildable driver is —
    skipped and logged, never raised — so the fetch path is untouched and the
    handler's reason stays ``provider_disabled``. ``REPLICATOR_ALLOW_ANY_CHECKOUT=1``
    is the override, read by the script rather than here (``src/worker/checkout.py``).
    """
    writers: dict[str, AsyncGcsDriver] = {}
    refusal: str | None = None
    asked = False
    for alias, binding in aliases.bindings.items():
        if binding.provider != "gcs":
            continue
        # Asked here rather than at the top: a worker with nothing provisioned —
        # every worker on this VM today — should not pay a subprocess for a
        # question about a write it will never attempt. Asked once and cached for
        # the table, because the answer cannot differ between two bindings read
        # in the same process.
        if not asked:
            refusal, asked = checkout_refusal(), True
        if refusal is not None:
            logger.error(
                "refusing to build a provider writer — this checkout is not main's code",
                extra={
                    "alias": alias,
                    "provider": binding.provider,
                    "refusal": refusal,
                    "detail": (
                        "commands naming it are refused provider_disabled; "
                        "set REPLICATOR_ALLOW_ANY_CHECKOUT=1 to build it anyway"
                    ),
                },
            )
            continue
        try:
            writers[alias] = AsyncGcsDriver(binding.bucket)
        except Exception as exc:
            logger.error(
                "could not build a provider writer — this alias will be refused",
                extra={
                    "alias": alias,
                    "provider": binding.provider,
                    "error": f"{type(exc).__name__}: {exc}",
                    "detail": "commands naming it are refused provider_disabled",
                },
            )
    return writers


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


def _prepare_storage(settings: Settings) -> tuple[BlobStore, Path | None]:
    """Build the blob store this deployment is configured for, and provision it.

    Returns the local root alongside the store, or ``None`` under the
    object-store backend — which is how the caller knows *not* to run the three
    things that only make sense over a filesystem: creating the directory,
    warning about its traversal modes, and sweeping it.

    Splitting the two backends here rather than inside the store keeps the
    difference where an operator's configuration lives. Everything downstream
    takes a ``BlobStore`` and cannot tell which one it got, which is the property
    #7 is for.
    """
    if settings.blob_backend == "gcs":
        store = GcsBlobStore(
            settings.blob_bucket,
            prefix=settings.blob_prefix,
            timeout_seconds=settings.blob_timeout_seconds,
        )
        preflight_object_store(store, settings)
        logger.info(
            "storing blobs in an object store",
            extra={
                "blob_bucket": settings.blob_bucket,
                "blob_prefix": settings.blob_prefix,
                # The horizon this worker will publish on every blob_available,
                # logged beside the bucket because the thing that actually reaps
                # is a lifecycle rule configured somewhere this process cannot
                # see (CR #11). Nothing keeps the two in step, and a rule shorter
                # than this number announces a window the bucket will not honour
                # — the one way blob_expires_at can be wrong in the direction
                # that leaves a consumer holding a dead blob_uri. Greppable at
                # boot is not a guard, but it is the difference between an
                # operator comparing two numbers and an operator guessing one.
                "blob_ttl_seconds": settings.blob_ttl_seconds,
                "blob_timeout_seconds": settings.blob_timeout_seconds,
                "detail_lifecycle": (
                    "reaping is the bucket lifecycle rule on daysSinceCustomTime; "
                    "it must be at least blob_ttl_seconds or the published "
                    "blob_expires_at is longer than the bucket will honour"
                ),
                # Said at boot because the environment still carries the ceiling
                # and an operator would reasonably assume it applies. It bounds a
                # shared disk; a bucket is not one, and re-deriving a bucket's
                # size would mean listing every object every cycle to compute a
                # number the lifecycle rule already acts on. What still holds is
                # the per-blob cap; what replaces the rest is a budget alert,
                # which no process here can enforce.
                "detail": (
                    "retention is the bucket lifecycle rule; "
                    "REPLICATOR_BLOB_MAX_TOTAL_BYTES is not enforced on this backend"
                ),
                "max_blob_bytes": settings.max_blob_bytes,
            },
        )
        return store, None

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
    return LocalBlobStore(blob_dir), blob_dir


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
    # `blob_dir` is None under the object-store backend, and every local-only
    # step goes with it: the directory, the traversal warning, and the sweep.
    store, blob_dir = _prepare_storage(settings)

    owns_signals = stop is None
    client = Redis.from_url(settings.redis_url)
    # Constructed inside the try, like the signal handlers: anything opened
    # between here and the try would leak the Redis client if it raised.
    fetcher: AsyncFetchDriver | None = None
    # Bound before the try for the same reason ``fetcher`` is: the finally block
    # below iterates it, and a failure between here and its assignment would
    # raise NameError over the real error.
    writers: dict[str, AsyncGcsDriver] = {}
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
        # One driver per provisioned binding, built **here** and not per command:
        # ``storage.Client()`` resolves ADC synchronously — key files, and on a
        # GCE-style host the metadata server — so constructing it inside the loop
        # would put a blocking credential lookup on the event loop once per
        # replicate command. Empty on a host with nothing provisioned, which is
        # every host until an operator writes an alias table.
        #
        # **Two blocking calls, both deliberate, both only here** (CR #4). The
        # second is the checkout guard's `subprocess.run`, up to
        # `GUARD_TIMEOUT_SECONDS`. Neither starves anything: no task exists yet,
        # so the loop has nothing else to run. What they do cost is shutdown
        # latency — signal handlers are installed by this point, so a SIGTERM
        # arriving inside that window sets `stop` but is not *seen* until the
        # call returns. Bounded, startup-only, and cheaper than the alternative:
        # `to_thread` here would buy responsiveness during a window in which
        # there is nothing to respond to. Move either off the loop only if it
        # ever moves out of startup.
        writers = build_writers(aliases)
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
        # backend holds — the object store's client pool (#7) — would silently be
        # two (CR #18). Built above, before the broker client, because a storage
        # misconfiguration should fail the boot before anything is opened.
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
                    # None where nothing measures the store (#7). The ceiling
                    # bounds a shared disk, and `BlobUsage` only ever falls when
                    # a sweep re-measures it — so passing the number to a
                    # backend with no sweep would let the byte path's own
                    # estimate climb to it and park every command there,
                    # permanently, waiting on a sweep that does not exist.
                    ceiling_bytes=(None if blob_dir is None else settings.blob_max_total_bytes),
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
                handler=build_replicate_handler(
                    store=store,
                    aliases=aliases,
                    writers=writers,
                    # The success fact is the handler's to publish, exactly as
                    # blob_available is the byte path's — the loop sees failures.
                    complete=build_completion_publisher(client=client),
                    write_timeout_seconds=settings.replicate_write_timeout_seconds,
                ),
                reporter=build_replicate_reporter(client=client),
                spec=REPLICATE_SPEC,
                stop=stop,
            ),
            # Retention is a task only over a filesystem. Under the
            # object-store backend the window is a bucket lifecycle rule and
            # `blob_dir` is None, so the sweep is not started rather than started
            # and made to do nothing — a parked no-op task would still be a
            # second thing that can end the worker.
            *(
                []
                if blob_dir is None
                else [run_sweeper(root=blob_dir, settings=settings, usage=usage, stop=stop)]
            ),
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
        for alias, writer in writers.items():
            # Ours because we built them: the driver closes the transport it
            # owns, and a client left open holds an HTTP session past shutdown.
            #
            # Guarded per writer (CR #30) because this loop runs *before*
            # ``client.aclose()``: unguarded, one raising driver skipped every
            # writer after it and leaked the Redis client too — a shutdown path
            # where the first failure costs every release that follows it.
            try:
                await writer.aclose()
            except Exception as exc:
                logger.warning(
                    "failed to close a provider writer",
                    extra={"alias": alias, "error": f"{type(exc).__name__}: {exc}"},
                )
        await client.aclose()


def main() -> None:
    """Console entry point."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
