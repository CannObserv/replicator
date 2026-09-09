"""Replicator against a broker that has hit its cap, and the shape of a DLQ write (#79).

Two of CannObserv/broker#1's open items need the same apparatus, so they are one
module rather than two.

**R5 (broker#6) — behaviour under ``OOM command not allowed``.** The broker runs
``maxmemory-policy noeviction`` with an explicit ``maxmemory``, which converts an
untrimmed stream from a kernel OOM-kill of ``redis-server`` into a bounded,
retryable error. The cap is instance-wide: once it bites, ``XADD`` is refused for
every producer on the broker, whichever stream filled it. Replicator classified
``OutOfMemoryError`` transient in #20 on the strength of reading redis-py's class
hierarchy; nothing had ever run the path. These tests are that run.

**D3 (broker#2) — the dead-letter command form.** The ACL draft grants this
service ``~<its topics>.dlq`` on the *inference* that ``co_core_aio.bus.dead_letter``
issues ``XADD <topic>.dlq``. That path appeared in neither ``MONITOR`` capture the
draft was built from, so it is the one command in the inventory taken from reading
rather than from observing. ``test_the_dead_letter_write_is_an_xadd_to_the_topic_dlq``
observes it, through ``MONITOR``, on the broker's own wire.

**Never the shared broker.** ``maxmemory`` is instance-wide and ``co-broker``
carries production traffic for three services, so lowering the cap to force the
error there is an outage. Every test here drives a ``redis-server`` this module
*spawns*, on a loopback port of its own — and ``capped_server`` refuses to hand
over a client to a server it did not start (see its ``process_id`` check, which is
not defensive decoration: writing this issue up found port 6399 already occupied
by a sibling service's identical experiment, and the first probe run silently
reconfigured *its* cap).

**A second refusal shape, since #82.** A broker this module owns can also be made
to refuse a write for the *other* reason broker#1 introduces — an ACL that does
not grant it. The apparatus is the same one, so the denial is exercised here
rather than in a module of its own: ``test_an_acl_denial_is_retried_like_a_cap``.

``REPLICATOR_TEST_REDIS_URL`` is deliberately never read here. That variable names
the Archiver-operated broker, which is exactly the server this module must not
touch.
"""

import asyncio
import contextlib
import shutil
import socket
import time
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest
from co_core.pure.adapters.bus.streams import dlq_name
from co_core.pure.models.changes import (
    ContentFetchCommand,
    ContentReplicateCommand,
    FetchFailedEvent,
)
from co_core_aio.bus import AsyncBusConsumer
from redis.asyncio import Redis
from redis.exceptions import NoPermissionError, OutOfMemoryError, ResponseError

from src.core.config import get_settings
from src.core.errors import (
    FailureReason,
    PermanentFetchError,
    PermanentReplicateError,
    ReplicateReason,
)
from src.storage.local import LocalBlobStore
from src.worker.handler import build_handler
from src.worker.loop import (
    _TRANSIENT_ERRORS,
    FETCH_SPEC,
    REPLICATE_SPEC,
    Outcome,
    claim_once,
    poll_once,
    process_message,
    run_loop,
)
from src.worker.reporter import build_failure_reporter
from tests.worker.conftest import FakeFetcher, collected_reports, decoded_facts, make_command
from tests.worker.test_loop_spec import make_replicate_command

pytestmark = pytest.mark.integration

GROUP = "replicator.oomtest"
CONSUMER = "replicator@oomtest"

# The cap the experiment runs at, and the issue's own number. A fresh 7.0.15
# reports ~660 KiB of ``used_memory`` before a single key exists, so 1 MiB leaves
# a few hundred KiB of headroom — enough that filling it is a real fill and quick
# enough that it is not a wall-clock tax.
CAP_BYTES = 1024 * 1024

# One ballast entry, and the size is the load-bearing part of it. Redis compares
# ``used_memory`` against ``maxmemory`` at the moment a ``denyoom`` command runs
# — it does not model the write's cost — and the client's own argument buffer
# counts toward that number. So a *large* ballast entry can be refused and then
# free enough on the error reply to put usage back under the cap, which is how
# the first version of this module observed a refusal followed immediately by a
# successful ``XADD``. Small entries cross the line and stay there; ``cap()``
# confirms it with a canary rather than trusting either size.
BALLAST_BYTES = 512

# A bound, not an expectation: the fill loop must not become an infinite one if a
# future Redis stops refusing writes at its own cap. ~1,200 entries reach a 1 MiB
# cap from a fresh instance at this size.
MAX_BALLAST_ENTRIES = 20_000

# The stream the fill writes to, and the one ``cap()`` checks the refusal is
# *sticky* on. Constants rather than literals at the two call sites because
# ``relieve()`` has to drop exactly what ``cap()`` wrote: two spellings that
# drifted would leave the ballast behind, and only the fixture's ``flushall``
# would catch it.
BALLAST_TOPIC = "replicator.oomtest.ballast"
CANARY_TOPIC = "replicator.oomtest.canary"

# Timers for the loop-level tests. Production is 60s/5s; the properties here are
# about *what* the loop does across a retry, not about how long it waits.
CLAIM_MIN_IDLE_MS = 50
READ_BLOCK_MS = 100
BACKOFF_BASE_SECONDS = 0.02
BACKOFF_MAX_SECONDS = 0.1

# How long a test waits for the loop to reach an outcome once the cap clears.
# Generous against a shared VM; the loop normally gets there in well under a
# second.
RECOVERY_TIMEOUT_SECONDS = 15

# How long the spawned broker gets to answer its first PING, and how long it
# gets to exit on a SIGTERM before it is killed outright.
STARTUP_TIMEOUT_SECONDS = 10
SHUTDOWN_TIMEOUT_SECONDS = 5

# The gap between two looks at a state only the broker can change.
POLL_INTERVAL_SECONDS = 0.05

# How long ``observing`` waits for MONITOR to attach, and for the last reply to
# arrive before it stops listening. Both ends need it: a capture that starts late
# misses the commands under test, and one that stops early truncates them.
MONITOR_SETTLE_SECONDS = 0.2


async def wait_for(predicate, *, what: str) -> None:
    """Poll ``predicate`` until it holds, or fail saying what never happened.

    ``asyncio.timeout`` around the same loop would report a bare
    ``TimeoutError`` from inside a ``while`` — true, and silent about which wait
    expired.
    """
    deadline = time.monotonic() + RECOVERY_TIMEOUT_SECONDS
    while True:
        if await predicate():
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"waited {RECOVERY_TIMEOUT_SECONDS}s and {what} never happened")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


@contextlib.asynccontextmanager
async def observing(client: Redis) -> AsyncGenerator[list[str]]:
    """Collect every command the broker sees, as ``MONITOR`` reports it.

    The list fills as the body runs and is complete once the block exits. Both
    sleeps are settling time — one for ``MONITOR`` to attach before anything is
    issued, one for the last reply to arrive before the collector is cancelled —
    and they live here rather than in each test so the two capture sites cannot
    be tuned apart.

    Safe against this module's own broker only, which is the whole point of
    ``capped_server``: ``MONITOR`` reports *every* client's traffic, so on a
    shared instance these assertions would read somebody else's commands.
    """
    observed: list[str] = []
    async with client.monitor() as monitor:

        async def collect() -> None:
            async for entry in monitor.listen():
                observed.append(entry["command"])

        collector = asyncio.create_task(collect())
        await asyncio.sleep(MONITOR_SETTLE_SECONDS)
        try:
            yield observed
        finally:
            await asyncio.sleep(MONITOR_SETTLE_SECONDS)
            collector.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await collector


@dataclass(frozen=True, slots=True)
class CappedBroker:
    """A Redis this module owns outright, plus the two moves the experiment needs.

    ``cap`` and ``relieve`` are methods rather than fixtures because a single test
    uses both, in order, and the interesting assertions live *between* them.
    """

    client: Redis

    async def cap(self) -> int:
        """Fill the instance until it refuses writes. Returns the entries it took.

        The cap is imposed *and then reached* rather than simply configured below
        the current usage, because "the cap bites" and "the cap is set" are
        different states: only the first is what production reaches, and only the
        first leaves ``used_memory`` where the next allocation genuinely fails.

        The first refusal is not the finish line — see ``BALLAST_BYTES``. The
        canary is what says the *next* write will be refused too, which is the
        state every test below assumes when it hands the broker to the worker.
        """
        await self.client.config_set("maxmemory", CAP_BYTES)
        payload = "x" * BALLAST_BYTES
        for written in range(MAX_BALLAST_ENTRIES):
            try:
                await self.client.xadd(BALLAST_TOPIC, {"payload": payload})
            except OutOfMemoryError:
                if await self.refuses_writes():
                    return written
        raise AssertionError(
            f"{MAX_BALLAST_ENTRIES} writes at {BALLAST_BYTES}B did not reach a "
            f"{CAP_BYTES}B cap — this broker is not enforcing maxmemory"
        )

    async def refuses_writes(self) -> bool:
        """Whether the broker refuses a write too small to matter.

        **The state is asked of the broker, not computed from `INFO memory`.**
        Comparing ``used_memory`` against ``maxmemory`` looks equivalent and is
        not: the two sit within a few hundred bytes of each other once the cap
        is reached, and the argument buffer of whichever command is running
        counts toward the first number (see ``BALLAST_BYTES``). A test asserting
        the cap still bites would read a momentary dip as "uncapped" and fail a
        run that was behaving correctly. An actual refusal cannot be misread.
        """
        try:
            await self.client.xadd(CANARY_TOPIC, {"payload": "."})
        except OutOfMemoryError:
            return True
        return False

    async def relieve(self) -> None:
        """Lift the cap and drop the ballast, in that order.

        ``DEL`` is a write, and a broker at its cap admits it — ``DEL`` is not
        ``denyoom``. Lifting first anyway: the order that works regardless is the
        one worth writing down, and an operator raising ``maxmemory`` is the real
        remedy this models.
        """
        await self.client.config_set("maxmemory", 0)
        await self.client.delete(BALLAST_TOPIC, CANARY_TOPIC)


def _free_port() -> int:
    """A loopback port nothing is listening on, at the moment it is asked for."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture(scope="session")
async def capped_server(tmp_path_factory) -> AsyncGenerator[Redis]:
    """A ``redis-server`` this session spawns, owns, and kills.

    Session-scoped because a spawn is per-run work; the per-test fixture below
    resets the state. Started **uncapped** — a test asks for the cap when it wants
    it, and a fixture that arrived pre-capped could not prove the fill did
    anything.

    ``--appendonly no --save ''`` so nothing is ever written to disk: this
    instance exists for one assertion each and has nothing worth persisting, and a
    background rewrite is memory pressure the experiment did not ask for.

    **The ``process_id`` check is the point of the fixture.** Binding a free port
    is a race, and losing it is not a connection error — it is a client
    successfully talking to somebody else's broker, which these tests then
    ``CONFIG SET maxmemory`` on. That happened once by hand while #79 was being
    written. Redis reports its own pid in ``INFO server``, so the identity is
    checkable, and a mismatch fails the run rather than skipping it.
    """
    if shutil.which("redis-server") is None:
        pytest.skip("redis-server is not on PATH — no scratch broker to cap")

    port = _free_port()
    workdir = tmp_path_factory.mktemp("oom-broker")
    process = await asyncio.create_subprocess_exec(
        "redis-server",
        "--port",
        str(port),
        "--bind",
        "127.0.0.1",
        "--maxmemory",
        "0",
        "--maxmemory-policy",
        "noeviction",
        "--appendonly",
        "no",
        "--save",
        "",
        "--dir",
        str(workdir),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    client = Redis.from_url(f"redis://127.0.0.1:{port}/0")
    try:
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        while True:
            try:
                await client.execute_command("PING")
                break
            except Exception:  # anything but an answer means "not up yet"
                if time.monotonic() > deadline or process.returncode is not None:
                    raise AssertionError(
                        f"the scratch broker on port {port} never answered "
                        f"(exit={process.returncode})"
                    ) from None
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
        server = await client.info("server")
        assert server["process_id"] == process.pid, (
            f"port {port} is answered by pid {server['process_id']}, not the "
            f"{process.pid} this fixture started — refusing to reconfigure a "
            f"broker this test does not own"
        )
        yield client
    finally:
        await client.aclose()
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except TimeoutError:
                # Killed *and* awaited: an unreaped child is a zombie for the
                # rest of the session, which is a cheap thing to get wrong in a
                # fixture other fixtures get copied from.
                process.kill()
                await process.wait()


@pytest.fixture
async def broker(capped_server) -> AsyncGenerator[CappedBroker]:
    """The owned broker, uncapped and empty, for one test.

    The reset is in teardown as well as setup: a test that leaves the instance at
    its cap would otherwise fail the *next* one inside its fixtures, where the
    failure reads as unrelated.
    """
    await capped_server.config_set("maxmemory", 0)
    await capped_server.flushall()
    try:
        yield CappedBroker(client=capped_server)
    finally:
        await capped_server.config_set("maxmemory", 0)
        await capped_server.flushall()


@pytest.fixture
async def denied_client(broker, topic, blobs_topic) -> AsyncGenerator[Redis]:
    """A client whose credential reaches the command stream and not the fact stream.

    The denial is a **key-pattern** one, which is the shape broker#1's draft is
    most likely to get wrong: omitting a topic from a service's ``~`` patterns is
    a quieter mistake than forgetting a command, and it is the one that produced
    archiver's ``content.blobs`` boundary becoming enforcement rather than
    documentation. ``+@all`` on purpose — restricting commands too would leave
    ambiguous which half of the ACL refused the write.

    The user is dropped in teardown: ``ACL SETUSER`` survives ``FLUSHALL``, so the
    ``broker`` fixture's reset does not reach it and a leaked user would still be
    on the instance for every later test in the session.
    """
    user = f"denied-{uuid.uuid4().hex[:8]}"
    password = uuid.uuid4().hex
    await broker.client.execute_command(
        "ACL", "SETUSER", user, "on", f">{password}", f"~{topic}", f"~{dlq_name(topic)}", "+@all"
    )
    url = broker.client.connection_pool.connection_kwargs
    client = Redis(host=url["host"], port=url["port"], username=user, password=password)
    try:
        yield client
    finally:
        await client.aclose()
        await broker.client.execute_command("ACL", "DELUSER", user)


@pytest.fixture
def oom_settings():
    """Production settings with this module's identity and its short timers."""
    return get_settings().model_copy(
        update={
            # No ``consumer_name``: the ``consumer`` fixture passes ``CONSUMER``
            # to ``AsyncBusConsumer`` directly, so a setting here would look
            # wired and change nothing.
            "consumer_group": GROUP,
            "claim_min_idle_ms": CLAIM_MIN_IDLE_MS,
            "read_block_ms": READ_BLOCK_MS,
            "error_backoff_base_seconds": BACKOFF_BASE_SECONDS,
            "error_backoff_max_seconds": BACKOFF_MAX_SECONDS,
        }
    )


@pytest.fixture
def topic() -> str:
    """A scratch command stream. Unique per test, though the instance is flushed."""
    return f"replicator.oomtest.{uuid.uuid4().hex}"


@pytest.fixture
def blobs_topic(topic) -> str:
    """A scratch stand-in for ``content.blobs``, derived from this test's topic.

    Never the real stream: a `fetch_failed` written there during a test would
    tell an issuer that a command it is waiting on has failed, and a
    `blob_available` would announce bytes under a `tmp_path` that is gone before
    anything could open them. Some callers publish facts to it and some only
    need a name the cap can refuse — both want it off the live stream.
    """
    return f"{topic}.blobs"


@pytest.fixture
async def consumer(broker, topic) -> AsyncBusConsumer:
    """A consumer on the scratch stream, with its group created before the cap."""
    consumer = AsyncBusConsumer(broker.client, topic=topic, group=GROUP, consumer=CONSUMER)
    await consumer.ensure_group(start_id="0")
    return consumer


def publishing_handler(broker, blobs_topic, tmp_path, oom_settings):
    """The real byte path over a real store, with only the fetch faked.

    The publish is what the cap refuses, so it has to be the genuine one — a
    stubbed handler raising ``OutOfMemoryError`` by hand would assert the loop's
    classification against a fiction, which is precisely what #20 already did and
    what this module exists to replace.
    """
    return build_handler(
        fetcher=FakeFetcher(),
        store=LocalBlobStore(tmp_path),
        client=broker.client,
        settings=oom_settings,
        blobs_topic=blobs_topic,
    )


async def deliver(broker, consumer, topic, *, command_id: str):
    """Put one well-formed command on the stream and read it into the PEL.

    ``command_id`` is required rather than defaulted: a test that asserts on a
    dedupe key spells the id a second time, and an id defaulted here is one the
    two spellings can drift apart on — where the assertion is ``== 0``, which
    passes for a key nothing ever wrote (CR #13).
    """
    await broker.client.xadd(topic, make_command(command_id=command_id))
    (message,) = await consumer.read(count=1, block_ms=READ_BLOCK_MS)
    return message


async def refusing_handler(command: ContentFetchCommand) -> None:
    """A handler whose failure is deterministic, so the loop closes the command.

    Module-level for the reason ``conftest.py`` keeps ``noop_handler`` there:
    five copies of a two-line handler is five things to keep in agreement, and
    the reason token is what selects the closing path under test.
    """
    raise PermanentFetchError("404 from the origin", reason=FailureReason.HTTP_STATUS)


async def pending_count(client, topic: str) -> int:
    return int((await client.xpending(topic, GROUP))["pending"])


def pending(client, topic: str, count: int):
    """A ``wait_for`` predicate: the PEL holds exactly ``count`` entries.

    A factory rather than a lambda over ``pending_count`` — a lambda returns the
    *coroutine*, which is truthy for every value the broker could report, so the
    wait would pass instantly and prove nothing.
    """

    async def holds() -> bool:
        return await pending_count(client, topic) == count

    return holds


def delivered_at_least(client, topic: str, message_id: str, attempts: int):
    """A ``wait_for`` predicate: the broker has delivered this entry ``attempts`` times.

    Absent from the PEL counts as zero rather than raising: the wait starts before
    the loop has taken delivery, and "not there yet" is the state it is waiting
    out.
    """

    async def reached() -> bool:
        entries = await client.xpending_range(topic, GROUP, min=message_id, max=message_id, count=1)
        return bool(entries) and int(entries[0]["times_delivered"]) >= attempts

    return reached


async def times_delivered(client, topic: str, message_id: str) -> int:
    entries = await client.xpending_range(topic, GROUP, min=message_id, max=message_id, count=1)
    return int(entries[0]["times_delivered"])


async def test_a_full_cap_refuses_a_publish_as_out_of_memory(broker, blobs_topic):
    """The exception the rest of this module is about, produced by a real broker.

    ``OutOfMemoryError`` subclasses ``ResponseError`` — the family that otherwise
    means "this command will never work" — which is the whole reason #20 had to
    name it explicitly in ``_TRANSIENT_ERRORS`` rather than relying on the
    connection-error arm. Both halves are asserted here: what the broker raises,
    and that the loop's tuple catches it.
    """
    written = await broker.cap()

    assert written, "the cap must be reached by writing, not arrive pre-reached"
    with pytest.raises(OutOfMemoryError) as raised:
        await broker.client.xadd(blobs_topic, {"event_type": "blob_available"})

    assert isinstance(raised.value, ResponseError)
    assert "used memory > 'maxmemory'" in str(raised.value)
    assert isinstance(raised.value, _TRANSIENT_ERRORS)


async def test_an_acl_denial_is_retried_like_a_cap(
    broker, topic, consumer, blobs_topic, denied_client, oom_settings, tmp_path
):
    """#82: a grant the operator got wrong must not close a valid command.

    The second refusal broker#1 introduces, and the one this service was alone in
    handling badly. ``NoPermissionError`` is a ``ResponseError`` subclass, so
    before #82 it reached ``_handle_unclassified``, burnt the delivery ceiling
    over five reclaims and then dead-lettered — closing a perfectly valid command
    with a terminal ``fetch_failed(handler_error)`` about a fault one
    ``ACL SETUSER`` fixes, and orphaning bytes already on disk.

    Driven through the **real** publish path against a **real** denial: the
    handler holds a credential that may write the command stream and its DLQ and
    not the fact stream, which is precisely the mistake of omitting a topic from a
    service's key patterns. The loop's own plumbing stays on the owning client, so
    what is under test is the classification of the publish rather than an
    ACL-scoped consume path the cutover has not settled yet.

    The outcome is the cap's outcome, and deliberately so: retry, nothing acked,
    nothing announced, nothing dead-lettered. An ACL that is never fixed retries
    forever rather than dead-lettering — the trade ``_TRANSIENT_ERRORS`` states.
    """
    # The refusal itself, so a test that later stopped denying anything is visible
    # rather than silently green.
    with pytest.raises(NoPermissionError):
        await denied_client.xadd(blobs_topic, {"event_type": "blob_available"})

    message = await deliver(broker, consumer, topic, command_id="cmd-denied")
    handler = build_handler(
        fetcher=FakeFetcher(),
        store=LocalBlobStore(tmp_path),
        client=denied_client,
        settings=oom_settings,
        blobs_topic=blobs_topic,
    )

    outcome = await process_message(
        message,
        client=broker.client,
        consumer=consumer,
        group=GROUP,
        handler=handler,
        settings=oom_settings,
        reporter=collected_reports(),
        spec=FETCH_SPEC,
    )

    assert outcome is Outcome.RETRY
    assert await pending_count(broker.client, topic) == 1
    assert await broker.client.exists(dlq_name(topic)) == 0
    assert await broker.client.exists(blobs_topic) == 0
    assert await broker.client.exists(FETCH_SPEC.dedupe_key("cmd-denied")) == 0


async def test_the_consume_path_still_runs_while_the_cap_bites(broker, topic, consumer):
    """Only ``denyoom`` commands are refused, and the consume path holds none.

    This is why an OOM is a *publishing* incident for Replicator rather than a
    total one: the frame can still be read, its PEL entry inspected, re-read,
    reclaimed and acked while the broker refuses every write that grows the
    dataset. It is also the evidence behind the command list in
    ``docs/CONVENTIONS.md``, so it asserts every command that list names — taken
    from the server rather than from ``COMMAND INFO``, whose flags say which
    commands *are* ``denyoom`` without saying what that leaves the loop able to do.

    **The reclaim is asserted by what it returned, not by its not raising.** An
    ``XAUTOCLAIM`` that takes nothing is not evidence it would have taken
    something, which is the vacuous assertion ``test_main_integration.py`` already
    had to be rewritten to avoid (CR round 3). Not raising is the *other* half —
    a refused command raises — and both are worth having.
    """
    command_id = "cmd-before-the-cap"
    await broker.client.xadd(topic, make_command(command_id=command_id))
    await broker.cap()

    # The refused half first, next to the fill that provoked it. Ordering, not
    # taste: ``used_memory`` sits within a few hundred bytes of the cap and moves
    # with whatever command is running (see ``BALLAST_BYTES``), so a refusal
    # asserted six commands later is asserted against a boundary that may have
    # drifted underneath it. These are the two writes the loop makes that a
    # capped broker will not take — the fact (and, on the closing paths, the DLQ
    # copy), and the dedupe key.
    with pytest.raises(OutOfMemoryError):
        await broker.client.xadd(dlq_name(topic), {"payload": "x"})
    with pytest.raises(OutOfMemoryError):
        await broker.client.set(FETCH_SPEC.dedupe_key(command_id), "1", nx=True, ex=60)

    # The admitted half. Each of these would raise if the cap covered it, so the
    # assertions are about what each returned as well as about reaching a reply.
    (message,) = await consumer.read(count=1, block_ms=READ_BLOCK_MS)
    assert await broker.client.xpending(topic, GROUP)
    # By id, which is the form ``dead_letter_anomaly`` issues — the unbounded
    # ``XRANGE key - +`` is admitted identically and is not the command an ACL
    # will be asked about.
    reread = await broker.client.xrange(topic, min=message.message_id, max=message.message_id)
    assert [entry[0].decode() for entry in reread] == [message.message_id]
    _cursor, claimed, _deleted = await broker.client.xautoclaim(topic, GROUP, CONSUMER, 0, "0-0")
    assert [entry[0].decode() for entry in claimed] == [message.message_id]
    assert await broker.client.exists(FETCH_SPEC.dedupe_key(command_id)) == 0
    assert await broker.client.execute_command("PING")
    await consumer.ack(message.message_id)
    assert await pending_count(broker.client, topic) == 0


async def test_a_refused_fact_leaves_the_command_pending_and_undead_lettered(
    broker, topic, consumer, blobs_topic, oom_settings, tmp_path
):
    """The headline answer for broker#6: retry, not drop, and not dead-letter.

    Store-then-publish means the bytes are already on disk when the ``XADD`` is
    refused, so the retry is a re-run of work that mostly succeeded — which
    content-addressed storage absorbs. What matters on the bus is that nothing was
    acked, nothing was announced, and nothing was written to ``<topic>.dlq``: the
    PEL still names the command, which is the durable record of intent this
    service has instead of an outbox.
    """
    command_id = "cmd-refused-fact"
    message = await deliver(broker, consumer, topic, command_id=command_id)
    await broker.cap()

    outcome = await process_message(
        message,
        client=broker.client,
        consumer=consumer,
        group=GROUP,
        handler=publishing_handler(broker, blobs_topic, tmp_path, oom_settings),
        settings=oom_settings,
        reporter=collected_reports(),
        spec=FETCH_SPEC,
    )

    assert outcome is Outcome.RETRY
    assert await pending_count(broker.client, topic) == 1
    assert await broker.client.exists(dlq_name(topic)) == 0
    assert await broker.client.exists(blobs_topic) == 0
    assert await broker.client.exists(FETCH_SPEC.dedupe_key(command_id)) == 0


async def test_a_capped_broker_never_burns_the_delivery_ceiling(
    broker, topic, consumer, blobs_topic, oom_settings, tmp_path
):
    """ "Retried indefinitely", asserted past the number that would end it.

    ``REPLICATOR_MAX_DELIVERY_ATTEMPTS`` is read from ``XPENDING``'s counter,
    which advances on every reclaim — so an OOM classified as *unclassified*
    would dead-letter a perfectly valid command after a handful of reclaims, and
    the operator would find it in ``content.fetch.dlq`` with a ``handler_error``
    naming somebody else's incident. The transient arm is what makes the number
    below irrelevant, and this drives it well past it to say so.
    """
    settings = oom_settings.model_copy(update={"max_delivery_attempts": 3})
    message = await deliver(broker, consumer, topic, command_id="cmd-ceiling")
    await broker.cap()
    handler = publishing_handler(broker, blobs_topic, tmp_path, settings)

    outcomes = []
    for _ in range(settings.max_delivery_attempts + 2):
        outcomes.append(
            await process_message(
                message,
                client=broker.client,
                consumer=consumer,
                group=GROUP,
                handler=handler,
                settings=settings,
                reporter=collected_reports(),
                spec=FETCH_SPEC,
            )
        )
        await asyncio.sleep(CLAIM_MIN_IDLE_MS / 1000)
        reclaimed = await claim_once(broker.client, consumer, settings, group=GROUP)
        # Named rather than unpacked blind: an empty list here is a missed idle
        # window, and `not enough values to unpack` would send the next reader
        # into claim_once looking for a bug that is not there.
        assert reclaimed, f"nothing was reclaimable after {CLAIM_MIN_IDLE_MS}ms idle"
        (message,) = reclaimed

    assert outcomes == [Outcome.RETRY] * (settings.max_delivery_attempts + 2)
    assert await times_delivered(broker.client, topic, message.message_id) > (
        settings.max_delivery_attempts
    )
    assert await pending_count(broker.client, topic) == 1
    assert await broker.client.exists(dlq_name(topic)) == 0


async def test_the_loop_completes_the_command_once_the_cap_clears(
    broker, topic, consumer, blobs_topic, oom_settings, tmp_path
):
    """The re-arm question: a loop that retried through an OOM must not be wedged.

    Nothing here restarts the worker. The same ``run_loop`` that was refused
    reclaims its own pending entry once the operator raises ``maxmemory``, and the
    command closes normally — fact published, message acked, PEL empty. That is
    the property the cap was chosen for: a bounded, retryable error rather than a
    dead client.
    """
    await broker.client.xadd(topic, make_command(command_id="cmd-recovers"))
    await broker.cap()
    stop = asyncio.Event()
    loop = asyncio.create_task(
        run_loop(
            client=broker.client,
            consumer=consumer,
            group=GROUP,
            settings=oom_settings,
            handler=publishing_handler(broker, blobs_topic, tmp_path, oom_settings),
            reporter=build_failure_reporter(client=broker.client, blobs_topic=blobs_topic),
            spec=FETCH_SPEC,
            stop=stop,
        )
    )
    try:
        # Let it fail at least once against the cap, so the recovery below is a
        # recovery rather than a first attempt that happened to run late.
        await wait_for(
            pending(broker.client, topic, 1),
            what="the loop took delivery of the command",
        )
        assert await broker.refuses_writes()
        assert await broker.client.exists(blobs_topic) == 0

        await broker.relieve()

        await wait_for(
            pending(broker.client, topic, 0),
            what="the loop acked the command after the cap lifted",
        )
    finally:
        stop.set()
        await asyncio.wait_for(loop, timeout=5)

    assert await broker.client.xlen(blobs_topic) == 1
    assert await broker.client.exists(dlq_name(topic)) == 0
    assert await broker.client.exists(FETCH_SPEC.dedupe_key("cmd-recovers")) == 1


async def test_a_dead_letter_refused_at_the_cap_strands_nothing(
    broker, topic, consumer, blobs_topic, oom_settings
):
    """The other half of the cap's blast radius: the DLQ write is an ``XADD`` too.

    A deterministically bad command reaching a capped broker cannot be closed —
    the fact is refused and so is the dead-letter copy. What must not happen is a
    *partial* close: ``dead_letter`` acks inside itself, after its ``XADD``, so a
    refusal leaves the frame exactly where it was. The error escapes
    ``process_message`` (only the handler call is wrapped), which makes it
    ``run_loop``'s cycle failure rather than the message's fate — and the message
    is redelivered and closed properly when the cap lifts.
    """

    message = await deliver(broker, consumer, topic, command_id="cmd-permanent")
    await broker.cap()

    with pytest.raises(OutOfMemoryError):
        await process_message(
            message,
            client=broker.client,
            consumer=consumer,
            group=GROUP,
            handler=refusing_handler,
            settings=oom_settings,
            reporter=build_failure_reporter(client=broker.client, blobs_topic=blobs_topic),
            spec=FETCH_SPEC,
        )

    assert await pending_count(broker.client, topic) == 1
    assert await broker.client.exists(dlq_name(topic)) == 0
    assert await broker.client.exists(blobs_topic) == 0

    await broker.relieve()

    outcome = await process_message(
        message,
        client=broker.client,
        consumer=consumer,
        group=GROUP,
        handler=refusing_handler,
        settings=oom_settings,
        reporter=build_failure_reporter(client=broker.client, blobs_topic=blobs_topic),
        spec=FETCH_SPEC,
    )

    assert outcome is Outcome.DEAD_LETTERED
    ((_id, entry),) = await broker.client.xrange(dlq_name(topic))
    assert entry[b"dlq_reason"] == b"handler reported a permanent failure"
    assert await pending_count(broker.client, topic) == 0
    facts = await broker.client.xrange(blobs_topic)
    assert len(facts) == 1


async def test_a_sustained_cap_on_the_closing_path_retries_rather_than_exiting(
    broker, topic, consumer, blobs_topic, oom_settings
):
    """The dead-letter path escalates differently, and stops short of a restart.

    A refused *fact* is the message's own outcome — ``Outcome.RETRY``, no cycle
    failure. A refused *dead-letter* escapes ``process_message`` (only the handler
    call is wrapped), so it is a **cycle** failure, and
    ``max_consecutive_cycle_failures`` exists to re-raise out of ``run_loop`` and
    let systemd restart the unit.

    It does not get there, and the reason is worth stating because it reads as a
    bug until it is traced: the counter resets on any cycle that completes, and
    the cycle *after* a refused dead-letter finds the entry too young to reclaim
    and returns an empty read — which counts as a completed cycle. So a capped
    broker produces a fail/idle alternation that never accumulates
    ``max_consecutive_cycle_failures`` in a row, and the outcome is the same one
    the byte path gets: retry at the reclaim cadence, indefinitely, with nothing
    acked. The counter still does its job against the failure it was written for
    (a broker that refuses the *read* too, where every cycle fails).

    Six refusals were observed here before this was rewritten from a test that
    asserted the restart; ``times_delivered`` is the broker-side evidence, and
    ``loop.done()`` is the assertion that the worker is still the one making them.
    """

    settings = oom_settings.model_copy(update={"max_consecutive_cycle_failures": 3})
    message_id = (
        await broker.client.xadd(topic, make_command(command_id="cmd-permanent"))
    ).decode()
    await broker.cap()
    stop = asyncio.Event()
    loop = asyncio.create_task(
        run_loop(
            client=broker.client,
            consumer=consumer,
            group=GROUP,
            settings=settings,
            handler=refusing_handler,
            reporter=build_failure_reporter(client=broker.client, blobs_topic=blobs_topic),
            spec=FETCH_SPEC,
            stop=stop,
        )
    )
    try:
        await wait_for(
            delivered_at_least(
                broker.client,
                topic,
                message_id,
                # The setting this test overrides, not the delivery ceiling: one
                # reclaim is one refused dead-letter, so more of them than the
                # cycle-failure ceiling admits *consecutively* is the whole
                # claim. Keyed to `max_delivery_attempts` it would have been an
                # accident that the numbers happened to be ordered right.
                settings.max_consecutive_cycle_failures + 1,
            ),
            what="the loop retried the refused dead-letter past the cycle-failure ceiling",
        )
        assert not loop.done(), "the loop exited instead of riding the cap out"
        assert await broker.client.exists(dlq_name(topic)) == 0

        await broker.relieve()

        await wait_for(
            pending(broker.client, topic, 0),
            what="the loop dead-lettered the command after the cap lifted",
        )
    finally:
        stop.set()
        await asyncio.wait_for(loop, timeout=5)

    assert await broker.client.xlen(dlq_name(topic)) == 1


async def test_the_dead_letter_write_is_an_xadd_to_the_topic_dlq(
    broker, topic, consumer, blobs_topic, oom_settings
):
    """broker#2's inferred command, observed on the wire instead (D3).

    ``MONITOR`` because that is what the ACL inventory was built from, and because
    the question is what the *broker* sees rather than what redis-py was asked to
    do. The dead-letter is two commands, in this order: an ``XADD`` to
    ``<topic>.dlq`` — one entry, no ``MAXLEN``, no ``NOMKSTREAM``, so the stream is
    created on first use — and an ``XACK`` of the original on the command stream.

    The scratch topic is what an ACL would have to admit for this test; the
    assertion that ties it to the grant is the last one, which reads the same
    ``dlq_name`` the production topics go through.
    """

    message = await deliver(broker, consumer, topic, command_id="cmd-observed")

    async with observing(broker.client) as observed:
        outcome = await process_message(
            message,
            client=broker.client,
            consumer=consumer,
            group=GROUP,
            handler=refusing_handler,
            settings=oom_settings,
            reporter=collected_reports(),
            spec=FETCH_SPEC,
        )

    assert outcome is Outcome.DEAD_LETTERED
    writes = [line for line in observed if line.startswith(("XADD", "XACK"))]
    assert len(writes) == 2, observed
    assert writes[0].startswith(f"XADD {topic}.dlq * ")
    assert "dlq_reason handler reported a permanent failure" in writes[0]
    assert writes[1] == f"XACK {topic} {GROUP} {message.message_id}"

    # What the grant has to name in production, from the same function the
    # observed form came out of. Both command streams, because Replicator
    # dead-letters on both and triages both since broker#1 Phase 5.
    assert dlq_name(FETCH_SPEC.label) == "content.fetch.dlq"
    assert dlq_name(REPLICATE_SPEC.label) == "content.replicate.dlq"


async def test_the_replicate_stream_dead_letters_by_the_same_two_commands(
    broker, topic, consumer, oom_settings
):
    """The second command stream's form, observed rather than reasoned from the first.

    One loop serves both streams (#29), so the pair below *should* be the pair
    above with a different topic — and that is a claim about code an ACL is being
    written from, which makes it worth a capture rather than an inference.
    broker#2 grants ``~content.replicate.dlq`` on this basis, and until this test
    existed the only observation of it lived in a comment on that issue, produced
    by a script nothing re-runs.

    The refusal is ``alias_unknown``, which the replicate contract refuses **before
    any credential is touched** (T2) — so this reaches the dead-letter path with no
    GCS identity, no alias table, and no writer.

    The dedupe key is asserted too, and it is the one thing here that is *not* the
    fetch path with a different topic: the namespaces are per stream, so a
    ``content.replicate`` command can never dedupe against a ``content.fetch`` one.
    """

    async def refusing_replicate_handler(command: ContentReplicateCommand) -> None:
        raise PermanentReplicateError(
            "the alias named by this command is not provisioned on this host",
            reason=ReplicateReason.ALIAS_UNKNOWN,
        )

    command_id = "rep-observed"
    await broker.client.xadd(topic, make_replicate_command(command_id=command_id))
    (message,) = await consumer.read(count=1, block_ms=READ_BLOCK_MS)

    async with observing(broker.client) as observed:
        outcome = await process_message(
            message,
            client=broker.client,
            consumer=consumer,
            group=GROUP,
            handler=refusing_replicate_handler,
            settings=oom_settings,
            reporter=collected_reports(),
            spec=REPLICATE_SPEC,
        )

    assert outcome is Outcome.DEAD_LETTERED
    writes = [line for line in observed if line.startswith(("XADD", "XACK"))]
    assert len(writes) == 2, observed
    assert writes[0].startswith(f"XADD {topic}.dlq * ")
    assert "dlq_reason handler reported a permanent failure" in writes[0]
    assert writes[1] == f"XACK {topic} {GROUP} {message.message_id}"

    # Per stream, and a collision here would be the worst failure shape available:
    # the second command acking having done nothing, silently.
    assert REPLICATE_SPEC.dedupe_key(command_id) != FETCH_SPEC.dedupe_key(command_id)
    assert await broker.client.exists(REPLICATE_SPEC.dedupe_key(command_id)) == 0


async def test_a_frame_that_will_not_decode_is_dead_lettered_by_the_same_two_commands(
    broker, topic, consumer, oom_settings
):
    """The second route to ``<topic>.dlq``, and the one an ACL is likeliest to miss.

    ``dead_letter_anomaly`` reaches the same ``XADD``/``XACK`` pair, but only after
    an ``XRANGE`` re-read of the frame by id — ``from_wire`` raises from inside
    ``read``, so there is no field map to copy and ``XADD`` rejects an empty one.
    A grant covering the dead-letter write but not the re-read would jam poison
    recovery specifically, which is the failure mode that reached 110 entries in
    CannObserv/archiver#162.
    """
    await broker.client.xadd(topic, {"event_type": "content_fetch", "payload": "not json"})

    async with observing(broker.client) as observed:
        assert await poll_once(broker.client, consumer, oom_settings, group=GROUP) == []

    assert any(line.startswith(f"XRANGE {topic} ") for line in observed), observed
    assert any(line.startswith(f"XADD {topic}.dlq * ") for line in observed), observed
    assert any(line.startswith(f"XACK {topic} {GROUP} ") for line in observed), observed
    assert await pending_count(broker.client, topic) == 0


async def test_a_closing_fact_that_the_cap_refuses_is_swallowed_not_raised(
    broker, topic, consumer, blobs_topic, oom_settings
):
    """The reporter's swallow, exercised against the broker that provokes it.

    ``fetch_failed`` is published *before* the dead-letter, so a refused fact must
    not abandon the close — the DLQ entry is the durable record, and a raise here
    would strand the frame in the PEL and re-close it minutes later as an
    unclassified handler error. The cap is lifted between the two writes to
    isolate the one under test, which is the only way to observe a failed announce
    followed by a successful dead-letter.
    """

    message = await deliver(broker, consumer, topic, command_id="cmd-silent")
    reporter = build_failure_reporter(client=broker.client, blobs_topic=blobs_topic)
    await broker.cap()

    async def relieving_reporter(report) -> None:
        """Announce under the cap, then lift it — the fact fails, the DLQ write does not."""
        try:
            await reporter(report)
        finally:
            await broker.relieve()

    outcome = await process_message(
        message,
        client=broker.client,
        consumer=consumer,
        group=GROUP,
        handler=refusing_handler,
        settings=oom_settings,
        reporter=relieving_reporter,
        spec=FETCH_SPEC,
    )

    assert outcome is Outcome.DEAD_LETTERED
    assert await broker.client.exists(blobs_topic) == 0, "the fact was refused, as intended"
    assert await broker.client.xlen(dlq_name(topic)) == 1
    assert await pending_count(broker.client, topic) == 0


async def test_the_fact_stream_carries_the_close_when_the_broker_can_take_it(
    broker, topic, consumer, blobs_topic, oom_settings
):
    """The control for the test above: uncapped, the same close announces itself.

    Without it, "no fact on the stream" is not evidence the cap refused one — it
    is equally consistent with a reporter that never publishes at all.
    """

    message = await deliver(broker, consumer, topic, command_id="cmd-announced")

    outcome = await process_message(
        message,
        client=broker.client,
        consumer=consumer,
        group=GROUP,
        handler=refusing_handler,
        settings=oom_settings,
        reporter=build_failure_reporter(client=broker.client, blobs_topic=blobs_topic),
        spec=FETCH_SPEC,
    )

    assert outcome is Outcome.DEAD_LETTERED
    (fact,) = await decoded_facts(broker.client, blobs_topic)
    assert isinstance(fact, FetchFailedEvent)
    assert fact.command_id == "cmd-announced"
