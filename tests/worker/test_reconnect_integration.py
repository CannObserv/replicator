"""The worker resumes consuming after the broker goes away and comes back (#94).

**This is the rehearsal CannObserv/broker#94 asked for**, and it exists because
the property it checks had only ever been asserted against a fake. The fakes in
`tests/worker/test_loop_resilience.py` raise a synthetic `ConnectionError` from a
monkeypatched `read`; the 2026-09-16 outage produced `redis.exceptions.TimeoutError:
Timeout connecting to server` out of the real connection stack, several frames
lower, and the difference between those two is the whole reason a rehearsal was
asked for rather than another unit test.

Every test here drives a `redis-server` this module **spawns**, on a loopback
port of its own, and stops and restarts it mid-test. Never the shared broker:
stopping `co-broker` would take down three services, and the fixture checks the
server's own reported pid before it touches anything, the way
`test_oom_integration.py` does.

**The AOF is on, deliberately.** A restart that loses the stream and the group is
not the incident being modelled — the broker came back at 15:26:34 with its AOF
intact, so the group survived and only the connection had to be remade. An
instance with `--appendonly no` would model a much rarer failure and would
silently turn every test here into an assertion about `NOGROUP`.

**What this file does not cover, and cannot.** systemd's restart semantics are
where the 2026-09-16 incident was actually lost: the worker exited as designed,
and three `ExecStartPre` refusals then spent the start limit. A pytest cannot
model a unit. `scripts/rehearse_reconnect.sh` drives that half against real
systemd; this file owns the in-process half and the shape of the handoff between
them — that a worker started fresh against a recovered broker consumes without a
human step.
"""

import asyncio
import shutil
import socket
import time
from collections.abc import AsyncGenerator

import pytest
from co_core.pure.models.changes import ContentFetchCommand
from redis.asyncio import Redis

from src.core.config import Settings, get_settings
from src.worker.loop import FETCH_SPEC, run_loop
from src.worker.main import build_consumer
from tests.worker.conftest import GROUP, TOPIC, collected_reports, make_command, noop_handler

pytestmark = pytest.mark.integration

# How long the spawned broker gets to answer its first PING, and how long a
# terminate gets before it is killed.
STARTUP_TIMEOUT_SECONDS = 10.0
SHUTDOWN_TIMEOUT_SECONDS = 5.0
POLL_INTERVAL_SECONDS = 0.05

# The loop settings these tests run under. Deliberately tiny: the property is
# "does it come back", not "does it wait the production interval", and a
# rehearsal that took the production 10 minutes to fail would not be run.
FAST_BACKOFF_BASE = 0.01
FAST_BACKOFF_MAX = 0.05

# Two ceilings, because the two halves of the property need opposite ones and a
# single value cannot serve both. A refused connection fails in microseconds, so
# the ceiling — not the wall clock — is what decides whether an outage is
# survivable: at N failures the loop has slept only ~N x FAST_BACKOFF_MAX before
# giving up. RIDE_OUT_CEILING buys ~10s of tolerance so a 0.5s outage is
# unambiguously inside it; GIVE_UP_CEILING exits in well under a second so the
# sustained-outage tests do not pad the suite.
RIDE_OUT_CEILING = 200
GIVE_UP_CEILING = 4

# The outage the ride-out test opens. Comfortably inside RIDE_OUT_CEILING's
# tolerance and comfortably longer than one backoff, so the loop provably failed
# several cycles rather than never noticing.
SURVIVABLE_OUTAGE_SECONDS = 0.5


def _command_ids():
    """Distinct command ids — the dedupe keys are per command_id, so a reused
    one would be skipped as already handled and read as "never consumed"."""
    n = 0
    while True:
        n += 1
        yield f"cmd-rehearsal-{n}"


_ids = _command_ids()


def _free_port() -> int:
    """A loopback port nothing is listening on, at the moment it is asked for."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class RestartableBroker:
    """A `redis-server` on a fixed port that a test can stop and start again.

    The port is chosen once and reused across restarts — that is the point. A
    client built against it keeps its URL, so what the test exercises is
    redis-py remaking a connection to an address that went away and came back,
    which is what happened on 2026-09-16.
    """

    def __init__(self, port: int, workdir) -> None:
        self.port = port
        self.workdir = workdir
        self.url = f"redis://127.0.0.1:{port}/0"
        self._process: asyncio.subprocess.Process | None = None

    async def start(self) -> None:
        """Spawn the server and block until it answers as itself."""
        assert self._process is None, "already running"
        self._process = await asyncio.create_subprocess_exec(
            "redis-server",
            "--port",
            str(self.port),
            "--bind",
            "127.0.0.1",
            # State must survive the restart — see the module docstring.
            "--appendonly",
            "yes",
            "--dir",
            str(self.workdir),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        client = Redis.from_url(self.url)
        try:
            deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
            while True:
                try:
                    await client.execute_command("PING")
                    break
                except Exception:  # anything but an answer means "not up yet"
                    if time.monotonic() > deadline or self._process.returncode is not None:
                        raise AssertionError(
                            f"the scratch broker on port {self.port} never answered "
                            f"(exit={self._process.returncode})"
                        ) from None
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
            # The same identity check test_oom_integration.py makes, for the same
            # reason: binding a free port is a race, and losing it means stopping
            # somebody else's server rather than failing.
            server = await client.info("server")
            assert server["process_id"] == self._process.pid, (
                f"port {self.port} is answered by pid {server['process_id']}, not "
                f"the {self._process.pid} this fixture started — refusing to stop "
                f"a broker this test does not own"
            )
        finally:
            await client.aclose()

    async def stop(self) -> None:
        """Terminate the server and wait until the port refuses connections."""
        if self._process is None:
            return
        process, self._process = self._process, None
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except TimeoutError:
                process.kill()
                await process.wait()
        # Returning before the socket is actually closed would let the test's
        # first "outage" read succeed, which is the one thing it must not do.
        deadline = time.monotonic() + SHUTDOWN_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            with socket.socket() as probe:
                probe.settimeout(POLL_INTERVAL_SECONDS)
                if probe.connect_ex(("127.0.0.1", self.port)) != 0:
                    return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        raise AssertionError(f"port {self.port} still accepts connections after stop()")


@pytest.fixture
async def outage_broker(tmp_path_factory) -> AsyncGenerator[RestartableBroker]:
    """A spawned broker this test owns, running, cleaned up however it ends."""
    if shutil.which("redis-server") is None:
        pytest.skip("redis-server is not on PATH — nothing to take down and bring back")

    broker = RestartableBroker(_free_port(), tmp_path_factory.mktemp("reconnect-broker"))
    await broker.start()
    try:
        yield broker
    finally:
        await broker.stop()


def _settings(give_up_after: int) -> Settings:
    """Production settings with the outage knobs shrunk to test speed.

    `give_up_after` is a parameter, not a constant, because the two halves of
    the property need opposite values — see the ceilings above.
    """
    get_settings.cache_clear()
    return get_settings().model_copy(
        update={
            "error_backoff_base_seconds": FAST_BACKOFF_BASE,
            "error_backoff_max_seconds": FAST_BACKOFF_MAX,
            "max_consecutive_cycle_failures": give_up_after,
            "consumer_group": GROUP,
            "consumer_name": "replicator@rehearsal",
        }
    )


async def _publish(client: Redis, topic: str, frame: dict[str, str]) -> None:
    """Put one well-formed wire frame on the stream."""
    await client.xadd(topic, frame)


async def _run_until(consumed: asyncio.Event, task: asyncio.Task, deadline: float) -> None:
    """Wait for `consumed`, failing with the loop's own error if it died first.

    Racing the loop task against the event rather than just awaiting the event:
    a loop that died reports its own exception here, instead of this timing out
    and blaming the deadline for a broker error several frames down.
    """
    waiter = asyncio.ensure_future(consumed.wait())
    done, _ = await asyncio.wait(
        {waiter, task}, timeout=deadline, return_when=asyncio.FIRST_COMPLETED
    )
    if task in done:
        waiter.cancel()
        raise AssertionError(f"the loop exited before consuming: {task.exception()!r}")
    if not done:
        waiter.cancel()
        raise AssertionError("nothing was consumed before the deadline")
    waiter.cancel()


@pytest.fixture
def consumed_commands() -> tuple[list[str], object]:
    """A handler that records what it was given and flags an event."""
    seen: list[str] = []
    event = asyncio.Event()

    async def record(command: ContentFetchCommand) -> None:
        seen.append(command.command_id)
        event.set()

    return seen, (record, event)


async def test_the_loop_rides_out_an_outage_and_consumes_again(outage_broker, consumed_commands):
    """The short-outage half: the broker goes away and back, the worker never exits.

    This is the 2026-09-10 shape — a 2m23s restart — asserted for the first time
    against a socket that really closes.
    """
    seen, (record, event) = consumed_commands
    settings = _settings(RIDE_OUT_CEILING)
    client = Redis.from_url(outage_broker.url)
    consumer = build_consumer(client, settings)
    await consumer.ensure_group(start_id="0")

    stop = asyncio.Event()
    task = asyncio.ensure_future(
        run_loop(
            client=client,
            consumer=consumer,
            group=GROUP,
            settings=settings,
            handler=record,
            reporter=collected_reports(),
            spec=FETCH_SPEC,
            stop=stop,
        )
    )
    try:
        # Healthy first, so a later success cannot be mistaken for "it never broke".
        await _publish(client, TOPIC, make_command(command_id=next(_ids)))
        await _run_until(event, task, deadline=10)
        assert len(seen) == 1, seen

        await outage_broker.stop()
        # Long enough for several cycles to fail, short of the give-up ceiling.
        await asyncio.sleep(SURVIVABLE_OUTAGE_SECONDS)
        assert not task.done(), f"the loop exited during a survivable outage: {task}"

        await outage_broker.start()
        event.clear()
        await _publish(client, TOPIC, make_command(command_id=next(_ids)))
        await _run_until(event, task, deadline=10)

        assert len(seen) == 2, seen
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.aclose()


async def test_a_sustained_outage_exits_so_the_unit_can_restart(outage_broker):
    """The long-outage half: the loop gives up rather than looking alive and idle.

    The exit is the design and it stays — a wedged worker must read as `failed`,
    not as `active (running)` consuming nothing. What #94 showed is that the exit
    is only half a mechanism; the other half is the unit restarting into a broker
    that came back, which the next test covers.
    """
    settings = _settings(GIVE_UP_CEILING)
    client = Redis.from_url(outage_broker.url)
    consumer = build_consumer(client, settings)
    await consumer.ensure_group(start_id="0")

    await outage_broker.stop()

    with pytest.raises(Exception) as caught:
        async with asyncio.timeout(20):
            await run_loop(
                client=client,
                consumer=consumer,
                group=GROUP,
                settings=settings,
                handler=noop_handler,
                reporter=collected_reports(),
                spec=FETCH_SPEC,
                stop=asyncio.Event(),
            )
    # Not a TimeoutError from the guard above: the loop must have raised the
    # broker's own error, which is what makes the process exit non-zero and
    # `Restart=on-failure` fire.
    assert not isinstance(caught.value, TimeoutError), "the loop hung instead of exiting"
    await client.aclose()


async def test_a_restarted_worker_resumes_with_no_human_step(outage_broker, consumed_commands):
    """**The property CannObserv/broker#94 asked for.**

    An outage longer than the exit threshold, then the broker returns, then the
    worker is started again exactly as `Restart=on-failure` would start it — a
    fresh client, a fresh consumer, the same group — and consumption resumes with
    nobody typing anything.

    The command is published *while the broker is down on the far side of the
    restart*, so what is asserted is not "a fresh worker can read a fresh stream"
    but "the work that arrived during the outage is picked up", which is what was
    stranded on 2026-09-16.
    """
    seen, (record, event) = consumed_commands
    settings = _settings(GIVE_UP_CEILING)

    # The first worker: run it into the ground against a broker that is gone.
    first_client = Redis.from_url(outage_broker.url)
    first_consumer = build_consumer(first_client, settings)
    await first_consumer.ensure_group(start_id="0")
    await outage_broker.stop()

    with pytest.raises(Exception):
        async with asyncio.timeout(20):
            await run_loop(
                client=first_client,
                consumer=first_consumer,
                group=GROUP,
                settings=settings,
                handler=record,
                reporter=collected_reports(),
                spec=FETCH_SPEC,
                stop=asyncio.Event(),
            )
    await first_client.aclose()
    assert seen == [], "nothing should have been consumed while the broker was down"

    # The broker comes back with its AOF, and a command is waiting.
    await outage_broker.start()
    seeding = Redis.from_url(outage_broker.url)
    stranded = "cmd-stranded"
    await _publish(seeding, TOPIC, make_command(command_id=stranded))
    await seeding.aclose()

    # systemd's restart, modelled: a brand-new process against the same group.
    second_client = Redis.from_url(outage_broker.url)
    second_consumer = build_consumer(second_client, settings)
    await second_consumer.ensure_group(start_id="0")
    stop = asyncio.Event()
    task = asyncio.ensure_future(
        run_loop(
            client=second_client,
            consumer=second_consumer,
            group=GROUP,
            settings=settings,
            handler=record,
            reporter=collected_reports(),
            spec=FETCH_SPEC,
            stop=stop,
        )
    )
    try:
        await _run_until(event, task, deadline=15)
        assert seen == [stranded], seen
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await second_client.aclose()
