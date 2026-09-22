"""A fetch's whole wall time, bounded end to end (#104).

httpx applies a timeout per *operation* — connect, pool, write, and each read —
so the one a command carries never bounded a fetch: a body trickling in under the
read timeout, or a chain of hops each inside it, held the serial consume path as
long as the origin kept going. ``REPLICATOR_MAX_FETCH_SECONDS`` is the bound, and
it sits around the whole of ``_fetch``: the destination guard's resolve, every
hop, and every read.

The trickling origin here is a real socket, not a mock, because the premise is a
claim about httpx's timeout semantics and only a real read can show it.
"""

import asyncio
import time
from collections.abc import AsyncGenerator

import httpx
import pytest
from co_core_aio.fetch import AsyncFetchDriver

from src.core.config import get_settings
from src.core.errors import TransientFetchError
from src.storage.local import LocalBlobStore
from src.worker.egress import GuardedTransport, blocked_networks
from src.worker.handler import build_handler
from tests.worker.conftest import FakeFetcher, command, published_facts

# The whole-fetch deadline these tests run under, and the per-operation timeout
# the command asks for: far apart, so which of the two fired is never a race.
DEADLINE_SECONDS = 0.5
READ_TIMEOUT_SECONDS = 3.0

# The trickle: one byte per interval, every interval well inside the read timeout,
# for a body whose whole delivery takes far longer than the deadline.
TRICKLE_INTERVAL_SECONDS = 0.1
TRICKLE_BYTES = 40


class SlowFetcher(FakeFetcher):
    """A fetch that takes ``seconds`` to answer, and records whether it finished."""

    def __init__(self, seconds: float) -> None:
        super().__init__()
        self._seconds = seconds
        self.finished = False

    async def execute(self, effect):
        await asyncio.sleep(self._seconds)
        self.finished = True
        return await super().execute(effect)


class RaisingFetcher(FakeFetcher):
    """A fetch that fails with a builtin ``TimeoutError`` of its own, at once."""

    async def execute(self, effect):
        raise TimeoutError("the fetcher's own timeout, not the deadline")


@pytest.fixture
def deadline_settings():
    return get_settings().model_copy(update={"max_fetch_seconds": DEADLINE_SECONDS})


@pytest.fixture
def bounded(fake_redis, tmp_path, deadline_settings):
    """The real handler under a short whole-fetch deadline."""

    def build(fetcher):
        return build_handler(
            fetcher=fetcher,
            store=LocalBlobStore(tmp_path),
            client=fake_redis,
            settings=deadline_settings,
        )

    return build


async def test_a_fetch_past_the_deadline_is_abandoned_transiently(bounded, fake_redis):
    """Transient, like any timeout: the command retries at the reclaim cadence.

    Not a new terminal reason. A slow origin now may be a fast one later, and a
    fetch that genuinely needs longer is the operator's to allow — the deadline is
    theirs, not the issuer's, so a fact telling the issuer about it would name a
    number the issuer cannot change.
    """
    fetcher = SlowFetcher(seconds=DEADLINE_SECONDS * 10)
    handler = bounded(fetcher)

    started = time.monotonic()
    with pytest.raises(TransientFetchError, match=f"{DEADLINE_SECONDS}-second fetch deadline"):
        await handler(command())

    assert time.monotonic() - started < DEADLINE_SECONDS * 4
    assert not fetcher.finished
    assert await published_facts(fake_redis) == []


async def test_a_fetch_inside_the_deadline_is_untouched(bounded, fake_redis):
    handler = bounded(SlowFetcher(seconds=DEADLINE_SECONDS / 10))

    await handler(command())

    assert len(await published_facts(fake_redis)) == 1


async def test_a_timeout_that_is_not_the_deadline_passes_through_as_itself(bounded):
    """Only the deadline's own expiry is renamed; anything else keeps its type.

    A builtin ``TimeoutError`` is transient in the loop already, so this is about
    the message rather than the fate: a journal line blaming the deadline for a
    fetcher's own timeout would send an operator to raise the wrong number.
    """
    handler = bounded(RaisingFetcher())

    with pytest.raises(TimeoutError, match="the fetcher's own timeout"):
        await handler(command())


@pytest.fixture
async def trickling_origin() -> AsyncGenerator[str]:
    """A local origin that declares a body and then sends it one byte at a time."""

    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % TRICKLE_BYTES)
            for _ in range(TRICKLE_BYTES):
                await writer.drain()
                await asyncio.sleep(TRICKLE_INTERVAL_SECONDS)
                writer.write(b"x")
            await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/slow"
    finally:
        server.close()
        await server.wait_closed()


async def test_a_trickling_origin_cannot_hold_the_worker_past_the_deadline(
    trickling_origin, bounded
):
    """#104's measurement, bounded: every read inside its timeout, the fetch not.

    Over a plain client, since the guard refuses loopback — which is the point of
    the guard, and not what this is about. Unbounded, this body takes
    ``TRICKLE_BYTES * TRICKLE_INTERVAL_SECONDS`` (4 s) and succeeds.
    """
    async with httpx.AsyncClient() as client:
        handler = bounded(AsyncFetchDriver(client))

        started = time.monotonic()
        with pytest.raises(TransientFetchError, match="fetch deadline"):
            await handler(command(url=trickling_origin, timeout_seconds=READ_TIMEOUT_SECONDS))

    assert time.monotonic() - started < READ_TIMEOUT_SECONDS


async def test_the_deadline_covers_the_guards_resolve(bounded):
    """The resolve runs inside the deadline, so the stop budget need not add it again.

    The guard caps a resolve at ``RESOLVE_TIMEOUT_SECONDS`` on its own; a slower
    one here shows the deadline reaches it first, which is what lets
    ``tests/test_deploy.py`` sum one fetch term rather than a fetch term plus a
    resolve term per hop.
    """

    async def stalled(host: str, port: int) -> list[str]:
        await asyncio.sleep(DEADLINE_SECONDS * 10)
        return ["93.184.216.34"]

    def never_reached(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("the request went out after a resolve the deadline abandoned")

    transport = GuardedTransport(
        httpx.MockTransport(never_reached), blocked=blocked_networks(None), resolve=stalled
    )
    async with httpx.AsyncClient(transport=transport) as client:
        handler = bounded(AsyncFetchDriver(client))

        with pytest.raises(TransientFetchError, match="fetch deadline"):
            await handler(command())
