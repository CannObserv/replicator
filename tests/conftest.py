"""Shared test fixtures — HTTP client and a fake Redis broker (no database).

Replicator is DB-free: its durable state is the Redis consumer group's pending
entries list plus content-addressed blobs on disk, so there is no engine,
session, or savepoint machinery here.
"""

import logging
import os
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.core.config import get_settings

# Scratch database for live-broker tests. Deliberately not db 0 — see real_redis.
DEFAULT_TEST_REDIS_URL = "redis://localhost:6379/15"

# Keys a crashed run left behind get a TTL rather than an immediate delete: long
# enough that a concurrent run's stream is never pulled out from under it.
LEFTOVER_TTL_SECONDS = 300


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Drop the lru_cache so per-test monkeypatched env is actually read."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient]:
    """AsyncClient wired to the app, with its lifespan actually run.

    `ASGITransport` does not drive startup/shutdown, so wrapping the client in
    `lifespan(app)` is what keeps API tests honest — otherwise every route is
    exercised against an app that never started, and a lifespan that raises on
    every real boot leaves the suite green (CR #13).

    Root logging is saved and restored because startup installs a handler
    globally; without this, the first API test would reconfigure logging for
    everything that follows it.
    """
    from src.api.main import app, lifespan

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        async with lifespan(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                yield c
    finally:
        root.handlers, root.level = saved_handlers, saved_level


@pytest.fixture
async def fake_redis() -> AsyncGenerator:
    """In-memory Redis standing in for the Archiver-operated broker.

    Streams-capable, so consumer-group calls exercise real command semantics
    without a live server. Integration tests that need the genuine article are
    marked ``@pytest.mark.integration`` and excluded by default.
    """
    from fakeredis.aioredis import FakeRedis

    client = FakeRedis(decode_responses=False)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def real_redis() -> AsyncGenerator:
    """A live Redis client on a scratch database — ``@pytest.mark.integration`` only.

    Some properties are about *when* Redis acts, not what state results, and
    fakeredis diverges on exactly those (GH #3). Those assertions need the real
    server, which on this VM is the Archiver-operated broker.

    **Never db 0.** That database carries the live ``content.fetch`` stream the
    running ``replicator.service`` is consuming, so a test frame written there
    would be fetched for real. Tests using this fixture confine themselves to
    scratch stream keys as well — the database guard is the backstop, not the
    plan. Point ``REPLICATOR_TEST_REDIS_URL`` elsewhere to use another server.

    The guard reads the db redis-py *resolved*, not the one the URL path
    implies: a ``?db=0`` query parameter overrides the path, and a unix-socket
    URL has no path to inspect at all (CR #1). Missing means 0 — redis-py's own
    default.

    Skips rather than fails when no broker answers, so ``-m integration`` stays
    runnable off the VM.
    """
    url = os.environ.get("REPLICATOR_TEST_REDIS_URL", DEFAULT_TEST_REDIS_URL)
    client = Redis.from_url(url)

    db = client.connection_pool.connection_kwargs.get("db", 0)
    if db == 0:
        await client.aclose()
        pytest.fail(f"REPLICATOR_TEST_REDIS_URL must not target db 0 (got {url} -> db {db})")

    try:
        # execute_command rather than ping(): redis-py types ping()'s return as
        # the sync/async union, which `ty` reports as a non-awaitable.
        await client.execute_command("PING")
    except (RedisError, OSError) as exc:
        await client.aclose()
        pytest.skip(f"live Redis unavailable at {url}: {exc}")

    try:
        await _expire_leftovers(client)
        yield client
    finally:
        await client.aclose()


async def _expire_leftovers(client: Redis) -> None:
    """Bound the lifetime of scratch keys an abnormally-terminated run left behind.

    Teardown deletes what a passing test made; a SIGKILL between the two leaks a
    stream onto the shared broker forever (CR #5). Expiring rather than deleting
    keeps this safe to run while another session's tests are in flight.
    """
    async for key in client.scan_iter(match="replicator.itest.*"):
        await client.expire(key, LEFTOVER_TTL_SECONDS)
