"""Shared test fixtures — HTTP client and a fake Redis broker (no database).

Replicator is DB-free: its durable state is the Redis consumer group's pending
entries list plus content-addressed blobs on disk, so there is no engine,
session, or savepoint machinery here.
"""

import logging
import os
from collections.abc import AsyncGenerator
from urllib.parse import urlparse

import pytest
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.core.config import get_settings

# Scratch database for live-broker tests. Deliberately not db 0 — see real_redis.
TEST_REDIS_URL = os.environ.get("REPLICATOR_TEST_REDIS_URL", "redis://localhost:6379/15")


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

    Skips rather than fails when no broker answers, so ``-m integration`` stays
    runnable off the VM.
    """
    if urlparse(TEST_REDIS_URL).path in ("", "/", "/0"):
        pytest.fail(f"REPLICATOR_TEST_REDIS_URL must not target db 0 (got {TEST_REDIS_URL})")

    client = Redis.from_url(TEST_REDIS_URL)
    try:
        await client.ping()
    except (RedisError, OSError) as exc:
        await client.aclose()
        pytest.skip(f"live Redis unavailable at {TEST_REDIS_URL}: {exc}")

    try:
        yield client
    finally:
        await client.aclose()
