"""Shared test fixtures — HTTP client and a fake Redis broker (no database).

Replicator is DB-free: its durable state is the Redis consumer group's pending
entries list plus content-addressed blobs on disk, so there is no engine,
session, or savepoint machinery here.
"""

import logging
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

from src.core.config import get_settings


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
