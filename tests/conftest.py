"""Shared test fixtures — HTTP client, a fake Redis broker, and a live one.

The fake serves the default suite; ``real_redis`` serves ``@pytest.mark.integration``,
where the assertion is about *when* Redis acts rather than what state results.

Replicator is DB-free: its durable state is the Redis consumer group's pending
entries list plus content-addressed blobs on disk, so there is no engine,
session, or savepoint machinery here.
"""

import logging
import os
from collections.abc import AsyncGenerator, Mapping

import pytest
from co_core_aio.gcs import AsyncGcsDriver
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from redis.exceptions import AuthenticationError, AuthorizationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.core.config import get_settings

# Scratch database for live-broker tests. Deliberately not db 0 — see real_redis.
DEFAULT_TEST_REDIS_URL = "redis://localhost:6379/15"

# Keys a crashed run left behind get a TTL rather than an immediate delete: long
# enough that a concurrent run's stream is never pulled out from under it. Sized
# above pytest's `timeout = 300` (pyproject.toml), which is how long a
# concurrent test is permitted to run — change one, revisit the other.
LEFTOVER_TTL_SECONDS = 900

# Production configuration an agent's shell is *told* to carry: AGENTS.md's
# Common Commands snippet sources `/etc/replicator/.env` before any repo command,
# so `uv run pytest` inherits the worker's ADC and — once #50 provisions it — the
# production alias table. Neither has any business reaching a test, and the
# autouse fixture below removes both rather than trusting no test reads them.
PRODUCTION_ENV = ("REPLICATOR_REPLICATION_ALIASES_FILE", "GOOGLE_APPLICATION_CREDENTIALS")

# The test destination, and the identity to reach it with. **Neither has a
# default** (#38, #51): absent means the `@pytest.mark.gcs` tests skip, and never
# means "use whatever the code would have picked" — which on a worker configured
# for production is the production bucket. `REPLICATOR_TEST_REDIS_URL` may
# default because db 15 on localhost cannot be the live database and `real_redis`
# refuses db 0 outright; no bucket name has that property.
TEST_BUCKET_ENV = "REPLICATOR_TEST_GCS_BUCKET"
TEST_CREDENTIALS_ENV = "REPLICATOR_TEST_GCS_CREDENTIALS"


def resolve_test_bucket(env: Mapping[str, str]) -> str | None:
    """The configured test bucket, or ``None``. Deliberately not a lookup with a default."""
    return env.get(TEST_BUCKET_ENV)


def guarded_init(original, expected: str | None):
    """Wrap ``AsyncGcsDriver.__init__`` so a wrong bucket is refused before ADC.

    The refusal has to precede the call through, not follow it: the real
    ``__init__`` builds ``storage.Client()`` in its own body, which reads the key
    file and on a GCE-style host reaches the metadata server. Checking afterwards
    would authenticate first and object second.

    ``expected=None`` means *no* bucket is acceptable — the state every test that
    is not marked ``gcs`` runs in, so a real driver cannot be constructed by
    accident anywhere in the default suite.

    The bucket is read from either call form. `AsyncGcsDriver(bucket=...)` is as
    legal as the positional call, and a guard that inspected ``args[0]`` alone
    would pass every keyword call straight through while reading as though it
    refused them.
    """

    def refuse(self, *args, **kwargs):
        bucket = args[0] if args else kwargs.get("bucket")
        if expected is None:
            raise AssertionError(
                f"a test that is not marked @pytest.mark.gcs constructed a real "
                f"AsyncGcsDriver({bucket!r}) — mark it, or use a stub driver"
            )
        if bucket != expected:
            raise AssertionError(
                f"refusing AsyncGcsDriver({bucket!r}): a gcs test may only reach "
                f"{expected!r}, the bucket named by {TEST_BUCKET_ENV}"
            )
        return original(self, *args, **kwargs)

    return refuse


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _no_production_destination(request, monkeypatch):
    """Production is unreachable from every test, and reachable from none by default.

    Two halves, because the two failure modes are different. The environment
    scrub handles *inheritance* — the production ADC and alias table an agent's
    shell was told to load. The driver patch handles *construction*, which is the
    only way a bucket name becomes a write, and it fires before any credential is
    touched (see ``guarded_init``).

    A ``@pytest.mark.gcs`` test opts back in explicitly: it gets the test bucket
    and the test identity, both from variables with no default, and skips when
    either is absent.

    Nothing in the tree reaches a real driver today — ``test_main_writers.py``
    stubs it — but that is an accident of how those tests are written rather than
    a property anyone asserted, and it is the accident #38 objects to.
    """
    for name in PRODUCTION_ENV:
        monkeypatch.delenv(name, raising=False)

    expected = None
    if request.node.get_closest_marker("gcs"):
        expected = resolve_test_bucket(os.environ)
        if not expected:
            pytest.skip(f"{TEST_BUCKET_ENV} is unset — no test bucket to write to (#50)")
        credentials = os.environ.get(TEST_CREDENTIALS_ENV)
        if not credentials:
            pytest.skip(f"{TEST_CREDENTIALS_ENV} is unset — no test identity to write as (#50)")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", credentials)

    monkeypatch.setattr(AsyncGcsDriver, "__init__", guarded_init(AsyncGcsDriver.__init__, expected))


@pytest.fixture
def gcs_bucket(request) -> str:
    """The provisioned test bucket, for a ``@pytest.mark.gcs`` test.

    The skip and the credential swap already happened in the autouse fixture
    above — this is only the name, so a test does not read the environment
    itself and cannot read it with a fallback.
    """
    assert request.node.get_closest_marker("gcs"), "gcs_bucket requires @pytest.mark.gcs"
    bucket = resolve_test_bucket(os.environ)
    assert bucket
    return bucket


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


@pytest.fixture(scope="session")
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
    runnable off the VM — but an *authentication* failure is a misconfiguration,
    not an absent broker, and is re-raised so it can never masquerade as a clean
    skip (CR #9).

    Session-scoped: the connection and the leftover sweep are per-run work, not
    per-test (CR #12). It is still lazy — the default suite never requests it,
    so no connection is opened when the marker is deselected.
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
    except (AuthenticationError, AuthorizationError):
        # Both subclass redis's ConnectionError, so they must be caught first —
        # a narrowed `except` alone would still swallow them into a skip.
        await client.aclose()
        raise
    except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
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
