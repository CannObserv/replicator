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
from src.storage.gcs import GcsBlobStore

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

# The temp-blob destination (#7), and deliberately **not** the same bucket as the
# replicate one. The grants are opposites: the replicate SA holds no `delete` at
# all, which is what puts T4's "never overwrite, never delete" at IAM rather than
# only in our code, while a temp store exists to have its objects expire. One
# bucket serving both means either the temp store cannot reap or the permanent
# store can be erased — and only one of those two failures is recoverable.
#
# No default, for the reason `TEST_BUCKET_ENV` has none: absent means skip, never
# "use whatever the code would have picked".
TEST_BLOB_BUCKET_ENV = "REPLICATOR_TEST_BLOB_BUCKET"


def resolve_test_bucket(env: Mapping[str, str]) -> str | None:
    """The configured test bucket, or ``None``. Deliberately not a lookup with a default."""
    return env.get(TEST_BUCKET_ENV)


def resolve_test_blob_bucket(env: Mapping[str, str]) -> str | None:
    """The configured temp-blob test bucket, or ``None``. No default, same rule (#7)."""
    return env.get(TEST_BLOB_BUCKET_ENV)


def guarded_init(
    original,
    expected: str | None,
    *,
    label: str = "AsyncGcsDriver",
    env_name: str = TEST_BUCKET_ENV,
    marked: bool = False,
):
    """Wrap a bucket-taking ``__init__`` so a wrong bucket is refused before ADC.

    Two constructors are wrapped by this now — ``AsyncGcsDriver`` (replicate
    destinations) and ``GcsBlobStore`` (#7's temp store) — which is why ``label``
    and ``env_name`` are parameters. A refusal that named the wrong class, or
    sent an operator to the wrong environment variable, is a refusal that costs
    more than it saves.

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
        if kwargs.get("client") is not None:
            # An injected client is the whole reason this guard exists, inverted:
            # what it refuses is a constructor that resolves ADC *in its own
            # body* and reaches a bucket by name. A caller that supplies the
            # client has already made that impossible — there is no credential to
            # resolve and no bucket to reach except the one the client offers —
            # and a test cannot build a real client here anyway, because the
            # scrub above leaves no identity for `storage.Client()` to find.
            #
            # Without this, `tests/storage/test_gcs.py` would have to be marked
            # `gcs` to test decisions that touch no network, which is precisely
            # the mark losing its meaning.
            return original(self, *args, **kwargs)
        if expected is None and marked:
            # Marked, so the intent was legitimate; the host just has not
            # provisioned *this* destination. Distinguished from the unmarked
            # refusal because the remedies are opposite — one is a code change,
            # the other an operator one — and the two destinations are
            # provisioned independently, so a host with one and not the other is
            # an ordinary state rather than a broken one.
            raise AssertionError(
                f"{label}({bucket!r}) needs {env_name}, which is unset — depend on "
                f"the bucket fixture, which skips instead of failing"
            )
        if expected is None:
            raise AssertionError(
                f"a test that is not marked @pytest.mark.gcs constructed a real "
                f"{label}({bucket!r}) — mark it, or use a stub"
            )
        if bucket != expected:
            raise AssertionError(
                f"refusing {label}({bucket!r}): a gcs test may only reach "
                f"{expected!r}, the bucket named by {env_name}"
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

    A ``@pytest.mark.gcs`` test opts back in explicitly: it gets the test
    identity, and whichever of the two test buckets the host has provisioned,
    all from variables with no default. A missing *identity* skips here; a
    missing *bucket* skips in the fixture that hands it over, because the
    replicate destination and #7's temp-blob destination are provisioned
    independently and a host with one should still run its tests.

    Nothing in the tree reaches a real driver today — ``test_main_writers.py``
    stubs it — but that is an accident of how those tests are written rather than
    a property anyone asserted, and it is the accident #38 objects to.

    **What this does not cover, and what covers it instead** (CR #8). The patch
    is on ``AsyncGcsDriver``; a test reaching for ``google.cloud.storage``
    directly goes around it. Unmarked, the scrub above is what stops it — with no
    ``GOOGLE_APPLICATION_CREDENTIALS`` there is no identity to resolve. Marked,
    it has one, and the thing standing between it and production is **IAM**: the
    test SA holds ``objectAdmin`` on the test bucket and no write at all on the
    production one (docs/DEPLOYMENT.md names both). That is deliberate rather than
    residual — a fixture is a promise this repo makes to itself, and the grant is
    the one a mistake cannot talk its way past. ``test_replicate_writer_gcs.py``
    uses the raw client for exactly this reason: its assertions must not run
    through the driver they are checking.
    """
    for name in PRODUCTION_ENV:
        monkeypatch.delenv(name, raising=False)

    marked = request.node.get_closest_marker("gcs") is not None
    expected_driver = expected_store = None
    if marked:
        credentials = os.environ.get(TEST_CREDENTIALS_ENV)
        if not credentials:
            pytest.skip(f"{TEST_CREDENTIALS_ENV} is unset — no test identity to write as (#50)")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", credentials)
        # Resolved independently, and neither absence skips here: the two
        # destinations are provisioned separately (#7 adds the second), so a host
        # with one and not the other must still run the tests for the one it has.
        # The per-bucket skip belongs to the fixture a test actually depends on.
        expected_driver = resolve_test_bucket(os.environ)
        expected_store = resolve_test_blob_bucket(os.environ)

    monkeypatch.setattr(
        AsyncGcsDriver,
        "__init__",
        guarded_init(AsyncGcsDriver.__init__, expected_driver, marked=marked),
    )
    monkeypatch.setattr(
        GcsBlobStore,
        "__init__",
        guarded_init(
            GcsBlobStore.__init__,
            expected_store,
            label="GcsBlobStore",
            env_name=TEST_BLOB_BUCKET_ENV,
            marked=marked,
        ),
    )


@pytest.fixture
def gcs_bucket(request) -> str:
    """The provisioned replicate test bucket, for a ``@pytest.mark.gcs`` test.

    The credential swap already happened in the autouse fixture above. The skip
    lives here rather than there because #7 added a second destination with its
    own variable: skipping every marked test when *either* is unset would tie
    two provisionings that have nothing to do with each other.

    A test still never reads the environment itself, and so still cannot read it
    with a fallback.
    """
    assert request.node.get_closest_marker("gcs"), "gcs_bucket requires @pytest.mark.gcs"
    bucket = resolve_test_bucket(os.environ)
    if not bucket:
        pytest.skip(f"{TEST_BUCKET_ENV} is unset — no test bucket to write to (#50)")
    return bucket


@pytest.fixture
def gcs_blob_bucket(request) -> str:
    """The provisioned temp-blob test bucket, for a ``@pytest.mark.gcs`` test (#7).

    Its twin above, one destination over. Separate because the grants are
    opposites — see ``TEST_BLOB_BUCKET_ENV``.
    """
    assert request.node.get_closest_marker("gcs"), "gcs_blob_bucket requires @pytest.mark.gcs"
    bucket = resolve_test_blob_bucket(os.environ)
    if not bucket:
        pytest.skip(f"{TEST_BLOB_BUCKET_ENV} is unset — no temp-blob bucket to write to (#7)")
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
