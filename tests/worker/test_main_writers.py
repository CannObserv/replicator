"""Provider writers at startup: one per binding, and none of them fatal (#29).

Split from ``test_main.py`` by concern rather than size: everything here is about
the window between reading the alias table and entering the loop, where a
credential is resolved for the first time and a misconfiguration is most likely.

Three properties, and the first is the one that had a bug. A driver **is** a
bucket — ``AsyncGcsDriver`` takes one in its constructor and never sees another —
so the collection of them has to be keyed by whatever selects a bucket. Keying it
by *provider* collapsed every ``gcs`` binding onto one entry, which meant an alias
could write into a bucket it was never bound to: the T3 containment check runs on
the prefix, and nothing downstream re-checks the bucket the driver already holds.
Invisible with one alias provisioned, which is every host today (CR #26).
"""

import json

import pytest

import src.worker.main
from src.worker.aliases import AliasBinding, AliasTable
from src.worker.main import build_writers, run
from tests.worker.test_main import _stopped


@pytest.fixture
def alias_file(tmp_path):
    """Write an alias table and point the settings at it."""

    def write(table: dict) -> str:
        path = tmp_path / "aliases.json"
        path.write_text(json.dumps(table))
        return str(path)

    return write


class StubDriver:
    """Records the bucket it was built for, and whether it was closed."""

    built: list["StubDriver"] = []

    def __init__(self, bucket, **kwargs):
        self.bucket = bucket
        self.closed = False
        StubDriver.built.append(self)

    async def create_if_absent(self, effect):
        raise AssertionError("no write expected in these tests")

    async def aclose(self):
        self.closed = True


@pytest.fixture(autouse=True)
def _fresh_builds():
    StubDriver.built = []
    yield
    StubDriver.built = []


@pytest.fixture
def wired(monkeypatch, fake_redis, tmp_path):
    """``run()`` with a fake broker, a stub driver, and the handler's kwargs captured."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    monkeypatch.setattr("src.worker.main.AsyncGcsDriver", StubDriver)

    captured: dict = {}
    real = src.worker.main.build_replicate_handler

    def capture(**kwargs):
        captured.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr("src.worker.main.build_replicate_handler", capture)
    return captured


async def test_two_gcs_aliases_get_one_driver_each(monkeypatch, alias_file, wired):
    """CR #26. Keyed by alias, because the alias is what names a bucket.

    Provisioned together, these two used to collapse to a single ``{"gcs": ...}``
    entry — so a command naming ``public`` reached the driver holding
    ``co-gcs-internal`` and wrote outside the root its binding declared. The
    second driver was also never closed, because it was no longer in the dict the
    shutdown path iterates.
    """
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "public": {"provider": "gcs", "bucket": "co-gcs-replication"},
                "private": {"provider": "gcs", "bucket": "co-gcs-internal"},
            }
        ),
    )

    await run(_stopped())

    writers = wired["writers"]
    assert {alias: writer.bucket for alias, writer in writers.items()} == {
        "public": "co-gcs-replication",
        "private": "co-gcs-internal",
    }


async def test_every_driver_built_is_a_driver_closed(monkeypatch, alias_file, wired):
    """The other half of the same bug: a driver dropped from the dict leaks its
    HTTP session, because the shutdown path iterates the dict and not the builds."""
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "public": {"provider": "gcs", "bucket": "co-gcs-replication"},
                "private": {"provider": "gcs", "bucket": "co-gcs-internal"},
            }
        ),
    )

    await run(_stopped())

    assert len(StubDriver.built) == 2
    assert all(driver.closed for driver in StubDriver.built)


async def test_a_driver_that_cannot_be_built_does_not_stop_the_worker(
    monkeypatch, alias_file, wired, capsys
):
    """CR #29. ``storage.Client()`` resolves ADC in the constructor and raises.

    ``load_alias_table`` promises in as many words that a replicate
    misconfiguration must not take down a worker whose actual job is
    ``content.fetch``, and goes to real lengths to keep that true. Building the
    driver one line later must not undo it: an expired key file would otherwise
    stop the fetch path too.
    """

    def refuse(bucket, **kwargs):
        raise RuntimeError("could not automatically determine credentials")

    monkeypatch.setattr("src.worker.main.AsyncGcsDriver", refuse)
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file({"public": {"provider": "gcs", "bucket": "co-gcs-replication"}}),
    )

    await run(_stopped())

    assert wired["writers"] == {}
    assert "could not build a provider writer" in capsys.readouterr().out


async def test_one_unbuildable_driver_does_not_cost_the_others(monkeypatch, alias_file, wired):
    """Per binding, not all-or-nothing — the same shape ``load_alias_table`` uses
    for one unusable entry in an otherwise readable table."""

    def selective(bucket, **kwargs):
        if bucket == "co-gcs-internal":
            raise RuntimeError("no credentials for this one")
        return StubDriver(bucket)

    monkeypatch.setattr("src.worker.main.AsyncGcsDriver", selective)
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "public": {"provider": "gcs", "bucket": "co-gcs-replication"},
                "private": {"provider": "gcs", "bucket": "co-gcs-internal"},
            }
        ),
    )

    await run(_stopped())

    assert list(wired["writers"]) == ["public"]


async def test_a_binding_for_a_provider_with_no_driver_builds_nothing():
    """``gdrive`` and ``ia`` have no conditional create, so there is nothing to
    build for them — and building nothing is what makes the handler's refusal
    ``provider_disabled`` rather than a crash. Constructed directly because
    ``load_alias_table`` will not admit a provider outside ``KNOWN_PROVIDERS``;
    the guard has to hold anyway, since that tuple is what grows first when a
    provider lands."""
    table = AliasTable({"drive": AliasBinding(provider="gdrive")})

    assert build_writers(table) == {}
    assert StubDriver.built == []


async def test_a_writer_that_fails_to_close_does_not_leak_the_redis_client(
    monkeypatch, alias_file, wired, fake_redis, capsys
):
    """CR #30. The close loop runs *before* ``client.aclose()``.

    Unguarded, one raising driver skipped every writer after it and left the
    Redis client open — a shutdown path where the first failure costs every
    release that follows it.
    """

    class Unclosable(StubDriver):
        async def aclose(self):
            raise RuntimeError("the transport was already gone")

    def build(bucket, **kwargs):
        return Unclosable(bucket) if bucket == "co-gcs-internal" else StubDriver(bucket)

    monkeypatch.setattr("src.worker.main.AsyncGcsDriver", build)
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "private": {"provider": "gcs", "bucket": "co-gcs-internal"},
                "public": {"provider": "gcs", "bucket": "co-gcs-replication"},
            }
        ),
    )

    closed: list[bool] = []
    original = fake_redis.aclose

    async def record():
        closed.append(True)
        await original()

    monkeypatch.setattr(fake_redis, "aclose", record)

    await run(_stopped())

    assert closed == [True]
    assert any(w.closed for w in StubDriver.built if not isinstance(w, Unclosable))
    assert "failed to close a provider writer" in capsys.readouterr().out
