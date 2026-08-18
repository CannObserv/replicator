"""Provider writers at startup: one per binding, and none of them fatal (#29).

Split from ``test_main.py`` by concern rather than size: everything here is about
the window between reading the alias table and entering the loop, where a
credential is resolved for the first time and a misconfiguration is most likely.

The first property here is the one that had a bug. A driver **is** a
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
    ``example-internal-bucket`` and wrote outside the root its binding declared. The
    second driver was also never closed, because it was no longer in the dict the
    shutdown path iterates.
    """
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "public": {"provider": "gcs", "bucket": "example-replication-bucket"},
                "private": {"provider": "gcs", "bucket": "example-internal-bucket"},
            }
        ),
    )

    await run(_stopped())

    writers = wired["writers"]
    assert {alias: writer.bucket for alias, writer in writers.items()} == {
        "public": "example-replication-bucket",
        "private": "example-internal-bucket",
    }


async def test_every_driver_built_is_a_driver_closed(monkeypatch, alias_file, wired):
    """The other half of the same bug: a driver dropped from the dict leaks its
    HTTP session, because the shutdown path iterates the dict and not the builds."""
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "public": {"provider": "gcs", "bucket": "example-replication-bucket"},
                "private": {"provider": "gcs", "bucket": "example-internal-bucket"},
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
        alias_file({"public": {"provider": "gcs", "bucket": "example-replication-bucket"}}),
    )

    await run(_stopped())

    assert wired["writers"] == {}
    assert "could not build a provider writer" in capsys.readouterr().out


async def test_one_unbuildable_driver_does_not_cost_the_others(monkeypatch, alias_file, wired):
    """Per binding, not all-or-nothing — the same shape ``load_alias_table`` uses
    for one unusable entry in an otherwise readable table."""

    def selective(bucket, **kwargs):
        if bucket == "example-internal-bucket":
            raise RuntimeError("no credentials for this one")
        return StubDriver(bucket)

    monkeypatch.setattr("src.worker.main.AsyncGcsDriver", selective)
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "public": {"provider": "gcs", "bucket": "example-replication-bucket"},
                "private": {"provider": "gcs", "bucket": "example-internal-bucket"},
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
        return Unclosable(bucket) if bucket == "example-internal-bucket" else StubDriver(bucket)

    monkeypatch.setattr("src.worker.main.AsyncGcsDriver", build)
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file(
            {
                "private": {"provider": "gcs", "bucket": "example-internal-bucket"},
                "public": {"provider": "gcs", "bucket": "example-replication-bucket"},
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


# --------------------------------------------------------------------------
# The checkout guard (#52)
# --------------------------------------------------------------------------

# `scripts/check_main_checkout.sh` keeps the *service* off unmerged code, as an
# `ExecStartPre`. A dev worker is started with `uv run python -m src.worker.main`
# and runs no `ExecStartPre` at all — while inheriting, per AGENTS.md's own shell
# snippet, the production ADC and (once #50 provisions it) the production alias
# table. So the question gets asked again here, at the moment a binding becomes a
# live write identity.
#
# Skip, don't raise: the posture two tests above, and for the same reason —
# a replicate misconfiguration must not take down a worker whose job is
# `content.fetch`. The alias ends up with no writer, so the handler refuses it
# `provider_disabled`, which is accurate and names the operator act that fixes it.


async def test_a_refused_checkout_builds_no_writer(monkeypatch, alias_file, wired, capsys):
    monkeypatch.setattr(
        "src.worker.main.checkout_refusal",
        lambda: "check_main_checkout: HEAD is on 'wip', not 'main'",
    )
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file({"public": {"provider": "gcs", "bucket": "example-replication-bucket"}}),
    )

    await run(_stopped())

    assert wired["writers"] == {}
    assert StubDriver.built == []
    out = capsys.readouterr().out
    assert "not main's code" in out
    assert "not 'main'" in out


async def test_a_refused_checkout_withholds_the_writer_and_nothing_else(
    monkeypatch, alias_file, wired
):
    """The worker still runs, and the table is still read.

    Only the write identity is withheld — the binding is still provisioned, which
    is what makes the handler's refusal `provider_disabled` rather than
    `unknown_alias`. An operator reading the fact learns the destination exists
    and this host will not write to it, which is the true statement.
    """
    monkeypatch.setattr("src.worker.main.checkout_refusal", lambda: "unmerged")
    monkeypatch.setenv(
        "REPLICATOR_REPLICATION_ALIASES_FILE",
        alias_file({"public": {"provider": "gcs", "bucket": "example-replication-bucket"}}),
    )

    await run(_stopped())

    assert wired["writers"] == {}
    assert list(wired["aliases"].bindings) == ["public"]


async def test_the_guard_is_not_consulted_without_a_binding_to_build(monkeypatch):
    """No provider binding, no question to ask — and no subprocess.

    A worker that replicates nothing is every worker on this VM today. Asking git
    on each of their startups would be a cost paid by the case the guard is not
    about.
    """
    asked = []
    monkeypatch.setattr("src.worker.main.checkout_refusal", lambda: asked.append(True))

    assert build_writers(AliasTable({})) == {}
    assert build_writers(AliasTable({"drive": AliasBinding(provider="gdrive")})) == {}
    assert asked == []


async def test_an_accepted_checkout_is_asked_once_for_the_whole_table(monkeypatch, alias_file):
    """One verdict per startup, not one per binding.

    The answer cannot differ between two bindings read from the same table in the
    same process, and `checkout_refusal` shells out to `bash` to get it.
    """
    asked = []
    monkeypatch.setattr("src.worker.main.checkout_refusal", lambda: asked.append(True) or None)
    monkeypatch.setattr("src.worker.main.AsyncGcsDriver", StubDriver)
    table = AliasTable(
        {
            "public": AliasBinding(provider="gcs", bucket="example-replication-bucket"),
            "private": AliasBinding(provider="gcs", bucket="example-internal-bucket"),
        }
    )

    writers = build_writers(table)

    assert sorted(writers) == ["private", "public"]
    assert asked == [True]
