"""Settings contract: REPLICATOR_-prefixed env, unprefixed BUILD_ID, safe defaults."""

import ast
from pathlib import Path

import pytest
from co_core.pure.adapters.bus.streams import (
    CONTENT_FETCH,
    CONTENT_FETCH_POLICY,
    CONTENT_REPLICATE,
    group_name,
    stream_kind,
)
from pydantic import ValidationError

from src.core import config as config_module
from src.core.config import Settings, get_settings


def test_defaults_match_the_shared_vm(monkeypatch):
    for var in (
        "REPLICATOR_REDIS_URL",
        "REPLICATOR_BLOB_DIR",
        "REPLICATOR_CONSUMER_GROUP",
        "BUILD_ID",
    ):
        monkeypatch.delenv(var, raising=False)

    settings = get_settings()
    assert settings.redis_url == "redis://localhost:6379/0"
    assert settings.blob_dir == Path("blobs")
    # command semantics => exactly one group cluster-wide
    assert settings.consumer_group == "replicator.fetch"
    assert settings.build_id == "dev"


def test_replicator_prefixed_env_overrides(monkeypatch):
    monkeypatch.setenv("REPLICATOR_REDIS_URL", "redis://broker:6379/3")
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", "/var/lib/replicator/blobs")

    settings = get_settings()
    assert settings.redis_url == "redis://broker:6379/3"
    assert settings.blob_dir == Path("/var/lib/replicator/blobs")


def test_build_id_is_unprefixed(monkeypatch):
    """systemd's ExecStartPre stamps a generic BUILD_ID, not REPLICATOR_BUILD_ID."""
    monkeypatch.setenv("BUILD_ID", "abc1234")
    assert get_settings().build_id == "abc1234"


def _group_field_defaults() -> dict[str, ast.expr]:
    """The ``default=`` expression of each group Field, straight from the source.

    Structural because the property is structural. Comparing the *value* against
    ``group_name(...)`` cannot see the difference between deriving and spelling —
    both sides evaluate to the same string today, so the assertion holds either way
    (CR round 4, verified by reverting the defaults to literals and watching the
    test pass). The AST is where "derived" is actually visible, and this repo
    already reads it that way in ``test_boundaries.py`` and ``test_destinations.py``.
    """
    source = Path(config_module.__file__).read_text()
    settings_class = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.ClassDef) and node.name == "Settings"
    )
    wanted = {"consumer_group", "replicate_consumer_group"}
    defaults: dict[str, ast.expr] = {}
    for node in settings_class.body:
        if not (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)):
            continue
        if node.target.id not in wanted or not isinstance(node.value, ast.Call):
            continue
        for keyword in node.value.keywords:
            if keyword.arg == "default":
                defaults[node.target.id] = keyword.value
    return defaults


def test_the_group_defaults_are_derived_not_spelled():
    """Both group names are *computed* by co-core's ``group_name``, not written out.

    The convention is co-core's to define — ``<service>.<stream-suffix>``
    (cannobserv#384, v0.13.1) — so spelling the results here is a second copy of a
    rule this repo does not own, free to drift the way the cluster's group naming
    already did once. Deriving them means a convention change arrives with the
    wheel rather than needing to be noticed by a person.

    Asserted against the syntax tree, not the resolved value: see
    ``_group_field_defaults`` for why the obvious value comparison proves nothing.
    """
    defaults = _group_field_defaults()
    assert set(defaults) == {"consumer_group", "replicate_consumer_group"}

    called = {}
    for field, expr in defaults.items():
        assert isinstance(expr, ast.Call), f"{field} spells its group name instead of deriving it"
        # ``unparse`` rather than matching a node shape, so the assertion survives
        # ``group_name(...)`` and ``streams.group_name(...)`` alike — the import
        # style is not the property under test.
        called[field] = ast.unparse(expr)

    assert called == {
        "consumer_group": "streams.group_name(streams.CONTENT_FETCH, SERVICE_NAME)",
        "replicate_consumer_group": "streams.group_name(streams.CONTENT_REPLICATE, SERVICE_NAME)",
    }


def test_the_derived_groups_are_the_ones_live_on_the_broker(monkeypatch):
    """And what they derive *to* is what the broker already has.

    The companion to the structural test above, and a separate property: that one
    says the names are computed, this one says the computation still yields
    ``replicator.fetch`` / ``replicator.replicate`` — audited on the broker
    2026-09-01 (#384). A co-core release that changed the convention fails here
    rather than at ``ensure_group``, where the symptom is a *new empty group*
    beside the real one and a worker reading a stream nothing delivers to.
    """
    for var in ("REPLICATOR_CONSUMER_GROUP", "REPLICATOR_REPLICATE_CONSUMER_GROUP"):
        monkeypatch.delenv(var, raising=False)

    settings = get_settings()
    assert (settings.consumer_group, settings.replicate_consumer_group) == (
        "replicator.fetch",
        "replicator.replicate",
    )


def test_the_policy_stream_takes_no_group():
    """`content.fetch-policy` is config/state — groupless, and now machine-checkable.

    Replicator reads it with no group, no ack and no DLQ. That was a rule stated in
    prose and enforced only by `policy.py` doing the right thing; since v0.13.1 the
    taxonomy is queryable, so the rule can be asserted against co-core's own
    classification rather than restated here.
    """
    assert stream_kind(CONTENT_FETCH_POLICY) == "config_state"
    assert stream_kind(CONTENT_FETCH) == "command"
    assert stream_kind(CONTENT_REPLICATE) == "command"

    with pytest.raises(ValueError, match="no consumer group"):
        group_name(CONTENT_FETCH_POLICY, "replicator")


def test_the_hostname_never_reaches_the_consumer_name(monkeypatch):
    """Unset by default — the name is derived from the *group*, at the wiring seam.

    Host-derived was a leak with no symptom until it fired (#77, archiver#156): a
    consumer registration persists until an explicit ``XGROUP DELCONSUMER``, which
    nothing calls, so every hostname change minted a fresh one and abandoned the
    old along with its PEL. It also misattributed — this VM's hostname is literally
    ``watcher``, so Replicator's consumers read as Watcher's on a broker all three
    services share.

    The patched hostname is the assertion: it must not appear anywhere in the
    settings, so no future default can quietly reintroduce the derivation.
    """
    monkeypatch.delenv("REPLICATOR_CONSUMER_NAME", raising=False)
    monkeypatch.setattr("socket.gethostname", lambda: "vm-1")
    assert get_settings().consumer_name is None


def test_consumer_name_override_still_wins(monkeypatch):
    """The override is how a *second* worker on one host gets its own PEL.

    Two workers sharing a name would share a pending-entries list, which makes
    independent ``claim_stale`` recovery impossible. The derived default names
    slot 1; a second member is configured to ``-2``.
    """
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator-fetch-2")
    assert get_settings().consumer_name == "replicator-fetch-2"


def test_each_group_has_its_own_name_override(monkeypatch):
    """One override per group, mirroring the two group settings themselves.

    A single process-wide override applied to *both* loops, so the documented
    dev-worker invocation registered a ``replicator-fetch-…`` name inside the
    ``replicator.replicate`` group — a consumer whose name misstates its group,
    which is the defect class #77 exists to remove (CR round 1).
    """
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator-fetch-2")
    monkeypatch.delenv("REPLICATOR_REPLICATE_CONSUMER_NAME", raising=False)

    settings = get_settings()
    assert settings.consumer_name == "replicator-fetch-2"
    assert settings.replicate_consumer_name is None


def test_the_two_command_groups_may_not_collide(monkeypatch):
    """One group name per stream, because the override key is the group.

    **Not because the PELs would cross** — they would not, and this docstring said
    they would until CR round 2 checked it against a broker. A group is identified
    by *(stream key, group name)*, so the same name on both command streams is two
    unrelated groups; ``test_a_group_name_is_scoped_to_its_stream`` pins it.

    What the collision actually breaks is ``consumer_name_for``, which decides
    which name override applies by comparing against the group. Equal groups leave
    that question unanswerable, so the config is refused where it is written.
    """
    monkeypatch.setenv("REPLICATOR_CONSUMER_GROUP", "replicator.shared")
    monkeypatch.setenv("REPLICATOR_REPLICATE_CONSUMER_GROUP", "replicator.shared")

    with pytest.raises(ValidationError, match="must name different groups"):
        get_settings()


def test_worst_case_outage_bounds_the_absorb_window(monkeypatch):
    """The figure the unit's StartLimitIntervalSec is sized against (CR #22)."""
    monkeypatch.setenv("REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES", "20")
    monkeypatch.setenv("REPLICATOR_ERROR_BACKOFF_MAX_SECONDS", "30")

    assert get_settings().worst_case_outage_seconds == 600


def test_retention_defaults_are_the_ones_archiver_was_told(monkeypatch):
    """The 7-day TTL is a published commitment (archiver#118), not a tunable guess."""
    for var in (
        "REPLICATOR_BLOB_TTL_SECONDS",
        "REPLICATOR_BLOB_TEMP_GRACE_SECONDS",
        "REPLICATOR_BLOB_MAX_TOTAL_BYTES",
    ):
        monkeypatch.delenv(var, raising=False)

    settings = get_settings()
    assert settings.blob_ttl_seconds == 604_800
    assert settings.blob_temp_grace_seconds == 3_600
    assert settings.blob_max_total_bytes == 2 * 1024**3


def test_the_temp_grace_is_far_shorter_than_the_ttl(monkeypatch):
    """Two clocks over one tree, and confusing them reaps a live write.

    A temporary exists across a single store; a blob has to outlive a consumer's
    backlog. Defaulting them anywhere near each other would mean either debris
    lingering for a week or a writer's os.replace racing the sweep.
    """
    settings = get_settings()
    assert settings.blob_temp_grace_seconds < settings.blob_ttl_seconds


def test_retention_env_overrides(monkeypatch):
    monkeypatch.setenv("REPLICATOR_BLOB_TTL_SECONDS", "60")
    monkeypatch.setenv("REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS", "5")
    monkeypatch.setenv("REPLICATOR_BLOB_MAX_TOTAL_BYTES", "1024")

    settings = get_settings()
    assert settings.blob_ttl_seconds == 60
    assert settings.blob_sweep_interval_seconds == 5
    assert settings.blob_max_total_bytes == 1024


def test_the_pacing_default_matches_what_watcher_already_commits_to(monkeypatch):
    """1.0s is Watcher's own DEFAULT_MIN_INTERVAL, and that is the whole argument.

    The politeness *numbers* belong to the issuer under the boundaries charter,
    so until the policy stream carries them the interim must not invent one. A
    change here is a change to what the cluster promises origins (#12).
    """
    monkeypatch.delenv("REPLICATOR_MIN_HOST_INTERVAL_SECONDS", raising=False)

    assert get_settings().min_host_interval_seconds == 1.0


def test_pacing_can_be_disabled_and_configured(monkeypatch):
    monkeypatch.setenv("REPLICATOR_MIN_HOST_INTERVAL_SECONDS", "0")
    assert get_settings().min_host_interval_seconds == 0

    get_settings.cache_clear()
    monkeypatch.setenv("REPLICATOR_MIN_HOST_INTERVAL_SECONDS", "2.5")
    assert get_settings().min_host_interval_seconds == 2.5


@pytest.mark.parametrize("value", ["-1", "7200"], ids=["negative", "over-the-cap"])
def test_an_out_of_range_pacing_interval_fails_at_startup(monkeypatch, value):
    """The cap has to bite where the comment says it does (CR #14).

    Past an hour the command parks and re-parks without ever dead-lettering —
    transient failures are exempt from the delivery ceiling — while the issuer's
    reaper concludes loss. A fat-fingered extra zero must fail loudly at
    construction rather than become a black hole that reads as healthy.
    """
    monkeypatch.setenv("REPLICATOR_MIN_HOST_INTERVAL_SECONDS", value)

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("value", ["0", "-1", "1e30"], ids=["zero", "negative", "absurd"])
def test_an_out_of_range_blob_ttl_fails_at_startup(monkeypatch, value):
    """The TTL is arithmetic now, so an absurd value is a crash rather than a knob (CR #7).

    Since #28 the handler publishes ``stored_at + blob_ttl_seconds``. An
    unbounded float makes that addition raise ``OverflowError`` — *after* the
    bytes are on disk, and not a transient error, so the command burns the
    delivery ceiling and dead-letters while its blob stays behind as an orphan.
    A config typo should not be able to manufacture those.

    Zero and negative are refused for a plainer reason: they expire every blob
    the moment it is written, which the sweep would carry out.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_TTL_SECONDS", value)

    with pytest.raises(ValidationError):
        Settings()


def test_the_blob_backend_defaults_to_local(monkeypatch):
    """#7 ships the object store dark: the compiled-in default does not move.

    Watcher parses ``blob_uri`` into a filesystem path and re-issues — uncapped —
    when it cannot open one (CannObserv/watcher#275). A default that flipped with
    the code would turn every deploy of this branch into an unbounded re-fetch
    loop against real origins. The backend moves when an operator moves it.
    """
    for var in ("REPLICATOR_BLOB_BACKEND", "REPLICATOR_BLOB_BUCKET", "REPLICATOR_BLOB_PREFIX"):
        monkeypatch.delenv(var, raising=False)

    settings = get_settings()
    assert settings.blob_backend == "local"
    assert settings.blob_bucket == ""
    assert settings.blob_prefix == "blobs"


def test_the_gcs_backend_is_selected_by_env(monkeypatch):
    monkeypatch.setenv("REPLICATOR_BLOB_BACKEND", "gcs")
    monkeypatch.setenv("REPLICATOR_BLOB_BUCKET", "a-temp-bucket")
    monkeypatch.setenv("REPLICATOR_BLOB_PREFIX", "/tmp-blobs/")

    settings = get_settings()
    assert settings.blob_backend == "gcs"
    assert settings.blob_bucket == "a-temp-bucket"
    # Normalized at the edge: the prefix is joined into an object key, and a
    # stray slash either side produces `//` or a key rooted differently from the
    # one `uri_for` derives — which is the comparison the replicate guard makes.
    assert settings.blob_prefix == "tmp-blobs"


def test_an_unknown_blob_backend_fails_at_startup(monkeypatch):
    """A typo must not fall back to a working backend and hide itself."""
    monkeypatch.setenv("REPLICATOR_BLOB_BACKEND", "gcs2")

    with pytest.raises(ValidationError):
        get_settings()


def test_the_gcs_backend_without_a_bucket_fails_at_startup(monkeypatch):
    """No bucket, no store — and the failure belongs at boot, not at the first fetch.

    Deferring it would announce `gs:///<key>` on `content.blobs`, a URI no
    consumer can open and none can distinguish from a reaped blob.
    """
    monkeypatch.setenv("REPLICATOR_BLOB_BACKEND", "gcs")
    monkeypatch.delenv("REPLICATOR_BLOB_BUCKET", raising=False)

    with pytest.raises(ValidationError, match="REPLICATOR_BLOB_BUCKET"):
        get_settings()


def test_the_blob_timeout_is_configurable_and_below_the_shutdown_budget(monkeypatch):
    """CR #8: the store's timeout is host configuration, like every other timeout.

    It is also the one that runs beyond cancellation — `asyncio.to_thread` — so
    it lands in the unit's `TimeoutStopSec` budget rather than merely bounding a
    handler. `tests/test_deploy.py` asserts the sum; this fixes the default it
    sums.
    """
    monkeypatch.delenv("REPLICATOR_BLOB_TIMEOUT_SECONDS", raising=False)
    assert get_settings().blob_timeout_seconds == 30.0

    monkeypatch.setenv("REPLICATOR_BLOB_TIMEOUT_SECONDS", "45")
    get_settings.cache_clear()
    assert get_settings().blob_timeout_seconds == 45.0


def test_a_non_positive_blob_timeout_fails_at_startup(monkeypatch):
    """Zero is not "no timeout" — it is an operation that can never complete."""
    monkeypatch.setenv("REPLICATOR_BLOB_TIMEOUT_SECONDS", "0")

    with pytest.raises(ValidationError):
        get_settings()
