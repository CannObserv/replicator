"""Settings contract: REPLICATOR_-prefixed env, unprefixed BUILD_ID, safe defaults."""

from pathlib import Path

import pytest
from pydantic import ValidationError

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


def test_consumer_names_are_host_distinct(monkeypatch):
    """Two workers sharing a consumer name would share a PEL and break recovery."""
    monkeypatch.delenv("REPLICATOR_CONSUMER_NAME", raising=False)
    monkeypatch.setattr("socket.gethostname", lambda: "vm-1")
    assert get_settings().consumer_name == "replicator@vm-1"


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
