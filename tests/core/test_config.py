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
