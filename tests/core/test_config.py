"""Settings contract: REPLICATOR_-prefixed env, unprefixed BUILD_ID, safe defaults."""

from pathlib import Path

from src.core.config import get_settings


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
