"""Application settings via pydantic-settings.

Single source of env access — no other module calls ``os.environ.get()`` for
runtime configuration.

Env files (``/etc/replicator/.env``, repo ``.env``) are loaded by systemd or the
developer before launch — never by this module.

Replicator-owned settings carry the ``REPLICATOR_`` prefix so they never collide
with a sibling service's variables on the shared VM (the archiver/watcher/notifier
convention). ``BUILD_ID`` is deliberately unprefixed: it is stamped generically by
the systemd unit's ``ExecStartPre``.
"""

import socket
from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_consumer_name() -> str:
    """Identify this worker within the consumer group.

    Redis Streams tracks pending entries per consumer name, so two workers
    sharing a name would also share a PEL and could not be recovered
    independently by ``claim_stale``. Host-derived keeps them distinct without
    configuration; override via ``REPLICATOR_CONSUMER_NAME`` when running more
    than one worker per host.
    """
    return f"replicator@{socket.gethostname()}"


class Settings(BaseSettings):
    """Runtime configuration."""

    model_config = SettingsConfigDict(extra="ignore")

    # Redis is Archiver-operated cluster infrastructure; this is a client URL.
    # The default matches scripts/check_redis_floor.sh so the startup floor
    # guard checks the same broker the worker will actually connect to.
    redis_url: str = Field(
        default="redis://localhost:6379/0", validation_alias="REPLICATOR_REDIS_URL"
    )

    # Temp-storage root for the local-filesystem blob backend. "Temporary" means
    # the bytes live long enough for durable replication to collect them;
    # retention policy is out of MVP scope.
    blob_dir: Path = Field(default=Path("blobs"), validation_alias="REPLICATOR_BLOB_DIR")

    # content.fetch carries command semantics => exactly one consumer group
    # cluster-wide, with competing consumers inside it.
    consumer_group: str = Field(
        default="replicator.fetch", validation_alias="REPLICATOR_CONSUMER_GROUP"
    )
    consumer_name: str = Field(
        default_factory=_default_consumer_name, validation_alias="REPLICATOR_CONSUMER_NAME"
    )

    # How long a poll blocks waiting for a new message. Bounds worst-case
    # shutdown latency: SIGTERM is checked between polls, and a blocking
    # XREADGROUP is left to expire rather than cancelled mid-flight, so
    # systemd's TimeoutStopSec must exceed this plus the handler's budget.
    read_block_ms: int = Field(default=5_000, validation_alias="REPLICATOR_READ_BLOCK_MS")

    # start_id applies only at group *creation* — once replicator.fetch exists
    # this value is inert, and switching to "0" (drain the backlog) additionally
    # needs a manual XGROUP SETID. Kept configurable so the eventual change is a
    # config edit rather than a code change.
    consumer_start_id: str = Field(default="$", validation_alias="REPLICATOR_CONSUMER_START_ID")

    # How long a pending entry must sit untouched before another worker may
    # reclaim it. This is also the retry cadence: a transiently-failed message
    # is left unacked and comes back through the same claim_stale path, so
    # crash recovery and retry are one mechanism, not two.
    claim_min_idle_ms: int = Field(default=60_000, validation_alias="REPLICATOR_CLAIM_MIN_IDLE_MS")

    # Delivery ceiling for failures the loop could not classify (a handler bug,
    # say) before they are dead-lettered. Read from XPENDING's delivery counter,
    # which only advances on a claim_stale reclaim — so this is a ceiling in
    # *time* (attempts x claim_min_idle_ms), not in retries. Transient failures
    # are exempt; deterministic ones dead-letter on the first failure without
    # ever reaching it.
    max_delivery_attempts: int = Field(
        default=5, validation_alias="REPLICATOR_MAX_DELIVERY_ATTEMPTS"
    )

    # Backoff for a poll cycle that raised — a broker outage, not a bad message.
    # Escalates base * 2**(n-1) to the cap so a down Redis is not hammered.
    error_backoff_base_seconds: float = Field(
        default=1.0, validation_alias="REPLICATOR_ERROR_BACKOFF_BASE_SECONDS"
    )
    error_backoff_max_seconds: float = Field(
        default=30.0, validation_alias="REPLICATOR_ERROR_BACKOFF_MAX_SECONDS"
    )

    # Consecutive failed cycles before the loop stops absorbing and re-raises.
    # The backoff must not turn "dies on every blip" into "never dies at all": a
    # worker that cannot reach Redis looks alive to systemd (nothing exits, so
    # Restart=on-failure never fires) while doing no work, and a permanently
    # wrong REPLICATOR_REDIS_URL would fail silently forever. At the escalating
    # cadence the default is ~8 minutes of continuous failure — long enough to
    # ride out a broker restart, short enough that a misconfiguration surfaces
    # via a real restart, which also re-runs the Redis floor check. The unit's
    # StartLimitIntervalSec is sized against this; changing one means revisiting
    # the other.
    max_consecutive_cycle_failures: int = Field(
        default=20, validation_alias="REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES"
    )

    # Lifetime of the per-command_id dedupe key. Redelivery is bounded by the
    # PEL, which is unbounded in principle, so no TTL is provably sufficient:
    # a day covers any realistic outage, costs one small key per command, and
    # expiry degrades to a re-run that content-addressed storage absorbs.
    dedupe_ttl_seconds: int = Field(
        default=86_400, validation_alias="REPLICATOR_DEDUPE_TTL_SECONDS"
    )

    log_level: str = Field(default="INFO", validation_alias="REPLICATOR_LOG_LEVEL")

    # Stamped by the systemd unit's ExecStartPre; "dev" outside systemd.
    build_id: str = Field(default="dev", validation_alias="BUILD_ID")


@lru_cache
def get_settings() -> Settings:
    """Return the shared Settings instance."""
    return Settings()
