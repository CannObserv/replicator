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

    log_level: str = Field(default="INFO", validation_alias="REPLICATOR_LOG_LEVEL")

    # Stamped by the systemd unit's ExecStartPre; "dev" outside systemd.
    build_id: str = Field(default="dev", validation_alias="BUILD_ID")


@lru_cache
def get_settings() -> Settings:
    """Return the shared Settings instance."""
    return Settings()
