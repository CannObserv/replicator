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
    # the bytes live long enough for durable replication to collect them — see
    # the retention settings below for how long that is and who decided.
    blob_dir: Path = Field(default=Path("blobs"), validation_alias="REPLICATOR_BLOB_DIR")

    # How long a blob survives after it was last referenced. Measured from mtime,
    # which the store refreshes on the short-circuit path, so a re-fetch of
    # unchanged bytes restarts the clock — the fact announcing a blob is never
    # pointing at one already partway through its TTL.
    #
    # The number cannot be derived here: it has to exceed the consumption latency
    # of whoever reads content.blobs, which is archiver's or watcher's to state
    # (archiver#118). Seven days is deliberately far above any plausible answer
    # rather than a measured figure, so the open question is a confirmation
    # rather than a blocker. Raise it if a consumer says it needs longer.
    blob_ttl_seconds: float = Field(
        default=7 * 24 * 60 * 60, validation_alias="REPLICATOR_BLOB_TTL_SECONDS"
    )

    # How often the sweep walks the tree. Also the staleness bound on the
    # measured byte total the ceiling reads — between sweeps that number is the
    # byte path's own running estimate.
    blob_sweep_interval_seconds: float = Field(
        default=900.0, validation_alias="REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS"
    )

    # How long a `.tmp` in the blob tree may live before the sweep treats it as
    # debris. Deliberately unrelated to the TTL and far shorter: a temporary
    # exists only across a single write, so anything older is what a SIGKILL
    # mid-store left behind. Still an hour rather than seconds, because reaping
    # a live one makes the writer's os.replace fail with ENOENT and dead-letters
    # a command whose bytes were fine.
    blob_temp_grace_seconds: float = Field(
        default=3_600.0, validation_alias="REPLICATOR_BLOB_TEMP_GRACE_SECONDS"
    )

    # Ceiling on everything the blob tree holds. A TTL alone does not bound disk
    # — a burst fills it well inside any retention window — and the VM is shared
    # with archiver, watcher, and notifier, so filling it is a cluster-wide
    # outage rather than a Replicator one.
    #
    # Crossing it does NOT shorten the TTL. Reaping a blob a consumer has been
    # promised would convert a local disk problem into a blob_uri that cannot be
    # opened in another repo, with no local symptom. The byte path stops fetching
    # instead, transiently, so commands wait on the bus until space frees.
    #
    # 2 GiB against the VM's few gigabytes of headroom: room for far more than
    # the seed harness produces, well short of an outage.
    blob_max_total_bytes: int = Field(
        default=2 * 1024 * 1024 * 1024, validation_alias="REPLICATOR_BLOB_MAX_TOTAL_BYTES"
    )

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

    # Minimum spacing between two requests to the same host — the interim
    # politeness default (#12), replacing the limiter Watcher stops exercising
    # the moment its fetch path becomes a publish path (watcher#245).
    #
    # 1.0 s is Watcher's own DEFAULT_MIN_INTERVAL, chosen precisely because it
    # invents nothing: the *numbers* are the issuer's under the boundaries
    # charter, and until the policy stream carries them the least-wrong value is
    # the one the cluster already commits to. Enforcement is mechanism and
    # belongs here; this default is a stand-in for a decision, not the decision.
    #
    # 0 disables pacing: an operator escape hatch, and the value the pacer's own
    # unit tests pin. A deployment setting it is choosing no politeness at all.
    #
    # Capped at an hour (CR #8). Past that the mechanism is the wrong one rather
    # than a stricter setting of the right one: the command parks and re-parks
    # for an hour of reclaim cycles, never dead-letters (transient failures are
    # exempt from the delivery ceiling), and the issuer's own reaper — the
    # backstop the contract requires precisely because silence carries no
    # cause — will have concluded loss long before. A fat-fingered extra zero
    # should fail at startup, not become a black hole that reads as healthy.
    min_host_interval_seconds: float = Field(
        default=1.0, ge=0, le=3600, validation_alias="REPLICATOR_MIN_HOST_INTERVAL_SECONDS"
    )

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

    # Ceiling on a single fetched body. A storage guard, not a memory one: the
    # co-core fetch driver reads the whole response into memory before returning
    # it (httpx `response.content`), so by the time this is checked the bytes are
    # already resident. Enforcing it would need a streaming fetch co-core does
    # not expose today. What it does buy is a bound on what reaches the blob
    # directory on a shared VM, where filling the disk is a cluster-wide outage
    # rather than a Replicator one. 64 MiB is far above any observed page and
    # far below the VM's headroom.
    max_blob_bytes: int = Field(
        default=64 * 1024 * 1024, validation_alias="REPLICATOR_MAX_BLOB_BYTES"
    )

    # Ceiling on a command's own timeout_seconds (#11). Not a default — an
    # omitted field still gets the fetch driver's 30s — but the most an issuer
    # may ask for.
    #
    # A setting rather than a constant because the right value depends on the
    # corpus, which is the argument for the field existing at all: small JSON
    # endpoints and slow PDF-serving portals have no one right timeout.
    #
    # It is a guard, not a preference. The consume path reads count=1 and
    # processes serially, so one command's timeout is a lien on every other
    # command in the group — an unbounded value parks the whole worker.
    #
    # Bounded above by the unit's TimeoutStopSec, which must exceed this plus
    # REPLICATOR_READ_BLOCK_MS plus an in-flight sweep: a fetch past that window
    # is SIGKILLed mid-flight on every deploy. Changing one means revisiting the
    # other (deploy/replicator.service).
    max_fetch_timeout_seconds: float = Field(
        default=120.0, validation_alias="REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS"
    )

    log_level: str = Field(default="INFO", validation_alias="REPLICATOR_LOG_LEVEL")

    # Stamped by the systemd unit's ExecStartPre; "dev" outside systemd.
    build_id: str = Field(default="dev", validation_alias="BUILD_ID")

    @property
    def worst_case_outage_seconds(self) -> float:
        """Upper bound on how long the worker absorbs a broker outage before exiting.

        The number ``deploy/replicator.service``'s ``StartLimitIntervalSec`` is
        sized against: the window must fit ``StartLimitBurst`` of these, or a
        permanently unreachable Redis produces exits too far apart to trip the
        limiter and the unit reads as ``active (running)`` forever. Logged at
        startup so the value is in the journal rather than only in two comments.

        An upper bound, not the exact figure — the first few cycles back off
        less than the cap.
        """
        return self.max_consecutive_cycle_failures * self.error_backoff_max_seconds


@lru_cache
def get_settings() -> Settings:
    """Return the shared Settings instance."""
    return Settings()
