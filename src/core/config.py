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

from functools import lru_cache
from pathlib import Path
from typing import Literal

from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.streams import group_name
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# This service's segment in every consumer-group name it owns. One constant rather
# than the literal at each ``group_name`` call: the two groups are the same
# service, and a typo in one of two spellings creates a second real group at
# ``ensure_group`` rather than failing.
SERVICE_NAME = "replicator"

# Also the default of ``build_replicate_handler``'s ``write_timeout_seconds``,
# which imports it from here rather than repeating it (CR #43, #46): one number
# with two spellings drifts into two numbers, and the handler's default is what a
# directly-constructed handler gets while the field below is what the worker gets.
DEFAULT_WRITE_TIMEOUT_SECONDS = 120

# The object store's per-operation timeout, imported by ``src.storage.gcs`` so a
# directly constructed store and the worker's agree by construction rather than
# by two literals staying equal (CR #8) — the same arrangement
# ``DEFAULT_WRITE_TIMEOUT_SECONDS`` has with the replicate handler.
DEFAULT_BLOB_TIMEOUT_SECONDS = 30.0


class Settings(BaseSettings):
    """Runtime configuration."""

    model_config = SettingsConfigDict(extra="ignore")

    # Redis is Archiver-operated cluster infrastructure; this is a client URL.
    # The default matches scripts/check_redis_floor.sh so the startup floor
    # guard checks the same broker the worker will actually connect to.
    redis_url: str = Field(
        default="redis://localhost:6379/0", validation_alias="REPLICATOR_REDIS_URL"
    )

    # Which backend holds the fetched bytes, and therefore what scheme
    # ``blob_available.blob_uri`` carries (#7).
    #
    # **``local`` is the compiled-in default and stays that way — permanently, not
    # until some later flip** (decided 2026-08-20). Two separate reasons, and the
    # second outlives the first:
    #
    # 1. A default that moved with the code would deploy a *cluster* decision. A
    #    `file://` URI makes every consumer share this host, and a worker that
    #    started announcing `gs://` before its consumers could read it would put
    #    every watched item into re-fetch-until-capped against live origins
    #    (CannObserv/watcher#275). One repo's merge must not be able to do that.
    # 2. The object store needs a bucket, a lifecycle rule and two IAM grants
    #    that no checkout carries. `local` is the only backend that works from a
    #    fresh clone with nothing provisioned, which makes it the right default
    #    for every test run, every dev worker, and every CI job — the population
    #    that is *always* larger than the production deployments.
    #
    # So the production posture lives in ``/etc/replicator/.env``, where a
    # deployment's configuration belongs, and the repo keeps the default that is
    # correct with nothing set up. ``tests/test_boundaries.py`` pins it.
    #
    # A Literal rather than a free string: a typo that fell back to a working
    # backend would be a silent misconfiguration, and the failure it produces —
    # blobs written where nobody looks — has no local symptom.
    blob_backend: Literal["local", "gcs"] = Field(
        default="local", validation_alias="REPLICATOR_BLOB_BACKEND"
    )

    # The bucket the ``gcs`` backend writes temp blobs into. Empty is legal only
    # while the backend is ``local`` — the validator below refuses the pairing
    # that would otherwise announce ``gs:///<key>``.
    #
    # Deliberately **not** the replication bucket. These are arbitrary bytes from
    # arbitrary origins held for days, and the destination for permanent
    # artifacts is grant-restricted precisely so nothing can delete from it —
    # which is the opposite of what a temp store needs. Separate bucket, separate
    # lifecycle rule, separate grant.
    blob_bucket: str = Field(default="", validation_alias="REPLICATOR_BLOB_BUCKET")

    # Key prefix inside that bucket. Normalized to carry no leading or trailing
    # slash, because it is joined into an object key and a stray one produces
    # either ``//`` or a key rooted somewhere ``uri_for`` would not derive — and
    # ``uri_for`` is what the replicate guard compares a message's ``blob_uri``
    # against (T3a). A prefix that round-trips differently through the two is a
    # refusal of a blob this store really did mint.
    blob_prefix: str = Field(default="blobs", validation_alias="REPLICATOR_BLOB_PREFIX")

    # How long one object-store operation may block before it gives up. A
    # setting rather than a constant in the store (CR #8) for the reason every
    # other timeout here is one: it is a property of this host's link to the
    # provider, and `src/core/config.py` is the single source of env access.
    #
    # **It lands directly in the unit's shutdown budget.** Storage runs inside
    # `asyncio.to_thread`, which puts it beyond cancellation, so SIGTERM waits it
    # out exactly as it waits out an in-flight sweep — which is why 30 s and not
    # the fetch ceiling's 120: `TimeoutStopSec` has to cover a poll, a pacing
    # sleep, the slowest fetch *and* this, and `tests/test_deploy.py` now asserts
    # the sum (CR #5).
    blob_timeout_seconds: float = Field(
        default=DEFAULT_BLOB_TIMEOUT_SECONDS,
        gt=0,
        validation_alias="REPLICATOR_BLOB_TIMEOUT_SECONDS",
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
    #
    # Bounded since #28 made it arithmetic rather than only a comparison: the
    # handler publishes ``stored_at + blob_ttl_seconds`` as blob_expires_at, and
    # an unbounded float makes that addition raise OverflowError *after* the
    # bytes are stored — not a transient error, so the command walks the delivery
    # ceiling into the DLQ and leaves its blob behind as an orphan. The ceiling is
    # ten years: far past any retention anyone would ask for, and far short of
    # what datetime arithmetic refuses (CR #7).
    blob_ttl_seconds: float = Field(
        default=7 * 24 * 60 * 60,
        gt=0,
        le=10 * 365 * 24 * 60 * 60,
        validation_alias="REPLICATOR_BLOB_TTL_SECONDS",
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
    #
    # **Derived, not spelled** (cannobserv#384, co-core v0.13.1). The convention is
    # `<service>.<stream-suffix>`, and it is co-core's to define — a literal here
    # would be a second copy of a rule this repo does not own, free to drift the
    # way the cluster's group naming already drifted once. Evaluates to
    # "replicator.fetch", the name live on the broker, so this is a no-op today and
    # a tripwire tomorrow: a convention change arrives with the wheel and fails a
    # test, rather than reaching `ensure_group` and creating a *new empty group*
    # beside the real one for the worker to read nothing from.
    consumer_group: str = Field(
        default=group_name(streams.CONTENT_FETCH, SERVICE_NAME),
        validation_alias="REPLICATOR_CONSUMER_GROUP",
    )
    # The fetch group's consumer-name **override**, unset on every host — the name
    # itself is derived from the group at the wiring seam (#77).
    #
    # It was host-derived (``replicator@{gethostname()}``), which is a leak with no
    # symptom until it fires: a consumer registration persists until an explicit
    # ``XGROUP DELCONSUMER`` that nothing calls, so every hostname change minted a
    # fresh registration and abandoned the old one along with its PEL — reclaimable
    # only by an ``XAUTOCLAIM`` at ``min_idle_time``. Archiver reached seven
    # registrations on the production broker, six of them dead (archiver#156).
    #
    # Derivation cannot live here as a field default: it needs the *group*, and the
    # group is not known until the reader is wired.
    #
    # Set it to give a *second* member of the fetch group its own PEL (slot ``-2``
    # upward), or to keep a dev worker's registration off the service's — the
    # documented way to test a branch on this VM. Two workers sharing a name share
    # a PEL, which makes independent ``claim_stale`` recovery impossible.
    consumer_name: str | None = Field(default=None, validation_alias="REPLICATOR_CONSUMER_NAME")

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
    # The second command stream's group (#29). Separate from consumer_group
    # because the two streams are separate command queues: one group per stream,
    # competing consumers within each.
    #
    # **Not because a shared name would cross their PELs — it would not** (CR
    # round 2). A group is identified by (stream key, group name), so one name on
    # both streams is two unrelated groups with separate pending-entry lists;
    # `test_a_group_name_is_scoped_to_its_stream` pins that against a live broker.
    # `_the_command_groups_stay_distinct` still refuses the collision, for the
    # narrower reason recorded there: the consumer-name override is keyed by group.
    replicate_consumer_group: str = Field(
        default=group_name(streams.CONTENT_REPLICATE, SERVICE_NAME),
        validation_alias="REPLICATOR_REPLICATE_CONSUMER_GROUP",
    )
    # The replicate group's override, paired with its group exactly as the fetch
    # pair above (CR round 1). One override per group and not one per *process*:
    # a single name applied to both loops registered a ``replicator-fetch-…``
    # consumer inside ``replicator.replicate``, so the name misstated its own
    # group — the defect class #77 exists to remove, reintroduced through the one
    # path the docs tell a developer to take. The names stayed distinct, so
    # nothing shared a PEL; it was ``XINFO`` that lied.
    replicate_consumer_name: str | None = Field(
        default=None, validation_alias="REPLICATOR_REPLICATE_CONSUMER_NAME"
    )

    # Where the alias table lives, or None on a host that does not replicate.
    #
    # A *path*, not the table itself: the provisioned set is host state, which
    # puts it in the charter's env channel, but it is a table and one
    # REPLICATOR_* variable per alias per field is a shape env does not hold. The
    # contract's phrase is "env-referenced host config" (T2), and this is that.
    #
    # Unset is the safe default and the current state of every host: nothing
    # provisioned means every replicate command is refused, so enabling
    # replication is an explicit operator act rather than a consequence of a
    # message arriving (T5).
    replication_aliases_file: Path | None = Field(
        default=None, validation_alias="REPLICATOR_REPLICATION_ALIASES_FILE"
    )

    # How long one conditional create may run before the provider gives up.
    #
    # Surfaced rather than inherited (CR #38): the SDK's own default is 120s and
    # nobody chose it. The number matters for the same reason the fetch timeout
    # does — a hung write holds its PEL entry for the whole window, and on the
    # write side that window is also how long a large blob has to reach a
    # permanent store over whatever link this host has.
    replicate_write_timeout_seconds: int = Field(
        default=DEFAULT_WRITE_TIMEOUT_SECONDS,
        gt=0,
        validation_alias="REPLICATOR_REPLICATE_WRITE_TIMEOUT_SECONDS",
    )

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

    @field_validator("blob_prefix")
    @classmethod
    def _normalize_blob_prefix(cls, value: str) -> str:
        """Strip the slashes a key join would otherwise double or root wrongly."""
        return value.strip("/")

    @model_validator(mode="after")
    def _gcs_backend_needs_a_bucket(self) -> "Settings":
        """Refuse the pairing whose only symptom is in another repo.

        Checked here rather than in the store's constructor so it fails at
        settings construction — before the consumer group is joined and before a
        command can be read. A worker that boots on this misconfiguration fetches
        successfully, stores nowhere reachable, and publishes a ``blob_uri`` a
        consumer cannot tell from a reaped one.
        """
        if self.blob_backend == "gcs" and not self.blob_bucket:
            raise ValueError(
                "REPLICATOR_BLOB_BACKEND=gcs needs REPLICATOR_BLOB_BUCKET — "
                "there is no default bucket, and guessing one is how bytes land "
                "somewhere nobody reads"
            )
        return self

    @model_validator(mode="after")
    def _the_command_groups_stay_distinct(self) -> "Settings":
        """One group name per command stream — because the *override key* needs it.

        **Not for PEL safety, which was the reason this repo gave for years and it
        is wrong** (CR round 2). A consumer group is identified by *(stream key,
        group name)*, so the same name on ``content.fetch`` and
        ``content.replicate`` creates two unrelated groups that merely spell alike:
        their pending-entry lists are separate, and ``XAUTOCLAIM`` on one cannot
        see the other's. ``tests/worker/test_main_integration.py`` pins that
        against a live broker, since fakeredis is not the authority on what Redis
        scopes.

        The real constraint is local and narrow: ``consumer_name_for`` picks which
        name override applies by comparing against this group. With the two equal
        the question has no answer, and the wiring seam would have to invent one —
        so a config that makes it ambiguous is refused where it is written rather
        than resolved arbitrarily at boot.

        Legibility is the secondary reason and the older one: two same-named groups
        in ``XINFO`` on a broker three services share is a reading someone gets
        wrong under time pressure.
        """
        if self.consumer_group == self.replicate_consumer_group:
            raise ValueError(
                "REPLICATOR_CONSUMER_GROUP and REPLICATOR_REPLICATE_CONSUMER_GROUP "
                f"must name different groups (both are {self.consumer_group!r}) — "
                "the consumer-name override is chosen by group, so two groups "
                "spelled alike leave no way to tell which override applies"
            )
        return self

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
