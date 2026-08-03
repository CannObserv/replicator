# replicator

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

Replicator owns content fetching, temporary storage, and fingerprinting for the cluster — the
network-bound, byte-handling work re-homed out of Watcher. It is driven by **commands** on the
Redis change bus and reports outcomes as **facts**:

```
content.fetch (command)  →  fetch  →  fingerprint  →  temp-store  →  blob_available (fact)
                         ↘  closed without bytes  ──────────────→  fetch_failed  (fact)
```

Both facts land on `content.blobs`, so an issuer's one consumer group sees either outcome of
its command.

The founding design lives in
[`docs/plans/2026-06-25-replicator-mvp-design.md`](docs/plans/2026-06-25-replicator-mvp-design.md).

**Issuing `content.fetch` commands?** Read
[`docs/contracts/content-fetch-issuer-contract.md`](docs/contracts/content-fetch-issuer-contract.md)
first — it is the normative issuer contract and its permanent home. Publish through co-core's
`to_wire`, never hand-rolled fields; and because the wire carries no domain identity, correlation is
entirely the issuer's job. Most ways of getting either wrong fail silently.

## Shape

**Worker-first.** The primary process is a bus consumer (a `co-core-aio` consumer group on the
`content.fetch` stream), not an HTTP API. A thin FastAPI app exposes `/health` for local checks;
it is not part of the MVP loop and is dev-only until a status surface is wanted.

Redis is **Archiver-operated cluster infrastructure** — Replicator is a client and does not ship
or manage a broker. The `>=7.0` server floor is Replicator-critical: `AsyncBusConsumer.claim_stale`
reads `XAUTOCLAIM`'s three-element reply, added in Redis server 7.0.

## Setup

```bash
# Mirror the private cannobserv package index into ./.wheelhouse (co-core, co-core-aio).
# Requires GOOGLE_APPLICATION_CREDENTIALS (see Environment below).
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py

uv sync
uv run pre-commit install
```

## Environment

Two env files, with a deliberate boundary between them:

1. **`/etc/replicator/.env`** — production configuration, managed manually on the VM.
   **This is the only file the systemd service reads.**
2. **`.env`** (repo root, git-ignored) — dev/agent secrets, e.g. GitHub PATs. Loaded by
   developers and agents in a shell; **never** by the service, which has no use for them.

For local work, load both:

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
```

Variables the service uses (all in `/etc/replicator/.env`). The **Default** column is the
value baked into `src/core/config.py` — several are overridden on the VM, so check
`/etc/replicator/.env` before assuming a default applies. On this deployment
`REPLICATOR_BLOB_DIR` is `/var/lib/replicator/blobs`, not the `blobs` shown below.

**If you pre-create the blob directory, make it and every parent traversable** (`0755`).
The worker sets modes only on directories it creates itself — an existing one keeps
whatever mode its operator gave it — so a `0700` level anywhere in the chain leaves every
`blob_uri` unopenable by the service that consumes the fact. Startup logs a warning naming
each blocking level.

**Blobs are temporary and are reaped.** A second task in the worker walks the tree every
`REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS`, removing blobs untouched for
`REPLICATOR_BLOB_TTL_SECONDS`, `.tmp` debris older than `REPLICATOR_BLOB_TEMP_GRACE_SECONDS`,
and the shard directories those emptied. The TTL runs from *last reference*, so re-fetching
unchanged bytes restarts it. Disk pressure never shortens it: over
`REPLICATOR_BLOB_MAX_TOTAL_BYTES` the worker stops fetching and leaves commands on the bus
rather than deleting bytes a consumer was promised.

| Variable | Default | Purpose |
|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | — | SA key for the wheelhouse mirror (`/etc/replicator/co-pypi-reader.json`) |
| `REPLICATOR_REDIS_URL` | `redis://localhost:6379/0` | Change-bus client URL |
| `REPLICATOR_BLOB_DIR` | `blobs` | Temp-storage root for fetched bytes. Resolved to an absolute path — `file://` URIs require it |
| `REPLICATOR_BLOB_TTL_SECONDS` | `604800` | How long a blob survives after it was **last referenced** (7 days). A commitment made to archiver (archiver#118), not a local knob — raise it if a `content.blobs` consumer needs longer |
| `REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS` | `900` | How often the tree is walked. Also the staleness bound on the size the ceiling reads |
| `REPLICATOR_BLOB_TEMP_GRACE_SECONDS` | `3600` | How long a `.tmp` may live before the sweep treats it as debris. Far shorter than the TTL — a temporary exists only across a single write |
| `REPLICATOR_BLOB_MAX_TOTAL_BYTES` | `2147483648` | Ceiling on everything the tree holds (2 GiB). Crossing it **pauses fetching**; it never reaps a blob still inside its TTL |
| `REPLICATOR_MAX_BLOB_BYTES` | `67108864` | Ceiling on one fetched body (64 MiB). A *storage* guard, not a memory one — co-core's fetch driver buffers the whole response first. Over it ⇒ DLQ |
| `REPLICATOR_CONSUMER_GROUP` | `replicator.fetch` | Consumer group on `content.fetch` |
| `REPLICATOR_CONSUMER_NAME` | `replicator@<hostname>` | This worker's identity in the group — never share one |
| `REPLICATOR_CONSUMER_START_ID` | `$` | Group start position. Applies only at group *creation*; changing it later also needs `XGROUP SETID` |
| `REPLICATOR_READ_BLOCK_MS` | `5000` | Blocking-read window. Bounds shutdown latency, so the unit's `TimeoutStopSec` must exceed it plus the handler budget and an in-flight sweep |
| `REPLICATOR_CLAIM_MIN_IDLE_MS` | `60000` | Idle time before a pending entry may be reclaimed — also the retry cadence |
| `REPLICATOR_MAX_DELIVERY_ATTEMPTS` | `5` | Deliveries of an *unclassified* failure before the DLQ |
| `REPLICATOR_DEDUPE_TTL_SECONDS` | `86400` | Lifetime of the `replicator:cmd:<command_id>` dedupe key |
| `REPLICATOR_ERROR_BACKOFF_BASE_SECONDS` | `1.0` | Backoff after a failed poll cycle (broker outage), escalating `base * 2**(n-1)` |
| `REPLICATOR_ERROR_BACKOFF_MAX_SECONDS` | `30.0` | Cap on that backoff |
| `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` | `20` | Failed cycles before the worker exits so the unit restarts (~8 min). Paired with the unit's `StartLimitIntervalSec` |
| `REPLICATOR_LOG_LEVEL` | `INFO` | Root log level |
| `BUILD_ID` | `dev` | Git SHA, stamped by the unit's `ExecStartPre` |

## Seeding a fetch

Nothing in the cluster issues `content.fetch` commands until the Watcher cutover, so
`scripts/seed_fetch.py` is the issuer. The target is never defaulted — `--redis-url` and
`--topic` are both required, and db 0 + `content.fetch` (the one pair the running worker
consumes, and therefore actually fetches over the network) additionally needs `--production`:

```bash
# Fetches the local /health app — a target we control, so the smoke test costs
# nobody else a request. Start it first (see Dev server below).
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/0 --topic content.fetch \
  --production --watch http://localhost:8041/health
```

`--watch` tails the fact stream until each command has an outcome — a `blob_available`, or a
`fetch_failed` naming the reason it closed. It reads `content.blobs` for `content.fetch` and
`<topic>.blobs` otherwise, so a scratch seed watches its own facts. Add `--dry-run` to print
the frames without contacting a broker at all.

## Test & lint

```bash
uv run pytest                          # default suite; integration tests deselected
uv run pytest --no-cov -m integration  # live-broker tests (scratch db, never db 0)
uv run ruff check .
```

Full command reference: [`docs/COMMANDS.md`](docs/COMMANDS.md).

## Dev server

The FastAPI `/health` app runs on port 8041 (port 8040 belongs to systemd if/when the API is
promoted to a deployed surface):

```bash
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload
```

## Deploy

The systemd unit lives at [`deploy/replicator.service`](deploy/replicator.service) and runs the
**worker**, not the API. To install on a fresh host:

```bash
# Copy into systemd's path
sudo cp deploy/replicator.service /etc/systemd/system/replicator.service
sudo systemctl daemon-reload
sudo systemctl enable --now replicator

# Tail logs
sudo journalctl -u replicator -f
```

Production secrets live in `/etc/replicator/.env` (managed manually on the VM, not in the repo).
The unit's `ExecStartPre` writes the current git SHA to `/run/replicator/build-id` and exposes it
as `BUILD_ID`, and asserts the Redis `>=7.0` floor via `scripts/check_redis_floor.sh`.

Because `ExecStart` runs `--frozen --no-sync`, run `uv sync --frozen` as part of the deploy, before
`systemctl restart`.
