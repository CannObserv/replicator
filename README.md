# replicator

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

Replicator owns content fetching, temporary storage, and fingerprinting for the cluster — the
network-bound, byte-handling work re-homed out of Watcher. It is driven by **commands** on the
Redis change bus and reports outcomes as **facts**:

```
content.fetch (command)  →  fetch  →  fingerprint  →  temp-store  →  blob_available (fact)
                         ↘  closed without bytes  ──────────────→  fetch_failed  (fact)

content.fetch-policy (config)  →  per-host request spacing applied to that fetch
```

Both facts land on `content.blobs`, so an issuer's one consumer group sees either outcome of
its command.

`content.fetch-policy` is the third stream kind and the only one Replicator reads without a
consumer group: it carries how often each host may be asked, last-write-wins per host, replayed
from the beginning at every boot. The numbers are the issuer's — Replicator enforces spacing,
it does not decide it. See
[`docs/contracts/replicator-boundaries.md`](docs/contracts/replicator-boundaries.md).

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

Variables the service uses all live in `/etc/replicator/.env`. **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)
is the authoritative reference** — every variable, its default, and the reasoning behind
each one. Below is only what this deployment actually overrides; anything absent from
`/etc/replicator/.env` runs on the default baked into `src/core/config.py`.

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

| Variable | Set on this VM | Purpose |
|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | `/etc/replicator/co-pypi-reader.json` | SA key for the wheelhouse mirror |
| `REPLICATOR_REDIS_URL` | `redis://localhost:6379/0` | Change-bus client URL |
| `REPLICATOR_BLOB_DIR` | `/var/lib/replicator/blobs` | Temp-storage root — **not** the `blobs` default |
| `REPLICATOR_CONSUMER_NAME` | `replicator@<hostname>` | This worker's identity in the group — never share one |
| `REPLICATOR_LOG_LEVEL` | `INFO` | Root log level |

`BUILD_ID` is stamped by the unit's `ExecStartPre` rather than set in the env file. Every
other `REPLICATOR_*` setting — the blob TTL and ceilings, the consumer group and start id,
the read window and pacing fallback, the reclaim and backoff numbers — is on its default;
see [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for what each one is and why.

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
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload --log-config src/core/log_config.json
```

`--log-config` routes uvicorn's own `uvicorn` / `uvicorn.access` / `uvicorn.error` loggers — which
ship with `propagate=False` and plain-text handlers of their own — through the same JSON formatter
`configure_logging()` installs on the root logger. Without it the dev server emits mixed-format
output: plain-text access lines interleaved with JSON app records.

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
as `BUILD_ID`, asserts the Redis `>=7.0` floor via `scripts/check_redis_floor.sh`, and refreshes the
wheelhouse via `scripts/sync_wheelhouse.py` — whose journald output is **plain text, not JSON**, by
design (see [docs/STYLE.md](docs/STYLE.md), "Not everything in the journal is JSON").

Because `ExecStart` runs `--frozen --no-sync`, run `uv sync --frozen` as part of the deploy, before
`systemctl restart`.
