# replicator

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

Replicator owns content fetching, temporary storage, and fingerprinting for the cluster — the
network-bound, byte-handling work re-homed out of Watcher. It is driven by **commands** on the
Redis change bus and reports outcomes as **facts**:

```
content.fetch (command)  →  fetch  →  fingerprint  →  temp-store  →  blob_available (fact)
```

The founding design lives in
[`docs/plans/2026-06-25-replicator-mvp-design.md`](docs/plans/2026-06-25-replicator-mvp-design.md).

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

Two env files, loaded in order (later values override):

1. `/etc/replicator/.env` — production secrets, managed manually on the VM.
2. `.env` (repo root, git-ignored) — dev/agent secrets.

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
```

## Test & lint

```bash
uv run pytest
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
