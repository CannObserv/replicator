# Commands

## Setup

```bash
# Mirror the private cannobserv index into ./.wheelhouse. Run BEFORE uv sync on a
# fresh clone and after any co-core version bump — co-core / co-core-aio resolve
# from that directory via [tool.uv] find-links, not from PyPI.
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py

uv sync
uv run pre-commit install
```

The sync authenticates with Application Default Credentials. On the VM that is the
service-account key at `GOOGLE_APPLICATION_CREDENTIALS`
(`/etc/replicator/co-pypi-reader.json`); in CI it is the keyless WIF token written by
`google-github-actions/auth`. Either identity needs only `roles/storage.objectViewer`.

## Environment

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
```

## Worker

```bash
# Run the bus consumer locally. Use a distinct consumer name when the live
# service is also running — a shared name means a shared pending-entries list.
REPLICATOR_CONSUMER_NAME="replicator@$(whoami)-dev" uv run python -m src.worker.main

# Ctrl-C (or SIGTERM) finishes the in-flight message, acks it, and exits 0.
```

### Inspecting the consume path

```bash
# Pending entries: who holds what, and how long it has been idle.
redis-cli XPENDING content.fetch replicator.fetch - + 10

# Delivery counts (the DLQ ceiling reads times_delivered from here).
redis-cli XPENDING content.fetch replicator.fetch IDLE 0 - + 10

# Dead-lettered frames.
redis-cli XLEN content.fetch.dlq
redis-cli XRANGE content.fetch.dlq - + COUNT 5

# Dedupe keys (one per handled command, TTL REPLICATOR_DEDUPE_TTL_SECONDS).
redis-cli --scan --pattern 'replicator:cmd:*' | head
```

## API (dev only)

```bash
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload
curl -s localhost:8041/health | jq
```

## Tests

```bash
uv run pytest                              # full suite, coverage gate active
uv run pytest --no-cov tests/worker/       # subset; skip the gate (it measures all of src/)
uv run pytest -m integration               # requires the live VM Redis
```

## Lint

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check          # non-gating, advisory only
```

## Redis

Redis is Archiver-operated shared infrastructure — inspect, don't administer.

```bash
bash scripts/check_redis_floor.sh                       # assert the >=7.0 server floor
redis-cli -u "${REPLICATOR_REDIS_URL:-redis://localhost:6379/0}" INFO server | grep redis_version

# Bus inspection
redis-cli XINFO STREAM content.fetch
redis-cli XINFO GROUPS content.fetch
redis-cli XINFO CONSUMERS content.fetch replicator.fetch
redis-cli XLEN content.fetch.dlq                        # dead-lettered frames
```

## Submodules

```bash
git submodule update --init --recursive       # after a fresh clone
bash .skills/doctor.sh                        # repair dangling skill symlinks
git submodule update --remote --merge         # pull upstream skill changes
```

## Deploy

```bash
sudo cp deploy/replicator.service /etc/systemd/system/replicator.service
sudo systemctl daemon-reload
sudo systemctl enable --now replicator

uv sync --frozen && sudo systemctl restart replicator   # after merging to main
sudo journalctl -u replicator -f
```
