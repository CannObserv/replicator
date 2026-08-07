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

### Seeding commands

`scripts/seed_fetch.py` is the MVP's command issuer — nothing else publishes to
`content.fetch` until the Watcher cutover (parent strategy Phase 4). The target is
never defaulted: `--redis-url` and `--topic` are both required.

```bash
# Safe rehearsal: print the frames, contact nothing.
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.seed \
  --dry-run https://example.test/a

# Scratch database — reaches no worker.
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.seed \
  https://example.test/a https://example.test/b

# The live loop. --production is required for db 0 + content.fetch, because the
# running service will fetch these URLs for real. --watch tails the fact stream
# until each command has an outcome — blob_available, or a fetch_failed naming
# the reason. Exit 1 if a command failed or no fact ever arrived.
# The target below is the local /health app — start it first (see API, below).
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/0 --topic content.fetch \
  --production --watch http://localhost:8041/health
```

`--watch` reads `content.blobs` for `content.fetch` and `<topic>.blobs` otherwise, so the
scratch invocation above watches its own facts rather than production's. `--blobs-topic`
overrides that. One stream, both outcomes: an issuer needs a single consumer group to see
whether its command produced bytes or a reason.

`--info-source-id` sets the domain key the command carries and both facts echo, required on the
wire since co-core 0.8.0 (#28). It defaults to `seed-harness-not-a-real-info-source`, which no
issuer's InfoSource table contains — so a fact a seed run puts on a shared stream is recognizably
synthetic. Pass a real id when the point of the run is watching an issuer's correlation end to end.

`--header` and `--timeout` set the command's per-fetch request options (#11). They apply to
every URL in the run, and omitting them is the pre-#11 wire exactly.

```bash
# Pin the User-Agent — the fingerprint-continuity case Watcher needs at cutover.
# --header is repeatable; the name is case-insensitive (the worker folds it).
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.seed \
  --header 'User-Agent: watcher/0.1.0' --header 'Accept: text/html' \
  --timeout 5 --watch https://example.test/a

# Exercise the refusal path: a Host override is refused before any request goes
# out, closing the command as fetch_failed / invalid_request_options.
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.seed \
  --header 'Host: elsewhere.test' --watch https://example.test/a
```

The script rejects a malformed `--header` and a repeated name (exit 2) but deliberately does
**not** pre-empt the worker's refusal list — sending a refused header is how the refusal is
exercised against a live worker. The full list is in
[`docs/contracts/content-fetch-issuer-reference.md`](contracts/content-fetch-issuer-reference.md).

Watch the other side with `sudo journalctl -u replicator -f`.

### Inspecting the consume path

```bash
# Pending entries: id, holder, idle ms, and delivery count — the last field is
# the times_delivered the DLQ ceiling reads. Add `IDLE <ms>` before the range to
# filter to entries idle at least that long (what claim_stale would reclaim).
redis-cli XPENDING content.fetch replicator.fetch - + 10

# Dead-lettered frames.
redis-cli XLEN content.fetch.dlq
redis-cli XRANGE content.fetch.dlq - + COUNT 5

# Dedupe keys (one per handled command, TTL REPLICATOR_DEDUPE_TTL_SECONDS).
redis-cli --scan --pattern 'replicator:cmd:*' | head

# Facts published — content.blobs carries both outcomes. On blob_available,
# blob_uri points at REPLICATOR_BLOB_DIR and the fingerprint is the filename, so
# `sha256sum` on the blob must reproduce it.
redis-cli XLEN content.blobs
redis-cli XRANGE content.blobs - + COUNT 5

# Just the failures. Matches the payload JSON, which is one line per entry and
# carries the whole fact — do NOT grep the bare token, which also hits the
# hoisted event_type field and interleaves half-records. A dead-lettered command
# should appear here *and* in content.fetch.dlq — the fact is the issuer's
# surface, the DLQ is the operator's.
redis-cli XRANGE content.blobs - + COUNT 200 | grep '"event_type":"fetch_failed"'
```

## API (dev only)

```bash
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload --log-config src/core/log_config.json
curl -s localhost:8041/health | jq
```

## Tests

```bash
uv run pytest                              # full suite, coverage gate active
uv run pytest --no-cov tests/worker/       # subset; skip the gate (it measures all of src/)
uv run pytest --no-cov -m integration      # requires the live VM Redis; skip the gate
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

### Politeness — `content.fetch-policy` (#19)

Where to start when a host is being fetched more or less often than expected. The stream is
config/state: last-write-wins per host, read **without a consumer group**, replayed from the
beginning at every worker boot.

```bash
# Has the producer published anything at all? An empty stream is not an error —
# it means every host resolves to REPLICATOR_MIN_HOST_INTERVAL_SECONDS — but it
# is the first thing to rule out, and it looks identical to a working consumer.
redis-cli XLEN content.fetch-policy
redis-cli XRANGE content.fetch-policy - + COUNT 10

# What one host is actually paced at. `revoked: true` is a tombstone meaning
# "no explicit policy", not "no limit" — it falls back to the env default.
redis-cli XRANGE content.fetch-policy - + COUNT 500 | grep '"host":"example.test"'

# Expected EMPTY. A group here is a bug: every worker needs every message, so a
# group would compete for them and grow a PEL nothing acks or drains.
redis-cli XINFO GROUPS content.fetch-policy
```

The worker's own view, from the journal — what it rebuilt at boot and what it has applied since:

```bash
sudo journalctl -u replicator | grep 'fetch policy replay complete'   # tracked_hosts, messages, duration_ms
sudo journalctl -u replicator -f | grep 'applied a host fetch policy' # host, min_interval, and the default beside it
sudo journalctl -u replicator | grep 'stricter than the fallback'     # raise REPLICATOR_MIN_HOST_INTERVAL_SECONDS
```

`tracked_hosts: 0` with a non-empty `XLEN` means messages arrived and none applied — check for
`ignoring a ...` warnings on the same boot. The last grep is the one that needs acting on: it
names a host whose real policy is stricter than the fallback that would replace it if the
policy were revoked or missed on a replay.

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

# The installed unit is a COPY, not a symlink — a merge that touched
# deploy/replicator.service needs the cp above re-run before the reload, or the
# worker comes up on new code under the old unit with nothing to show for it.
diff /etc/systemd/system/replicator.service deploy/replicator.service

sudo journalctl -u replicator -f
```
