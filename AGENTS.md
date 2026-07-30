# replicator — Agent Guidelines

Be terse. Prefer fragments over full sentences. Skip filler and preamble. Sacrifice grammar for density. Lead with the answer or action.

## Project Overview

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

Owns content fetching, temp storage, and fingerprinting — the network-bound, byte-handling work re-homed out of Watcher. Driven by **commands** on the Redis change bus; reports outcomes as **facts**.

```
content.fetch (command) → fetch → fingerprint → temp-store → blob_available (fact)
```

Founding design: `docs/plans/2026-06-25-replicator-mvp-design.md`. Parent strategy lives in archiver (`docs/plans/2026-06-25-observer-cluster-integration-strategy-design.md`); Replicator is its Phase 3.

**Worker-first.** Primary process = bus consumer (`src/worker/main.py`), not an HTTP API. The FastAPI app is a `/health` surface only, dev-only until a status endpoint is wanted.

## Development Methodology

TDD required. Red → Green → Refactor. No production code without a failing test first.

## Environment & Tooling

Python ≥3.12, uv, pytest, ruff. `ty` is available as a **non-gating** type checker (`uv run ty check`) — advisory only; no pre-commit or CI gate.

**co-core comes from the wheelhouse, not PyPI.** `co-core` / `co-core-aio` resolve from `./.wheelhouse`, mirrored from the private GCS index `gs://co-gcs-pypi` by `scripts/sync_wheelhouse.py` via `[tool.uv] find-links`. Run the sync **before** `uv sync` on a fresh clone or after a version bump:

```bash
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
```

Auth is ADC: on the VM the SA key at `GOOGLE_APPLICATION_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`), in CI a keyless WIF token. Pin the current minor — `>=0.7,<0.8`.

## Code Exploration Policy

SocratiCode is the preferred semantic-search tool for this repo (once indexed; the index lives in `.socraticodecontextartifacts.json` once `codebase_index` has run). Its MCP tools are **deferred** — schemas load only after a `ToolSearch` prefetch.

**Negative rule.** For broad semantic questions ("where is X", "how does Y work", "what depends on Z"), use SocratiCode MCP tools first. Reach for `grep`/`ripgrep` only on exact strings (error messages, log lines, known symbols). Reserve the Explore subagent for path-pattern walks (e.g. "all `*.py` under `src/worker/`"), not semantic search.

| Goal | Tool |
|------|------|
| Where is X defined / how does Y work / what files touch Z | `codebase_search` |
| Exact string/regex match (errors, log lines, known symbols) | `grep` / `rg` |
| Blast radius of changing/deleting a file or function | `codebase_impact` |
| What does an entry point actually do? | `codebase_flow` |
| Callers and callees of a function | `codebase_symbol` |
| Imports/dependents of a file | `codebase_graph_query` |
| Deployment topology, runbook context | `codebase_context` / `codebase_context_search` |

Prefetch query — run via `ToolSearch` at session start:

`select:mcp__plugin_socraticode_socraticode__codebase_search,mcp__plugin_socraticode_socraticode__codebase_symbol,mcp__plugin_socraticode_socraticode__codebase_symbols,mcp__plugin_socraticode_socraticode__codebase_flow,mcp__plugin_socraticode_socraticode__codebase_impact,mcp__plugin_socraticode_socraticode__codebase_graph_query,mcp__plugin_socraticode_socraticode__codebase_status,mcp__plugin_socraticode_socraticode__codebase_context,mcp__plugin_socraticode_socraticode__codebase_context_search`

## Project Layout

```
src/worker/     — Bus consumer; the primary process
src/worker/main.py   — Entry point: client lifetime, consumer group, message loop
src/api/        — FastAPI app (/health only; not part of the MVP loop)
src/api/main.py — App factory, lifespan, router registration
src/core/       — Shared domain logic, logging, config
src/core/logging.py  — configure_logging() + get_logger()
src/core/config.py   — Settings / env access (see Environment Variables)
scripts/        — sync_wheelhouse.py, check_redis_floor.sh
tests/          — Mirrors src/ structure; integration tests in `@pytest.mark.integration`
docs/           — Reference docs (COMMANDS, SKILLS); docs/plans/ holds implementation plans
deploy/         — Systemd unit + deployment config
.wheelhouse/    — Local mirror of the private cannobserv index (git-ignored except .gitkeep)
```

## Infrastructure

**Single-VM setup.** Code committed to main is the deployed code. Replicator shares the VM with archiver, watcher, and notifier.

| Service | Framework | Port | Managed by |
|---|---|---|---|
| Worker (live) | asyncio bus consumer | — | `systemctl` (`replicator.service`) |
| API (dev) | FastAPI | 8041 | manual uvicorn |

The worker binds no port. Port 8040 is reserved for Replicator's API should it ever be deployed; 8041 is the dev port. Neighbours: watcher 8000/8001, archiver 8020/8021, notifier 9000/9001. The exe.dev proxy transparently forwards ports 3000–9999; the dev server is reachable at `https://replicator.exe.xyz:8041/`.

### Redis is Archiver-operated — Replicator connects, it does not run its own

The Redis change bus is Archiver-operated cluster infrastructure (the shared VM's `redis-server.service`). Replicator is a **client**: never ship a broker, never claim ownership.

**Redis ≥ 7.0 is Replicator-critical.** Replicator is the cluster's first user of `AsyncBusConsumer.claim_stale`, which reads `XAUTOCLAIM`'s three-element reply — the deleted-ids element added in Redis **server** 7.0. Below that, the crash-recovery path raises. `scripts/check_redis_floor.sh` guards this as an `ExecStartPre`. (The VM runs 7.0.15.)

The **redis-py client** resolves `>=5,<8` transitively via `co-core-aio[bus]`. Don't re-pin it narrower.

## Server Lifecycle

**`replicator.service` runs the worker.** It binds no port, so there is no port to conflict over — but only one process should hold a given consumer name at a time.

| Situation | Action |
|---|---|
| Code committed to main | `uv sync --frozen && sudo systemctl restart replicator` |
| Testing a worktree/branch | `uv run python -m src.worker.main` (set a distinct `REPLICATOR_CONSUMER_NAME`) |
| Debugging the live service | `sudo journalctl -u replicator -f` |
| After editing `deploy/replicator.service` | `sudo systemctl daemon-reload && sudo systemctl restart replicator` |
| After a co-core version bump | re-run `sync_wheelhouse.py`, then `uv sync` |

`ExecStart` uses `--frozen --no-sync`, so dependency sync is a deploy step, not a service-start side effect.

**Dev server workflow** (the `/health` app, port 8041 so a future live service stays up):

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload
```

## Environment Variables

Two env files, with a hard boundary between them:

1. **`/etc/replicator/.env`** — production configuration. Survives repo resets and worktree switches. Managed manually on the VM. **The only file `replicator.service` reads.**
2. **`.env`** (repo root, git-ignored) — dev/agent secrets: `GH_TOKEN` plus the per-repo PATs (`GH_TOKEN_ARCHIVER`, `GH_TOKEN_WATCHER`, `GH_TOKEN_CANNOBSERV`, `GH_TOKEN_SKILLS`). Never commit.

**The service must never load the repo `.env`.** Those PATs carry org-wide write access the worker has no use for; handing them to a process whose job is fetching public URLs widens the blast radius of any crash dump or subprocess for no benefit. Anything the service genuinely needs belongs in `/etc/replicator/.env`.

For shell commands (dev only), load both:

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
```

Replicator-owned settings carry the `REPLICATOR_` prefix so they never collide with a sibling service on the shared VM. `BUILD_ID` is deliberately unprefixed — the systemd unit stamps it generically.

In `.env` (dev/agent only — never read by the service):
- `GH_TOKEN` — GitHub PAT for this repo (used by `gh` CLI)
- `GH_TOKEN_ARCHIVER` / `GH_TOKEN_WATCHER` / `GH_TOKEN_CANNOBSERV` / `GH_TOKEN_SKILLS` — per-repo PATs. Cross-repo work is **filed as an issue**, never edited directly: each repo owns its own review, CI, and deploy cycle, and `main` is the deployed code. Pass the right one as `GH_TOKEN` for a given `gh` call.

In `/etc/replicator/.env` (read by the service):
- `GOOGLE_APPLICATION_CREDENTIALS` — SA key for the wheelhouse mirror (`/etc/replicator/co-pypi-reader.json`)
- `REPLICATOR_REDIS_URL` — change-bus client URL; default `redis://localhost:6379/0`
- `REPLICATOR_BLOB_DIR` — temp-storage root for fetched bytes; default `blobs`
- `REPLICATOR_CONSUMER_GROUP` — consumer group on `content.fetch`; default `replicator.fetch`
- `REPLICATOR_CONSUMER_NAME` — this worker's identity within the group; defaults to `replicator@<hostname>`. Two workers must never share one — Redis tracks pending entries per consumer name, and a shared name makes independent `claim_stale` recovery impossible
- `REPLICATOR_LOG_LEVEL` — default `INFO`
- `BUILD_ID` — git SHA stamped by the systemd unit's `ExecStartPre`; defaults to `"dev"` outside systemd

## Bus Conventions

Replicator is a **consumer** first. Follow the conventions co-core and the archiver producer established:

- **At-least-once ⇒ idempotent.** Two idempotency keys, two levels: the **command** dedupes on `command_id`, the **fact** on `content_fingerprint`. Content-addressed storage makes re-storing identical bytes a no-op regardless.
- **Consumers must be idempotent; producers own the outbox.** The cluster split (parent strategy, "Delivery + correctness") assigns the transactional outbox to producers with a DB system of record. Replicator has none — its durable record of intent is the consumer group's PEL, recovered via `claim_stale`. Do not add a Postgres outbox to the consume path.
- **Validation posture:** use the canonical `extra="ignore"` models; **branch on `schema_version` before destructuring**; tolerate additive producer fields. Never use the strict `*Emit` classes on the consume path.
- **Batch-poison caveat:** `AsyncBusConsumer.read(count>1)` raises `BusMessageAnomaly` on a malformed frame *before* returning the well-formed ones in the batch. Read `count=1`, or catch the anomaly and route via `dead_letter`. `from_wire` is deliberately fail-loud.
- **DLQ is a shipped seam, not a TODO:** `dead_letter(message_id, fields)` copies the frame to `<topic>.dlq` and acks the original. Deterministic failure ⇒ DLQ; transient failure ⇒ retry.
- **Bus clients are injection-only** — the co-core driver never opens or closes the `redis.asyncio.Redis` client. The worker owns one for its lifetime.
- `sha256` lives at `co_core.pure.util.hashing`, not `co_core.pure.extract` (which carries `simhash`, `Chunk`, and the parsers). Import parsers from submodules — they are not re-exported from `__init__`.

**Testing the bus.** `tests/conftest.py` ships a `fake_redis` fixture (fakeredis, Streams-capable) — consumer-group behaviour is testable without a broker, and assertions should read the broker's own view (`xinfo_groups` / `xinfo_consumers`) rather than co-core's private attributes, which are not a stable contract. Anything that genuinely needs the live Archiver-operated Redis goes behind `@pytest.mark.integration` and is excluded by default.

## Common Commands

```bash
# Mirror the private index, then install
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
uv sync

# Load environment (required before running the worker or gh)
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a

# Run tests
uv run pytest

# Run a subset of tests (skip the coverage gate, which measures all of src/)
uv run pytest --no-cov tests/path/to/test.py

# Run integration tests (requires the live VM Redis)
uv run pytest -m integration

# Run linter
uv run ruff check .

# Run the worker locally
uv run python -m src.worker.main

# FastAPI dev server (/health only)
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload
```

Full reference: `docs/COMMANDS.md`

## Agent Skills

Skills in `skills/` (agentskills.io) and `.claude/skills/` (Claude Code). Reference: `docs/SKILLS.md`

## Conventions

**Commit Messages:**
```
#<number> [type]: <description>      # with issue
[type]: <description>                # without issue
```
Types: feat, fix, refactor, docs, test, chore

**Logging:**
```python
from src.core.logging import get_logger

logger = get_logger(__name__)
```
Entry points only: `configure_logging()` is called once inside the FastAPI `lifespan` or the worker's `run()`. Never in library modules.

**Date & Time:**
- All UTC
- ISO 8601: `YYYY-MM-DDTHH:MM:SS.ffffffZ` (timestamps), `YYYY-MM-DD` (dates)

**General:**
- No inline module imports; all at file top
- Docstrings for public modules, classes, functions
- Test structure mirrors source (`src/foo.py` → `tests/test_foo.py`)
- Explicit imports only
- Small, focused functions
