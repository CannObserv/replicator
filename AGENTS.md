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

<!-- BEGIN socraticode-policy -->
## Code Exploration Policy

SocratiCode is the preferred semantic-search tool for this repo. Code is indexed into the local Qdrant store + on-disk graph by `codebase_index`; the project's non-code knowledge (design plans, the systemd unit, the command reference) is registered in `.socraticodecontextartifacts.json` and embedded by `codebase_context_index`. Its MCP tools are **deferred** — schemas load only after a `ToolSearch` prefetch.

**Negative rule.** For broad semantic questions ("where is X", "how does Y work", "what depends on Z"), use SocratiCode MCP tools first. Reach for `grep`/`ripgrep` only on exact strings (error messages, log lines, known symbols). Reserve the Explore subagent for path-pattern walks (e.g. "all `*.py` under `src/worker/`"), not semantic search.

| Goal | Tool |
|------|------|
| Where is X defined / how does Y work / what files touch Z | `codebase_search` |
| Exact string/regex match (errors, log lines, known symbols) | `grep` / `rg` |
| Blast radius of changing/deleting a file or function | `codebase_impact` |
| What does an entry point actually do? | `codebase_flow` |
| Callers and callees of a function | `codebase_symbol` |
| Imports/dependents of a file | `codebase_graph_query` |
| Bus contracts, deploy topology, MVP design rationale, env vars | `codebase_context` / `codebase_context_search` |

**Cross-repo search.** `SOCRATICODE_LINKED_PROJECTS` (in `.claude/settings.local.json`, gitignored) links the archiver, watcher, and notifier checkouts, so `codebase_search` spans the cluster. Use it for the co-core contracts, the parent integration strategy, and the producer-side outbox precedent — all of which live in archiver, not here. Linked projects contribute results only once they are themselves indexed.

Prefetch query — run via `ToolSearch` at session start:

`select:mcp__plugin_socraticode_socraticode__codebase_search,mcp__plugin_socraticode_socraticode__codebase_symbol,mcp__plugin_socraticode_socraticode__codebase_symbols,mcp__plugin_socraticode_socraticode__codebase_flow,mcp__plugin_socraticode_socraticode__codebase_impact,mcp__plugin_socraticode_socraticode__codebase_graph_query,mcp__plugin_socraticode_socraticode__codebase_status,mcp__plugin_socraticode_socraticode__codebase_context,mcp__plugin_socraticode_socraticode__codebase_context_search`
<!-- END socraticode-policy -->

## Project Layout

```
src/worker/     — Bus consumer; the primary process
src/worker/main.py   — Entry point: client lifetime, consumer group, signals
src/worker/loop.py   — The consume path: poll → dispatch → ack, DLQ, dedupe, recovery
src/worker/handler.py — The byte path behind the Handler seam: fetch → fingerprint → store → publish
src/storage/    — Temp storage; BlobStore protocol + the local-FS backend
src/storage/base.py  — BlobStore protocol (store / exists / open)
src/storage/local.py — Content-addressed local backend; file:// URIs, sharded paths
src/api/        — FastAPI app (/health only; not part of the MVP loop)
src/api/main.py — App factory, lifespan, router registration
src/core/       — Shared domain logic, logging, config
src/core/errors.py   — TransientFetchError / PermanentFetchError (handler failure vocabulary)
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

Read by neither env file — test-only, defined in `tests/conftest.py`:
- `REPLICATOR_TEST_REDIS_URL` — live broker for `@pytest.mark.integration`; default `redis://localhost:6379/15`. Must not resolve to db 0 (the fixture fails outright if it does) — see **Testing the bus**

In `/etc/replicator/.env` (read by the service):
- `GOOGLE_APPLICATION_CREDENTIALS` — SA key for the wheelhouse mirror (`/etc/replicator/co-pypi-reader.json`)
- `REPLICATOR_REDIS_URL` — change-bus client URL; default `redis://localhost:6379/0`
- `REPLICATOR_BLOB_DIR` — temp-storage root for fetched bytes; default `blobs`. Resolved to an absolute path at store construction — `file://` URIs require it
- `REPLICATOR_MAX_BLOB_BYTES` — ceiling on one fetched body; default `67108864` (64 MiB). A **storage** guard, not a memory one: co-core's fetch driver buffers the whole response before returning it, so the bytes are already resident when this is checked. Over the ceiling ⇒ `PermanentFetchError` ⇒ DLQ
- `REPLICATOR_CONSUMER_GROUP` — consumer group on `content.fetch`; default `replicator.fetch`
- `REPLICATOR_CONSUMER_NAME` — this worker's identity within the group; defaults to `replicator@<hostname>`. Two workers must never share one — Redis tracks pending entries per consumer name, and a shared name makes independent `claim_stale` recovery impossible
- `REPLICATOR_CONSUMER_START_ID` — group start position; default `"$"` (new messages only), `"0"` drains the backlog. Applies **only at group creation** — once `replicator.fetch` exists, changing this also needs a manual `XGROUP SETID`
- `REPLICATOR_READ_BLOCK_MS` — blocking-read window; default `5000`. Bounds worst-case shutdown latency, so the unit's `TimeoutStopSec` must exceed it plus the handler budget
- `REPLICATOR_CLAIM_MIN_IDLE_MS` — idle time before a pending entry may be reclaimed; default `60000`. Doubles as the retry cadence
- `REPLICATOR_MAX_DELIVERY_ATTEMPTS` — delivery ceiling for *unclassified* failures before DLQ; default `5`. Counted from XPENDING's delivery counter, which only advances on a reclaim ⇒ a bound in time, not retries
- `REPLICATOR_ERROR_BACKOFF_BASE_SECONDS` / `REPLICATOR_ERROR_BACKOFF_MAX_SECONDS` — backoff for a poll *cycle* that raised (broker outage); defaults `1.0` / `30.0`, escalating `base * 2**(n-1)`
- `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` — consecutive failed cycles before the worker exits so the unit restarts; default `20` (~8 min at the default backoff). Paired with the unit's `StartLimitIntervalSec=3600` / `StartLimitBurst=3`
- `REPLICATOR_DEDUPE_TTL_SECONDS` — lifetime of the `replicator:cmd:<command_id>` dedupe key; default `86400`
- `REPLICATOR_LOG_LEVEL` — default `INFO`
- `BUILD_ID` — git SHA stamped by the systemd unit's `ExecStartPre`; defaults to `"dev"` outside systemd

## Bus Conventions

Replicator is a **consumer** first. Follow the conventions co-core and the archiver producer established:

- **At-least-once ⇒ idempotent.** Two idempotency keys, two levels: the **command** dedupes on `command_id`, the **fact** on `content_fingerprint`. Content-addressed storage makes re-storing identical bytes a no-op regardless.
- **Consumers must be idempotent; producers own the outbox.** The cluster split (parent strategy, "Delivery + correctness") assigns the transactional outbox to producers with a DB system of record. Replicator has none — its durable record of intent is the consumer group's PEL, recovered via `claim_stale`. Do not add a Postgres outbox to the consume path.
- **Validation posture:** use the canonical `extra="ignore"` models; **branch on `schema_version` before destructuring**; tolerate additive producer fields. Never use the strict `*Emit` classes on the consume path.
- **Batch-poison caveat:** `AsyncBusConsumer.read(count>1)` raises `BusMessageAnomaly` on a malformed frame *before* returning the well-formed ones in the batch. Read `count=1`, or catch the anomaly and route via `dead_letter`. `from_wire` is deliberately fail-loud.
- **DLQ is a shipped seam, not a TODO:** `dead_letter(message_id, fields)` copies the frame to `<topic>.dlq` and acks the original. Deterministic failure ⇒ DLQ; transient failure ⇒ retry.
- **A frame that fails to decode has no fields.** `from_wire` raises from *inside* `read`/`claim_stale`, so the anomaly carries `topic` + `message_id` only — but `dead_letter` XADDs the fields it is given and `XADD` rejects an empty map. Re-read the raw frame by id (`XRANGE topic id id`) and fall back to a synthesized record when the entry has been trimmed. `src/worker/loop.py::dead_letter_anomaly`.
- **`from_wire`'s dispatch table is global.** A `blob_available` frame XADDed to `content.fetch` decodes cleanly into the wrong model rather than raising — `isinstance`-check the payload before destructuring.
- **`claim_stale` is the retry path, not just crash recovery.** A transiently-failed message is left unacked and comes back through the same reclaim, so retry cadence = `REPLICATOR_CLAIM_MIN_IDLE_MS`. Call it with `count=1`: XAUTOCLAIM transfers ownership and resets the idle clock on every entry it returns *before* co-core decodes them, and it restarts at `0-0` each call, so a poison entry jams recovery permanently unless it is DLQ'd first.
- **Retry accounting is XPENDING's `times_delivered`**, not a side counter. It only advances on a reclaim.
- **A consumer appears in `XINFO CONSUMERS` only after its first *delivered* message.** An empty poll registers nothing, so an absent consumer entry is not evidence a worker is down — a liveness check built on it reports every idle worker as dead. Recovery is unaffected: `claim_stale` reclaims by group and idle time, not by a pre-existing consumer entry.
- **A failing *message* and a failing *cycle* are different.** `process_message` decides a message's fate; a broker refusing reads/acks/DLQ writes is `run_loop`'s problem — it backs off (`REPLICATOR_ERROR_BACKOFF_BASE_SECONDS` → `_MAX_SECONDS`) and retries, then re-raises after `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` so a permanently wrong `REPLICATOR_REDIS_URL` surfaces as a restart instead of a worker that looks alive while doing nothing. The unit's `StartLimitIntervalSec` is sized against that ceiling — change one, revisit the other.
- **Bus clients are injection-only** — the co-core driver never opens or closes the `redis.asyncio.Redis` client. The worker owns one for its lifetime.
- **Store, then publish — never the reverse.** A crash between the two must not leave a `blob_available` pointing at bytes that are not there: a consumer would read the fact, fail to open the blob, and have no way to ask again. The opposite gap (stored bytes, no fact) repairs itself — the message stays unacked and the reclaim re-runs a handler that content-addressed storage makes a no-op.
- **A blob is `file://<blob_dir>/<ab>/<cd>/<sha256>.bin`.** Sharded two levels to bound directory fan-out; the extension is a constant `.bin`, **never** derived from `media_type` — identical octets can arrive under different Content-Types, and two paths for one fingerprint would defeat `exists()` as a short-circuit. Writes go through a temp file + `os.replace`: presence at a content-addressed path is what readers take as proof the bytes are complete. Design: `docs/plans/2026-07-31-replicator-mvp-open-questions-design.md`.
- **Blob modes are set on creation only.** Files land at `0644`, directories the worker creates at `0755` — both by explicit `chmod`, since `mkstemp` creates at `0600` and `mkdir`'s mode is masked by the umask. A directory that **already exists is never re-chmod'd**: it belongs to whoever provisioned it, and `chmod` on an unowned-but-writable mount raises `EPERM`. The cost is a silent trap — a `0700` level anywhere in the chain stores and publishes normally while no other service can open the `blob_uri` — so `warn_if_unreachable` walks `blob_dir` **and every parent** at startup and names each blocking level. Traversal needs `+x` all the way up, and the likeliest mistake is a restrictive parent over a fine leaf. `src/storage/local.py::ensure_directory`, `src/worker/main.py::warn_if_unreachable`.
- **Fetch outcomes carry the loop's vocabulary, not HTTP's.** 5xx / 408 / 429 ⇒ `TransientFetchError`; every other non-2xx (including a body-less 304, which passes `is_success`) ⇒ `PermanentFetchError`. httpx's exception hierarchy is **disjoint from the builtin `ConnectionError`/`TimeoutError`** the loop already treats as transient, so `src/worker/handler.py` maps it explicitly — leaving it unmapped would burn the delivery ceiling on an origin outage.
- **`from_wire`'s topic and message_id are keyword-only** — `from_wire(fields, topic=..., message_id=...)`. The founding plan's API table showed them positionally.
- `sha256` lives at `co_core.pure.util.hashing`, not `co_core.pure.extract` (which carries `simhash`, `Chunk`, and the parsers). Import parsers from submodules — they are not re-exported from `__init__`.

**Testing the bus.** `tests/conftest.py` ships a `fake_redis` fixture (fakeredis, Streams-capable) — consumer-group behaviour is testable without a broker, and assertions should read the broker's own view (`xinfo_groups` / `xinfo_consumers`) rather than co-core's private attributes, which are not a stable contract. Anything that genuinely needs the live Archiver-operated Redis goes behind `@pytest.mark.integration` and is excluded by default.

**Where fakeredis diverges.** It is sound for consumer-group *mechanics* — what state a command leaves behind — but diverges on *lifecycle* and *blocking* semantics: it registers a consumer on an empty `XREADGROUP` (real Redis waits for a delivery, GH #3) and it ignores `block` (worked around by `IDLE_SLEEP_SECONDS` in `src/worker/loop.py`). Rule of thumb: an assertion about **what state results** is safe against the fake; an assertion about **when Redis does something** needs a live broker. Both divergences were found by running against the real server, not by the suite.

Live-broker tests use the `real_redis` fixture (`tests/conftest.py`), which connects to `REPLICATOR_TEST_REDIS_URL` (default `redis://localhost:6379/15`), skips when nothing answers (an *auth* failure re-raises — a misconfigured broker must not pass as a skip), expires stray `replicator.itest.*` keys from crashed runs once per session, and refuses db 0 outright — db 0 carries the live `content.fetch` stream that the running `replicator.service` consumes, so a test frame written there would be fetched for real. Confine such tests to scratch stream keys (`replicator.itest.<uuid>`); the database guard is the backstop, not the plan.

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

# Run integration tests (requires the live VM Redis; --no-cov because the
# coverage gate measures all of src/, which these tests do not exercise)
uv run pytest --no-cov -m integration

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
- Test structure mirrors source (`src/foo.py` → `tests/test_foo.py`). A module whose tests outgrow one file splits by concern, not by helper: `tests/worker/test_loop_dlq.py`, `test_loop_recovery.py`, … with the shared wiring in that package's `conftest.py`. Concern is the default axis; **environment** is the one exception — tests needing a live broker split off with an `_integration` suffix (`tests/worker/test_main_integration.py`), so the filename says what the marker enforces
- Explicit imports only
- Small, focused functions
