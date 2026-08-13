# replicator — Agent Guidelines

Be terse. Prefer fragments over full sentences. Skip filler and preamble. Sacrifice grammar for density. Lead with the answer or action.

## Project Overview

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

Owns content fetching, temp storage, and fingerprinting — the network-bound, byte-handling work re-homed out of Watcher. Driven by **commands** on the Redis change bus; reports outcomes as **facts**.

```
content.fetch (command) → fetch → fingerprint → temp-store → blob_available (fact)
                        ↘ closed without bytes ───────────────→ fetch_failed  (fact)
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

Auth is ADC: on the VM the SA key at `GOOGLE_APPLICATION_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`), in CI a keyless WIF token. Pin the current minor — `>=0.9.4,<0.10`. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead: 0.8.0 requires `info_source_id` on all three fetch payloads, so a skew is a ValidationError at construction (#19, #28); 0.9.4 cuts the replicate contracts, so it is an ImportError at load and never reaches a running worker (#29).

<!-- BEGIN socraticode-policy -->
## Code Exploration Policy

SocratiCode is the preferred semantic-search tool for this repo. Code is indexed into the local Qdrant store + on-disk graph by `codebase_index`; the project's non-code knowledge (design plans, the systemd unit, the command reference) is registered in `.socraticodecontextartifacts.json` and embedded by `codebase_context_index`. Its MCP tools are **deferred** — schemas load only after a `ToolSearch` prefetch.

**The manifest is a source, not the artifact.** Nothing re-embeds it — no hook, no CI step — so editing a `description` there changes what the repo says and not what `codebase_context_search` returns. Re-run `codebase_context_index` in the same change, or the highest-authority answer an agent gets stays the stale one (#19 CR #17).

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

Prefetch query — the `SessionStart` hook in `.claude/settings.json` prints the exact `select:` argument every session. Run it via `ToolSearch` before broad exploration; it is not repeated here.
<!-- END socraticode-policy -->

## Project Layout

`src/worker/` is the primary process — the bus consumer, with the byte path
(`handler.py`), the failure fact (`reporter.py`), the retention sweep, the pacer,
and the `content.fetch-policy` reader behind their own seams. `src/storage/` is
the content-addressed temp store behind the `BlobStore` protocol; `src/api/` is
the dev-only `/health` app; `src/core/` holds config, logging, and the handler's
failure vocabulary. `tests/` mirrors `src/`. Every module with the job it owns:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Infrastructure

**Single-VM setup.** Code committed to main is the deployed code. Replicator shares the VM with archiver, watcher, and notifier.

The worker binds no port; 8041 is the dev API port and 8040 is reserved. **Redis
is Archiver-operated** — Replicator is a client, never ships a broker, never
claims ownership — and server **≥ 7.0** is Replicator-critical because
`claim_stale` reads `XAUTOCLAIM`'s three-element reply. `scripts/check_redis_floor.sh`
guards it as an `ExecStartPre`. Ports, neighbours, and the redis-py pin:
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Server Lifecycle

`replicator.service` runs the worker. Deploy committed code with `uv sync --frozen
&& sudo systemctl restart replicator`; debug with `sudo journalctl -u replicator -f`;
test a branch with `uv run python -m src.worker.main` under a distinct
`REPLICATOR_CONSUMER_NAME`. **After editing `deploy/replicator.service`, `cp` it to
`/etc/systemd/system/`** — the installed unit is a copy, not a symlink, so
`daemon-reload` alone silently re-reads the old file and the mismatch has no
symptom until a directive matters. Full lifecycle table and the dev-server
invocation: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

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

Every variable the service reads, with the reasoning behind each default:
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Bus Conventions

Replicator is a **consumer** first. Follow the conventions co-core and the archiver producer established:

- **At-least-once ⇒ idempotent.** The command dedupes on `command_id`; both facts
  are keyed per *occurrence* (`content_fingerprint:command_id`,
  `command_id:occurred_at`), so nothing an issuer waits on can collapse — storage
  identity and correlation identity are not interchangeable. `info_source_id`
  rides both and is **echoed, never read**, as is replicate's
  `info_item_rep_spec_id`: each `test_boundaries.py` carve-out is one field
  wide, and adding one edits the charter (#28, #29).
- **Store, then publish — never the reverse.** A fact pointing at bytes that are
  not there is unrepairable by the consumer; stored bytes with no fact repair
  themselves on the reclaim.
- **Read `count=1`.** `AsyncBusConsumer.read(count>1)` raises on a malformed frame
  *before* returning the well-formed ones, and `claim_stale` at `count>1` lets a
  poison entry jam recovery permanently.
- **`from_wire` is fail-loud and its dispatch table is global** — `isinstance`-check
  every decoded payload before destructuring. Use the canonical `extra="ignore"`
  models on the consume path, never the strict `*Emit` classes, and branch on
  `schema_version` first.
- **Deterministic failure ⇒ DLQ; transient failure ⇒ retry.** `dead_letter` acks
  inside itself, so a fact is published *before* it. Retry cadence is
  `REPLICATOR_CLAIM_MIN_IDLE_MS`; a failing *cycle* is `run_loop`'s problem, not
  the message's.
- **Consumers must be idempotent; producers own the outbox.** Replicator has no DB
  — its durable record of intent is the consumer group's PEL. Do not add a
  Postgres outbox to the consume path.
- **Three stream kinds, three sets of rules.** `content.fetch` is the command
  stream, `content.blobs` carries both outcomes (`blob_available` and
  `fetch_failed`), and `content.fetch-policy` is read **groupless** — no group, no
  ack, no DLQ. Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before changing
  what any of them carries.
- **Nothing but the seed script writes to `content.fetch`.** `scripts/seed_fetch.py`
  requires `--production` for the one combination the live worker consumes — a
  frame there is fetched for real.
- **Three normative contracts bound the wire and the roadmap**, all under
  `docs/contracts/` and linked from sibling repos: the `content.fetch` issuer
  contract, the boundaries charter, and — settled ahead of its code — the
  `content.replicate` one (#34). The fetch contract splits into a read-through
  half and a lookup half (`-contract` / `-reference`, #24); both are normative. `tests/test_boundaries.py` enforces eight charter
  invariants in CI; change a charter and its tests together.

Blob paths, modes, and the retention sweep: [docs/STORAGE.md](docs/STORAGE.md).
Fakeredis's divergences from the live broker, and the keys an integration run may
touch: [docs/TESTING.md](docs/TESTING.md).

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
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload --log-config src/core/log_config.json
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

The logging stack — one formatter, two installers, and the two journald lines that
are deliberately not JSON: [docs/STYLE.md](docs/STYLE.md).

**Date & Time:**
- All UTC
- ISO 8601: `YYYY-MM-DDTHH:MM:SS.ffffffZ` (timestamps), `YYYY-MM-DD` (dates)

**General:**
- No inline module imports; all at file top
- Docstrings for public modules, classes, functions
- Test structure mirrors source (`src/foo.py` → `tests/test_foo.py`). A module whose tests outgrow one file splits by concern, not by helper: `tests/worker/test_loop_dlq.py`, `test_loop_recovery.py`, … with the shared wiring in that package's `conftest.py`. Concern is the default axis; **environment** is the one exception — tests needing a live broker split off with an `_integration` suffix (`tests/worker/test_main_integration.py`), so the filename says what the marker enforces
- Explicit imports only
- Small, focused functions

## Detail Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — module-by-module layout and every bus contract's reasoning; read before changing what a stream carries
- [docs/STORAGE.md](docs/STORAGE.md) — blob paths and modes, the three populations under `REPLICATOR_BLOB_DIR`, TTL and ceiling semantics
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — VM topology, ports, the systemd unit's lifecycle, and every environment variable the service reads
- [docs/TESTING.md](docs/TESTING.md) — where fakeredis diverges from the live broker, and which keys an integration run may create
- [docs/STYLE.md](docs/STYLE.md) — the logging stack: formatter, installers, and the non-JSON journald lines
- [docs/COMMANDS.md](docs/COMMANDS.md) — every runnable command, with flags
- [docs/SKILLS.md](docs/SKILLS.md) — vendored skill inventory and refresh procedure
- [docs/contracts/content-fetch-issuer-contract.md](docs/contracts/content-fetch-issuer-contract.md) — what a `content.fetch` producer must do; normative, linked from issuer repos
- [docs/contracts/content-fetch-issuer-reference.md](docs/contracts/content-fetch-issuer-reference.md) — its lookup half: the refusal list, the failure taxonomy, the silent conditions, trust posture
- [docs/contracts/replicator-boundaries.md](docs/contracts/replicator-boundaries.md) — what Replicator may become; run its three tests against any proposed capability
- [docs/contracts/content-replicate-issuer-contract.md](docs/contracts/content-replicate-issuer-contract.md) — the replicate trust model and issuer obligations, settled ahead of the code (#34)
