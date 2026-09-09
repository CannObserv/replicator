# replicator — Agent Guidelines

Be terse. Prefer fragments over full sentences. Skip filler and preamble. Sacrifice grammar for density. Lead with the answer or action.

## Project Overview

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

**Worker-first.** Primary process = bus consumer (`src/worker/main.py`), not an HTTP API. The FastAPI app is a `/health` surface only, dev-only until a status endpoint is wanted.

The command → fact flow, and what each module owns:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Development Methodology

TDD required. Red → Green → Refactor. No production code without a failing test first.

## Environment & Tooling

Python ≥3.12, uv, pytest, ruff. `ty` is a **non-gating** type checker (`uv run ty check`) — advisory, no pre-commit or CI gate.

**co-core comes from the wheelhouse, not PyPI.** `co-core` / `co-core-aio` resolve from `./.wheelhouse`, mirrored from the private GCS index `gs://co-gcs-pypi` by `scripts/sync_wheelhouse.py` via `[tool.uv] find-links`. Run the sync **before** `uv sync` on a fresh clone or after a version bump:

```bash
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
```

Auth is ADC. Pin the current minor — `>=0.13.1,<0.14` — and raise the **patch** floor with every co-core feature the code starts depending on; the ways a skew has already failed are in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

<!-- BEGIN socraticode-policy -->
## Code Exploration Policy

SocratiCode is the preferred semantic-search tool here once indexed (local Qdrant
store + on-disk graph; manifest `.socraticodecontextartifacts.json`). Its MCP tools
are **deferred** — schemas load only after the `ToolSearch` prefetch that
`.claude/hooks/socraticode-reminder.sh` prints each session.

**Negative rule.** Use SocratiCode MCP tools first for semantic questions ("where is
X", "how does Y work", "what depends on Z"). Reach for `grep`/`rg` only on exact
strings (error messages, log lines, known symbols). Reserve the Explore subagent for
path-pattern walks (`*.py` under `src/worker/`), not semantic search.

| Goal | Tool |
|------|------|
| Where is X defined / how does Y work / what touches Z | `codebase_search` |
| Exact string or regex (errors, log lines, known symbols) | `grep` / `rg` |
| Imports/dependents of a file · blast radius of a change | `codebase_graph_query` / `codebase_impact` |

Full tool table, prefetch query, per-tool guidance, cross-repo search:
[docs/SOCRATICODE.md](docs/SOCRATICODE.md).
<!-- END socraticode-policy -->

## Code Exploration Notes (repo-specific)

**The manifest is a source, not the artifact.** Nothing re-embeds it, so re-run `codebase_context_index` in the same change as a `description` edit — otherwise the highest-authority answer an agent gets stays the stale one (#19 CR #17).

**`mcp-driver.mjs` lies twice — silently through the `skills/` symlink (skills#177), falsely from a worktree (skills#180).** Use `"$SOCRATICODE_DRIVER"`; disbelieve health findings outside the main checkout. Both in [docs/SKILLS.md](docs/SKILLS.md).

## Project Layout

`src/worker/` is the primary process — the bus consumer, with the byte path, the
failure fact, the retention sweep, the pacer, and the `content.fetch-policy` reader
each behind their own seam. `src/storage/` is the content-addressed temp store behind
the `BlobStore` protocol — **two backends** (`local`, `gcs`), selected by
`REPLICATOR_BLOB_BACKEND`, default `local` (#7). `src/api/` is the dev-only `/health`
app; `src/core/` holds config, logging, and the consume path's failure vocabulary;
`tests/` mirrors `src/`. Every module with the job it owns:
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Infrastructure

**Single-VM setup.** Code committed to main is the deployed code; the VM is shared with archiver, watcher, and notifier.

The worker binds no port; 8041 is the dev API port and 8040 is reserved. **Redis is
Archiver-operated** — Replicator is a client, never ships a broker — and server
**≥ 7.0** is Replicator-critical because `claim_stale` reads `XAUTOCLAIM`'s
three-element reply, guarded by `scripts/check_redis_floor.sh` as an `ExecStartPre`.
Ports, neighbours, and the redis-py pin: [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Server Lifecycle

`replicator.service` runs the worker. Deploy committed code with `git pull --ff-only
&& uv sync --frozen && sudo systemctl restart replicator` — `git push` instead of the
pull when the merge happened here.

Three that bite, each symptomless until it matters:

- **The service refuses to start off `main`, or off unpushed commits** (#37, #48).
  `REPLICATOR_ALLOW_ANY_CHECKOUT=1` overrides; a dev worker asks the same question
  at the writer (#52).
- **`/etc/systemd/system/replicator.service` is a copy, not a symlink** — `cp` it
  after every edit to `deploy/`, because `daemon-reload` alone re-reads the old file.
- **The daily skills-refresh hook commits without pushing**, which is one of the
  states the checkout guard refuses. Check `git status -sb` before a restart.

Every deploy situation, the guard's verdict table, and the dev-server invocation:
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Environment Variables

Two env files, and the boundary between them is a security boundary, not a
convention:

1. **`/etc/replicator/.env`** — production config. **The only file `replicator.service` reads.**
2. **`.env`** (repo root, git-ignored) — dev/agent secrets, chiefly org-wide GitHub PATs. Never commit.

**The service must never load the repo `.env`.** Those PATs carry write access the
worker has no use for, and a fetcher of public URLs must not widen their blast
radius. Anything the service needs goes in `/etc/replicator/.env`.

New settings take the `REPLICATOR_` prefix — the VM is shared, and the prefix is
what keeps a sibling service from colliding. `BUILD_ID` is the one deliberate
exception, stamped generically by the unit.

For shell commands (dev only), load both — the snippet is under Common Commands.
Every variable, which file carries it, and each default's reasoning:
[docs/ENVIRONMENT.md](docs/ENVIRONMENT.md).

## Bus Conventions

Replicator is a **consumer** first — follow what co-core and the archiver producer established:

- **At-least-once ⇒ idempotent.** The command dedupes on `command_id`; both facts
  are keyed per *occurrence* (`content_fingerprint:command_id`,
  `command_id:occurred_at`). `info_source_id` and replicate's
  `info_item_rep_spec_id` are **echoed, never read** — each `test_boundaries.py`
  carve-out is one field wide, and adding one edits the charter (#28, #29).
- **Two blob backends, one seam.** `local` announces `file://` and `gcs` announces
  `gs://`; `local` is the compiled-in default **by decision, not by schedule**.
  Every `BlobStore` call from a coroutine goes through `asyncio.to_thread`, which
  puts it in the unit's shutdown budget, not just the handler's.
  [docs/STORAGE.md](docs/STORAGE.md) is the authority — read it before touching
  either store.
- **Store, then publish — never the reverse.** A fact pointing at absent bytes is
  unrepairable by the consumer; stored bytes with no fact repair themselves on the
  reclaim.
- **Read `count=1`.** `AsyncBusConsumer.read(count>1)` raises on a malformed frame
  *before* returning the well-formed ones, and `claim_stale` at `count>1` lets a
  poison entry jam recovery permanently.
- **`from_wire` is fail-loud and its dispatch table is global** — `isinstance`-check
  every decoded payload before destructuring. Use the canonical `extra="ignore"`
  models on the consume path, never the strict `*Emit` classes, and branch on
  `schema_version` first.
- **Deterministic ⇒ DLQ; transient ⇒ retry; completed without bytes ⇒ fact + ack,
  no DLQ (#17).** `dead_letter` acks inside itself, so a fact is published
  *before* it — as `XADD <topic>.dlq` then `XACK`, the form broker#2's ACL grants
  (#79). Retry cadence is `REPLICATOR_CLAIM_MIN_IDLE_MS`; a failing *cycle* is
  `run_loop`'s problem, not the message's.
- **A capped broker refuses only its `denyoom` commands, and the worker retries
  the two it meets at runtime (#79).** `XADD` and `SET` are refused and retried
  indefinitely — `OutOfMemoryError` is transient and exempt from the delivery
  ceiling — while the consume path reads, acks and reclaims throughout. The
  third, `XGROUP CREATE … MKSTREAM`, is boot-only and does **not** retry: a first
  boot against a capped broker exits and systemd restarts. Verified against a
  broker the tests spawn, **never the shared one**. Never answer an OOM with a
  client-level retry, which republishes an `XADD` the broker already applied.
- **An ACL denial is transient too (#82).** `NoPermissionError` is the second
  `ResponseError` subclass in `_TRANSIENT_ERRORS`, so a grant broker#1's cutover
  got wrong backs off instead of closing valid commands with a terminal
  `fetch_failed(handler_error)` — at the deliberate cost that a grant nobody
  fixes retries forever.
- **The `replicator:cmd:*` keys are the only non-stream keys on the broker (#80).**
  Per-stream dedupe — `SET NX EX` after a *completing* close, `EXISTS` before the
  handler — so losing them costs one TTL window of re-fetches, never correctness:
  [docs/CONVENTIONS.md](docs/CONVENTIONS.md#the-replicatorcmd-keys).
- **Consumers must be idempotent; producers own the outbox.** Replicator has no DB
  — its durable record of intent is the consumer group's PEL. Do not add a
  Postgres outbox to the consume path.
- **Three stream kinds, three sets of rules.** `content.fetch` and
  `content.replicate` are command streams (one group each, competing consumers);
  `content.blobs` and `content.artifacts` each carry both outcomes of their
  command; `content.fetch-policy` is read **groupless** — no group, no ack, no
  DLQ.
- **The replicate loop writes for `gcs` (#29)** — create-if-absent, `blob_uri` never
  resolved as a path, writers keyed by alias, refusals before credentials, provider
  failures classified by HTTP status. Read
  [docs/CONVENTIONS.md](docs/CONVENTIONS.md) first.
- **Nothing but the seed script writes to `content.fetch`.** `scripts/seed_fetch.py`
  requires `--production` for the one combination the live worker consumes: a frame
  there is fetched for real.
- **Three normative contracts bound the wire and the roadmap** — four documents
  under `docs/contracts/`, linked from sibling repos and indexed below.
  `tests/test_boundaries.py` enforces the charter in CI; change a charter and its
  tests together.

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

# Integration tests (live VM Redis, or a broker the test spawns; --no-cov —
# these do not exercise all of src/)
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

**Date & Time:**
- All UTC
- ISO 8601: `YYYY-MM-DDTHH:MM:SS.ffffffZ` (timestamps), `YYYY-MM-DD` (dates)

**General:** imports at file top and explicit, docstrings on public modules,
classes and functions, small focused functions, and tests mirroring source.
Those with their rationale and ruff gate, plus the logging stack — its formatter,
its installers, and the journald lines deliberately not JSON:
[docs/STYLE.md](docs/STYLE.md).

## Detail Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — founding design, module by module; read before changing one
- [docs/STREAMS.md](docs/STREAMS.md) — what each stream carries, one bullet per rule `AGENTS.md` states in a line
- [docs/CONVENTIONS.md](docs/CONVENTIONS.md) — the rules common to every stream: idempotency, validation, DLQ, `claim_stale`; and the `replicator:cmd:*` keys (#80)
- [docs/STORAGE.md](docs/STORAGE.md) — blob paths and modes, the three populations under `REPLICATOR_BLOB_DIR`, TTL and ceilings
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — VM topology, ports, the unit's lifecycle, the co-core pin
- [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md) — every variable either env file carries, and the boundary between them
- [docs/TESTING.md](docs/TESTING.md) — fakeredis's divergences, the keys an integration run may create, and why production `co-gcs-replication` is unreachable from every test (#38)
- [docs/STYLE.md](docs/STYLE.md) — the logging stack: formatter, installers, and the non-JSON journald lines
- [docs/COMMANDS.md](docs/COMMANDS.md) — every runnable command, with flags
- [docs/SKILLS.md](docs/SKILLS.md) — vendored skill inventory, refresh procedure, doc-check sensitive paths
- [docs/SOCRATICODE.md](docs/SOCRATICODE.md) — full tool table, prefetch query, per-tool gotchas, cross-repo search
- [docs/contracts/content-fetch-issuer-contract.md](docs/contracts/content-fetch-issuer-contract.md) — what a `content.fetch` producer must do; normative, linked from issuer repos
- [docs/contracts/content-fetch-issuer-reference.md](docs/contracts/content-fetch-issuer-reference.md) — its lookup half: refusal list, failure taxonomy, silent conditions, trust posture
- [docs/contracts/replicator-boundaries.md](docs/contracts/replicator-boundaries.md) — what Replicator may become; run its three tests against any proposed capability
- [docs/contracts/content-replicate-issuer-contract.md](docs/contracts/content-replicate-issuer-contract.md) — the replicate trust model and issuer obligations (#34)
