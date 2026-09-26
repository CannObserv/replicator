# replicator — Agent Guidelines

Be terse. Prefer fragments over full sentences. Skip filler and preamble. Sacrifice grammar for density. Lead with the answer or action.

## Project Overview

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

**Worker-first.** Primary process = bus consumer (`src/worker/main.py`), not an HTTP API. The FastAPI app is a dev-only `/health` surface.

The command → fact flow: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Development Methodology

TDD required: no production code without a failing test first.

## Environment & Tooling

Python ≥3.12, uv, pytest, ruff. `ty` is a **non-gating** type checker (`uv run ty check`) — advisory, no pre-commit or CI gate.

**co-core comes from the wheelhouse, not PyPI.** `co-core` / `co-core-aio` / `co-core-sync` resolve from `./.wheelhouse`, mirrored from the private GCS index `gs://co-gcs-pypi` by `scripts/sync_wheelhouse.py` via `[tool.uv] find-links`. Run the sync **before** `uv sync` on a fresh clone or after a version bump:

```bash
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
```

Auth is ADC. Pin the current minor — `>=0.19.6,<0.20` — and raise the **patch** floor with every co-core feature the code starts depending on; the ways a skew has already failed are in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

<!-- BEGIN socraticode-policy -->
## Code Exploration Policy

SocratiCode is the preferred semantic-search tool here once indexed (manifest
`.socraticodecontextartifacts.json`). Its MCP tools are **deferred** — schemas
load only after the `ToolSearch` prefetch that
`.claude/hooks/socraticode-reminder.sh` prints each session.

**Negative rule.** Use SocratiCode MCP tools first for semantic questions
("where is X", "how does Y work", "what depends on Z"). Reach for `grep`/`rg`
only on exact strings (error messages, log lines, known symbols). Reserve the
Explore subagent for path-pattern walks (`*.py` under `src/worker/`), not
semantic search.

| Goal | Tool |
|------|------|
| Where is X defined / how does Y work / what touches Z | `codebase_search` |
| Exact string or regex (errors, log lines, known symbols) | `grep` / `rg` |
| Imports/dependents of a file · blast radius of a change | `codebase_graph_query` / `codebase_impact` |

Full tool table, prefetch hook, per-tool guidance: [`docs/SOCRATICODE.md`](docs/SOCRATICODE.md).
<!-- END socraticode-policy -->

## Code Exploration Notes (repo-specific)

**The store is `co-index`'s shared Qdrant, not a local one** — kept here: the policy block names no store (skills#328).

**The manifest is a source, not the artifact.** Nothing re-embeds it — run `codebase_update` in the same change as a `description` edit, or the stalest answer carries the most authority (#19 CR #17).

**Cap anything that launches a SocratiCode server** — uncapped, one cost this cluster 58 min of bus (#94). Invocations in [docs/COMMANDS.md](docs/COMMANDS.md). A green `codebase_search` is also not evidence every linked sibling answered: the silent-skip modes, and which sibling is in one, are in [docs/SOCRATICODE.md](docs/SOCRATICODE.md).

## Project Layout

`src/worker/` is the primary process — the bus consumer; `tests/` mirrors `src/`.
Every module with the job it owns, the seams each sits behind, and the file-level
map: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Infrastructure

**Own VM, `co-replicator`** (tailnet `replicator`, #88), also the dev workspace.
Main is the deployed code.

Worker binds no port; 8001 is dev, 8000 reserved. **The broker is `co-broker`**
(CannObserv/broker); Replicator is a client, never ships one. Server **≥ 7.0** is
critical — `claim_stale_page` reads `XAUTOCLAIM`'s three-element reply — guarded by
`scripts/check_redis_floor.sh`. Ports, redis-py pin: [docs/INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md).

## Server Lifecycle

`replicator.service` runs the worker. Deploy committed code with `git pull --ff-only
&& uv sync --frozen && sudo systemctl restart replicator` — `git push` instead of the
pull when the merge happened here.

Two that bite, each symptomless until it matters:

- **The service refuses to start off `main`, or off unpushed commits** (#37, #48).
  `REPLICATOR_ALLOW_ANY_CHECKOUT=1` overrides; a dev worker asks the same question
  at the writer (#52).
- **Everything installed from `deploy/` is a copy** — `cp` after every edit;
  `daemon-reload` re-reads the old file, the sysctl drop-in needs
  `sysctl --system`, and tailscaled's a tailscaled restart (#113). The
  `OnFailure=` handler's missed `cp` is invisible until the first failure; read
  what it recorded with `journalctl -t replicator-failure`, never `-u`.

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

New settings take the `REPLICATOR_` prefix (cohort convention). `BUILD_ID` is the
one deliberate exception, stamped generically by the unit.

Every variable, which file carries it, and each default's reasoning:
[docs/ENVIRONMENT.md](docs/ENVIRONMENT.md).

## Bus Conventions

Replicator is a **consumer** first — follow what co-core and the archiver producer established:

- **At-least-once ⇒ idempotent.** The command dedupes on `command_id`; both facts
  are keyed per *occurrence* (`content_fingerprint:command_id`,
  `command_id:occurred_at`). `info_source_id` and replicate's
  `info_item_rep_spec_id` are **echoed, never read** — a `test_boundaries.py`
  carve-out is one field wide, and adding one edits the charter (#28, #29).
- **Two blob backends, one seam.** `REPLICATOR_BLOB_BACKEND` selects them: `local`
  announces `file://` and `gcs` announces `gs://`, and `local` stays the
  compiled-in default deliberately (#7). Every `BlobStore` call from a coroutine
  goes through `asyncio.to_thread`.
  [docs/STORAGE.md](docs/STORAGE.md) is the authority for either store.
- **Store, then publish — never the reverse.** A fact pointing at absent bytes is
  unrepairable by the consumer; stored bytes with no fact repair themselves on the
  reclaim.
- **Read `count=1`.** `AsyncBusConsumer.read(count>1)` raises on a malformed frame
  *before* returning the well-formed ones. Recovery claims at `count=1` too — for
  the #98 turn bound, not poison (#109).
- **`from_wire` is fail-loud and its dispatch table is global** — `isinstance`-check
  every decoded payload before destructuring. Use the canonical `extra="ignore"`
  models on the consume path, never the strict `*Emit` classes, and branch on
  `schema_version` first.
- **Deterministic ⇒ DLQ; transient ⇒ retry; completed without bytes ⇒ fact + ack,
  no DLQ (#17).** `dead_letter` acks inside itself, so a fact is published
  *before* it; **draining the queue is ours too** (broker#12, #86). Retry cadence
  is `REPLICATOR_CLAIM_MIN_IDLE_MS`; a failing *cycle* is `run_loop`'s problem,
  not the message's.
- **A capped broker and an ACL denial are both transient (#79, #82).**
  `OutOfMemoryError` and `NoPermissionError` are the two `ResponseError`
  subclasses in `_TRANSIENT_ERRORS`, exempt from the delivery ceiling, so an OOM
  is a *publishing* incident and a wrong grant backs off rather than closing
  valid commands. Boot-only `XGROUP CREATE … MKSTREAM` is the one refusal that
  does **not** retry. Cap a broker the tests spawn, **never the shared one**, and
  never answer an OOM with a client-level retry, which
  republishes an `XADD` the broker already applied. Each classification and what it
  costs: [docs/CONVENTIONS.md](docs/CONVENTIONS.md).
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
- **The replicate loop writes for `gcs` (#29)** — create-if-absent, and `blob_uri` is
  never resolved as a path. Read [docs/CONVENTIONS.md](docs/CONVENTIONS.md) first.
- **A fetch may not reach loopback, RFC 1918, or the tailnet (#89, #95).** Per redirect hop.
- **Watcher alone issues `content.fetch` (#90).** `scripts/seed_fetch.py` seeds
  scratch streams; the live one takes `--production` and Watcher's identity, never
  `replicator`'s.
- **The `docs/contracts/` documents are normative**, indexed below.
  `tests/test_boundaries.py` enforces the charter in CI; change a charter and its
  tests together.

## Common Commands

```bash
# Mirror the private index first — command under Environment & Tooling
uv sync

# Load environment — dev only (required before running the worker or gh)
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a

# Run tests
uv run pytest

# Run a subset of tests (skip the coverage gate, which measures all of src/)
uv run pytest --no-cov tests/path/to/test.py

# Integration tests (a scratch redis-server, or one the test spawns; --no-cov again)
uv run pytest --no-cov -m integration

# Run linter
uv run ruff check .

# Run the worker locally
uv run python -m src.worker.main

# FastAPI dev server (/health only)
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8001 --reload --log-config src/core/log_config.json
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

**Logging:** `from src.core.logging import get_logger`, then `logger = get_logger(__name__)`.
Entry points only: `configure_logging()` is called once inside the FastAPI `lifespan` or the worker's `run()`. Never in library modules.
The stack itself: [docs/STYLE.md](docs/STYLE.md).

**Date & Time:**
- All UTC
- ISO 8601: `YYYY-MM-DDTHH:MM:SS.ffffffZ` (timestamps), `YYYY-MM-DD` (dates)

**General:** house style — imports, docstrings, function size, tests mirroring
source — each with its rationale and ruff gate in [docs/STYLE.md](docs/STYLE.md).

## Detail Docs

- [ARCHITECTURE.md](docs/ARCHITECTURE.md) — founding design, the command → fact flow, module by module; read before changing one
- [STREAMS.md](docs/STREAMS.md) — what each stream carries, one bullet per rule `AGENTS.md` states in a line
- [POLITENESS.md](docs/POLITENESS.md) — per-host pacing: the policy stream, sleep vs park, 429/503 escalation
- [CONVENTIONS.md](docs/CONVENTIONS.md) — the rules common to every stream: idempotency, validation, DLQ, `claim_stale`, and the `replicator:cmd:*` keys (#80)
- [STORAGE.md](docs/STORAGE.md) — blob paths and modes, the populations under `REPLICATOR_BLOB_DIR`, TTL and ceilings
- [DEPLOYMENT.md](docs/DEPLOYMENT.md) — the unit's lifecycle, its start guards, the co-core pin, the host's memory tunables
- [FAILURE-NOTIFICATION.md](docs/FAILURE-NOTIFICATION.md) — the `OnFailure=` handler: `REPLICATOR_NOTIFY_*`, notifier mode, delivery scoring
- [INFRASTRUCTURE.md](docs/INFRASTRUCTURE.md) — VM topology, ports, the broker, and the buckets either side of the test/production line
- [tailscale.md](docs/reference/tailscale.md) — this node: tailnet, ACL, DNS, broker latency
- [ENVIRONMENT.md](docs/ENVIRONMENT.md) — every variable either env file carries, and the boundary between them
- [TESTING.md](docs/TESTING.md) — fakeredis's divergences, the keys an integration run may create, why production `co-gcs-replication` is unreachable (#38)
- [STYLE.md](docs/STYLE.md) — the logging stack: formatter, installers, the non-JSON journald lines
- [COMMANDS.md](docs/COMMANDS.md) — every runnable command, with flags
- [SKILLS.md](docs/SKILLS.md) — vendored skill inventory, refresh procedure, doc-check lists
- [SOCRATICODE.md](docs/SOCRATICODE.md) — full tool table, prefetch hook, per-tool gotchas, cross-repo search
- [content-fetch-issuer-contract.md](docs/contracts/content-fetch-issuer-contract.md) — what a `content.fetch` producer must do; linked from issuer repos
- [content-fetch-issuer-reference.md](docs/contracts/content-fetch-issuer-reference.md) — its lookup half, request side: refusal list, trust posture, envelope keys
- [content-fetch-outcome-reference.md](docs/contracts/content-fetch-outcome-reference.md) — its result side: both facts field by field, failure taxonomy, silent conditions, the DLQ
- [replicator-boundaries.md](docs/contracts/replicator-boundaries.md) — what Replicator may become; run its three tests against any proposed capability
- [content-replicate-issuer-contract.md](docs/contracts/content-replicate-issuer-contract.md) — the replicate trust model and issuer obligations (#34)
- [content-replicate-issuer-reference.md](docs/contracts/content-replicate-issuer-reference.md) — its reasoning half: the trust comparison, T3a, T4, T6, the exemption
