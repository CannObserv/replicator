# replicator — Agent Guidelines

Be terse. Prefer fragments over full sentences. Skip filler and preamble. Sacrifice grammar for density. Lead with the answer or action.

## Project Overview

Retrieval, fingerprinting, and temporary storage layer for the Cannabis Observer cluster.

Owns content fetching, temp storage, and fingerprinting — the network-bound, byte-handling work re-homed out of Watcher. Driven by **commands** on the Redis change bus; reports outcomes as **facts**.

```
content.fetch (command) → fetch → fingerprint → temp-store → blob_available (fact)
                        ↘ closed without bytes ───────────────→ fetch_failed  (fact)
content.replicate (cmd) → guards → create-if-absent ────────→ replication_complete (fact)
                                 ↘ refused / conflict ──────→ replication_failed  (fact)
```

**Worker-first.** Primary process = bus consumer (`src/worker/main.py`), not an HTTP API. The FastAPI app is a `/health` surface only, dev-only until a status endpoint is wanted.

## Development Methodology

TDD required. Red → Green → Refactor. No production code without a failing test first.

## Environment & Tooling

Python ≥3.12, uv, pytest, ruff. `ty` is available as a **non-gating** type checker (`uv run ty check`) — advisory only; no pre-commit or CI gate.

**co-core comes from the wheelhouse, not PyPI.** `co-core` / `co-core-aio` resolve from `./.wheelhouse`, mirrored from the private GCS index `gs://co-gcs-pypi` by `scripts/sync_wheelhouse.py` via `[tool.uv] find-links`. Run the sync **before** `uv sync` on a fresh clone or after a version bump:

```bash
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
```

Auth is ADC: on the VM the SA key at `GOOGLE_APPLICATION_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`), in CI a keyless WIF token. Pin the current minor — `>=0.9.4,<0.10`. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead — a ValidationError at construction (0.8.0 requires `info_source_id` on all three fetch payloads, #19/#28) or an ImportError at load, never reaching a running worker (0.9.4 cuts the replicate contracts, #29).

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

**The manifest is a source, not the artifact.** Nothing re-embeds it — no hook, no CI step — so editing a `description` there changes what the repo says and not what `codebase_context_search` returns. Re-run `codebase_context_index` in the same change, or the highest-authority answer an agent gets stays the stale one (#19 CR #17).

**`mcp-driver.mjs` lies twice — silently through the `skills/` symlink (skills#177), falsely from a worktree (skills#180).** Use `"$SOCRATICODE_DRIVER"`; disbelieve health findings outside the main checkout. Both in [docs/SKILLS.md](docs/SKILLS.md).

## Project Layout

`src/worker/` is the primary process — the bus consumer, with the byte path, the
failure fact, the retention sweep, the pacer, and the `content.fetch-policy` reader
each behind their own seam. `src/storage/` is the content-addressed temp store behind
the `BlobStore` protocol; `src/api/` is the dev-only `/health` app; `src/core/` holds
config, logging, and the consume path's failure vocabulary. `tests/` mirrors
`src/`. Every module with the job it owns:
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
symptom until a directive matters. **Refuses to start off `main`** (#37) —
`scripts/check_main_checkout.sh`; `REPLICATOR_ALLOW_ANY_CHECKOUT=1` overrides.
Full lifecycle table and the dev-server invocation:
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

## Environment Variables

Two env files, with a hard boundary between them:

1. **`/etc/replicator/.env`** — production configuration. Survives repo resets and worktree switches. Managed manually on the VM. **The only file `replicator.service` reads.**
2. **`.env`** (repo root, git-ignored) — dev/agent secrets: `GH_TOKEN` plus the per-repo PATs (`GH_TOKEN_ARCHIVER`, `GH_TOKEN_WATCHER`, `GH_TOKEN_CANNOBSERV`, `GH_TOKEN_SKILLS`). Never commit.

**The service must never load the repo `.env`.** Those PATs carry org-wide write access the worker has no use for; handing them to a process whose job is fetching public URLs widens the blast radius of any crash dump or subprocess for no benefit. Anything the service genuinely needs belongs in `/etc/replicator/.env`.

For shell commands (dev only), load both — the snippet is under Common Commands.

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
- **Three stream kinds, three sets of rules.** `content.fetch` and
  `content.replicate` are command streams (one group each, competing consumers);
  `content.blobs` and `content.artifacts` each carry both outcomes of their
  command; `content.fetch-policy` is read **groupless** — no group, no ack, no
  DLQ.
- **The replicate loop writes for `gcs` (#29).** T4's create-if-absent, so a
  redelivery onto matching bytes re-emits the same `public_url` and differing bytes
  are a terminal conflict. `blob_uri` is **never resolved as a path** — fingerprint
  out, compared against `store.uri_for()`. Writers are keyed **by alias**, and every
  refusal happens before any credential is touched. Provider failures classify by
  HTTP status — 4xx closes the command, 5xx/408/429 and statusless errors leave it
  pending — because a transient failure is exempt from the delivery ceiling and
  publishes no fact at all, so misclassifying one strands the issuer forever
  (#29 CR #26, #27).
- **Nothing but the seed script writes to `content.fetch`.** `scripts/seed_fetch.py`
  requires `--production` for the one combination the live worker consumes — a
  frame there is fetched for real.
- **Three normative contracts bound the wire and the roadmap** — four documents,
  all under `docs/contracts/`, linked from sibling repos and indexed below.
  `tests/test_boundaries.py` enforces eight charter invariants in CI; change a
  charter and its tests together.

Where the reasoning lives:

- What each stream carries — [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- The rules common to all of them — [docs/CONVENTIONS.md](docs/CONVENTIONS.md)
- Blob paths, modes, and the retention sweep — [docs/STORAGE.md](docs/STORAGE.md)
- Fakeredis's divergences, and the keys an integration run may touch — [docs/TESTING.md](docs/TESTING.md)

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

# Integration tests (live VM Redis; --no-cov — these do not exercise all of src/)
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

The logging stack — one formatter, two installers, and the journald lines that
are deliberately not JSON: [docs/STYLE.md](docs/STYLE.md).

**Date & Time:**
- All UTC
- ISO 8601: `YYYY-MM-DDTHH:MM:SS.ffffffZ` (timestamps), `YYYY-MM-DD` (dates)

**General:**
- No inline module imports; all at file top
- Docstrings for public modules, classes, functions
- Test structure mirrors source (`src/foo.py` → `tests/test_foo.py`); splitting rules — by concern, and the `_integration` exception — in [docs/TESTING.md](docs/TESTING.md)
- Explicit imports only
- Small, focused functions

## Detail Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — founding design, module-by-module layout, and what each stream carries; read before changing one
- [docs/CONVENTIONS.md](docs/CONVENTIONS.md) — the co-core/Redis Streams rules common to every stream: idempotency, validation, DLQ, `claim_stale`
- [docs/STORAGE.md](docs/STORAGE.md) — blob paths and modes, the three populations under `REPLICATOR_BLOB_DIR`, TTL and ceiling semantics
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) — VM topology, ports, the systemd unit's lifecycle, and every environment variable the service reads
- [docs/TESTING.md](docs/TESTING.md) — where fakeredis diverges from the live broker, and which keys an integration run may create
- [docs/STYLE.md](docs/STYLE.md) — the logging stack: formatter, installers, and the non-JSON journald lines
- [docs/COMMANDS.md](docs/COMMANDS.md) — every runnable command, with flags
- [docs/SKILLS.md](docs/SKILLS.md) — vendored skill inventory and refresh procedure
- [docs/SOCRATICODE.md](docs/SOCRATICODE.md) — the full tool table, the prefetch query, per-tool gotchas, and cross-repo search
- [docs/contracts/content-fetch-issuer-contract.md](docs/contracts/content-fetch-issuer-contract.md) — what a `content.fetch` producer must do; normative, linked from issuer repos
- [docs/contracts/content-fetch-issuer-reference.md](docs/contracts/content-fetch-issuer-reference.md) — its lookup half: the refusal list, the failure taxonomy, the silent conditions, trust posture
- [docs/contracts/replicator-boundaries.md](docs/contracts/replicator-boundaries.md) — what Replicator may become; run its three tests against any proposed capability
- [docs/contracts/content-replicate-issuer-contract.md](docs/contracts/content-replicate-issuer-contract.md) — the replicate trust model and issuer obligations, settled ahead of the code (#34)
