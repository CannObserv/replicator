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

Auth is ADC: on the VM the SA key at `GOOGLE_APPLICATION_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`), in CI a keyless WIF token. Pin the current minor — `>=0.7.5,<0.8`. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10).

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
src/worker/reporter.py — The failure fact behind the FailureReporter seam: fetch_failed on content.blobs
src/worker/retention.py — The sweep task: cadence, usage accounting, ceiling reporting
src/worker/pacing.py — Per-host request spacing; the interim politeness default (#12)
src/storage/    — Temp storage; BlobStore protocol + the local-FS backend
src/storage/base.py  — BlobStore protocol (store / exists / open)
src/storage/local.py — Content-addressed local backend; file:// URIs, sharded paths
src/storage/sweeper.py — Retention: TTL reap, stale temps, empty shards; the measured size
src/api/        — FastAPI app (/health only; not part of the MVP loop)
src/api/main.py — App factory, lifespan, router registration
src/core/       — Shared domain logic, logging, config
src/core/errors.py   — TransientFetchError / PermanentFetchError + FailureReason (handler failure vocabulary)
src/core/logging.py  — build_json_formatter() + ColorMessageFilter + configure_logging() + get_logger()
src/core/log_config.json — uvicorn --log-config; routes uvicorn's own loggers through that formatter
src/core/config.py   — Settings / env access (see Environment Variables)
scripts/        — sync_wheelhouse.py, check_redis_floor.sh, seed_fetch.py
scripts/seed_fetch.py — the MVP command issuer; publishes content.fetch, --watch tails the facts
tests/          — Mirrors src/ structure; integration tests in `@pytest.mark.integration`
docs/           — Reference docs (COMMANDS, SKILLS)
docs/contracts/ — Normative contracts, linked to from sibling repos: the issuer-facing half of the wire, and the boundaries charter (what Replicator may become)
docs/plans/     — Implementation plans
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
| After editing `deploy/replicator.service` | `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart replicator` |
| After a co-core version bump | re-run `sync_wheelhouse.py`, then `uv sync` |

`ExecStart` uses `--frozen --no-sync`, so dependency sync is a deploy step, not a service-start side effect.

**`/etc/systemd/system/replicator.service` is a *copy*, not a symlink to `deploy/`.** So the `cp` above is load-bearing and `daemon-reload` alone silently does nothing — systemd re-reads the installed file, which is still the old one. The failure has no symptom at restart: the worker comes up on the new code under the *old* unit, and the mismatch only surfaces the first time a directive actually matters. Nothing guards it, either — `tests/test_deploy.py` reads the repo file, which is exactly the copy that is still correct. Diff the two when a restart follows a unit edit (#11 deploy).

The copy is deliberate, for the same reason `/etc/replicator/.env` is not read from the repo: the live unit must survive a repo reset, a worktree switch, or a branch checkout that happens to be mid-edit.

**Dev server workflow** (the `/health` app, port 8041 so a future live service stays up):

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload --log-config src/core/log_config.json
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
- `REPLICATOR_BLOB_TTL_SECONDS` — how long a blob survives after it was **last referenced**; default `604800` (7 days). Measured from mtime, which the store refreshes on its short-circuit. The number is a published commitment to archiver (archiver#118), not a local tuning knob — raise it if a `content.blobs` consumer says it needs longer
- `REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS` — how often the tree is walked; default `900`. Also the staleness bound on the measured byte total the ceiling reads
- `REPLICATOR_BLOB_TEMP_GRACE_SECONDS` — how long a `.tmp` may live before the sweep treats it as debris; default `3600`. Deliberately unrelated to the TTL and far shorter — see **Retention**
- `REPLICATOR_BLOB_MAX_TOTAL_BYTES` — ceiling on everything the blob tree holds; default `2147483648` (2 GiB). Crossing it pauses fetching (`TransientFetchError`); it never shortens the TTL
- `REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS` — the most a command's own `timeout_seconds` may ask for; default `120`. Not a default (an omitted field still gets the driver's 30 s) but a ceiling, and a guard rather than a preference: the consume path is serial, so one issuer's timeout is a lien on every other command in the group. Over it ⇒ `PermanentFetchError`. Bounded above by the unit's `TimeoutStopSec` — change one, revisit the other
- `REPLICATOR_MAX_BLOB_BYTES` — ceiling on one fetched body; default `67108864` (64 MiB). A **storage** guard, not a memory one: co-core's fetch driver buffers the whole response before returning it, so the bytes are already resident when this is checked. Over the ceiling ⇒ `PermanentFetchError` ⇒ DLQ
- `REPLICATOR_CONSUMER_GROUP` — consumer group on `content.fetch`; default `replicator.fetch`
- `REPLICATOR_CONSUMER_NAME` — this worker's identity within the group; defaults to `replicator@<hostname>`. Two workers must never share one — Redis tracks pending entries per consumer name, and a shared name makes independent `claim_stale` recovery impossible
- `REPLICATOR_CONSUMER_START_ID` — group start position; default `"$"` (new messages only), `"0"` drains the backlog. Applies **only at group creation** — once `replicator.fetch` exists, changing this also needs a manual `XGROUP SETID`
- `REPLICATOR_READ_BLOCK_MS` — blocking-read window; default `5000`. Bounds worst-case shutdown latency, so the unit's `TimeoutStopSec` must exceed it plus the handler budget **plus an in-flight sweep** — `asyncio.to_thread` puts the tree walk beyond cancellation, so SIGTERM waits it out
- `REPLICATOR_MIN_HOST_INTERVAL_SECONDS` — minimum spacing between two requests to the same host; default `1.0`. The **interim** politeness default (#12) standing in for the `content.fetch.policy` stream: enforcement is mechanism and lives here, the numbers are the issuer's and do not yet travel over the bus. 1.0 is Watcher's own `DEFAULT_MIN_INTERVAL`, chosen because it invents nothing — the cutover changes who paces, not how much. A wait ≤ `REPLICATOR_READ_BLOCK_MS` is slept through in the handler; a longer one raises `TransientFetchError` and parks the command, so the effective floor on a *parked* wait is `REPLICATOR_CLAIM_MIN_IDLE_MS`. `0` disables pacing outright — an operator escape hatch, and a deployment that sets it is choosing to have no politeness at all. Capped at `3600`: past an hour the command parks and re-parks without ever dead-lettering (transient failures are exempt from the delivery ceiling) while the issuer's reaper concludes loss, so a fat-fingered extra zero should fail at startup rather than read as healthy
- `REPLICATOR_CLAIM_MIN_IDLE_MS` — idle time before a pending entry may be reclaimed; default `60000`. Doubles as the retry cadence
- `REPLICATOR_MAX_DELIVERY_ATTEMPTS` — delivery ceiling for *unclassified* failures before DLQ; default `5`. Counted from XPENDING's delivery counter, which only advances on a reclaim ⇒ a bound in time, not retries
- `REPLICATOR_ERROR_BACKOFF_BASE_SECONDS` / `REPLICATOR_ERROR_BACKOFF_MAX_SECONDS` — backoff for a poll *cycle* that raised (broker outage); defaults `1.0` / `30.0`, escalating `base * 2**(n-1)`
- `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` — consecutive failed cycles before the worker exits so the unit restarts; default `20` (~8 min at the default backoff). Paired with the unit's `StartLimitIntervalSec=3600` / `StartLimitBurst=3`
- `REPLICATOR_DEDUPE_TTL_SECONDS` — lifetime of the `replicator:cmd:<command_id>` dedupe key; default `86400`
- `REPLICATOR_LOG_LEVEL` — default `INFO`. Governs the **root** logger only, which is the whole tree for the worker. Under the dev server's `--log-config`, uvicorn's own `uvicorn` / `uvicorn.access` / `uvicorn.error` loggers are pinned `INFO` by `src/core/log_config.json` and do not follow it (nor did they under uvicorn's built-in config), so setting `WARNING` will not silence access lines; root itself is `INFO` from boot until the lifespan's `configure_logging()` applies this value
- `BUILD_ID` — git SHA stamped by the systemd unit's `ExecStartPre`; defaults to `"dev"` outside systemd

## Bus Conventions

Replicator is a **consumer** first. Follow the conventions co-core and the archiver producer established:

- **The issuer contract is written down and lives here.** `docs/contracts/content-fetch-issuer-contract.md` is the normative statement of what a `content.fetch` producer must do — per-occasion `command_id`, `url` is not a correlation key, persist the `command_id → domain` map before publishing, correlate idempotently, and keep a reaper as a backstop for the outcomes no fact can carry. Issuer-side repos (Watcher, Phase 4) link to it rather than copying it. Anything asserted there is asserted about this repo's code; change one, change both (#8).
- **What Replicator is allowed to become is also written down.** `docs/contracts/replicator-boundaries.md` is the sibling charter: mechanism to Replicator, policy to the issuer, config over the bus, and an inbound admin HTTP API rejected by name. Run its three tests against any proposed capability, field, or setting before writing code — a database, domain vocabulary, or a write route is reached one defensible step at a time, not in one commit. `tests/test_boundaries.py` enforces eight invariants in CI, including the one that catches the regression review misses: an AST scan of `src/` for domain nouns in identifiers *and* string literals. Known violation recorded and pinned rather than omitted — `blob_uri` is host-local `file://` (#7). Change the charter and the tests together (#12).
- **`content.blobs` carries both outcomes.** `blob_available` on success, `fetch_failed` on a command closed without bytes (#9, co-core cannobserv#270 — v0.7.2). One stream so an issuer's single consumer group sees either. The reason is named at the *raise site* (`PermanentFetchError.reason`), never recovered from a message string, because three unrelated permanent conditions share one exception type. Three rows stay DLQ-only and permanently silent, and only one of them for want of an id: a frame that did not decode, a command whose `command_id` is blank (refused before the fetch — an empty id would otherwise take the dedupe key `replicator:cmd:` and make every later blank-id command a silent no-op, CR #6), and a frame that decoded to a **non-command payload** — the last is unreportable not because it lacks a `command_id` but because any it carries is *another command's* (`BlobAvailableEvent`'s names one that succeeded), so a terminal fact keyed on it would contradict a fact the issuer already applied (CR #1). `_close` refuses a correlator-less report at the one choke point rather than at each call site. Non-terminal facts are deferred (#9 §3): the stream is broadcast and nothing trims it, so a fact per reclaim during an origin outage is unbounded growth. `src/worker/reporter.py`.
- **`blob_available` carries the fetch, not just the bytes.** Six optional fields beyond the blob itself (#10, cannobserv#271 + #279 — v0.7.5): `final_url`, `status_code`, `fetched_at`, `content_type_raw`, `etag`, `last_modified`. Each is what Replicator holds at publish time and a broadcast consumer cannot recover now that fetching lives here rather than in Watcher. **`None` means nobody said it** — never a stand-in: `final_url` is never backfilled from `command.url` (an issuer could no longer tell "landed where I asked" from "nobody knows"), and `content_type_raw` is never backfilled with `DEFAULT_MEDIA_TYPE` (which is the value a consumer reads as "unknown, guess from the URL"). `media_type` keeps its normalized semantics beside the raw channel; the two are not interchangeable. `fetched_at` is stamped where the fetch *returns*, not at publish — `occurred_at` under a reclaim is minutes late. `status_code` is always 2xx on this fact, so it distinguishes 200 from 203/206 and is not a success branch. The passthroughs are **dropped over `MAX_HEADER_VALUE_LENGTH`, never truncated**: these are origin-controlled strings on a stream nothing trims, and a truncated ETag replayed in an `If-None-Match` is a validator that can never match. `src/worker/handler.py::_passthrough`.
- **The failure fact is a seam, not a call in the loop.** `FailureReporter` is injected exactly as `Handler` is, so `loop.py` stays ignorant of `content.blobs` and `blobs_topic` stays a defaulted argument a live-broker test can move. The loop owns the *decision* (`terminal` is "did this hit the delivery ceiling", which only the loop knows); the reporter owns the *publish*. `_close()` publishes then dead-letters, so **fact-before-ack** cannot be got wrong one call site at a time — `dead_letter` acks inside itself, and a fact published after it is lost outright on a crash. A failed fact-publish is **swallowed**, deliberately asymmetric with the byte path's `_publish`: there raising prevents an orphan blob, here the DLQ entry is already the durable record and raising would burn the delivery ceiling to reach the same DLQ minutes later.
- **A command shapes its own fetch, and everything unsendable is refused rather than fixed.** `headers` and `timeout_seconds` (#11, cannobserv#272 — v0.7.3) reach the driver as `FetchContent.headers` / `.timeout`; omitted means the pre-#11 wire byte-for-byte. Header names are **lower-cased before the merge** — `AsyncFetchDriver` merges `{"user-agent": DEFAULT, **effect.headers}` case-*sensitively*, and httpx does not resolve the collision: an unfolded `User-Agent` puts **two** field lines on the wire, default first, for the origin to disambiguate (measured, not inferred). That is exactly the fingerprint-continuity case Watcher needs at cutover. Refusals are **`PermanentFetchError(INVALID_REQUEST_OPTIONS)` raised before the fetch**: hop-by-hop and httpx-derived names (`host`, `content-length`, `proxy-*`, …), non-token names (padding included — OWS is a *value* rule, not a name one), values outside **printable US-ASCII**, case-collisions, over `MAX_REQUEST_HEADERS`/`MAX_REQUEST_HEADER_BYTES`, and a timeout that is non-finite, ≤ 0, or over `REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS`. Refused, never stripped or clamped — same argument as dropping an over-long passthrough rather than truncating it: a change to the fetch the issuer cannot see is one it cannot account for in its own fingerprints. **The value charset is pinned to what httpx can actually send, not to RFC 9110** — see `_HEADER_VALUE`'s comment for why each edge sits where it does (CR #1). The rule the guard exists to enforce: a value httpx refuses does not fail as a classified fetch error, so an unguarded one closes the command under the wrong reason or retries forever. `tests/worker/test_handler_request_options.py` walks the whole single-byte range against `httpx.Request` so the two cannot drift again. Validation runs **ahead of the storage ceiling** so a permanently-bad command does not park in the PEL waiting for a sweep. Header **names only** are logged, never values. `src/worker/handler.py::_request_options`.
- **Politeness is enforced here and decided elsewhere, and the interim is a default rather than a decision.** `src/worker/pacing.py` holds a host → last-request map in memory (derived, bounded, rebuildable — one of the three state shapes the boundaries charter permits) and reports a wait; `handler.py::_pace` spends it. **Two ways to spend it, split by duration, because neither works alone on a serial consume path**: a wait ≤ `REPLICATOR_READ_BLOCK_MS` is slept through, a longer one raises `TransientFetchError` and parks the command for `claim_stale`. Park-only was the obvious design and is wrong by 60× — a parked wait cannot be shorter than `REPLICATOR_CLAIM_MIN_IDLE_MS` (60 s) while the normal interval is 1 s, so every host would have been paced at 1/60th of today's rate, silently and in the safe direction. Sleep-only holds every *other* host's commands, and a SIGTERM, behind one origin's politeness. Transient in both directions so being polite can never burn the delivery ceiling. The pacer is built from settings when not injected — the seam fails **open**, and a byte path that quietly stopped pacing is indistinguishable from one that is working. Only a request that actually goes out calls `record()`: stamping a parked attempt would space the origin from requests it never received. This is the **interim** for the Phase 4 cutover, not the design — the numbers belong to the issuer and reach Replicator over `content.fetch.policy` (#12, watcher#245).
- **The timeout ceiling and the unit's `TimeoutStopSec` are one decision.** A command's own timeout replaced the driver's fixed 30 s as the handler's worst-case budget, and SIGTERM waits out the message in flight, so `TimeoutStopSec` must exceed `REPLICATOR_READ_BLOCK_MS` + `REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS` + an in-flight sweep. `tests/test_deploy.py` enforces the first two terms; the third is the margin.
- **At-least-once ⇒ idempotent.** Two idempotency keys, two levels: the **command** dedupes on `command_id`, the **fact** on `content_fingerprint` — and `fetch_failed` on neither, keyed `command_id:occurred_at` so a consumer's dedup-on-key cannot collapse a multi-emission sequence and drop the terminal event. The two serve different purposes and are not interchangeable — the fingerprint is *storage* identity, `command_id` is *correlation* identity, and a consumer deduping its inbox on the fingerprint silently loses the second of two commands that fetched identical bytes. Content-addressed storage makes re-storing identical bytes a no-op regardless.
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
- **`occurred_at` is enforced tz-aware UTC on every payload** since co-core v0.7.2 (cannobserv#273). Naive is rejected fail-loud rather than assumed UTC; aware non-UTC is normalized. Load-bearing beyond tidiness — `isoformat()` is half `fetch_failed`'s envelope key, and a naive value would serialize without an offset. Issuer-visible: a naive `occurred_at` now fails `from_wire` and dead-letters as an anomaly.
- **`from_wire`'s topic and message_id are keyword-only** — `from_wire(fields, topic=..., message_id=...)`. The founding plan's API table showed them positionally.
- `sha256` lives at `co_core.pure.util.hashing`, not `co_core.pure.extract` (which carries `simhash`, `Chunk`, and the parsers). Import parsers from submodules — they are not re-exported from `__init__`.

- **Nothing but the seed script writes to `content.fetch`.** `scripts/seed_fetch.py` requires `--redis-url` and `--topic` explicitly and additionally requires `--production` for the one combination the live worker consumes (db 0 **and** `content.fetch`) — a frame there is fetched for real. Its `--watch` reads the fact stream with a plain `XREAD` and never joins a group: a group left by an operator tool accumulates a PEL nothing drains. The stream watched follows `--topic` (`content.blobs` for `content.fetch`, `<topic>.blobs` otherwise), so a scratch seed does not sit watching production's facts.
- **The command and fact topics are defaulted arguments, not settings.** `build_consumer(..., topic=)` and `build_handler(..., blobs_topic=)` exist so a live-broker test can work on `replicator.itest.*` streams. No deployment wants a different stream, and configuring it would put the production one an operator's typo away.

### Retention

`docs/plans/2026-07-31-replicator-mvp-open-questions-design.md` §4 scope-cut retention; #5 settles it. Replicator is the producer in archiver's temp-cache protocol, where **the producer cleans up**.

- **The TTL runs from last reference, not first store.** `store` short-circuits on an existing content-addressed path but its caller publishes a fresh `blob_available` either way, so a re-fetch of unchanged bytes would otherwise announce a blob already partway through its TTL. `LocalBlobStore` therefore `os.utime`s on the short-circuit branch, swallowing `ENOENT` — the sweep can unlink between the existence check and the touch, and the fallout of that race is a `blob_uri` that fails to open, not a dead-lettered command.
- **The blob tree holds three populations, and they are not interchangeable.** Finished blobs (`<ab>/<cd>/<sha256>.bin`) reap on the TTL; in-flight temporaries (`.<sha256>.<random>.tmp`) are **not garbage** — reaping one makes the writer's `os.replace` fail with `ENOENT` and dead-letters a good command — so the sweep matches `*.bin`, never `iterdir()`, and ages temps out on their own much longer grace. Empty shard directories go **last**, by `rmdir` only, whose refusal to touch a non-empty directory is the safety property.
- **The ceiling is backpressure, not a faster clock.** Over `REPLICATOR_BLOB_MAX_TOTAL_BYTES` the byte path raises `TransientFetchError` *before* fetching, so the command stays in the PEL and returns via `claim_stale` once a sweep frees space. What it measures is everything the tree holds — surviving blobs **and** the temporaries the sweep is waiting out, since a crash loop fills the disk with debris the ceiling would otherwise not see. The per-population counts stay split in the sweep log so a rising temp count cannot hide inside a healthy blob one. Reaping a blob still inside its TTL to make room would convert a local disk problem into a `blob_uri` another repo cannot open — the one failure mode with no local symptom.
- **One `BlobUsage`, two writers.** The sweep's `observe` is the measured total; the byte path's `add` is the estimate between sweeps, because a burst can cross the ceiling long before the tree is walked again. Wiring the two halves to separate instances leaves both individually correct and the guard permanently unreachable — `tests/worker/test_main.py` pins the identity.
- **Orphans are recorded where they are exact.** A publish that fails after the store leaves bytes with no fact and no `command_id`, invisible to any query starting from `content.blobs`. `src/worker/handler.py::_publish` logs the fingerprint at that moment and re-raises untouched; the sweep then treats orphans as ordinary aged blobs. Reconciling the tree against `content.blobs` instead would make a *delete* decision depend on another service's stream-trimming policy.
- **The sweep runs in the worker, not a systemd timer.** A timer survives a crashed worker, but a worker that is not running is not writing blobs either. It rides the same stop event as the consume loop (`src/worker/loop.py::park`) and walks the tree via `asyncio.to_thread`, so retention never becomes a source of consume-path latency. A failed sweep is absorbed and retried next cycle — retention is not load-bearing for correctness, and the ceiling is the guard for a tree that cannot be reaped.

**Testing the bus.** `tests/conftest.py` ships a `fake_redis` fixture (fakeredis, Streams-capable) — consumer-group behaviour is testable without a broker, and assertions should read the broker's own view (`xinfo_groups` / `xinfo_consumers`) rather than co-core's private attributes, which are not a stable contract. Anything that genuinely needs the live Archiver-operated Redis goes behind `@pytest.mark.integration` and is excluded by default.

**Where fakeredis diverges.** It is sound for consumer-group *mechanics* — what state a command leaves behind — but diverges on *lifecycle* and *blocking* semantics: it registers a consumer on an empty `XREADGROUP` (real Redis waits for a delivery, GH #3) and it ignores `block` (worked around by `IDLE_SLEEP_SECONDS` in `src/worker/loop.py`). Rule of thumb: an assertion about **what state results** is safe against the fake; an assertion about **when Redis does something** needs a live broker. Both divergences were found by running against the real server, not by the suite.

Live-broker tests use the `real_redis` fixture (`tests/conftest.py`), which connects to `REPLICATOR_TEST_REDIS_URL` (default `redis://localhost:6379/15`), skips when nothing answers (an *auth* failure re-raises — a misconfigured broker must not pass as a skip), expires stray `replicator.itest.*` keys from crashed runs once per session, and refuses db 0 outright — db 0 carries the live `content.fetch` stream that the running `replicator.service` consumes, so a test frame written there would be fetched for real. Confine such tests to scratch stream keys via the `scratch_topic` fixture (`tests/worker/conftest.py`), whose teardown also removes `<topic>.dlq`; the database guard is the backstop, not the plan.

**One namespace the sweeper cannot reach.** `process_message` writes `replicator:cmd:<command_id>`, a constant prefix outside `replicator.itest.*`, so an end-to-end test deletes its own keys via the `dedupe_keys` fixture and shortens their TTL. `test_an_end_to_end_run_only_creates_predictable_keys` asserts the whole promise: every key a run creates is either an itest stream or a dedupe key.

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

**One formatter, two installers.** `build_json_formatter()` is the single definition of the JSON schema (`timestamp`, `level`, `logger`, `message`). `configure_logging()` installs it on the root logger; `src/core/log_config.json` names the *same factory* through dictConfig's `"()"` key, so there is no second fmt string to drift. The dev server must be launched with `--log-config src/core/log_config.json` — uvicorn's `uvicorn` / `uvicorn.access` / `uvicorn.error` loggers ship with `propagate=False` and their own plain-text handlers, so a root-only config never reaches them and the output is half JSON, half plain text. **The worker runs no uvicorn**, so `replicator.service` needs no `--log-config`; its `ExecStart` is `python -m src.worker.main` and `configure_logging()` is the whole story there (#14).

**Not everything in the journal is JSON.** The claim above is about the *app's own records*. `replicator.service`'s wheelhouse-sync `ExecStartPre` writes a **plain-text** line to journald on every start — `wheelhouse in sync: N downloaded, M already present -> …` on the happy path, `error: could not sync gs://…` on the non-fatal failure path (stderr, which journald captures the same way) — and that is by design, not a gap: it runs `uv run --no-project` — before the deploy's `uv sync`, in an environment holding `google-cloud-storage` and nothing else — so it cannot import `build_json_formatter()` and making it emit JSON would mean a hand-maintained second copy of the schema in the one file that structurally cannot single-source it. A shipper reading journald natively is unaffected (the message is a field alongside `_SYSTEMD_UNIT` / `_PID`); a pipeline that `json.loads` every `MESSAGE` must tolerate these two lines — the failure one especially, since it appears exactly when something is already wrong (#15, skills#83).

**The colour strip is a filter on the loggers, deliberately.** uvicorn attaches an ANSI-coloured duplicate of each lifecycle message as `extra={"color_message": ...}`, and every extra reaches the JSON payload. `ColorMessageFilter` deletes it from the record at its source — not via the formatter's `reserved_attrs`, and not on the stdout handler. Both alternatives scope the fix to *this* sink: a handler that builds its payload from the record's `__dict__` rather than a `logging.Formatter` resurrects the field, and OpenTelemetry's `LoggingHandler` is exactly that (its own reserved list does not cover `color_message`). Mutating the record once means the strip survives a sink swap with no failing test to warn you it had stopped working. `tests/core/test_logging.py` pins the filter's placement, not just its effect.

**Date & Time:**
- All UTC
- ISO 8601: `YYYY-MM-DDTHH:MM:SS.ffffffZ` (timestamps), `YYYY-MM-DD` (dates)

**General:**
- No inline module imports; all at file top
- Docstrings for public modules, classes, functions
- Test structure mirrors source (`src/foo.py` → `tests/test_foo.py`). A module whose tests outgrow one file splits by concern, not by helper: `tests/worker/test_loop_dlq.py`, `test_loop_recovery.py`, … with the shared wiring in that package's `conftest.py`. Concern is the default axis; **environment** is the one exception — tests needing a live broker split off with an `_integration` suffix (`tests/worker/test_main_integration.py`), so the filename says what the marker enforces
- Explicit imports only
- Small, focused functions
