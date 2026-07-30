# Replicator MVP — Design / Plan (handoff)

**Date:** 2026-06-25
**Updated:** 2026-07-30 — refreshed against the shipped bus layer. Phase 2 (co-core bus)
and Phase 2b (archiver producer, now live) landed since the first draft, so the
change-bus contracts, stream taxonomy, distribution mechanism, and Redis
operational ownership are no longer open — this revision replaces the original
"illustrative / stub-if-not-merged / open question" framing with the real
`co_core` APIs and the decisions from archiver#106/#107/#108/#109. The two content
contract questions the previous revision flagged are now **settled** in
cannobserv#266 → **co-core v0.7.0** (see **Contracts**): `content.fetch` is
URL-addressed (Replicator is the sole fingerprinter), and `blob_available` gained
`size_bytes` / `media_type` / `url` / optional `command_id`.
**Status:** Handoff draft for a new `CannObserv/replicator` repo
**Parent strategy:** `archiver/docs/plans/2026-06-25-observer-cluster-integration-strategy-design.md`
**Audience:** the team/agent standing up Replicator. Copy this doc into the new repo as its founding plan.

---

## Purpose

Replicator is the Cannabis Observer **retrieval + fingerprinting + storage** layer. In the target architecture it **owns content fetching, temporary storage, and fingerprinting** for the cluster — the network-bound, byte-handling work re-homed out of Watcher. It is driven by **commands** on the Redis bus and reports outcomes as **facts**.

The MVP's job is narrow: **prove the command → fetch → temp-store → fingerprint → fact loop** standalone, with co-core as the shared substrate, *without* requiring the Watcher cutover.

## Why a fact, not a synchronous reply

Fetching is network-dependent, slow, and retry-prone — a classic **asynchronous/job command**. The issuer doesn't block for an answer; it learns the outcome later from the `blob_available` fact. This is the bus's reason for existing in the cluster (see parent strategy §3).

---

## Repo shape (A/W/N pattern)

Stand up `CannObserv/replicator`, cloned on the VM, mirroring the archiver/watcher/notifier conventions:

- Python ≥3.12, **uv**, **ruff**, **pytest**; **TDD required** (Red → Green → Refactor).
- **Worker-first.** The primary process is a **bus consumer** (a co-core-aio consumer group), not an HTTP API. A thin optional FastAPI app may expose `/health` + status later; not required for the MVP loop.
- systemd unit (`replicator.service`) + a dev port if/when an API exists; `/etc/replicator/.env` for prod secrets, gitignored `.env` for dev.
- SocratiCode index (`.socraticodecontextartifacts.json`), skills submodules, SessionStart hooks — same as siblings.
- Consumes **co-core** via the **find-links GCS wheelhouse** precedent (see "co-core dependencies" — *not* the path-dev/git-pin the original draft assumed; that mechanism was superseded in archiver#72 Phase 0).

### Redis is Archiver-operated — Replicator connects, it does not run its own

Resolved in **archiver#109**: the Redis change bus is **Archiver-operated cluster infrastructure** (the shared VM's `redis-server.service`; Archiver's unit `Wants=/After=redis-server.service` and asserts the server floor via `check_redis_floor.sh`). Replicator is a **client**:

- Connect via `REPLICATOR_REDIS_URL` (default `redis://localhost:6379/0` on the shared VM). Do **not** ship a Redis server or claim ownership.
- `replicator.service` should `After=network.target redis-server.service` + `Wants=redis-server.service` so it orders behind the broker without hard-failing when it's absent.
- **Redis ≥ 7.0 server floor is Replicator-critical.** Replicator is the cluster's first user of `AsyncBusConsumer.claim_stale` (crash-recovery), which reads `XAUTOCLAIM`'s three-element reply — the deleted-ids element added in Redis **server** 7.0. A `< 7.0` server raises on the recovery path. Mirror archiver's `check_redis_floor.sh` as an `ExecStartPre` guard. (The VM currently runs 7.0.15.)
- The **redis-py client** resolves `>=5,<8` transitively via `co-core-aio[bus]` (the ceiling was corrected from a spurious `<7` in cannobserv#263). Don't re-pin it narrower.

## co-core dependencies the MVP uses

Depend on co-core from the **GCS wheelhouse** exactly as archiver/watcher do: `scripts/sync_wheelhouse.py` mirrors the private index → `./.wheelhouse`, resolved via `[tool.uv] find-links` + plain floors; CI resolves keyless via WIF, the VM/deploy via the `co-pypi-reader` SA key. Track `.wheelhouse/.gitkeep`. Pin the current minor — **co-core / co-core-aio `>=0.7,<0.8`** (v0.7.0 carries the #266-settled content contracts; archiver itself is still on 0.6.x, which is fine — its `info.changes` events are unchanged across the bump).

Required **extras**:

- **`co-core[extract]`** — fingerprint (`sha256`, + `simhash` for near-dup) and, if needed, the html/csv/pdf extractors. Fingerprint is the canonical impl, the parity anchor. Import parsers from submodules (`co_core.pure.extract.html`, …); they are not re-exported from `__init__`.
- **`co-core-aio[bus]`** — the Redis Streams driver (`co_core_aio.bus`, consumer group + producer).

Concrete APIs the MVP wires:

| Concern | co-core API |
|---|---|
| Fetch | `co_core_aio.fetch.AsyncFetchDriver.execute(co_core.effects.fetch.FetchContent(url)) -> FetchResult`; `FetchResult.is_2xx` for body-presence; `.aclose()` at shutdown |
| Fingerprint / extract | `co_core.pure.extract.*` — `sha256` (+ `simhash`) per `Chunk`; synchronous |
| Consume | `co_core_aio.bus.AsyncBusConsumer(client, topic=, group=, consumer=)` — `ensure_group` / `read` / `ack` / `claim_stale` / `dead_letter` |
| Publish | `co_core_aio.bus.AsyncBusPublisher(client).execute(co_core.effects.bus.BusPublish(topic, fields))` |
| Wire envelope | `co_core.pure.adapters.bus.envelope.to_wire(payload, key=None)` / `from_wire(fields, topic, message_id)` / `idempotency_key(payload)` |
| Payloads | `co_core.pure.models.changes` (see **Contracts**) |
| Stream names / DLQ | `co_core.pure.adapters.bus.streams` — `CONTENT_FETCH` / `CONTENT_BLOBS` / `INFO_CHANGES`; `dlq_name(topic)` → `<topic>.dlq` |
| Storage backends (durable phase, not MVP) | `co_core.effects.{gcs,gdrive,http}` (+ `co_core.pure.adapters.gdrive`). **No `ext/` package and no `local`/`http_io` backend exist today** — the MVP defines its own storage interface (below) with a local-FS first impl; wire co-core's GCS/GDrive effects only when durable replication lands (add Internet Archive then). |

Bus clients are **injection-only** (the driver never opens or closes the `redis.asyncio.Redis` client) — Replicator owns one long-lived client for the worker's lifetime.

---

## The MVP core loop

```
content.fetch (command)         AsyncBusConsumer, group "replicator.fetch"
        │  read → ack on success                (streams.CONTENT_FETCH)
        ▼
   dedup: seen command_id?       at-least-once redelivery ⇒ skip a command already
        │                        processed (command_id is the command idempotency key)
        ▼
   fetch bytes                   AsyncFetchDriver.execute(FetchContent(url))
        │
        ▼
   fingerprint                   co_core sha256 — AUTHORITATIVE (no expected value on
        │                        the command; Replicator is the sole fingerprinter)
        ▼
   temp-store bytes              configurable backend, key = content fingerprint
        │                        MVP backend = local FS (REPLICATOR_BLOB_DIR)
        │                        (content-addressed ⇒ re-storing same bytes is a no-op)
        ▼
   blob_available (fact)         AsyncBusPublisher → streams.CONTENT_BLOBS
                                 (fingerprint + blob_uri + size + media_type + url,
                                  command_id echoed for correlation)
```

### Contracts — **shipped in co-core** (`co_core.pure.models.changes`), not illustrative

These are the real models (stable v0.5.1 → v0.6.0), both `extra="ignore"` (consumer-safe). Consume the canonical classes; a producer emits via the strict `*Emit` (`extra="forbid"`) subclass if one is needed for emit-time typo-catch.

**Consume — `ContentFetchCommand`** (stream `streams.CONTENT_FETCH` = `content.fetch`; command semantics ⇒ **exactly one** consumer group, `replicator.fetch`; competing consumers; ack on success; `claim_stale` for crashed workers; `dead_letter` after N attempts):

```
{ schema_version: int = 1,
  event_type: "content_fetch",
  occurred_at: datetime,
  command_id: str,                 # wire idempotency key
  url: str }
```

**Emit — `BlobAvailableEvent`** (stream `streams.CONTENT_BLOBS` = `content.blobs`; fact semantics ⇒ broadcast, **one group per consuming service**):

```
{ schema_version: int = 1,
  event_type: "blob_available",
  occurred_at: datetime,
  content_fingerprint: str,        # wire idempotency key; content-addressed
  blob_uri: str,                   # opaque backend URI
  size_bytes: int,
  media_type: str,
  url: str,
  command_id: str | None = None }  # correlation back to the triggering command
```

> **These shapes are settled — cannobserv#266, released in co-core v0.7.0.** The
> resolution of the two questions the prior revision flagged:
>
> 1. **`content.fetch` is URL-addressed, not fingerprint-addressed.** The command
>    carries **no** expected fingerprint — the issuer decides *when* to fetch but
>    does not fetch, so **Replicator is the sole fingerprinter and the parity/drift
>    problem genuinely dissolves** (the strategy's original claim, now upheld — see
>    *Idempotency & fidelity*). The command's idempotency key is `command_id`
>    (the fingerprint can't be a command key — it isn't known until after the fetch).
> 2. **`blob_available` is enriched** with `size_bytes`, `media_type`, `url`, and an
>    optional `command_id` (correlation). No `info_source_id` — the generic content
>    contracts stay domain-agnostic; the issuer (Watcher, Phase 4) keeps its own
>    `command_id → info_source` mapping and re-associates when the fact returns.
>
> Both stay `schema_version=1` (no deployed producer/consumer existed at reshape
> time — the "one free reshape" window).

### Consumer best-practices (developed across archiver#106/#107/#108)

Replicator is a **consumer** — follow the conventions the archiver producer + co-core established:

- **At-least-once ⇒ idempotent.** Dedupe on `content_fingerprint` (the storage key already makes re-processing a no-op — see below).
- **Validation posture:** use the canonical `extra="ignore"` models; **branch on `schema_version` before destructuring**; tolerate additive producer fields. Do not use the strict `*Emit` classes on the consume path.
- **Batch-poison caveat:** `AsyncBusConsumer.read(count>1)` raises `BusMessageAnomaly` on a malformed frame *before* returning the well-formed ones in the batch. Read `count=1`, or catch the anomaly and route via `dead_letter`. `from_wire` is deliberately fail-loud.
- **DLQ is a shipped seam, not a TODO:** `AsyncBusConsumer.dead_letter(message_id, fields)` copies the frame to `dlq_name(topic)` (`<topic>.dlq`) and acks the original. archiver#107 (dead-lettering poison outbox rows) is the working precedent for the "deterministic failure ⇒ DLQ, transient failure ⇒ retry" split.
- **Consumer group + start position:** `ensure_group(start_id="$")` reads only new messages; `"0"` drains the backlog. Choose per intent (the MVP seed harness controls when commands appear, so either is fine).

### Temp-storage backend

- An **interface** (`store(bytes, fingerprint, media_type) -> backend_uri`, `open(fingerprint)`, `exists(fingerprint)`), with the **first impl = local filesystem** under `REPLICATOR_BLOB_DIR`.
- Designed for swap to an object store (then own infra) without touching the loop. The `backend_uri` is opaque to consumers (it becomes `blob_available.blob_uri`).
- **"Temporary"** = the bytes live long enough for downstream durable replication (later phase) to pick them up; retention policy is out of MVP scope.

### Idempotency & fidelity

- **Two idempotency keys, two levels.** The **command** dedupes on `command_id` (skip a redelivered `content.fetch`, avoiding a redundant fetch). The **fact** dedupes on `content_fingerprint` (downstream consumers of `blob_available` de-dup on it). Content-addressed storage makes re-storing identical bytes a no-op regardless.
- **Fidelity — parity dissolves (as the strategy intended).** `content.fetch` carries no expected fingerprint (cannobserv#266): the issuer decides *when* to fetch but never fetches, so **Replicator is the single fetcher and sole fingerprinter**. The `sha256` it computes at fetch time is authoritative and travels on `blob_available.content_fingerprint`. There is no second fetcher and therefore no parity/drift to reconcile — the exact property the boundary design was chosen for.

---

## MVP scope cuts (explicitly OUT)

- **Permanent/durable replication** per RepSpec provider (gcs/gdrive/ia), `path_template`, `credentials_alias`. MVP uses the local temp backend only.
- **Writeback to archiver** (recording a SourceRevision, `content_cache_uri`/`public_url`). The `blob_available` fact is sufficient to prove the loop; archiver-writeback is MVP+ (archiver exposes `POST /source-revisions` + `PATCH /source-revisions/{id}` for the cache fields when that phase arrives).
- **RepSpec resolution / reads from archiver.** Not needed for the loop.
- **Watcher cutover** (issuing `content.fetch`, consuming `blob_available`) — parent strategy Phase 4.
- Re-replication policy, blob GC/retention, cross-source dedup, robots/rate-limit/auth niceties.

## Build sequence (within the MVP)

1. Repo scaffold (A/W/N pattern) + co-core wiring via the **wheelhouse** mechanism (`sync_wheelhouse.py` + find-links; WIF in CI); validate `import co_core` / `co_core_aio` + the `[extract]`/`[bus]` extras in CI.
2. Wire the **shipped, settled** `content.fetch` / `blob_available` co-core models (co-core v0.7.0, cannobserv#266 — no stubbing, no open contract seams needed).
3. Bus consumer loop (`AsyncBusConsumer`, group `replicator.fetch`) with `read` (count=1) / `ack` / `claim_stale` / `dead_letter`; TDD against a fake/in-memory redis.
4. Fetch (`AsyncFetchDriver`) → temp-store (local backend) → fingerprint (verify vs command); TDD each step.
5. Emit `blob_available` via `AsyncBusPublisher` + `to_wire`.
6. Seed/test harness that issues a `content.fetch`; integration test the full loop end-to-end against the live VM Redis (Archiver-operated).

## Open questions for the Replicator team

Most of the original open questions are now settled (content contracts fixed in cannobserv#266 → co-core v0.7.0; stream taxonomy fixed in `streams.py`; distribution = wheelhouse; DLQ/retry have co-core seams + the archiver#107 precedent; Redis ownership resolved in archiver#109). Genuinely open:

- **Temp-storage `backend_uri` scheme + local-FS layout** (how `blob_uri` encodes the local path; forward-compatibility with an object-store URI).
- **Command issuer during the MVP window** — seed script vs an early watcher hook vs archiver — until the Phase 4 cutover makes Watcher the issuer.
- **Stop-at-fact vs archiver-writeback** for the MVP boundary (MVP+ writes a SourceRevision back to archiver).
