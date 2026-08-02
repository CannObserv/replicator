# The `content.fetch` issuer contract

**Status:** normative. **Home:** this file, in the Replicator repo — Replicator is the sole
consumer of `content.fetch` and the sole producer of `content.blobs`, so the contract lives with
the behaviour it describes. Issuer-side repos link here rather than copying; a copy would drift
from the code the day it was written.

**Audience:** any service that publishes a `ContentFetchCommand`. Today that is
[`scripts/seed_fetch.py`](../../scripts/seed_fetch.py). From Phase 4 it is Watcher.

**Changing this document.** "Link, don't copy" only holds if a change reaches the issuers. A change
to any MUST, or to the failure taxonomy, is announced on the open issuer-side trackers — currently
[CannObserv/watcher#241](https://github.com/CannObserv/watcher/issues/241) — in the same change that
edits this file. Whatever is asserted here is asserted about this repo's code: edit one, edit both.

**Why this document exists.** The bus wire shape is deliberately domain-agnostic: a `content.fetch`
*payload* carries `{command_id, url}` and a `content.blobs` payload carries
`{content_fingerprint, blob_uri, size_bytes, media_type, url, command_id?}` — both inside a co-core
envelope, which is a shape in its own right and the first thing a producer must get right (see **The
frame**). There is **no
`info_source_id`, and no other domain identity, anywhere in either payload** — Replicator fetches
bytes and knows nothing about what they mean. That keeps Replicator clean, and it pushes the whole
of correlation onto the issuer. Most of what follows fails *silently* when it is got wrong: no
error, no dead letter, no log on Replicator's side — just a fact nobody can match, or a command
that was never run.

Contracts settled in cannobserv#266 (co-core v0.7.0). Founding rationale:
[`docs/plans/2026-06-25-replicator-mvp-design.md`](../plans/2026-06-25-replicator-mvp-design.md).

---

## The frame (envelope)

**Publish through `to_wire`. Never hand-roll the fields.** What lands on the stream is a co-core
*envelope*, not the model's fields flattened — `co_core.pure.adapters.bus.envelope.to_wire(event)`
produces exactly six keys, all of them derived from the model. None are caller-supplied:

| Key | Value |
|---|---|
| `key` | the envelope's partition key, derived by `to_wire` — **the `command_id`** for a command, the `content_fingerprint` for a fact |
| `payload` | the model, JSON-serialized. This is where `command_id`, `url`, and everything in the tables below actually live |
| `event_type` | `content_fetch` / `blob_available` — how `from_wire` picks a model |
| `schema_version` | stringified |
| `occurred_at` | ISO 8601 UTC |
| `content_type` | `application/json` |

An issuer that `XADD`s `command_id` and `url` as top-level fields produces a frame `from_wire`
cannot decode. It raises `BusMessageAnomaly` from inside the consumer's `read`, and Replicator
routes it to `content.fetch.dlq` — **silently, by the terms of MUST-6**. This is the single
likeliest way to get the contract wrong, so it is stated before the field tables rather than after.

`key` is **not** load-bearing on the consume path: Replicator decodes `payload` and dedupes on
`payload.command_id`, never on the envelope key. Its value is operational — it is what makes a DLQ
entry correlatable without parsing JSON (see MUST-6), and it is what a future partitioned consumer
would shard on.

## The payload (inside `payload`)

**Command — `content.fetch`, `ContentFetchCommand`** (`co_core.pure.models.changes`):

| Field | Type | Notes |
|---|---|---|
| `schema_version` | `int` = 1 | Replicator supports **1 only**; anything else dead-letters (see the taxonomy) |
| `event_type` | `"content_fetch"` | |
| `occurred_at` | `datetime` | UTC. Not used for ordering or expiry by Replicator |
| `command_id` | `str` | **The idempotency key and the sole correlator.** See MUST-1 |
| `url` | `str` | What to fetch. **Not** a key. See MUST-3 |

**Fact — `content.blobs`, `BlobAvailableEvent`**:

| Field | Type | Notes |
|---|---|---|
| `schema_version` | `int` = 1 | |
| `event_type` | `"blob_available"` | |
| `occurred_at` | `datetime` | UTC, stamped at publish |
| `content_fingerprint` | `str` | sha256 of the bytes. Content identity — **not** a correlator |
| `blob_uri` | `str` | `file://<blob_dir>/<ab>/<cd>/<sha256>.bin`. Temporary; see MUST-7 |
| `size_bytes` | `int` | |
| `media_type` | `str` | Normalized, `charset` dropped; `application/octet-stream` when absent |
| `url` | `str` | Echoed from the command. Confirmation and debugging only |
| `command_id` | `str \| None` | Echoed from the command. `None` only for non-command emits |

Both models are `extra="ignore"`: additive producer fields are tolerated. Branch on
`schema_version` **before** destructuring, and never use the strict `*Emit` classes on a consume
path.

---

## What the issuer MUST do

### 1. Mint a fresh `command_id` per fetch *occasion*, never per resource

Replicator dedupes on `command_id` — `replicator:cmd:<command_id>`, TTL
`REPLICATOR_DEDUPE_TTL_SECONDS` (default 24 h, operator-tunable) — in
[`src/worker/loop.py::process_message`](../../src/worker/loop.py). A duplicate is **acked and
dropped**: no fetch, no fact, one `INFO` line on Replicator's side and nothing at all on the
issuer's.

So a `command_id` derived from anything resource-stable — a WatchedItem id, a hash of the URL, the
URL itself — means **the second legitimate re-fetch of that URL never happens**. Watcher, whose
entire job is re-fetching a URL over time to detect change, is precisely the service this breaks.

The trap is worse than a flat failure because it is **TTL-bounded**: re-fetches inside the dedupe
window vanish, re-fetches after it work. A daily cadence sits on the boundary of the default 24 h
and fails intermittently. A test run with two fetches a minute apart reproduces it; a test run with
two fetches a day apart does not.

Mint a ULID per fetch intent — one per call, not one per URL and not one per run.
[`scripts/seed_fetch.py::build_command`](../../scripts/seed_fetch.py) does exactly this and says why.

Uniqueness is required *for correctness* only within the dedupe TTL, but *for correlation* it must
be global and permanent — the issuer's own map is keyed on it.

### 2. Persist `command_id → domain` durably, **before** publishing

The bus carries nothing that can reconstruct the mapping. If the issuer crashes between minting the
id and recording it, the returning `blob_available` is uncorrelatable — there is no query, on any
stream, that recovers which InfoSource asked.

Persist first, then publish; outbox-style, symmetric to archiver's producer. (Replicator itself has
no outbox and must not grow one — its durable record of intent is the consumer group's PEL. The
outbox belongs to producers with a database, which is the issuer.)

Losing the map is recoverable but not free: the intent can be re-issued under a fresh
`command_id`, at the cost of another origin request. What is *not* recoverable is the in-flight
fact — it will arrive, match nothing, and have to be discarded.

### 3. Correlate on `command_id` only — `url` is not a key

Archiver's model permits **multiple InfoSources per URL** (non-unique `url`, different extraction
strategies), so `url → info_source` is one-to-many. An issuer that falls back to matching a fact by
its `url` will, sooner or later, attach bytes to the wrong InfoSource — silently, and in a way that
looks like correct data downstream.

`url` on the fact is confirmation and debugging. Nothing more.

### 4. Make correlation idempotent — one command can yield more than one fact

`blob_available` is at-least-once **per `command_id`**, not exactly-once. The dedupe key is written
*after* the handler returns, deliberately
([`src/worker/loop.py::process_message`](../../src/worker/loop.py)):
marking first would turn a crash between the mark and a completed handle into permanent loss. The
cost of that ordering is the opposite duplicate — a crash between a successful publish and the
`SET` re-runs the handler on reclaim and emits a **second fact carrying the same `command_id`**.

Applying a fact must therefore be safe to do twice. Same `command_id`, same `content_fingerprint`,
same `blob_uri` — an upsert, not an append.

### 5. Do **not** dedupe facts on `content_fingerprint`

`content_fingerprint` is the fact's idempotency key *for storage*: identical bytes are the same
blob, at the same path, and re-storing them is a no-op. It is **not** an idempotency key for
correlation.

Two commands — two occasions of the same URL, or two InfoSources sharing one URL — that return
identical bytes produce two facts with **the same fingerprint and the same `blob_uri`, and
different `command_id`s**. A consumer deduping its inbox on fingerprint drops the second fact and
loses that correlation entirely: the same silent-failure shape as MUST-1, arrived at from the
opposite direction.

Dedupe on `command_id`. Treat the fingerprint as content identity.

### 6. Time out your own pending entries — **there is no failure fact**

`co_core.pure.models.changes` defines no `fetch_failed` event, and Replicator's handler publishes
only on success. A command that fails permanently is copied to **`content.fetch.dlq`** and acked —
a stream no issuer reads, and which carries no notification of any kind. **Silence is the only
failure signal an issuer ever receives.**

Two consequences:

- The issuer's pending map needs its **own reaper**. Nothing else will ever close an entry.
- A timeout is grounds to **re-issue** (fresh `command_id`), not to conclude failure. There is no
  latency bound: transient failures retry indefinitely at the `REPLICATOR_CLAIM_MIN_IDLE_MS`
  cadence (default 60 s), and over `REPLICATOR_BLOB_MAX_TOTAL_BYTES` the command parks in the PEL
  until a retention sweep frees space. A command can legitimately be in flight for a long time.

Derive the reaper's timeout generously from the reclaim cadence rather than hardcoding a number —
the cadence is an operator setting on Replicator's host, and an issuer that pins 60 s starts
re-issuing under a live retry the day it is tuned. Expect duplicate work rather than assuming loss.
See the failure taxonomy below for exactly which outcomes are silent.

**The DLQ is readable, and worth reading.** Silence is the *guaranteed* signal; `content.fetch.dlq`
is an *available* one. It is an ordinary stream on the same broker, and every entry carries the
original envelope — so `key` is the failed `command_id` — plus `dlq_reason` and `dlq_original_id`.
An issuer that tails it can close a pending entry **deterministically, with a reason**, instead of
waiting out a timeout and re-issuing work that will fail the same way. Read it with a plain `XREAD`
and no consumer group: a group left behind by a non-owner accumulates a PEL nothing drains.

Keep the reaper regardless. A command stalled rather than failed appears in neither stream, and the
one anomaly class that dead-letters a *synthesized* record (a frame that failed to decode at all,
whose original entry has since been trimmed) carries no `key` to match on.

Inspection commands live in [`docs/COMMANDS.md`](../COMMANDS.md). Monitoring the DLQ on
Replicator's side remains an operator responsibility, not a bus-level one.

### 7. Copy the bytes before the blob expires

`blob_uri` is temporary. A blob is reaped once its mtime is older than
`REPLICATOR_BLOB_TTL_SECONDS` — currently 7 days, a published commitment to archiver
(archiver#118) rather than a knob Replicator turns freely, but still a *setting* on Replicator's
host. Treat it as a floor to ask about, not a constant to schedule against: consume the bytes
promptly and re-issue if the URI fails to open, rather than building a pipeline whose timing
assumes seven days.

The clock runs from **last reference by a fetch**, not last read by a consumer: Replicator `utime`s
the file when a re-fetch short-circuits on existing bytes
([`src/storage/local.py::_touch`](../../src/storage/local.py)), but a consumer opening the path does
not touch mtime. Holding a `blob_uri` for a week without a re-fetch loses the bytes.

Also: `blob_uri` is a **`file://` URI on Replicator's host**. The contract is VM-local today.
A consumer on another host cannot open it, and nothing on the wire says so.

---

## What Replicator guarantees

- **Store, then publish.** A fact never announces bytes that are not on disk. The reverse gap
  (bytes stored, publish failed) self-repairs: the message stays unacked, the reclaim re-runs a
  handler that content-addressed storage makes a no-op.
- **`command_id` is echoed** on every fact produced from a command.
- **The fingerprint is definitional.** Replicator is the cluster's sole fetcher and sole
  fingerprinter, so `content_fingerprint` *is* the content identity — there is nothing to
  cross-check it against. A consumer can still re-hash the bytes at `blob_uri` and compare; worth
  doing once the store crosses a boundary (object store, another host).
- **Identical bytes are one blob.** Content-addressed storage, so a re-fetch of unchanged content
  costs an origin request and nothing else.
- **At-least-once delivery**, both directions.

## What Replicator does **not** guarantee

- **No failure fact** — see MUST-6.
- **No latency bound**, and no SLA on turnaround.
- **No ordering.** Two commands issued in sequence may produce facts in either order.
- **No cross-command dedupe.** Two `command_id`s for one URL are two fetches and two facts, by
  design — that is what makes MUST-1 work.
- **No retention guarantee on `content.blobs`.** Replicator never trims it — `BusPublish` takes no
  `MAXLEN` and nothing in this repo issues `XTRIM` — so whatever policy applies is the broker
  operator's, not part of this contract. Do not treat the stream as an archive to reconcile
  against later.

---

## Failure taxonomy: what happens, and what the issuer sees

| Condition | Replicator's action | Issuer-visible |
|---|---|---|
| Duplicate `command_id` inside the dedupe window (default 24 h) | ack, no fetch | **nothing** — silent drop |
| `schema_version` ≠ 1 | `content.fetch.dlq` | **nothing** |
| Frame decodes to a non-`content_fetch` payload | `content.fetch.dlq` | **nothing** |
| Malformed frame (fails `from_wire`) | `content.fetch.dlq`, synthesized record | **nothing** |
| HTTP 4xx, or a body-less 304 | `content.fetch.dlq` | **nothing** |
| URL not fetchable (bad scheme / invalid URL) | `content.fetch.dlq` | **nothing** |
| Body over `REPLICATOR_MAX_BLOB_BYTES` (default 64 MiB) | `content.fetch.dlq` | **nothing** |
| HTTP 5xx / 408 / 429, or a network error | retry indefinitely, default ~60 s cadence | delayed fact, or nothing |
| Blob tree over `REPLICATOR_BLOB_MAX_TOTAL_BYTES` | parked in the PEL until a sweep frees space | delayed fact |
| Unclassified handler error | retried to the delivery ceiling (~4 reclaims / ~4 min at default settings), then DLQ | delayed fact, or nothing |
| Success | `blob_available` on `content.blobs` | the fact |

Every "nothing" row is why MUST-6 exists.

---

## Provenance and trust

`content.fetch` is an **unauthenticated capability**: any writer to the bus can make Replicator
issue an arbitrary outbound HTTP request from the cluster VM and store the response. There is no
signing, no allowlist, and no issuer identity on the frame.

Integrity rests entirely on **bus access control** — the broker is Archiver-operated and bound to
localhost on a single trusted VM. That is proportionate today. It stops being proportionate the
moment the bus spans hosts or tenants, at which point message signing or a URL allowlist becomes
the conversation. Not before.

Relatedly: nothing but the seed script writes to `content.fetch` today, and `seed_fetch.py`
requires `--production` for the one target the live worker consumes. A frame on that stream is
fetched for real.

---

## Open question: a `fetch_failed` fact

Silence-as-failure (MUST-6) is a deliberate consequence of the MVP scope, not an oversight — but it
is the contract's sharpest edge, and it makes every issuer build the same reaper.

The alternative is a `fetch_failed` fact on `content.blobs` (or a sibling stream) carrying
`command_id`, the reason, and whether it is terminal. That is a **co-core contract change**
(cannobserv), not a Replicator-local one, and it needs a consumer that wants it — Watcher, once
Phase 4 has a real pending map to close out. Tracked separately from this document; until it
exists, MUST-6 is the contract.
