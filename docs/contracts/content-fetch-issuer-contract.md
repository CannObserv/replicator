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
*payload* carries `{command_id, url}`, and `content.blobs` carries **both outcomes** of that
command — `{content_fingerprint, blob_uri, size_bytes, media_type, url, command_id?}` on success,
`{command_id, url, reason, terminal, status_code?, attempts?, detail?}` on failure. All three sit
inside a co-core envelope, which is a shape in its own right and the first thing a producer must
get right (see **The frame**). There is **no `info_source_id`, and no other domain identity,
anywhere in any of them** — Replicator fetches bytes and knows nothing about what they mean. That
keeps Replicator clean, and it pushes the whole of correlation onto the issuer. Most of what
follows fails *silently* when it is got wrong: no error, no dead letter, no log on Replicator's
side — just a fact nobody can match, or a command that was never run.

Contracts settled in cannobserv#266 (co-core v0.7.0); the failure fact added in cannobserv#270 and
the tz-aware `occurred_at` in cannobserv#273, both shipped in **co-core v0.7.2**. Founding
rationale:
[`docs/plans/2026-06-25-replicator-mvp-design.md`](../plans/2026-06-25-replicator-mvp-design.md).

---

## The frame (envelope)

**Publish through `to_wire`. Never hand-roll the fields.** What lands on the stream is a co-core
*envelope*, not the model's fields flattened — `co_core.pure.adapters.bus.envelope.to_wire(event)`
produces exactly six keys, all of them derived from the model. None are caller-supplied:

| Key | Value |
|---|---|
| `key` | the envelope's idempotency key, derived by `to_wire` — see the table below |
| `payload` | the model, JSON-serialized. This is where `command_id`, `url`, and everything in the tables below actually live |
| `event_type` | `content_fetch` / `blob_available` / `fetch_failed` — how `from_wire` picks a model |
| `schema_version` | stringified |
| `occurred_at` | ISO 8601 UTC, **tz-aware** — see the command table |
| `content_type` | `application/json` |

`key` is derived per payload type, and the three rules are not the same shape:

| Payload | Derived `key` |
|---|---|
| `content_fetch` | `command_id` |
| `blob_available` | `content_fingerprint` |
| `fetch_failed` | **`command_id:occurred_at`** — deliberately *not* the bare `command_id` |

The `fetch_failed` rule is load-bearing (cannobserv#270). One command can emit more than one
failure over its life, so a bare-`command_id` key would make a consumer that dedupes on the
envelope key collapse the sequence and drop the **terminal** event — the one that closes the
pending entry. Correlation rides on the `command_id` *field*, never on the key.

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
| `occurred_at` | `datetime` | **tz-aware UTC, enforced.** Not used for ordering or expiry by Replicator |
| `command_id` | `str` | **The idempotency key and the sole correlator.** See MUST-1 |
| `url` | `str` | What to fetch. **Not** a key. See MUST-3 |

> **`occurred_at` must carry a timezone.** Since co-core v0.7.2 (cannobserv#273) it is an
> `AwareDatetime` on every payload: a **naive** value is rejected fail-loud rather than assumed
> to be UTC, because "assume UTC" corrupts the instant when a producer stamps a naive local time.
> An aware non-UTC value is normalized. A naive one fails `from_wire` inside Replicator's `read`,
> which means it dead-letters as a malformed frame and is one of the rows that stay **silent** —
> the same shape as an issuer that flattened the envelope. `datetime.now(UTC)`, not
> `datetime.now()`.

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

**Fact — `content.blobs`, `FetchFailedEvent`** (co-core ≥ 0.7.2, cannobserv#270):

| Field | Type | Notes |
|---|---|---|
| `schema_version` | `int` = 1 | |
| `event_type` | `"fetch_failed"` | |
| `occurred_at` | `datetime` | tz-aware UTC, stamped at publish. Also half the envelope key |
| `command_id` | `str` | **Required — the correlator, and the whole point of the event** |
| `url` | `str` | Echoed. Confirmation and debugging only, exactly as on the success fact |
| `reason` | `str` | Stable token; see below. **Treat an unknown value as opaque** |
| `terminal` | `bool` | `True` = the command is closed, no blob will ever arrive. **Branch on this first** |
| `status_code` | `int \| None` | Set for `http_status`; absent otherwise |
| `attempts` | `int \| None` | Set only where the attempt count is *why* the command closed |
| `detail` | `str \| None` | Free text for the journal. **Never branch on it**, and never surface it to an end user — on `handler_error` it is the text of an exception Replicator did not anticipate, so its content is unbounded |

`reason` tokens, one per row of the failure taxonomy below:

| Token | Condition |
|---|---|
| `http_status` | 4xx, or a body-less 304 (`status_code` set) |
| `not_fetchable` | bad scheme / invalid URL |
| `too_large` | body over `REPLICATOR_MAX_BLOB_BYTES` |
| `unsupported_schema_version` | the command decoded at a `schema_version` Replicator does not support |
| `handler_error` | unclassified, and it exhausted the delivery ceiling (`attempts` set) |

co-core's own docstring also lists `wrong_payload_type`. **Replicator never emits it**, and an
issuer should not expect it: a frame that decoded to a non-`content.fetch` payload carries, at
most, *somebody else's* `command_id` — `BlobAvailableEvent`'s names a command that **succeeded**,
which is why a blob exists for it. A terminal failure keyed on that id would tell an issuer its
good bytes are never coming. That frame dead-letters and stays silent.

`reason` is a plain `str` and **not** a `Literal`, deliberately: a producer adding a token must
never crash an older `extra="ignore"` consumer. So the token list is additive, and a consumer
that has branched on `terminal` first is already correct for tokens that do not exist yet.

**Everything Replicator emits today is `terminal=True`.** Non-terminal facts — a 5xx or a 429
announcing itself while it is still retrying — are deferred (#9 §3): `content.blobs` is
broadcast and nothing trims it, so a fact per reclaim during an origin outage is unbounded
growth on a stream nobody prunes. The cost is stated plainly in MUST-6: a retrying command is
still invisible for as long as it retries. If an issuer needs the in-flight signal, say so on
its tracker rather than inferring one from silence.

All three models are `extra="ignore"`: additive producer fields are tolerated. Branch on
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

**This covers `fetch_failed` too, and there the duplicates are not even identical.** The failure
fact is published *before* the dead-letter (`dead_letter` acks inside itself, so a fact published
after it is lost outright on a crash in between). A crash in that window redelivers the command,
re-runs the failure, and publishes a **second `fetch_failed` with the same `command_id` and a
fresh `occurred_at`** — so its envelope key differs and consumer-side dedup-on-key will not
collapse it. That is by design (see **The frame**); it is also not something Replicator can
engineer away, since a deterministic `occurred_at` would need a durable store Replicator does not
have and must not grow.

Closing a pending entry must therefore be idempotent in both directions: applying the same
terminal failure twice, and applying a failure to an entry already closed.

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

And do not dedupe **`fetch_failed`** on `command_id` either — for the opposite reason. More than
one failure fact per command is expected (MUST-4), and once non-terminal facts exist a command
may legitimately emit several. Use `terminal` to decide whether the entry closes; use
`command_id` to decide *which* entry.

### 6. Handle `fetch_failed`, and **keep a reaper anyway**

A command that fails permanently now publishes a **`fetch_failed` fact on `content.blobs`**
(#9, co-core cannobserv#270) *and* is copied to `content.fetch.dlq` and acked. The fact is the
issuer's surface; the DLQ is the operator's. This added a signal — it did not replace one, and an
entry appears in both places.

So an issuer's primary mechanism is now the fact: consume `content.blobs`, branch on the payload
type, and on a `fetch_failed` with `terminal=True` close the pending entry **with a reason**. Off
one consumer group, since both outcomes share the stream.

**Silence has not gone away — it has narrowed.** Two conditions still produce nothing, and one
produces nothing *yet*:

- **A frame that fails `from_wire` entirely.** It has no payload, therefore no `command_id`,
  therefore nothing a fact could correlate on. DLQ-only. The commonest cause is an issuer that
  `XADD`ed flat fields instead of publishing through `to_wire` — see **The frame** — and, since
  co-core 0.7.2, a naive `occurred_at`.
- **A frame that decodes to a payload that is not a `content.fetch` command.** Not merely
  unreportable — *unsafe* to report. Most of the payload union has no `command_id` at all, and the
  members that do carry one that belongs to a different command: a misrouted `blob_available`
  names a command that **succeeded**. Announcing a terminal failure against it would contradict a
  fact the issuer has already applied, and MUST-4 covers duplicates, not contradictions. Silent by
  design.
- **A command whose `command_id` is blank.** A fact with no correlator closes nothing. Replicator
  logs a warning and dead-letters without announcing.
- **A command still retrying.** Replicator emits no non-terminal fact today (#9 §3), so a 5xx,
  a 429, a network error, or a blob tree over its ceiling is invisible for as long as it retries —
  and there is no latency bound on that: transient failures retry indefinitely at the
  `REPLICATOR_CLAIM_MIN_IDLE_MS` cadence (default 60 s), and over
  `REPLICATOR_BLOB_MAX_TOTAL_BYTES` the command parks in the PEL until a retention sweep frees
  space. A command can legitimately be in flight for a long time.

Which is why the **reaper stays**, demoted from primary mechanism to backstop:

- A timeout is still grounds to **re-issue** (fresh `command_id`), not to conclude failure.
- Derive the timeout generously from the reclaim cadence rather than hardcoding a number — the
  cadence is an operator setting on Replicator's host, and an issuer that pins 60 s starts
  re-issuing under a live retry the day it is tuned. Expect duplicate work rather than assuming
  loss.
- A `fetch_failed` publish that itself fails is swallowed on Replicator's side (the dead-letter
  still happens, and raising there would only burn the delivery ceiling and reach the same DLQ
  minutes later). Rare, logged, and another reason the reaper is not optional.

See the failure taxonomy below for exactly which outcomes are visible and which are not.

**The DLQ is still readable, and still worth reading** — for what the fact cannot carry. It is an
ordinary stream on the same broker, and every entry carries the original **command** envelope — so
`key` is the failed `command_id` — plus `dlq_reason` and `dlq_original_id`. Its value is now the
complement of the fact rather than a substitute for it: it is the only place the silent rows
above show up at all, and it preserves the offending frame itself, which no fact does. Read it with
a plain `XREAD` and no consumer group: a group left behind by a non-owner accumulates a PEL nothing
drains.

One caveat that survives unchanged: the anomaly class that dead-letters a *synthesized* record (a
frame that failed to decode at all, whose original entry has since been trimmed) carries no `key`
to match on.

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
- **Announce, then ack.** Every path that closes a command without a blob publishes its
  `fetch_failed` *before* the dead-letter that acks it, so a crash in between costs a duplicate
  fact (MUST-4) rather than losing the fact outright.
- **`command_id` is echoed** on every fact produced from a command, success or failure.
- **The fingerprint is definitional.** Replicator is the cluster's sole fetcher and sole
  fingerprinter, so `content_fingerprint` *is* the content identity — there is nothing to
  cross-check it against. A consumer can still re-hash the bytes at `blob_uri` and compare; worth
  doing once the store crosses a boundary (object store, another host).
- **Identical bytes are one blob.** Content-addressed storage, so a re-fetch of unchanged content
  costs an origin request and nothing else.
- **At-least-once delivery**, both directions.

## What Replicator does **not** guarantee

- **No *non-terminal* failure fact** — a command that is retrying announces nothing until it
  either succeeds or is closed. See MUST-6 and the `FetchFailedEvent` note above.
- **No failure fact for a frame that is not a command, or whose `command_id` is blank** — nothing
  to correlate one on, and for a foreign payload nothing *safe* to correlate one on. MUST-6.
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
| `schema_version` ≠ 1 | fact, then `content.fetch.dlq` | `fetch_failed` · `unsupported_schema_version` |
| Frame decodes to a non-`content_fetch` payload | `content.fetch.dlq` | **nothing** — any `command_id` in it is another command's |
| Command with a blank `command_id` | warning, then `content.fetch.dlq` | **nothing** — no correlator to key a fact on |
| Malformed frame (fails `from_wire`; includes a naive `occurred_at`) | `content.fetch.dlq`, synthesized record | **nothing** — no payload at all |
| HTTP 4xx, or a body-less 304 | fact, then `content.fetch.dlq` | `fetch_failed` · `http_status` (+ `status_code`) |
| URL not fetchable (bad scheme / invalid URL) | fact, then `content.fetch.dlq` | `fetch_failed` · `not_fetchable` |
| Body over `REPLICATOR_MAX_BLOB_BYTES` (default 64 MiB) | fact, then `content.fetch.dlq` | `fetch_failed` · `too_large` |
| HTTP 5xx / 408 / 429, or a network error | retry indefinitely, default ~60 s cadence | delayed fact, or **nothing while it retries** |
| Blob tree over `REPLICATOR_BLOB_MAX_TOTAL_BYTES` | parked in the PEL until a sweep frees space | delayed fact, or **nothing while it waits** |
| Unclassified handler error | retried to the delivery ceiling (~4 reclaims / ~4 min at default settings), then fact + DLQ | `fetch_failed` · `handler_error` (+ `attempts`) |
| Success | `blob_available` on `content.blobs` | the fact |

Every `fetch_failed` row carries `terminal=True` — the command is closed and no blob will arrive.
Every remaining "nothing" row is why MUST-6 keeps the reaper.

Note the two shapes of silence are not the same problem. The four rows with no usable
`command_id` are **permanently** silent, and an issuer's only recourse there is the reaper. The
two retrying rows are silent **for now** (#9 §3), and an issuer that would use an in-flight
signal should say so on its tracker rather than lengthening its timeout to compensate.

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

## Settled: the `fetch_failed` fact

**Resolved.** This document previously carried silence-as-failure as an open question, arguing the
fix needed "a consumer that wants it — Watcher, once Phase 4 has a real pending map to close out."
That consumer arrived ([CannObserv/watcher#241](https://github.com/CannObserv/watcher/issues/241)),
co-core shipped `FetchFailedEvent` in **v0.7.2**
([cannobserv#270](https://github.com/CannObserv/cannobserv/issues/270)), and Replicator publishes
it (#9). MUST-6 above is the current contract; the tables are the current shape.

Two things the resolution deliberately did **not** do, recorded so they are not re-litigated as
oversights:

- **Non-terminal facts are deferred** (#9 §3). `content.blobs` is broadcast and nothing trims it,
  so emitting a fact per reclaim while an origin is down is unbounded growth on a stream nobody
  prunes. The cost is real and named in MUST-6: a 429 backing off is invisible while it retries,
  which is one of the four Watcher behaviours cannobserv#270 set out to enable. Reopen it on the
  consumer's tracker if the reaper turns out to re-issue under live retries in practice.
- **The rows with no usable `command_id` stay DLQ-only.** Not a scope cut. `FetchFailedEvent`'s
  `command_id` is required, so a fact naming no command closes nothing; and for a frame that
  decoded to a *foreign* payload, the `command_id` it carries belongs to a different command —
  reporting it would be worse than silence, not better. The reaper is the mechanism there,
  permanently.
