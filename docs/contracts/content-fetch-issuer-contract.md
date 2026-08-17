# The `content.fetch` issuer contract

**Status:** normative. **Home:** this file — Replicator is the sole consumer of `content.fetch` and
the sole producer of `content.blobs`, so the contract lives with the behaviour it describes. Issuer
repos link here rather than copying; a copy drifts from the code the day it is written.

**Audience:** any service publishing a `ContentFetchCommand` — today
[`scripts/seed_fetch.py`](../../scripts/seed_fetch.py), from Phase 4 Watcher.

**Companion.** [`content-fetch-issuer-reference.md`](content-fetch-issuer-reference.md) carries the
parts an issuer *looks up* rather than reads through — equally normative, split out in #24 so this
file stays readable start to finish. Index at the end.

**Sibling document.** This settles the *wire*. [`replicator-boundaries.md`](replicator-boundaries.md)
settles the *service* — what Replicator is allowed to become, and therefore which proposed fields
this contract will never grow. Read it before proposing a payload addition (#12).

**Changing this document.** "Link, don't copy" only holds if a change reaches the issuers, so a
change to any MUST or to the failure taxonomy is announced on the open issuer-side trackers —
currently [CannObserv/watcher#241](https://github.com/CannObserv/watcher/issues/241) — in the same
change that edits this file. What is asserted here is asserted about this repo's code: edit both.

**Why this document exists.** One command stream and one fact stream carrying **both outcomes**,
each payload inside a co-core envelope — a shape in its own right, and the first thing a producer
must get right (see **The frame**). Fields are tabulated under **The payload**.

`info_source_id` is **carried, not understood** (cannobserv#300, #28): echoed verbatim onto both
facts, never parsed, deduped on, keyed on, or read by any routing or policy decision. It travels so
a consumer of a *broadcast* stream can tell what a fact is about without holding the issuer's
private map. [`replicator-boundaries.md`](replicator-boundaries.md) keeps it at that — its
executable half forbids the value in any branch, lookup key, or constructed string in `src/`.

Correlation still rests entirely on the issuer, and most of what follows fails *silently* when it is
got wrong: no error, no dead letter, no log on Replicator's side — just a fact nobody can match, or
a command that was never run.

Version history and the co-core floor:
[the reference](content-fetch-issuer-reference.md#version-history). Replicator requires
**co-core ≥ 0.8.0**, the release that made `info_source_id` and `BlobAvailableEvent.command_id`
required — a 0.7.x command does not degrade, it fails `from_wire` and dead-letters. Founding
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
| `payload` | the model, JSON-serialized — where everything in the tables below actually lives |
| `event_type` | `content_fetch` / `blob_available` / `fetch_failed` — how `from_wire` picks a model |
| `schema_version` | stringified |
| `occurred_at` | ISO 8601 UTC, **tz-aware** — see below |
| `content_type` | `application/json` |

`key` is derived per payload type, and the three rules differ:

| Payload | Derived `key` |
|---|---|
| `content_fetch` | `command_id` |
| `blob_available` | **`content_fingerprint:command_id`** — per *occurrence*, not per bytes |
| `fetch_failed` | **`command_id:occurred_at`** — deliberately *not* the bare `command_id` |

Neither fact is keyed on a bare identifier: a key naming less than the occurrence collapses
occurrences. `fetch_failed` (cannobserv#270) would drop a multi-failure command's **terminal**
event; `blob_available` (cannobserv#300, the bare `content_fingerprint` through 0.7.7) collapsed two
InfoSources fetching one URL into one fact naming whichever issuer won the race, and left the second
command never closed — which reads as a slow origin, not a bug. **The re-key is a delivery-behaviour
change**: emissions that used to collapse now all deliver. MUST-4 already required idempotence, so
it moves in the safe direction, but the volume differs. Correlation rides on the `command_id`
*field*, never on the key.

An issuer that `XADD`s `command_id` and `url` as top-level fields produces a frame `from_wire`
cannot decode: it raises `BusMessageAnomaly` inside the consumer's `read`, and Replicator routes it
to `content.fetch.dlq` — **silently, by the terms of MUST-6**. The single likeliest way to get this
contract wrong, so it is stated before the field tables rather than after.

`key` is **not** load-bearing on the consume path: Replicator dedupes on `payload.command_id`,
never on the envelope key. What its value *is* for:
[the reference](content-fetch-issuer-reference.md#what-the-envelope-key-is-for).

## The payload

### The command

**Command — `content.fetch`, `ContentFetchCommand`** (`co_core.pure.models.changes`):

| Field | Type | Notes |
|---|---|---|
| `schema_version` | `int` = 1 | Replicator supports **1 only**; anything else dead-letters (see the [failure taxonomy](content-fetch-issuer-reference.md#failure-taxonomy-what-happens-and-what-the-issuer-sees)) |
| `event_type` | `"content_fetch"` | |
| `occurred_at` | `datetime` | **tz-aware UTC, enforced.** Not used for ordering or expiry here |
| `command_id` | `str` | **The idempotency key and the sole correlator.** See MUST-1 |
| `url` | `str` | What to fetch. **Not** a key. See MUST-3 |
| `info_source_id` | `str` | **Required.** The domain object this fetch is for. Echoed onto both facts, read by nothing here (#28) |
| `headers` | `dict[str, str] \| None` | **Honoured since #11.** Merged over the fetcher's defaults, issuer wins. Guards below |
| `timeout_seconds` | `float \| None` | **Honoured since #11.** Seconds; bounded above. `None` = the driver default |

> **`occurred_at` must carry a timezone.** An `AwareDatetime` on every payload since co-core
> v0.7.2 (cannobserv#273): a **naive** value is rejected rather than assumed to be UTC, which would
> corrupt the instant. An aware non-UTC value is normalized. A naive one fails `from_wire` inside
> Replicator's `read`, so it dead-letters as a malformed frame and stays **silent** — the same shape
> as a flattened envelope. `datetime.now(UTC)`, not `datetime.now()`.

### Request options: what Replicator will send, and what it refuses (#11)

`headers` and `timeout_seconds` shape the individual fetch. Both are optional, and `None` on both is
**exactly** the pre-#11 behaviour — the fetcher's own `user-agent` and its 30 s timeout, byte for
byte — so an issuer that sends neither is unaffected by any of this. Two rules bind one that does:

- **Everything on the refusal list is refused, not adjusted.** A refusal is a terminal
  `fetch_failed` · `invalid_request_options` plus the DLQ, arriving **before any request goes
  out** — a refused command never reaches the origin at all.
- **Neither field touches identity.** `command_id` remains the sole dedupe key and correlator: two
  commands differing only in options are two fetch occasions, and a *redelivery* carrying different
  options is still the same command.

The refusal list itself — hop-by-hop and derived headers, the rules on names and values, the
ceilings, the timeout bounds, the header-name folding rule, and the reasoning behind each — is in
[the reference](content-fetch-issuer-reference.md#request-options-what-replicator-will-send-and-what-it-refuses-11).

### The success fact

**Fact — `content.blobs`, `BlobAvailableEvent`**:

| Field | Type | Notes |
|---|---|---|
| `schema_version` | `int` = 1 | |
| `event_type` | `"blob_available"` | |
| `occurred_at` | `datetime` | UTC, stamped at publish |
| `content_fingerprint` | `str` | sha256 of the bytes. Content identity, **not** a correlator |
| `blob_uri` | `str` | `file://<blob_dir>/<ab>/<cd>/<sha256>.bin`. Temporary — MUST-7 |
| `size_bytes` | `int` | |
| `media_type` | `str` | Normalized, `charset` dropped; `application/octet-stream` when absent |
| `url` | `str` | Echoed. Confirmation and debugging only |
| `command_id` | `str` | **Required.** Echoed from the command; half the envelope key |
| `info_source_id` | `str` | **Required.** Echoed verbatim. Replicator neither parses nor interprets it |
| `final_url` | `str \| None` | Where the fetch **landed** after redirects. `None` = *unknown*, see below |
| `status_code` | `int \| None` | **Always 2xx on this fact** — see below |
| `fetched_at` | `datetime \| None` | tz-aware UTC. When the bytes were on the **wire**, not when the fact was published |
| `content_type_raw` | `str \| None` | The **verbatim** `Content-Type`, `charset` and all. `None` = *the origin sent none*, see below |
| `etag` | `str \| None` | Verbatim, `W/` prefix and quotes included. Replay unparsed in `If-None-Match` |
| `last_modified` | `str \| None` | Verbatim, unparsed. Replay in `If-Modified-Since` |
| `blob_expires_at` | `datetime \| None` | **Populated since #28.** When the blob stops being retrievable at `blob_uri`. Prefer it to re-deriving MUST-7's TTL — see below |

The six enriched fields (cannobserv#271, `final_url` sourced by cannobserv#279, produced by #10)
carry what Replicator holds at publish time and a broadcast consumer cannot recover once fetching
lives here rather than in Watcher. Three rules govern reading them — **`None` means nobody said,
never "the default"**; **`status_code` is always 2xx here**, so it is not a success branch;
**"verbatim" excludes surrounding whitespace**, and an over-long value is dropped rather than
truncated — and one warning governs acting on them: **do not attempt conditional GET yet**, because
the *consumer* side is not ready. Replicator's half landed with **#17** — a matching validator earns
`fetch_failed` · `not_modified` · `terminal=True` with no blob and no DLQ entry, which is the
outcome you want — but a consumer with no branch for "no bytes, your last fingerprint stands" will
read it as content that has gone away. All four, with the reasoning, in
[the reference](content-fetch-issuer-reference.md#the-enriched-blob_available-fields).

### The failure fact

**Fact — `content.blobs`, `FetchFailedEvent`** (cannobserv#270):

| Field | Type | Notes |
|---|---|---|
| `schema_version` | `int` = 1 | |
| `event_type` | `"fetch_failed"` | |
| `occurred_at` | `datetime` | tz-aware UTC, stamped at publish. Also half the envelope key |
| `command_id` | `str` | **Required — the correlator, and the whole point of the event** |
| `url` | `str` | Echoed. Confirmation and debugging only, exactly as on the success fact |
| `info_source_id` | `str` | **Required.** Echoed verbatim, exactly as on the success fact |
| `reason` | `str` | Stable token; see below. **Treat an unknown value as opaque** |
| `terminal` | `bool` | `True` = the command is closed, no blob will ever arrive. **Branch on this first** |
| `status_code` | `int \| None` | Set for `http_status` and `not_modified`; absent otherwise |
| `attempts` | `int \| None` | Set only where the attempt count is *why* the command closed |
| `detail` | `str \| None` | Free text for the journal. **Never branch on it**, and never show it to an end user — on `handler_error` it is an unanticipated exception's text, so unbounded |

The tokens emitted today are `http_status`, `not_modified`, `not_fetchable`, `too_large`,
`unsupported_schema_version`, `invalid_request_options` and `handler_error` — one per row of the
[failure taxonomy](content-fetch-issuer-reference.md#failure-taxonomy-what-happens-and-what-the-issuer-sees),
which states each one's condition. `reason` is a plain `str`, not a `Literal`, so the list is
additive and a consumer branching on `terminal` first is already correct for tokens that do not
exist yet. Replicator never emits co-core's `wrong_payload_type`, and everything it emits today is
`terminal=True` — both deliberate, both in
[the reference](content-fetch-issuer-reference.md#reading-the-failure-fact).

**`not_modified` is a success wearing this event's name (#17).** A body-less 304 means the origin
agrees your copy is current: no blob is coming, and none is needed. It rides `fetch_failed` because
the event's real meaning is *this command will not produce a blob*, and `terminal` is the field that
carries it — which is why MUST-4's "treat an unknown `reason` as opaque" was worth insisting on.
Two consequences for an issuer: **do not treat it as content loss**, and **do not alert on
`fetch_failed` rate** — at steady state this token dominates, so the signal is
`fetch_failed where reason != "not_modified"`.

All three models are `extra="ignore"`, so additive producer fields are tolerated. Branch on
`schema_version` **before** destructuring, and never use the strict `*Emit` classes on a consume path.

---

## What the issuer MUST do

### 1. Mint a fresh `command_id` per fetch *occasion*, never per resource

Replicator dedupes on `command_id` — `replicator:cmd:<command_id>`, TTL
`REPLICATOR_DEDUPE_TTL_SECONDS` (default 24 h, operator-tunable) — in
[`src/worker/loop.py::process_message`](../../src/worker/loop.py). A duplicate is **acked and
dropped**: no fetch, no fact, one `INFO` line here and nothing at all on the issuer's side.

So a `command_id` derived from anything resource-stable — a WatchedItem id, a hash of the URL, the
URL itself — means **the second legitimate re-fetch of that URL never happens**. Watcher, whose job
is re-fetching a URL over time to detect change, is precisely the service this breaks.

The trap is worse than a flat failure because it is **TTL-bounded**: re-fetches inside the dedupe
window vanish, re-fetches after it work. A daily cadence sits on the boundary of the default 24 h and
fails intermittently — two fetches a minute apart reproduce it, two a day apart do not.

Mint a ULID per fetch intent — one per call, not one per URL and not one per run.
[`scripts/seed_fetch.py::build_command`](../../scripts/seed_fetch.py) does exactly this and says why.
Uniqueness is required *for correctness* only within the dedupe TTL, but *for correlation* it must
be global and permanent — the issuer's own map is keyed on it.

An **empty** `command_id` is not a `command_id`. Replicator dead-letters it before the fetch
rather than treating `replicator:cmd:` as a dedupe key — under which the second blank-id command
ever published would be a silent no-op, and the first would produce a `blob_available` nothing
can be matched against.

### 2. Persist `command_id → domain` durably, **before** publishing

Persist first, then publish; outbox-style, symmetric to archiver's producer. (Replicator has no
outbox and must not grow one — its durable record of intent is the consumer group's PEL, and the
outbox belongs to producers with a database.)

**Narrowed by cannobserv#300, and not retroactively.** From the step-2 deploy onward every fact
carries `info_source_id`, so a crash between minting an id and recording it costs the link to the
*occasion*, not the domain object. Before that deploy, and for any issuer still on 0.7.x, the
original statement holds: nothing on any stream recovers which InfoSource asked. The record stays
required on narrower grounds — request options, audit, MUST-6's reaper — but it is no longer the
only thing between a fetch and an orphaned blob.

### 3. Correlate on `command_id` only — `url` is not a key

Archiver's model permits **multiple InfoSources per URL** (non-unique `url`, different extraction
strategies), so `url → info_source` is one-to-many. An issuer matching a fact by its `url` will
sooner or later attach bytes to the wrong InfoSource — silently, and in a way that looks like
correct data downstream. `url` on the fact is confirmation and debugging, nothing more.

Since cannobserv#300 there is no *reason* to reach for it either: `info_source_id` is on both
outcomes, so an issuer that lost its `command_id` map can still tell which InfoSource a fact is
about, without the one-to-many guess that makes `url` wrong.

### 4. Make correlation idempotent — one command can yield more than one fact

`blob_available` is at-least-once **per `command_id`**, not exactly-once. The dedupe key is written
*after* the handler returns, deliberately
([`src/worker/loop.py::process_message`](../../src/worker/loop.py)): marking first would turn a
crash between the mark and a completed handle into permanent loss. The cost is the opposite
duplicate — a crash between a successful publish and the `SET` re-runs the handler on reclaim and
emits a **second fact carrying the same `command_id`**.

Applying a fact must therefore be safe to do twice. Same `command_id`, same `content_fingerprint`,
same `blob_uri` — an upsert, not an append.

**This covers `fetch_failed` too, and there the duplicates are not even identical** — a second
failure fact under the same `command_id` with a fresh `occurred_at`, so its envelope key differs and
consumer-side dedup-on-key will not collapse it ([why, and why Replicator cannot engineer it
away](content-fetch-issuer-reference.md#why-a-duplicate-failure-fact-is-not-identical)). Closing a
pending entry must therefore be idempotent in both directions: the same terminal failure applied
twice, and a failure applied to an entry already closed.

### 5. Do **not** dedupe facts on `content_fingerprint`

`content_fingerprint` is the fact's idempotency key *for storage*: identical bytes are the same
blob at the same path, and re-storing them is a no-op. It is **not** one for correlation.

Two commands — two occasions of one URL, or two InfoSources sharing it — that return identical
bytes produce two facts with **the same fingerprint and `blob_uri`, and different `command_id`s**. A
consumer deduping its inbox on fingerprint drops the second and loses that correlation entirely: the
same silent-failure shape as MUST-1, reached from the opposite direction.

Dedupe on `command_id`. Treat the fingerprint as content identity. cannobserv#300 closed the
producer half — the envelope key names the occurrence now — but the consumer rule is unchanged.

And do not dedupe **`fetch_failed`** on `command_id` either, for the opposite reason: more than one
failure fact per command is expected (MUST-4), and once non-terminal facts exist a command may
legitimately emit several. Use `terminal` to decide whether the entry closes, `command_id` to decide
*which* entry.

### 6. Handle `fetch_failed`, and **keep a reaper anyway**

A command that fails permanently publishes a **`fetch_failed` fact on `content.blobs`** (#9,
cannobserv#270) *and* is copied to `content.fetch.dlq` and acked. The fact is the issuer's surface,
the DLQ the operator's; an entry appears in both. So the issuer's primary mechanism is the fact:
consume `content.blobs`, branch on the payload type, and on a `fetch_failed` with `terminal=True`
close the pending entry **with a reason** — off one consumer group, since both outcomes share the
stream.

**Silence has not gone away — it has narrowed.** Four conditions still produce nothing. Three are
permanently silent, having no payload or no `command_id` *safe* to key a fact on; the fourth, a
command still retrying, is silent *for now* (#9 §3) with **no latency bound** — transient failures
retry at the `REPLICATOR_CLAIM_MIN_IDLE_MS` cadence indefinitely, and a tree over
`REPLICATOR_BLOB_MAX_TOTAL_BYTES` parks in the PEL until a sweep frees space. All four, and why
reporting one would be worse than silence, are in
[the reference](content-fetch-issuer-reference.md#the-four-silent-conditions).

Which is why the **reaper stays**, demoted from primary mechanism to backstop. A timeout is grounds
to **re-issue** (fresh `command_id`), not to conclude failure; derive it generously from the reclaim
cadence rather than hardcoding a number, since the cadence is an operator setting on Replicator's
host and an issuer that pins 60 s starts re-issuing under a live retry the day it is tuned. Expect
duplicate work rather than assuming loss.

**The DLQ is still worth reading** — the only place the silent rows appear at all, and it preserves
the offending frame, which no fact does. How to read it, and which outcomes are visible, are in the
reference's [DLQ](content-fetch-issuer-reference.md#reading-the-dlq) and
[failure taxonomy](content-fetch-issuer-reference.md#failure-taxonomy-what-happens-and-what-the-issuer-sees).

### 7. Copy the bytes before the blob expires

`blob_uri` is temporary. A blob is reaped once its mtime is older than
`REPLICATOR_BLOB_TTL_SECONDS` — currently 7 days, a published commitment to archiver (archiver#118)
rather than a knob Replicator turns freely, but still a *setting* on its host. Treat it as a floor
to ask about, not a constant to schedule against: consume the bytes promptly and re-issue if the URI
fails to open, rather than building a pipeline whose timing assumes seven days.

The clock runs from **last reference by a fetch**, not last read by a consumer — holding a
`blob_uri` for a week without a re-fetch loses the bytes. So **record `blob_expires_at` rather than
re-deriving a horizon** (cannobserv#301, carried since #28): deriving one hard-codes a policy
Replicator owns and starts the clock where no consumer can see it. The published value can only fall
**earlier** than the real reap, so acting on it is early, never too late; `None` means unknown, and
is recorded as absence rather than guessed.
[Mechanism](content-fetch-issuer-reference.md#how-the-blob-ttl-clock-runs).

Also: `blob_uri` is a **`file://` URI on Replicator's host** — the contract is VM-local today, a
consumer elsewhere cannot open it, and nothing on the wire says so.

---

## What Replicator guarantees

- **Store, then publish.** A fact never announces bytes that are not on disk. The reverse gap
  (bytes stored, publish failed) self-repairs: the message stays unacked, the reclaim re-runs a
  handler that content-addressed storage makes a no-op.
- **Announce, then ack.** Every path that closes a command without a blob publishes its
  `fetch_failed` *before* the dead-letter that acks it, so a crash in between costs a duplicate
  fact (MUST-4) rather than losing the fact outright.
- **`command_id` and `info_source_id` are echoed** on every fact, success or failure. Both are
  required on both outcomes, and `info_source_id` is copied verbatim.
- **The fingerprint is definitional.** Replicator is the cluster's sole fetcher and fingerprinter,
  so `content_fingerprint` *is* the content identity — there is nothing to cross-check it against.
- **Identical bytes are one blob.** Content-addressed storage, so a re-fetch of unchanged content
  costs an origin request and nothing else.
- **At-least-once delivery**, both directions.

## What Replicator does **not** guarantee

- **No *non-terminal* failure fact** — a command that is retrying announces nothing until it
  either succeeds or is closed. See MUST-6 and
  [the failure-fact notes](content-fetch-issuer-reference.md#reading-the-failure-fact).
- **No failure fact where there is nothing safe to key one on** — a frame that is not a
  `content.fetch` command (any `command_id` in it is another command's) or one whose `command_id` is
  blank. Both are dead-lettered rather than run. MUST-1, MUST-6.
- **No latency bound**, and no SLA on turnaround.
- **No promise that a burst runs at the rate it was issued (#12).** Requests to one host are
  spaced by at least `REPLICATOR_MIN_HOST_INTERVAL_SECONDS` (1 s by default), so 100 commands for
  one host means at least 100 s of fetching. Size a reaper's timeout (MUST-6) against the depth of
  your own burst, not against one fetch; different hosts are unaffected by each other. Whether a
  paced command is slept through or parked for the next reclaim depends on the deployed interval,
  and it changes what a reaper should expect —
  [the reference](content-fetch-issuer-reference.md#pacing-at-the-deployed-defaults).
- **No ordering.** Two commands issued in sequence may produce facts in either order.
- **No cross-command dedupe.** Two `command_id`s for one URL are two fetches and two facts, by
  design — that is what makes MUST-1 work.
- **No retention guarantee on `content.blobs`.** Replicator never trims it — `BusPublish` takes no
  `MAXLEN` and nothing here issues `XTRIM` — so whatever policy applies is the broker operator's,
  not part of this contract. It is not an archive to reconcile against later.

---

## Where the rest of the contract lives

- [`content-fetch-issuer-reference.md`](content-fetch-issuer-reference.md) — **equally normative**,
  and everything this file points at: the refusal list, the enriched fields, the failure taxonomy,
  the silent conditions, the DLQ, provenance and trust, and the version history.
- [`replicator-boundaries.md`](replicator-boundaries.md) — which payload fields this contract will
  never grow (#12).
- [`2026-07-31-fetch-failed-fact-settled.md`](../plans/2026-07-31-fetch-failed-fact-settled.md) —
  historical: how the silence-as-failure question was closed.
