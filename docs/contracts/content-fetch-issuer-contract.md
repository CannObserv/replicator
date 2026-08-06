# The `content.fetch` issuer contract

**Status:** normative. **Home:** this file, in the Replicator repo — Replicator is the sole
consumer of `content.fetch` and the sole producer of `content.blobs`, so the contract lives with
the behaviour it describes. Issuer-side repos link here rather than copying; a copy would drift
from the code the day it was written.

**Audience:** any service that publishes a `ContentFetchCommand`. Today that is
[`scripts/seed_fetch.py`](../../scripts/seed_fetch.py). From Phase 4 it is Watcher.

**Companion.** [`content-fetch-issuer-reference.md`](content-fetch-issuer-reference.md) carries the
parts an issuer *looks up* rather than reads through — the request-options refusal list, the
reasoning behind the enriched `blob_available` fields, the failure taxonomy, and the trust posture.
Equally normative; split out in #24 so this file stays readable start to finish. Index at the end.

**Sibling document.** This settles the *wire*. [`replicator-boundaries.md`](replicator-boundaries.md)
settles the *service* — what Replicator is allowed to become, and therefore which proposed fields
this contract will never grow. Read that one before proposing a payload addition (#12).

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

Version history and the co-core floor:
[the reference](content-fetch-issuer-reference.md#version-history). Replicator requires
**co-core ≥ 0.7.5**. Founding rationale:
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

`key` is **not** load-bearing on the consume path: Replicator dedupes on `payload.command_id`,
never on the envelope key. Its value is operational — see
[the reference](content-fetch-issuer-reference.md#what-the-envelope-key-is-for).

## The payload (inside `payload`)

### The command

**Command — `content.fetch`, `ContentFetchCommand`** (`co_core.pure.models.changes`):

| Field | Type | Notes |
|---|---|---|
| `schema_version` | `int` = 1 | Replicator supports **1 only**; anything else dead-letters (see the [failure taxonomy](content-fetch-issuer-reference.md#failure-taxonomy-what-happens-and-what-the-issuer-sees)) |
| `event_type` | `"content_fetch"` | |
| `occurred_at` | `datetime` | **tz-aware UTC, enforced.** Not used for ordering or expiry by Replicator |
| `command_id` | `str` | **The idempotency key and the sole correlator.** See MUST-1 |
| `url` | `str` | What to fetch. **Not** a key. See MUST-3 |
| `headers` | `dict[str, str] \| None` | co-core ≥ 0.7.3, **honoured since #11**. Merged over the fetcher's defaults, issuer wins. Guards below |
| `timeout_seconds` | `float \| None` | co-core ≥ 0.7.3, **honoured since #11**. Seconds; bounded above. `None` = the driver default |

> **`occurred_at` must carry a timezone.** Since co-core v0.7.2 (cannobserv#273) it is an
> `AwareDatetime` on every payload: a **naive** value is rejected fail-loud rather than assumed
> to be UTC, because "assume UTC" corrupts the instant when a producer stamps a naive local time.
> An aware non-UTC value is normalized. A naive one fails `from_wire` inside Replicator's `read`,
> which means it dead-letters as a malformed frame and is one of the rows that stay **silent** —
> the same shape as an issuer that flattened the envelope. `datetime.now(UTC)`, not
> `datetime.now()`.

### Request options: what Replicator will send, and what it refuses (#11)

`headers` and `timeout_seconds` shape the individual fetch. Both are optional; `None` on both means
**exactly** the pre-#11 behaviour — the fetcher's own `user-agent` and its 30 s timeout, byte for
byte. An issuer that sends neither is unaffected by any of this.

Two rules bind an issuer that does send them:

- **Everything on the refusal list is refused, not adjusted.** A refusal is a terminal
  `fetch_failed` · `invalid_request_options` plus the DLQ, arriving **before any request goes
  out** — a refused command never reaches the origin at all.
- **Neither field touches identity.** `command_id` remains the sole dedupe key and the sole
  correlator. Two commands differing only in options are two fetch occasions (MUST-1 unchanged);
  a *redelivery* carrying different options is still the same command and is still deduped.

The refusal list — hop-by-hop and derived headers, the token and byte-range rules on names and
values, the count and size ceilings, the timeout bounds — plus the header-name folding rule and the
reasoning behind each, is in
[the reference](content-fetch-issuer-reference.md#request-options-what-replicator-will-send-and-what-it-refuses-11).

### The success fact

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
| `final_url` | `str \| None` | co-core ≥ 0.7.3 (model), **≥ 0.7.5 to be populated**. Where the fetch **landed** after redirects. `None` = *unknown*, see below |
| `status_code` | `int \| None` | co-core ≥ 0.7.3. **Always 2xx on this fact** — see below |
| `fetched_at` | `datetime \| None` | co-core ≥ 0.7.3. tz-aware UTC. When the bytes were on the **wire**, not when the fact was published |
| `content_type_raw` | `str \| None` | co-core ≥ 0.7.3. The **verbatim** `Content-Type`, `charset` and all. `None` = *the origin sent none*, see below |
| `etag` | `str \| None` | co-core ≥ 0.7.3. Verbatim, `W/` prefix and quotes included. Replay unparsed in `If-None-Match` |
| `last_modified` | `str \| None` | co-core ≥ 0.7.3. Verbatim, unparsed. Replay in `If-Modified-Since` |

The six enriched fields (cannobserv#271, `final_url` sourced by cannobserv#279, produced by #10)
carry what Replicator holds at publish time and a broadcast consumer cannot recover once fetching
lives here rather than in Watcher. Three rules govern reading them, and one warning governs acting
on them — all four in
[`content-fetch-issuer-reference.md`](content-fetch-issuer-reference.md#the-enriched-blob_available-fields):

- **`None` means nobody said, never "the default"**, on `final_url` and `content_type_raw` alike.
- **`status_code` is always 2xx here** — it distinguishes 200 from 203, it is not a success branch.
- **"Verbatim" excludes surrounding whitespace**, and an over-long value is dropped, not truncated.
- **Do not attempt conditional GET yet.** Replicator honours the `headers` you send, so a validator
  *will* reach the origin — but a matching one earns a body-less 304, which Replicator still closes
  as a terminal `fetch_failed` · `http_status`. Tracked as **#17**. Until it lands, keep `etag` and
  `last_modified` in your own records and send the request unconditionally.

### The failure fact

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

`reason` tokens, one per row of the
[failure taxonomy](content-fetch-issuer-reference.md#failure-taxonomy-what-happens-and-what-the-issuer-sees):

| Token | Condition |
|---|---|
| `http_status` | 4xx, or a body-less 304 (`status_code` set) |
| `not_fetchable` | bad scheme / invalid URL |
| `too_large` | body over `REPLICATOR_MAX_BLOB_BYTES` |
| `unsupported_schema_version` | the command decoded at a `schema_version` Replicator does not support |
| `invalid_request_options` | `headers` or `timeout_seconds` are not sendable — see the [refusal list](content-fetch-issuer-reference.md#request-options-what-replicator-will-send-and-what-it-refuses-11) |
| `handler_error` | unclassified, and it exhausted the delivery ceiling (`attempts` set) |

`reason` is a plain `str`, not a `Literal`, so the token list is additive and a consumer that
branches on `terminal` first is already correct for tokens that do not exist yet. Replicator never
emits co-core's `wrong_payload_type`, and everything it emits today is `terminal=True` — both
deliberate, both explained in
[the reference](content-fetch-issuer-reference.md#reading-the-failure-fact).

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

An **empty** `command_id` is not a `command_id`. Replicator dead-letters it before the fetch
rather than treating `replicator:cmd:` as a dedupe key — under which the second blank-id command
ever published would be a silent no-op, and the first would produce a `blob_available` nothing
can be matched against.

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

Since the fact carries per-occasion fetch metadata (cannobserv#271), fingerprint-dedupe now also
loses **which fetch** a fact describes. Two facts sharing a fingerprint may carry different
`final_url`, `etag`, `last_modified`, and `fetched_at`; keeping only the first pins an issuer's
stored validators to whichever occasion arrived first and holds them there for as long as the
content is unchanged — precisely the period a conditional GET would have been useful.

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

**Silence has not gone away — it has narrowed.** Four conditions still produce nothing: a frame
that fails `from_wire` entirely, a frame that decodes to a payload that is not a `content.fetch`
command, a command whose `command_id` is blank, and a command that is still retrying. The first
three are permanently silent — no payload, or no `command_id` that is *safe* to key a fact on. The
fourth is silent *for now* (#9 §3), and has no latency bound: transient failures retry indefinitely
at the `REPLICATOR_CLAIM_MIN_IDLE_MS` cadence, and a blob tree over
`REPLICATOR_BLOB_MAX_TOTAL_BYTES` parks in the PEL until a sweep frees space. Each condition, and
why reporting the second would be worse than silence, is in
[the reference](content-fetch-issuer-reference.md#the-four-silent-conditions).

Which is why the **reaper stays**, demoted from primary mechanism to backstop:

- A timeout is still grounds to **re-issue** (fresh `command_id`), not to conclude failure.
- Derive the timeout generously from the reclaim cadence rather than hardcoding a number — the
  cadence is an operator setting on Replicator's host, and an issuer that pins 60 s starts
  re-issuing under a live retry the day it is tuned. Expect duplicate work rather than assuming
  loss.
- A `fetch_failed` publish that itself fails is swallowed on Replicator's side (the dead-letter
  still happens, and raising there would only burn the delivery ceiling and reach the same DLQ
  minutes later). Rare, logged, and another reason the reaper is not optional.

See the [failure taxonomy](content-fetch-issuer-reference.md#failure-taxonomy-what-happens-and-what-the-issuer-sees) for exactly
which outcomes are visible and which are not.

**The DLQ is still readable, and still worth reading** — it is the only place the silent rows show
up at all, and it preserves the offending frame, which no fact does. How to read it, and the one
anomaly class that carries no `key` to match on, are in
[the reference](content-fetch-issuer-reference.md#reading-the-dlq).

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
- **No failure fact for a frame that is not a `content.fetch` command** — any `command_id` it
  carries is another command's, so there is nothing *safe* to correlate one on. MUST-6.
- **No failure fact for a command whose `command_id` is blank** — nothing to correlate one on at
  all. It is dead-lettered before the fetch rather than run. MUST-1, MUST-6.
- **No latency bound**, and no SLA on turnaround.
- **No promise that a burst runs at the rate it was issued (#12).** Requests to one host are
  spaced by at least `REPLICATOR_MIN_HOST_INTERVAL_SECONDS` — 1 s by default, the interim
  stand-in for the politeness numbers until they travel over the bus. Publishing 100 commands
  for one host means at least 100 s of fetching. Size a reaper's timeout (MUST-6) against the
  depth of your own burst, not against one fetch. Commands for different hosts are unaffected
  by each other.
  Whether a paced command is slept through or parked for the next reclaim depends on the deployed
  interval — see [the reference](content-fetch-issuer-reference.md#pacing-at-the-deployed-defaults),
  since it changes what a reaper should expect.
- **No cross-command dedupe.** Two `command_id`s for one URL are two fetches and two facts, by
  design — that is what makes MUST-1 work.
- **No retention guarantee on `content.blobs`.** Replicator never trims it — `BusPublish` takes no
  `MAXLEN` and nothing in this repo issues `XTRIM` — so whatever policy applies is the broker
  operator's, not part of this contract. Do not treat the stream as an archive to reconcile
  against later.

---

## Where the rest of the contract lives

Split out in #24 so this document stays readable start to finish. Equally normative:

- [`content-fetch-issuer-reference.md`](content-fetch-issuer-reference.md) — the refusal list, the
  enriched `blob_available` fields, the **failure taxonomy**, the four silent conditions, reading
  the DLQ, and the **provenance and trust** posture.
- [`replicator-boundaries.md`](replicator-boundaries.md) — what Replicator may become, and therefore
  which payload fields this contract will never grow (#12).
- [`2026-07-31-fetch-failed-fact-settled.md`](../plans/2026-07-31-fetch-failed-fact-settled.md) —
  historical: how the silence-as-failure question was closed. MUST-6 above is the current contract.
