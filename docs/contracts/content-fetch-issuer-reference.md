# The `content.fetch` issuer reference

**Status:** normative, and a companion to
[`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md). Everything here binds
Replicator's behaviour exactly as the contract does — this file is not commentary.

**What is here rather than there.** The contract carries what an issuer must *do*: the frame,
the payload shapes, the eight MUSTs, and the guarantee/non-guarantee pair. This file carries
what an issuer *looks up* — the header and timeout rules it consults when a command is refused,
the reasoning behind the enriched `blob_available` fields, the condition-by-condition failure
taxonomy, the trust posture the whole capability rests on, and the mechanisms behind several of
the MUSTs, each of which states its rule there and explains itself here. The split exists so the
contract stays short enough to read start to finish; both files are normative, and a rule does not
become advisory by living here.

**Changing this document.** The same rule the contract states applies, and it covers the *guidance*
here as well as the rules: a change to the failure taxonomy, to the refusal rules, or to what an
issuer is told to do about either is announced on the issuer repos' trackers in the same change that
edits this file — see [the contract](content-fetch-issuer-contract.md) for how to pick the issue.
Guidance is the half an issuer actually implements, so "the rules did not change" is not a reason to
skip the announcement.

---

## Request options: what Replicator will send, and what it refuses (#11)

Set on the command, in [`ContentFetchCommand`](content-fetch-issuer-contract.md#the-command).

`headers` and `timeout_seconds` shape the individual fetch. Both are optional and both default to
`None`, which means **exactly** the pre-#11 behaviour: the fetcher's own `user-agent` and its 30 s
timeout, byte for byte. An issuer that sends neither is unaffected by any of the following.

**Header names are lower-cased before the merge, and the issuer wins.** The fetch driver merges
`{"user-agent": <default>, **your headers}` as a plain, case-*sensitive* dict. Without the fold a
capitalized `User-Agent` leaves both keys in the mapping and httpx sends **two** `User-Agent` field
lines — the default first, yours second — leaving the origin to decide which applies. Folding first
is what makes "issuer wins" a rule rather than a coincidence. Send `User-Agent` or `user-agent`;
either way exactly one line goes on the wire and it carries your value.

**Surrounding whitespace is dropped from a value** and nothing else is: RFC 9110 excludes OWS from
a field value in the first place, so `"  text/html  "` is sent as `text/html`. Nothing *inside* a
value is touched.

**Everything below is refused, not adjusted.** A refusal is a terminal
`fetch_failed` · `invalid_request_options` plus the DLQ, arriving before any request goes out — so
a refused command never reaches the origin at all. The reject-rather-than-fix posture is the same
one the `blob_available` passthroughs take: a header Replicator silently dropped, or a timeout it
silently shortened, is a change to your fetch that you cannot see and cannot account for in your
own fingerprints.

| Refused | Why |
|---|---|
| `connection`, `keep-alive`, `proxy-connection`, `te`, `trailer`, `transfer-encoding`, `upgrade` | Hop-by-hop (RFC 9110 §7.6.1) — they describe one connection, which httpx and h11 own |
| `host`, `content-length` | Not hop-by-hop: httpx derives both. Overriding `host` addresses one origin while contacting another |
| Any `proxy-*` header | Configures the hop rather than the request |
| A name that is not an RFC 9110 token | `user agent`, `user:agent`, an empty name, anything non-ASCII — and a name with surrounding whitespace, which RFC 9110 forbids before the colon and which is therefore malformed rather than trimmable |
| A value with any byte outside `\x20`–`\x7e` | Printable US-ASCII and SP only. Excludes CR, LF, NUL, every other control character, HTAB, and all of `obs-text` (`\x80`–`\xff`) — narrower than RFC 9110 permits, deliberately. A CRLF here is request splitting |
| Two names differing only in case | Folding them would silently discard one. Refused even when the values agree — the rule is about the shape, not the values |
| More than **32** headers, or more than **8192 bytes** of them | 8 KiB is the common origin-side limit (nginx, Apache), so past it the far end answers an opaque 400. The constants in [`src/worker/handler.py`](../../src/worker/handler.py) are authoritative |
| `timeout_seconds` that is zero, negative, NaN, or infinite | Not a duration |
| `timeout_seconds` over `REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS` (default **120**) | Replicator's consume path is serial, so your timeout is a lien on every *other* issuer's commands too. Ask an operator if 120 s is genuinely too short for a target |

**Neither field touches identity.** They ride inside `payload`, not the envelope: `command_id`
remains the sole dedupe key and the sole correlator. Two commands differing only in options are two
fetch occasions (MUST-1 unchanged); a *redelivery* carrying different options is still the same
command and is still deduped.

### `invalid_request_options` and stored values

**This is the one refusal an issuer can wedge itself on.** Every other terminal outcome describes
something about the *fetch*, and re-issuing eventually behaves differently. This one is terminal
**and** pre-request: it is decided entirely from the command, so an issuer that re-derives the same
command from stored state is refused identically every cycle, forever, without the origin ever
being contacted. The URL is not slow or broken — it is simply never fetched again, and nothing in
the fact stream distinguishes that from a resource nobody is asking about.

The realistic source is a **stored validator** — an `etag` kept from an earlier fact and replayed in
`If-None-Match` (see
[MUST-8](content-fetch-issuer-contract.md#8-do-not-send-a-validator-until-you-handle-not_modified)).
So take both halves, because they defend different things:

- **Screen the value before it goes on `headers`.** Apply the refusal rules above to a stored
  validator at mint time and send nothing that would be refused — the command then goes out
  unconditional, which costs one full fetch, instead of going out refused, which costs the fetch
  *and* the item's health signal. This is prevention, and it is the cheaper half.
- **Clear the stored value on the refusal, automatically.** On `invalid_request_options`, and on
  that reason alone, discard the stored request options the command was built from. This is the
  backstop for values that were stored under an older screen — including any stored before the
  screen existed — and it is what makes the wedge self-healing rather than operator-driven.

Watcher runs both: `sendable_validator()` refuses to mint an unsendable value, and of the
`fetch_failed` reasons, `clear_validators()` fires on this one alone. An unsendable value that only
a human can clear is an item that stops being fetched until a human notices, which is the failure
mode this refusal is least likely to advertise.

**Replicator no longer supplies this input from its own side (#60).** `etag` and `last_modified`
are screened against the refusal rules *before* they are published, so a validator replayed
verbatim off a `blob_available` passes the **value** rule by construction. The header count and
the byte total remain yours: they are computed across your whole command, and no screening
Replicator does on one value can speak for them. Both halves above still stand: a
consumer can be holding a value published before that fix, and the refusal set is versioned in
Replicator's code rather than on the wire, so a rule that narrows later would refuse a validator
minted under the old one.

**One more condition clears a stored pair, and it is not a refusal.** Watcher also forgets the
validators when bytes *arrive* and fail extraction. The reasoning generalizes to any consumer that
inherits a fingerprint across 304s: a matching validator produces no bytes, so nothing is extracted
and no fingerprint is recomputed — which means a broken extraction would be re-confirmed as a
*successful* check for as long as the origin keeps answering 304. Forgetting the pair forces the
next command to fetch in full and re-assert the failure.

---

## The enriched `blob_available` fields

Field types and defaults are in
[the success fact](content-fetch-issuer-contract.md#the-success-fact). This is why they are
shaped the way they are, and what an issuer owes its own consumer before acting on two of them.

The six enriched fields (cannobserv#271, `final_url` sourced by cannobserv#279, produced by #10)
carry what Replicator holds at publish time and a broadcast consumer cannot recover once fetching
lives here rather than in Watcher. `blob_expires_at` (cannobserv#301, populated by #28) is a
seventh, and describes the **store** rather than the fetch — see
[MUST-7](content-fetch-issuer-contract.md#7-copy-the-bytes-before-the-blob-expires). Three details
are the whole value of the six:

- **`None` means nobody said, never "the default".** `final_url` is `None` when the *driver* did
  not report a landing URL — **not** "no redirect occurred", and Replicator never substitutes the
  requested `url` to fill the gap. `content_type_raw` is `None` when the *origin* sent no
  `Content-Type` — deliberately **not** `application/octet-stream`, which is a value some
  consumers read as "unknown, guess from the URL" and which `media_type` (normalized, required)
  substitutes on its own channel. An issuer that collapses these to a default destroys the
  distinction it is being handed.
- **`status_code` is always 2xx here.** Every other status closes the command as a `fetch_failed`
  instead, so this field distinguishes 200 from 203 or 206 — it is not a success/failure branch,
  and a branch written as `if status_code == 200` will silently drop a 203.
- **"Verbatim" excludes surrounding whitespace, and a value Replicator cannot stand behind is
  dropped rather than repaired.** On the three header passthroughs — `content_type_raw`, `etag`,
  `last_modified` — nothing *inside* the value is touched: no case folding, no quote stripping, no
  date parsing. What is stripped is the whitespace around it, which RFC 9110 excludes from a field
  value in the first place. Four cases report `None`: an absent header, a blank or
  whitespace-only one ("present but empty" is not a distinction an issuer can act on), a value
  longer than Replicator's `MAX_HEADER_VALUE_LENGTH`
  ([`src/worker/handler.py`](../../src/worker/handler.py); currently 1024 characters, and the
  constant is authoritative), and — on `etag` and `last_modified` **only** — a value Replicator
  could not send back as a request header (#60). The last two are dropped rather than adjusted,
  and for the same shape of reason: these are origin-controlled strings on a broadcast stream
  nothing trims, and a *truncated* ETag replayed in an `If-None-Match` is a validator that can
  never match, while an *unsendable* one is one the request never carries, because Replicator
  refuses the command before contacting the origin — both worse than none. The unsendable set is the request-options set above: anything outside printable
  US-ASCII, interior HTAB and obs-text included.

  The asymmetry on the fourth case is deliberate. `content_type_raw` is published whatever it
  contains, because it is never replayed as a request header — no refusal can be built out of it
  — and it is recorded as an *observed fact* about the origin, which filtering would suppress for
  no gain.

> **These are per-*occasion* values, and since cannobserv#300 the fact is keyed per occasion too.**
> They describe the fetch that produced this fact, not the bytes. Through 0.7.7 the envelope key was
> the bare `content_fingerprint`, so two commands returning identical bytes collapsed to one fact and
> its `final_url` / `etag` / `last_modified` / `fetched_at` were the *first* emission's — a consumer
> replayed a stale `If-None-Match` for as long as that content stayed unchanged. **That caveat is
> dissolved**: every fetch now emits its own fact carrying its own validators. Recorded because a
> consumer written against 0.7.x may still carry a workaround for it. MUST-5 is unaffected — deduping
> an inbox on the fingerprint is still wrong, and still loses a correlation.

> **The seam is complete on both sides — which moves the question to your side.** `etag` and
> `last_modified` are the *read* half; since #11 the write half exists, so an `If-None-Match` you
> send **will** reach the origin; and since **#17** the outcome exists too: a matching validator
> earns `fetch_failed` · `not_modified` · `terminal=True` · `status_code=304`, with no blob, no
> dead-letter entry, and the command's dedupe key written. The reference consumer has handled that
> token since [CannObserv/watcher#249](https://github.com/CannObserv/watcher/issues/249) and
> implements store-and-replay as of
> [CannObserv/watcher#269](https://github.com/CannObserv/watcher/issues/269).
>
> None of that discharges the obligation for *your* consumer, which is why it is stated as
> [MUST-8](content-fetch-issuer-contract.md#8-do-not-send-a-validator-until-you-handle-not_modified)
> rather than as a note here. `not_modified` means *no bytes are coming and your last fingerprint
> still stands*; it arrives on the **failure** event, so a consumer without an explicit branch
> falls through to whatever its `fetch_failed` handling already does — very likely "the content is
> gone", "the fetch failed", or a health regression. That converts every successful no-change check
> into a spurious loss, and the better the origin's caching, the more of them there are.
>
> So the two fields are safe to *record* unconditionally and safe to *replay* only once that branch
> exists. Replay them verbatim when you do — parsing and re-serializing hands the origin a value it
> never sent, which cannot match.

---

## Failure taxonomy: what happens, and what the issuer sees

| Condition | Replicator's action | Issuer-visible |
|---|---|---|
| Duplicate `command_id` inside the dedupe window (default 24 h) | ack, no fetch | **nothing** — silent drop |
| `schema_version` ≠ 1 | fact, then `content.fetch.dlq` | `fetch_failed` · `unsupported_schema_version` |
| Frame decodes to a non-`content_fetch` payload | `content.fetch.dlq` | **nothing** — any `command_id` in it is another command's |
| Command with a blank `command_id` | `content.fetch.dlq`, before the fetch | **nothing** — no correlator to key a fact on |
| Malformed frame (fails `from_wire`; includes a naive `occurred_at`, and any 0.7.x command with no `info_source_id`) | `content.fetch.dlq`, synthesized record | **nothing** — no payload at all |
| HTTP 4xx — **412 included**, since a failed precondition on a GET is the issuer's error | fact, then `content.fetch.dlq` | `fetch_failed` · `http_status` (+ `status_code`) |
| HTTP 304 Not Modified — a conditional GET that **succeeded** (#17) | fact, ack, and **no DLQ entry**; the dedupe key is written | `fetch_failed` · `not_modified` (+ `status_code=304`) |
| URL not fetchable (bad scheme / invalid URL) | fact, then `content.fetch.dlq` | `fetch_failed` · `not_fetchable` |
| Body over `REPLICATOR_MAX_BLOB_BYTES` (default 64 MiB) | fact, then `content.fetch.dlq` | `fetch_failed` · `too_large` |
| Unsendable `headers` / `timeout_seconds` | fact, then `content.fetch.dlq`, **before the fetch** | `fetch_failed` · `invalid_request_options` |
| HTTP 5xx / 408 / 429, or a network error | retry indefinitely, default ~60 s cadence | delayed fact, or **nothing while it retries** |
| Blob tree over `REPLICATOR_BLOB_MAX_TOTAL_BYTES` | parked in the PEL until a sweep frees space | delayed fact, or **nothing while it waits** |
| Unclassified handler error | retried to the delivery ceiling (~4 reclaims / ~4 min at default settings), then fact + DLQ | `fetch_failed` · `handler_error` (+ `attempts`) |
| Success | `blob_available` on `content.blobs` | the fact |

Every `fetch_failed` row carries `terminal=True` — the command is closed and no blob will arrive.

**One row behaves unlike every other, and it is #17's.** The 304 row is the only one where
`terminal=True` is *good news*: no blob is coming because none is needed, and treating it as content
loss is the single most likely way to misread this stream. It is also the only closed command that
leaves **no `content.fetch.dlq` entry** — a successful no-change check is not operator-actionable,
and wherever conditional GET is in use it is the common outcome, so copying each one there would
bury the entries that matter. The cost, stated once: **`fetch_failed` volume is no longer a
failure signal.** Alert on `fetch_failed where reason != "not_modified"`.

The "nothing" rows are not one problem, and the reaper is not the answer to all of them:

- **Three rows have no usable `command_id`** — a non-`content_fetch` payload, a blank
  `command_id`, and a frame that failed to decode. Permanently silent, and the reaper is the
  only recourse. This is what MUST-6 keeps it for.
- **Two rows are silent only while the command is in flight** — a retrying transient failure and
  a blob tree over its ceiling. Silent **for now** (#9 §3); an issuer that would use an
  in-flight signal should say so on its tracker rather than lengthening its timeout to
  compensate.
- **The duplicate-`command_id` row is neither.** It is silent because the command was *already
  handled* — the first delivery ran and published its fact, so the issuer's entry is already
  closed. Reaping and re-issuing there sends the origin a second request for work that
  succeeded. The fix is MUST-1: mint a fresh id per fetch occasion and the row cannot occur.

---

## Reading the failure fact

Field types are in
[the failure fact](content-fetch-issuer-contract.md#the-failure-fact). Three behaviours are not
visible from the table.

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

---

## The four silent conditions

Named in [MUST-6](content-fetch-issuer-contract.md#6-handle-fetch_failed-and-keep-a-reaper-anyway);
this is each one and why it is silent.

**Silence has not gone away — it has narrowed.** Three conditions still produce nothing, and one
produces nothing *yet*:

- **A frame that fails `from_wire` entirely.** It has no payload, therefore no `command_id`,
  therefore nothing a fact could correlate on. DLQ-only. The commonest cause is an issuer that
  `XADD`ed flat fields instead of publishing through `to_wire` — see
  [The frame](content-fetch-issuer-contract.md#the-frame-envelope) — and, since
  co-core 0.7.2, a naive `occurred_at`.
- **A frame that decodes to a payload that is not a `content.fetch` command.** Not merely
  unreportable — *unsafe* to report. Most of the payload union has no `command_id` at all, and the
  members that do carry one that belongs to a different command: a misrouted `blob_available`
  names a command that **succeeded**. Announcing a terminal failure against it would contradict a
  fact the issuer has already applied, and MUST-4 covers duplicates, not contradictions. Silent by
  design.
- **A command whose `command_id` is blank.** Refused before the fetch and dead-lettered, with no
  fact — there is no correlator to key one on. Silent to the issuer, but *not* silently
  processed: an empty id is not a valid `command_id` (MUST-1), and accepting it would take the
  dedupe key `replicator:cmd:` under which every later blank-id command becomes a no-op.
- **A command still retrying.** Replicator emits no non-terminal fact today (#9 §3), so a 5xx,
  a 429, a network error, or a blob tree over its ceiling is invisible for as long as it retries —
  and there is no latency bound on that: transient failures retry indefinitely at the
  `REPLICATOR_CLAIM_MIN_IDLE_MS` cadence (default 60 s), and over
  `REPLICATOR_BLOB_MAX_TOTAL_BYTES` the command parks in the PEL until a retention sweep frees
  space. A command can legitimately be in flight for a long time.

---

## Reading the DLQ

The operator's surface, and the complement of *most* facts rather than a substitute for any.

**It is no longer the complement of every terminal fact (#17).** A body-less 304 closes its command
with a fact and an ack and writes **nothing** here — the first outcome to do so. So the DLQ is the
complement of every **failed** close, not of every terminal one, and a fact with no matching entry
is not evidence of a lost dead-letter.

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

---

## Provenance and trust

`content.fetch` is an **unauthenticated capability**: any writer to the bus can make Replicator
issue an arbitrary outbound HTTP request from the cluster VM and store the response. There is no
signing, no allowlist, and no issuer identity on the frame.

Integrity rests entirely on **bus access control** — the broker is Archiver-operated and bound to
localhost on a single trusted VM. That is proportionate today. It stops being proportionate the
moment the bus spans hosts or tenants, at which point message signing or a URL allowlist becomes
the conversation. Not before.

**`headers` widens that capability, and the widening is bounded here rather than by the broker.**
A bus writer can now attach an arbitrary header — an `Authorization` among them — to a host of its
own choosing. The trust model is unchanged (same localhost broker, same single VM), so the guards
in the refusal table above are not a substitute for it; they are the cheap part, taken because it
is cheap. Concretely they stop three things the broker's boundary says nothing about: a `Host`
override that contacts one origin while addressing another, a CRLF in a value that splits the
request into two, and a `timeout_seconds` large enough to park the serial consume path — that last
one a denial of service against every *other* issuer, not against the origin.

Two properties an issuer can rely on: a refused command is refused **before** any request goes out,
and header **values never reach the journal** — only names are logged, so an `Authorization` an
issuer attaches is not re-exposed one layer down.

Relatedly: nothing but the seed script writes to `content.fetch` today, and `seed_fetch.py`
requires `--production` for the one target the live worker consumes. A frame on that stream is
fetched for real.

**`content.replicate` does not inherit this section.** A write is bounded by nothing a read is, so
the argument — and an earlier escalation trigger — is made again in
[`content-replicate-issuer-contract.md`](content-replicate-issuer-contract.md) (#34).

---

## Version history

Contracts settled in cannobserv#266 (co-core v0.7.0); the failure fact added in cannobserv#270 and
the tz-aware `occurred_at` in cannobserv#273, both shipped in **co-core v0.7.2**; the enriched
`blob_available` metadata in cannobserv#271/#279 and the command's request options in
cannobserv#272, shipped in **v0.7.3** and **v0.7.5**. **v0.8.0** required `info_source_id` on all
three payloads and `command_id` on `blob_available`, re-keyed `blob_available` to
`content_fingerprint:command_id` (cannobserv#300), and added `blob_expires_at` (cannobserv#301).
Replicator requires **co-core ≥ 0.8.0**.
Founding rationale:
[`docs/plans/2026-06-25-replicator-mvp-design.md`](../plans/2026-06-25-replicator-mvp-design.md).

---

## What the envelope key is for

`key` is **not** load-bearing on the consume path: Replicator decodes `payload` and dedupes on
`payload.command_id`, never on the envelope key. Its value is operational — it is what makes a DLQ
entry correlatable without parsing JSON (see
[MUST-6](content-fetch-issuer-contract.md#6-handle-fetch_failed-and-keep-a-reaper-anyway)), and it
is what a future partitioned consumer
would shard on.

---

## Pacing at the deployed defaults

Qualifies the pacing entry under
[what Replicator does not guarantee](content-fetch-issuer-contract.md#what-replicator-does-not-guarantee).

**At the shipped defaults every wait is slept through inside the handler**, so the cost is
seconds of added turnaround and nothing else. Only when an operator configures the interval
*above* `REPLICATOR_READ_BLOCK_MS` (5 s) does a paced command instead stay pending for the
next reclaim, which moves the cadence from seconds to a minute. That is a deployment
decision, not a default — but it is the one that changes what a reaper should expect, so it
is stated here rather than left to be discovered.

---

## Why a duplicate failure fact is not identical

Expands [MUST-4](content-fetch-issuer-contract.md#4-make-correlation-idempotent--one-command-can-yield-more-than-one-fact).

**This covers `fetch_failed` too, and there the duplicates are not even identical.** The failure
fact is published *before* the dead-letter (`dead_letter` acks inside itself, so a fact published
after it is lost outright on a crash in between). A crash in that window redelivers the command,
re-runs the failure, and publishes a **second `fetch_failed` with the same `command_id` and a
fresh `occurred_at`** — so its envelope key differs and consumer-side dedup-on-key will not
collapse it. That is by design (see [The frame](content-fetch-issuer-contract.md#the-frame-envelope)); it is also not something Replicator can
engineer away, since a deterministic `occurred_at` would need a durable store Replicator does not
have and must not grow.

---

## How the blob TTL clock runs

Expands [MUST-7](content-fetch-issuer-contract.md#7-copy-the-bytes-before-the-blob-expires).

The clock runs from **last reference by a fetch**, not last read by a consumer, and **both backends
implement that same rule by different means**. A consumer reading the blob extends nothing, on
either.

| | `local` | `gcs` |
|---|---|---|
| What marks a reference | `utime` on the short-circuit ([`src/storage/local.py::_touch`](../../src/storage/local.py)) | `customTime` re-stamped on the same branch ([`src/storage/gcs.py::_touch`](../../src/storage/gcs.py)) |
| What reaps | the in-worker sweep, every `REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS` | a bucket lifecycle rule on `daysSinceCustomTime` |
| Granularity of the reap | seconds | **one day**, enforced asynchronously and often later |

The object-store column is why the commitment is phrased as *at least* seven days. A lifecycle rule
cannot express "7 days and not a second more", and its enforcement pass is not scheduled against
anything a consumer can observe, so the rule is set a day beyond the promise and the real deletion
drifts later from there.

`blob_expires_at` exposes the event either way, since no consumer can see it. The value is
`stored_at + REPLICATOR_BLOB_TTL_SECONDS`, read **before** the store, so it lands at or before the
moment the reap measures against — and every mechanism after it pushes the real reap later still.
The error is one-way: acting on the horizon is early, never too late. A later fetch pushes the
expiry out and emits a fresh fact carrying the new value.

**Nothing keeps the two halves in step automatically under `gcs`.** The published horizon comes from
`REPLICATOR_BLOB_TTL_SECONDS` and the reap comes from a lifecycle rule configured in Google Cloud;
a rule shorter than the setting would announce a window the bucket will not honour, which is the one
way this value can be wrong in the unsafe direction. The worker logs the horizon at boot beside the
bucket so the pairing is checkable, and `docs/DEPLOYMENT.md` records the provisioned rule.

Also, on the reaper [MUST-6](content-fetch-issuer-contract.md#6-handle-fetch_failed-and-keep-a-reaper-anyway)
keeps:

- A `fetch_failed` publish that itself fails is swallowed on Replicator's side (the dead-letter
  still happens, and raising there would only burn the delivery ceiling and reach the same DLQ
  minutes later). Rare, logged, and another reason the reaper is not optional.

---

## Consequences an issuer inherits from the MUSTs

Three follow-ons, each expanding a rule rather than adding one.

**Losing the `command_id` -> domain map**
([MUST-2](content-fetch-issuer-contract.md#2-persist-command_id--domain-durably-before-publishing)):

Losing the map is recoverable but not free: the intent can be re-issued under a fresh
`command_id`, at the cost of another origin request. What is *not* recoverable is the in-flight
fact — it will arrive, match nothing, and have to be discarded.

**Fingerprint-dedupe now loses more than a correlation**
([MUST-5](content-fetch-issuer-contract.md#5-do-not-dedupe-facts-on-content_fingerprint)) — the
same argument as the per-occasion blockquote above, from the consumer's side:

Since the fact carries per-occasion fetch metadata (cannobserv#271), fingerprint-dedupe now also
loses **which fetch** a fact describes. Two facts sharing a fingerprint may carry different
`final_url`, `etag`, `last_modified`, and `fetched_at`; keeping only the first pins an issuer's
stored validators to whichever occasion arrived first and holds them there for as long as the
content is unchanged — precisely the period a conditional GET would have been useful.

**Re-hashing the bytes** (against *the fingerprint is definitional*, under
[what Replicator guarantees](content-fetch-issuer-contract.md#what-replicator-guarantees)):
 A consumer can still re-hash the bytes at `blob_uri` and compare; worth
  doing once the store crosses a boundary (object store, another host).
