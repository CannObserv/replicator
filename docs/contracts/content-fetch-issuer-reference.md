# The `content.fetch` issuer reference

**Status:** normative, and a companion to
[`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md). Everything here binds
Replicator's behaviour exactly as the contract does — this file is not commentary.

**What is here rather than there.** The contract carries what an issuer must *do*: the frame,
the command, the eight MUSTs, and the guarantee/non-guarantee pair. The two references carry
what an issuer *looks up*, and they divide on the direction of the traffic. **This file is the
request half** — the header and timeout rules it consults when a command is refused, the envelope
and what its key is for, the pacing to expect, which co-core version carries what, what losing the
`command_id` map costs, and the trust posture the whole capability
rests on. The **outcome half** is
[`content-fetch-outcome-reference.md`](content-fetch-outcome-reference.md): both facts field by
field, the reasoning behind the enriched `blob_available` fields, the condition-by-condition failure taxonomy, the conditions that
report nothing, the dead-letter queue, and the mechanisms behind several of the MUSTs. The split
exists so the contract stays short enough to read start to finish and neither reference has to be
read start to finish to answer one question; all three are normative, and a rule does not become
advisory by living in any of them.

**Changing this document.** The same rule the contract states applies, and it covers the *guidance*
here as well as the rules: a change to the refusal rules, to the trust posture, or to what an
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
one [the `blob_available` passthroughs](content-fetch-outcome-reference.md#the-enriched-blob_available-fields) take: a header
Replicator silently dropped, or a timeout it
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
| `timeout_seconds` over `REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS` (default **120**) | Replicator's consume path is serial, so your timeout is a lien on every *other* issuer's commands too. Ask an operator if 120 s is genuinely too short for a target. It bounds each *operation* (the connect, each read); the whole fetch is bounded by `REPLICATOR_MAX_FETCH_SECONDS` (default **120**, never less than this), and a fetch past that is retried, not refused (#104) |

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

## Provenance and trust

`content.fetch` is an **unauthenticated capability**: any writer to the stream can make Replicator
issue an arbitrary outbound HTTP request from Replicator's host and store the response. There is no
signing, no allowlist, and no issuer identity on the frame.

Integrity rests on **bus access control plus a destination guard**, and that control is no longer a
loopback bind.
The broker runs on its own node (`co-broker`, CannObserv/broker#1) and Replicator on another
(`co-replicator`, #88), so what holds the line is **per-service Redis ACL users** — who may `XADD`
to `content.fetch` is a broker grant (CannObserv/broker#2) — and the **Tailscale ACL**, which admits
only the bus participants to the broker at all. **That grant is wider than declared today** (#90):
`replicator` can `XADD` the stream too — its read pattern meeting the `+xadd` its fact streams need
— a gap CannObserv/broker#14 closes, not a second issuer. This section named "the moment the bus
spans hosts" as the point where message signing or a URL allowlist becomes the conversation. That
moment passed, the conversation was #89, and it concluded in **neither of those**:

- **No message signing.** Nothing is added to the frame and there is still no issuer identity on
  it. The grant that is too wide is a *broker* grant, and CannObserv/broker#14's per-service
  selectors are where it narrows — the broker already knows which account wrote a frame, which is
  the fact signing would otherwise have to re-establish on the wire. Signing buys something only
  against a writer that *holds* a legitimate grant and is compromised, which is not a threat this
  contract claims today.
- **No URL allowlist.** Declined on the boundaries charter's first and third tests, and declined
  for the issuer's sake: deciding which URLs are fetched is Watcher's job across an open-ended
  corpus, and a list maintained here would be a second copy of that decision with no mechanism to
  stay in sync. The copy that drifts is the one that refuses a legitimate fetch.
- **A destination guard instead** (#95) — the `destination_refused` row of
  [the failure taxonomy](content-fetch-outcome-reference.md#failure-taxonomy-what-happens-and-what-the-issuer-sees). It bounds
  where a fetch may *point* rather than who may issue one, because those are different questions and
  only the second was ever answered by a grant.

**Why the guard, concretely.** The bytes a fetch returns are stored in `co-gcs-blobs` and announced
on `content.blobs`, where every consumer SA can read them — so an unbounded destination made this an
instrument for reading whatever Replicator's network position reaches and republishing it
cluster-wide. On these VMs that is not hypothetical: exeuntu's socket-activated Shelley agent UI
answers `GET http://127.0.0.1:9999/` with **200**, unauthenticated.

**What the guard does not close, stated rather than omitted:** a name that resolves to a public
address at check time and a private one at connect time — DNS rebinding — is still reachable in
principle. The guard resolves and checks every address a name answers with, then hands the name to
the transport, which resolves again. Closing that window means pinning the address through the
connection, which is materially more machinery than the threat justifies while both the tailnet ACL
and the broker's grants bound who can aim a command at a hostile origin.

**`headers` widens that capability, and the widening is bounded here rather than by the broker.**
A bus writer can now attach an arbitrary header — an `Authorization` among them — to a host of its
own choosing. The trust model is the one above — broker grants, not a signed frame — so the guards
in the refusal table above are not a substitute for it; they are the cheap part, taken because it
is cheap. Concretely they stop three things the broker's boundary says nothing about: a `Host`
override that contacts one origin while addressing another, a CRLF in a value that splits the
request into two, and a `timeout_seconds` large enough to park the serial consume path — that last
one a denial of service against every *other* issuer, not against the origin.

Two properties an issuer can rely on: a refused command is refused **before** any request goes out,
and header **values never reach the journal** — only names are logged, so an `Authorization` an
issuer attaches is not re-exposed one layer down.

Relatedly: Watcher is the only issuer, and `seed_fetch.py` requires `--production` for the one
target the live worker consumes. A frame on that stream is fetched for real.

**`content.replicate` does not inherit this section.** A write is bounded by nothing a read is, so
the argument — and an earlier escalation trigger — is made again in
[`content-replicate-issuer-contract.md`](content-replicate-issuer-contract.md) (#34).

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

## Losing the `command_id` map

**Losing the `command_id` -> domain map**
([MUST-2](content-fetch-issuer-contract.md#2-persist-command_id--domain-durably-before-publishing)):

Losing the map is recoverable but not free: the intent can be re-issued under a fresh
`command_id`, at the cost of another origin request. What is *not* recoverable is the in-flight
fact — it will arrive, match nothing, and have to be discarded.

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

## The envelope, key by key

What `to_wire` puts on the stream; the rule that governs it is
[The frame](content-fetch-issuer-contract.md#the-frame-envelope).

| Key | Value |
|---|---|
| `key` | the envelope's idempotency key, derived by `to_wire` — see the table below |
| `payload` | the model, JSON-serialized — where everything in the contract's payload tables actually lives |
| `event_type` | `content_fetch` / `blob_available` / `fetch_failed` — how `from_wire` picks a model |
| `schema_version` | stringified |
| `occurred_at` | ISO 8601 UTC, **tz-aware** — see [the command](content-fetch-issuer-contract.md#the-command) |
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

---

## What the envelope key is for

`key` is **not** load-bearing on the consume path: Replicator decodes `payload` and dedupes on
`payload.command_id`, never on the envelope key. Its value is operational — it is what makes a DLQ
entry correlatable without parsing JSON (see
[MUST-6](content-fetch-issuer-contract.md#6-handle-fetch_failed-and-keep-a-reaper-anyway)), and it
is what a future partitioned consumer
would shard on.

---
