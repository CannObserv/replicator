# Replicator boundaries charter

**Status:** normative. **Home:** this file, in the Replicator repo — the invariants are
about this repo's code and are enforced by
[`tests/test_boundaries.py`](../../tests/test_boundaries.py). Sibling repos link here rather
than copying.

**Audience:** anyone proposing a capability, payload field, or setting for Replicator —
including a future maintainer of this repo, who is the likelier author of the drift this
document exists to prevent.

**Relationship to the issuer contract.**
[`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md) settles the *wire*:
what a producer must do so a fact is never lost. This settles the *service*: what Replicator
is allowed to become. They are different documents and until #12 only one of them existed.

**Why now.** Phase 4 ([CannObserv/watcher#241](https://github.com/CannObserv/watcher/issues/241))
is the first time capability gets reallocated across the boundary, and the reallocation is not
one-directional: politeness enforcement wants to move *in*, alternate fetch drivers want to
move *in*, and per-URL validator state must be kept *out*. Each arrives as a reasonable-looking
field or a small settings table, and the cumulative result — a Replicator with a database,
domain vocabulary, and an admin API — is reached one defensible step at a time.

The invariants are all currently **true**. This is drift prevention from a good position, not
remediation.

---

## The rule

> **Replicator owns the mechanics of acquiring bytes from a network and holding them briefly.
> It never owns why, when, or what they mean.**

## The three tests

Any proposed capability, field, or setting runs these in order:

1. **Does it need durable per-resource history?** → **issuer.** Replicator's state must be exactly
   one of: content-addressed on disk (rebuildable by re-fetch), in-memory derived (rebuildable by
   replay), or in the broker (PEL, dedupe keys). State outside those three *is* a database,
   whatever it is called.
2. **Does it need cross-command coordination over a resource only the fetcher can see?**
   (a host's tolerance, the disk, a connection pool, a browser pool) → **Replicator.** Nobody else
   can see it, and N issuers being polite independently is a fiction.
3. **Can it be expressed without domain vocabulary?** If Replicator has to *read* the words
   InfoSource, InfoItem, WatchedItem, aspect, tenant → **issuer**, always. Carrying one unread is
   the one exception — see **Reviewing a proposed payload field**.

Tests 1 and 2 can both fire:

> **Mechanism to Replicator. Policy to the issuer. Config travels over the bus.**

### Worked example — per-host politeness

The case worth studying, because the tests do **not** resolve it to one side and the
mechanism/policy split is what finishes the job.

| Test | Politeness |
|---|---|
| 1 — durable per-resource history? | **No.** Per-*host*, and rebuildable from replay. Not the issuer on this test. |
| 2 — cross-command coordination over a resource only the fetcher can see? | **Yes.** A host's tolerance. "N issuers being polite independently is a fiction" is exactly this row. |
| 3 — expressible without domain vocabulary? | **Yes.** A hostname is a network fact, not an InfoSource. |

Test 2 fires alone, but a naive reading of it — "politeness is Replicator's, done" — would
move the *numbers* here too, and the numbers are operator policy that lives in Watcher's
`Domain` table today. The resolution is the split: Replicator **enforces**, the issuer
**decides**, and the decision travels over the bus (see **The policy stream**).

A reader who only skims the easy case below learns the pattern and misses the rule.

### Worked example — conditional GET

ETag/Last-Modified *look* like fetcher state. Test 1 sends them to the issuer: they are
durable per-resource history. So the fact returns them
([cannobserv#271](https://github.com/CannObserv/cannobserv/issues/271)) and the next command
replays them as request headers
([cannobserv#272](https://github.com/CannObserv/cannobserv/issues/272)). Replicator gains
conditional GET while holding zero bytes of per-URL history. Cite this one when a proposal
argues that some small table would be simpler.

## Where things live

**Replicator (mechanism):** fetch execution, redirect following, retry cadence,
fingerprinting, temp storage and its lifetime, disk ceiling, per-host pacing *enforcement*,
and — future — alternate fetch drivers (browser, archival capture) and escalation between
them.

**Issuer (policy/domain):** scheduling and cadence, extraction specs, change semantics,
notification, correlation, pending maps and reapers, per-URL validator state, and the
politeness *numbers*.

**Never Replicator's:** domain identity, extraction specs, change decisions, notification,
InfoSource correlation, scheduling. Named explicitly so a proposal has to argue against a
written line rather than into a vacuum.

## Config taxonomy

| Channel | Carries | Examples |
|---|---|---|
| **env** | facts about *this host* | blob TTL, disk ceiling, claim cadence, dedupe TTL |
| **command** | this occasion | `headers`, `timeout_seconds`, later a fetch `strategy` |
| **config stream** | cluster policy needing cross-command state | per-host politeness |

Env settings carry the `REPLICATOR_` prefix so they never collide with a sibling service on
the shared VM. **`BUILD_ID` is the one exemption** and is deliberate: the systemd unit's
`ExecStartPre` stamps it generically across the cluster's services, so prefixing it here would
mean a per-service variable name for one git SHA. It is exempted by name in the test, not
waved through — an exemption list that grows is this convention ending quietly.

**The fourth channel is rejected by name: an inbound admin HTTP API.** It is the easy path for
every future config need and it ends the property that makes this service testable and
relocatable.

**What that forbids is a write surface, not self-description.** The enforced invariant is:

> No route accepts state-changing input. Ingress is read-only liveness and self-description.

FastAPI's `/docs`, `/redoc`, `/docs/oauth2-redirect` and `/openapi.json` are read-only and
describe the one real route, so they are allowlisted rather than switched off. Disabling them
outside dev would add a config knob this taxonomy then has to place, and would make the app
under test differ from the app that ships — for a surface no deployment serves.

**The invariant that matters is about the worker, not the app.** `src/api/` is dev-only;
`replicator.service` runs the bus consumer, which binds no port. An ingress assertion scoped
to the FastAPI app could pass forever while an admin listener grew inside `src/worker/`. Both
are asserted.

### The policy stream (shipped, #19)

Upstream shipped in co-core **v0.7.7**
([CannObserv/cannobserv#285](https://github.com/CannObserv/cannobserv/issues/285)):
`CONTENT_FETCH_POLICY`, the `FetchPolicyState` payload, and `AsyncBusTailReader` — the
groupless replay-then-tail driver whose absence this section previously recorded as a gap.

**The stream is `content.fetch-policy`, with a hyphen.** The dotted `content.fetch.policy`
this document used to name collides with the `<topic>.dlq` derivation of the command stream.
Use the constant, never the literal.

Last-write-wins per host key. Replicator replays it from `0-0` at boot into memory and tails
it thereafter. No DB, rebuildable, no inbound calls, and the data stays owned by its producer.
`src/worker/policy.py`.

Four properties that must hold or the design fails quietly:

- the **owner republishes the full set** periodically and on change, so boot replay never
  depends on broker retention;
- the producer therefore XADDs with **`MAXLEN ~ N`**, N sized above the host count. Periodic
  republication onto a stream nothing trims is unbounded growth — the argument that deferred
  non-terminal facts in #9 §3 — and here it costs boot time too, since replay length would
  grow with policy history rather than with host count;
- an **unknown host resolves to a conservative default**, never to unlimited;
- **enforcement has a rate floor of `REPLICATOR_CLAIM_MIN_IDLE_MS`** (default 60 s). See below.

Four consumer-side rules that came out of building it, each one a way to be wrong silently:

- **`revoked` means "no explicit policy", not "no limit".** It is the tombstone LWW has no
  delete for, and the host falls back to the same conservative default an unknown host gets.
  `min_interval_seconds` is `None` on a tombstone by design, so **branch on `revoked` first** —
  a consumer that reaches for the interval stores a `None` and hands it on as a number.
- **`0.0` is a legal interval** meaning "this host needs no spacing", and it is falsy.
  `policy.get(host) or default` turns an explicit operator decision into a missing one.
- **The default's strictness cannot be asserted at startup.** A published interval has no upper
  bound, so there is no value to validate against short of importing the issuer's own backoff
  ceiling — the constant this indirection exists to avoid importing. What is enforceable is the
  moment a real policy turns out to be *stricter* than the fallback that would replace it on
  revocation or staleness, which is logged per host at apply time and is the number an operator
  raises.
- **Arrival order is not publication order.** The producer republishes its whole set
  periodically; a republish assembled from a snapshot taken before a change that already
  shipped would revert it, silently and in the loosening direction. The map holds the last
  applied `occurred_at` per host and applies on `>=` — `>=` rather than `>` so a full set
  stamped with one instant does not lose every host after the first.

And two that are about the frame rather than the policy:

- **`from_wire`'s dispatch table is global**, so a `blob_available` XADDed here decodes *cleanly*
  into the wrong model rather than raising. There is no anomaly to recover from, no group, and
  nothing to dead-letter — the only defence is an `isinstance` check before destructuring,
  exactly as the command path does.
- **`AsyncBusTailReader.replay()` cannot be used to do the replay.** It accumulates across many
  `read` calls and returns its list only on a clean finish, so any raise part-way through
  discards everything it read while the cursor has already advanced — a poison frame at position
  *k* silently loses the *k−1* policies ahead of it, permanently, and on a last-write-wins stream
  a lost policy is indistinguishable from one never published. Drive `read` and apply each batch
  as it arrives. Recorded here and not only in the code because the next consumer of this stream —
  or of any future config/state stream — will reach for the method whose name says what they
  want. Worth fixing upstream (cannobserv#285) so the driver's own docstring carries it.

Recovery from a frame that will never decode is **bounded and interruptible**: an anomaly is
evidence the broker is answering, so it must not count toward the outage backoff — which leaves
a run of them with nothing slowing it down, hence a `MAX_POISON_SKIPS` bound past which the boot
replay gives up (the tail resumes from the same cursor) and the tail parks. The replay also
rides the worker's stop event, because how long it runs is the producer's business: this
document asks the producer to `MAXLEN`, and Replicator cannot enforce it.

**Why a stream and not a Redis hash.** Broker state is explicitly permitted by test 1, so
`HGETALL` on a per-host hash is a reasonable reach and will be proposed. It is rejected
because it has no `schema_version`, no co-core model, and it couples Replicator to a key name
another service writes instead of to a payload contract. The stream keeps the same validation
posture as every other wire input.

**Producer:** Watcher for Phase 4 (the numbers live in its `Domain` table today). Because it
is bus-delivered, the producer can later move to Archiver — the natural home if a second
issuer ever exists — with no Replicator change at all. That portability is the reason for the
indirection.

**Enforcement mechanism:** when a host's bucket is dry, leave the message in the PEL and let
the reclaim bring it back. That is already the idiom for the disk ceiling — a policy check in
the handler, not new machinery — and it inherits the ceiling's safety property: the raise is a
`TransientFetchError`, which is exempt from the delivery ceiling, so a paced command cannot
DLQ for being paced.

**It also inherits the ceiling's granularity, and parking alone is therefore not a sufficient
mechanism.** A parked message returns via `claim_stale`, so the finest per-host spacing it can
express is `REPLICATOR_CLAIM_MIN_IDLE_MS` — **60 s by default**. Watcher's baseline today is
`DEFAULT_MIN_INTERVAL = 1.0` s (`src/core/rate_limiter.py`), backing off to
`BACKOFF_MAX_INTERVAL = 60.0` s. So parking matches the *backoff* case almost exactly and
misses the *normal* case by 60×: implemented naively, every host would be paced at 1/60th of
the rate the cluster runs at now. The failure is silent and in the safe direction, which is
what makes it easy to ship.

The constraint, stated so a design has to answer it: **a serial consume path cannot both sleep
for a short wait and stay available to other hosts.** Sleeping in the handler blocks every
other command in the group — which is why parking exists — and parking cannot express a
sub-reclaim interval.

**Resolved by splitting the wait by duration**, and shipped with the interim default below:
a wait no longer than one poll window (`REPLICATOR_READ_BLOCK_MS`) is slept through in the
handler, and anything longer parks. The bound is derived from an existing setting rather than
given its own, because it is the same quantity — a wait shorter than a poll the loop already
performs adds nothing to the shutdown latency `TimeoutStopSec` is sized for. The stop event
cuts the sleep short, and an interrupted wait is not an elapsed one: the command parks rather
than fetching unpaced on the way out. `src/worker/pacing.py`, `handler.py::_pace`.

### The fallback default (was the interim, #12 → #19)

`REPLICATOR_MIN_HOST_INTERVAL_SECONDS`, default **1.0 s** — Watcher's own `DEFAULT_MIN_INTERVAL`,
chosen precisely because it invents nothing. #12 shipped it as one number for every origin, standing
in for a stream that did not exist; #19 gave it its permanent job as what a host with **no explicit
policy** resolves to — unknown, revoked, or not yet replayed — and never "unlimited", because a boot
replay cannot tell a consumer whether the set it received is whole.

**`0` no longer disables pacing outright.** It is the fallback for unpublished hosts only; a host
with a policy is still paced by it. The alternative would let a local env var veto a value the
issuer published, inverting the ownership split this document settles. An operator wanting no
politeness at all says so per host, through the producer that owns the numbers.

Consistent with the charter on both halves: enforcement is mechanism (test 2 — nobody but the
fetcher sees a host's tolerance across commands), and a fallback number is not policy in the sense
test 3 cares about — it names no domain concept, and the table it defers to is the producer's. **Two in-memory maps** now, with different bounding rules and deliberately so:
the pacer's host → last-request map is consumer-derived and pruned to `MAX_TRACKED_HOSTS`,
while the policy map is bounded by what the producer publishes and is **never pruned** —
dropping an entry to honour a local limit would silently loosen a host's spacing, the exact
failure the stream exists to remove. Both are derived, rebuildable by replay, and hold no
domain vocabulary: the second of the three permitted state shapes, twice.

**Boot ordering is part of the design, not an implementation detail.** `replay()` runs
synchronously before the consume loop starts, or the worker's opening commands are paced against
an empty map — safe only because the fallback is the stricter number, and that is the one
assumption not worth spending on startup ordering. Mechanism, including why a failed replay is
absorbed rather than fatal: [ARCHITECTURE.md](../ARCHITECTURE.md).

**Known limitation, still open after #19: the host asked for is not always the host
reached.** httpx follows redirects inside the driver, so a URL that 301s elsewhere is paced under
the name the command carried, never under the name that served it — and a corpus funnelling into
one portal or CDN hits it at N times the intended rate, the failure politeness exists to prevent.
The fix would read `FetchResult.final_url`, but recording the landing host breaks "one request, one
record" and wants its own decision. #19 made it more defensible without making it automatic.
Recorded here for the same reason `blob_uri` is: an unwritten gap and a decorative charter are the
same thing to a reader.

**The stream is a precondition of the Phase 4 cutover, not a follow-on to it.** Watcher's limiter
(`src/core/rate_limiter.py::acquire_for_domain`, fed by 429s its own fetch path observes) is
load-bearing today and stops functioning the moment that fetch path becomes a publish path — it does
not fail, it silently becomes decorative, pacing publication rather than origin requests. #12's default closed that window on the consumer side and #19
supplies the numbers; **what remains is issuer-side** — Watcher publishing its `Domain` rows
onto this stream, tracked at
[CannObserv/watcher#245](https://github.com/CannObserv/watcher/issues/245). Until it does,
every host resolves to the fallback, which is the pre-#19 behaviour and is why the consumer
half could land first.

## Reviewing a proposed payload field

One question: **does this name a domain concept?** `politeness_key: str` passes — opaque to
Replicator. A field Replicator would have to *read* to do its job fails. The wire's
domain-agnosticism is the property the whole issuer contract is built on; it erodes one plausible
field at a time.

**`info_source_id` is the settled exception, and its shape is the precedent (cannobserv#300,
#28).** What made it acceptable is not that the field is small — it is that Replicator **never reads
it**: delete every line mentioning it and the byte path behaves identically. So the real question is
*does Replicator have to understand this to act on it?* If yes it fails whatever it is called; if no
it is freight.

**The rule governs payload *shapes*, not producer-owned token vocabularies.**
[`src/core/errors.py::FailureReason`](../../src/core/errors.py) is a local `StrEnum` of
`fetch_failed` `reason` tokens and stays local by design: co-core types that field as a plain `str`
rather than a `Literal` precisely so a producer adding a token cannot crash an older consumer, which
puts the vocabulary on the producer. Defining a wire *model* here would be the violation; owning the
tokens Replicator emits is the contract working.

## Known violation, tracked

`blob_uri` is a host-local `file://` path and nothing on the wire says so. Any consumer must
live on Replicator's VM — a shared-filesystem data-plane coupling in a service otherwise
reached only through the broker, and a constraint on the *issuer's* deployment topology that
the issuer never agreed to. Tracked in #7 (object-store blob backend), and **pinned by a
characterization test** so #7 flips a written line rather than quietly satisfying an unstated
one.

Recorded here rather than omitted: a charter that asserts an isolation the code does not have
teaches its readers that the document is decorative.

## Cluster-side corollary

The single-fetcher invariant is only true if issuers hold up their end. Watcher's create-time
`probe_url` currently fetches origins outside Replicator's politeness envelope; Phase 4
resolves it by making create asynchronous (item enters a probing state, a normal
`content.fetch` is issued, `final_url` on the fact fills in the resolved URL). After that,
**no service but Replicator fetches watched content.** Stated here because the invariant is
the cluster's, not this repo's alone — and worth an issuer-side test, in the issuer's repo.

---

## Enforcement

[`tests/test_boundaries.py`](../../tests/test_boundaries.py), run by
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) on every PR to `main`. The point
is failing a PR, not documenting an intention.

| Invariant | Test |
|---|---|
| No database | no persistence distribution in `uv.lock` (sqlalchemy, asyncpg, psycopg, alembic, …); no `sqlite3` / `shelve` / `dbm` / `pickle` import in `src/` |
| No domain vocabulary | AST scan of `src/`: `info_source`, `info_item`, `watched_item`, `tenant`, `aspect` appear in no identifier and no string literal — exact `info_source_id` exempted in the three emit-path modules only |
| The echoed key is never interpreted | AST scan of `src/`: every `info_source_id` occurrence is a field declaration, a parameter, the `info_source_id=` keyword, or that keyword's value; all else fails |
| Ingress is read-only | recursive route walk: every path in the allowlist, every method in `{GET, HEAD}` |
| The deployed process has no ingress | `src/worker/` imports no server framework; the unit runs `src.worker.main` with no `uvicorn` and no `--port` |
| No locally-defined wire models | no class in `src/` declares an `event_type` field — every wire payload comes from co-core |
| No issuer SDK | no dependency on a sibling repo's client in the lock |
| Config surface | every `Settings` field is `REPLICATOR_*`-prefixed except `build_id`, exempted by name; no `env_file`; no configuration or network call at import time |
| Known violation, pinned | `blob_uri` still starts `file://` (#7) |

Three notes on the implementation, because each encodes a decision that a "simplification"
would undo:

**The vocabulary scan is the load-bearing one, and it is AST-based for a reason.** It reads
identifiers and string literals only, skipping comments and docstrings. The grep this replaced
matched `both tasks watch one stop event` in a docstring, and a test whose first tripper is an
English sentence gets deleted rather than heeded. The bare verb `watch` is deliberately absent from
the token list; `watched_item`, the domain noun, is not. String literals are in scope because domain
leakage arrives as a dict key or log field as often as an attribute.

**The `info_source` exemption is a carve-out, and a second scan is what makes it one.** The wire
requires naming the field to copy it, so it is allowed in exactly `handler.py`, `reporter.py` and
`loop.py`, and only as the exact identifier — no `info_source_policy` map rides in behind it.
The second scan holds the real invariant as an **allow-list**: the four shapes a verbatim echo can
take, everything else refused. It began as a deny-list of reading positions; three review rounds each
found ones it had not enumerated, so it now asks the opposite question and is exhaustive. Naming the field is mechanics; keying on it is a domain model one commit at a time.
Two assertions guard the allowlist itself: it can never name config, storage or the API, and every
entry must be a file.

**The `event_type` check is an AST check on class bodies, not a grep.** `event_type` appears twice
in `src/worker/loop.py` legitimately — once in a comment, once reading a co-core model's own field.
A grep would cry wolf on both.

**The detectors are themselves tested.** Each scan runs against synthetic violating source, and
each corpus scan asserts its own file list is non-empty. A structural test that quietly walks zero
files passes forever while enforcing nothing — worse than no test, because this document cites it.

## Refs

- [`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md) — the wire contract this sits beside
- [`content-fetch-issuer-reference.md`](content-fetch-issuer-reference.md) — its lookup half
- #7 — object-store blob backend (the tracked violation)
- #9, #10, #11 — the Phase 4 contract additions
- #12 — this charter
- #19 — the policy stream's consumer half
- [CannObserv/cannobserv#285](https://github.com/CannObserv/cannobserv/issues/285) — the policy stream contract (co-core v0.7.7)
- [CannObserv/watcher#241](https://github.com/CannObserv/watcher/issues/241) — Phase 4 issuer
- [CannObserv/watcher#245](https://github.com/CannObserv/watcher/issues/245) — the politeness gap at cutover
- [CannObserv/archiver#72](https://github.com/CannObserv/archiver/issues/72) — cluster integration strategy
