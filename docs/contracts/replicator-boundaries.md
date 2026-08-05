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

1. **Does it need durable per-resource history?** → **issuer.** Replicator's state must be
   exactly one of: content-addressed on disk (rebuildable by re-fetch), in-memory derived
   (rebuildable by replay), or in the broker (PEL, dedupe keys). State outside those three
   *is* a database, whatever it is called.
2. **Does it need cross-command coordination over a resource only the fetcher can see?**
   (a host's tolerance, the disk, a connection pool, a browser pool) → **Replicator.** Nobody
   else can see it, and N issuers being polite independently is a fiction.
3. **Can it be expressed without domain vocabulary?** If it needs the words InfoSource,
   InfoItem, WatchedItem, aspect, tenant → **issuer**, always.

Tests 1 and 2 can both fire. When they do:

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

### The policy stream (agreed, not yet built)

Upstream model tracked at
[CannObserv/cannobserv#285](https://github.com/CannObserv/cannobserv/issues/285) —
`CONTENT_FETCH_POLICY`, `FetchPolicyEvent`, and the two gaps it surfaced: `BusPublish` cannot
carry a `MAXLEN`, so the trimming this section requires is currently unexpressible through
co-core, and `AsyncBusConsumer` is group-only, so the replay-then-tail read has no driver seam.

`content.fetch.policy` — last-write-wins per host key. Replicator replays it from `0-0` at
boot into memory and tails it thereafter. No DB, rebuildable, no inbound calls, and the data
stays owned by its producer.

Four properties that must hold or the design fails quietly:

- the **owner republishes the full set** periodically and on change, so boot replay never
  depends on broker retention;
- the producer therefore XADDs with **`MAXLEN ~ N`**, N sized above the host count. Periodic
  republication onto a stream nothing trims is unbounded growth — the argument that deferred
  non-terminal facts in #9 §3 — and here it costs boot time too, since replay length would
  grow with policy history rather than with host count;
- an **unknown host resolves to a conservative default**, never to unlimited;
- **enforcement has a rate floor of `REPLICATOR_CLAIM_MIN_IDLE_MS`** (default 60 s). See below.

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

### The interim default (shipped)

`REPLICATOR_MIN_HOST_INTERVAL_SECONDS`, default **1.0 s** — Watcher's own
`DEFAULT_MIN_INTERVAL`, chosen precisely because it invents nothing. The numbers are the
issuer's under this charter, so until they travel over the bus the least-wrong value is the one
the cluster already commits to; the cutover then changes *who* paces rather than *how much*.
`0` disables pacing outright, an operator escape hatch and a choice to have none.

Consistent with the charter on both halves: enforcement is mechanism (test 2 — nobody but the
fetcher can see a host's tolerance across commands), and a single default is not policy in the
sense test 3 cares about — it names no domain concept and carries no per-host table. The state
is a host → last-request map in memory: derived, bounded by pruning, and rebuildable by replay,
which is the second of the three permitted state shapes. A cold worker is polite from scratch,
which errs in the safe direction.

What it is **not** is the design. One number for every origin is exactly the "conservative
default" the policy stream exists to replace with real per-host values.

**The stream is a precondition of the Phase 4 cutover, not a follow-on to it.** Watcher's
limiter (`src/core/rate_limiter.py::acquire_for_domain`, fed by 429s its own fetch path
observes) is load-bearing today and stops functioning the moment that fetch path becomes a
publish path — it does not fail, it silently becomes decorative, pacing command publication
rather than origin requests. **The interim default above closes that window**, so the cutover
is no longer blocked on the stream; what remains is that one number for every origin is not
what the cluster wants for long. Tracked issuer-side at
[CannObserv/watcher#245](https://github.com/CannObserv/watcher/issues/245).

## Reviewing a proposed payload field

One question: **does this name a domain concept?** `politeness_key: str` passes — opaque to
Replicator. `info_source_id` fails. The wire's domain-agnosticism is the property the whole
issuer contract is built on; it erodes one plausible field at a time.

**The rule governs payload *shapes*, not producer-owned token vocabularies.**
[`src/core/errors.py::FailureReason`](../../src/core/errors.py) is a locally-defined `StrEnum`
of `fetch_failed` `reason` tokens and stays local by design: co-core types that field as a
plain `str` rather than a `Literal` precisely so a producer adding a token cannot crash an
older `extra="ignore"` consumer, which puts the vocabulary on the producer. Defining a wire
*model* here would be the violation; owning the tokens Replicator itself emits is the
contract working as intended.

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
| No domain vocabulary | AST scan of `src/`: `info_source`, `info_item`, `watched_item`, `tenant`, `aspect` appear in no identifier and no string literal |
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
matched `both tasks watch one stop event` in a docstring — and a test whose first tripper is an
English sentence is a test that gets deleted rather than heeded. The bare verb `watch` is
deliberately absent from the token list; `watched_item`, the domain noun, is not. String
literals are in scope alongside identifiers because domain leakage arrives as a dict key or a
log field (`detail={"info_source_id": ...}`) at least as often as it arrives as an attribute.

**The `event_type` check is an AST check on class bodies, not a grep.** `event_type` appears
twice in `src/worker/loop.py` legitimately — once in a comment, once reading a co-core model's
own field. A grep would cry wolf on both.

**The detectors are themselves tested.** Each scan has cases running it against synthetic
violating source, and each corpus scan asserts its own file list is non-empty. A structural
test that quietly walks zero files passes forever while enforcing nothing — which is worse
than no test, because this document then cites it.

## Refs

- [`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md) — the wire contract this sits beside
- #7 — object-store blob backend (the tracked violation)
- #9, #10, #11 — the Phase 4 contract additions
- #12 — this charter
- [CannObserv/watcher#241](https://github.com/CannObserv/watcher/issues/241) — Phase 4 issuer
- [CannObserv/watcher#245](https://github.com/CannObserv/watcher/issues/245) — the politeness gap at cutover
- [CannObserv/archiver#72](https://github.com/CannObserv/archiver/issues/72) — cluster integration strategy
