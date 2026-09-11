# Bus & consume-path conventions

The co-core / Redis Streams rules that hold for **every** stream Replicator
touches, with the reasoning that makes each one non-negotiable. `AGENTS.md`
states them in one line each; what a *particular* stream carries, and why, is in
[STREAMS.md](STREAMS.md).

- **Consumers must be idempotent; producers own the outbox.** The cluster split (parent strategy, "Delivery + correctness") assigns the transactional outbox to producers with a DB system of record. Replicator has none — its durable record of intent is the consumer group's PEL, recovered via `claim_stale`. Do not add a Postgres outbox to the consume path.
- **Validation posture:** use the canonical `extra="ignore"` models; **branch on `schema_version` before destructuring**; tolerate additive producer fields. Never use the strict `*Emit` classes on the consume path.
- **Batch-poison caveat:** `AsyncBusConsumer.read(count>1)` raises `BusMessageAnomaly` on a malformed frame *before* returning the well-formed ones in the batch. Read `count=1`, or catch the anomaly and route via `dead_letter`. `from_wire` is deliberately fail-loud.
- **DLQ is a shipped seam, not a TODO:** `dead_letter(message_id, fields)` copies the frame to `<topic>.dlq` and acks the original. Deterministic failure ⇒ DLQ; transient failure ⇒ retry. **Filling it is granted, emptying it is not** — `XADD <topic>.dlq` is in the ACL and `XDEL` is `NOPERM`, so under the current grant every frame this service parks is drained by the broker operator rather than from this host (#86, `docs/COMMANDS.md`). Whether that stays true is CannObserv/broker#12, which asks for `XDEL` narrowly on `content.{fetch,replicate}.dlq`; until it is decided, assume the request.
- **Three fates, not two (#17).** Transient ⇒ retry, no fact. Deterministic ⇒ fact, then DLQ. **Completed without bytes ⇒ fact, then ack, and no DLQ entry at all** — today only a body-less 304, which is a *successful* conditional GET. Four things follow, and each is a decision rather than an omission:
  - **The fate is the exception type, not the token.** `CompletedWithoutBlobError` is a sibling of `TransientError` / `PermanentError` under `HandlerError`, never a subclass of either — a subclass is swallowed by the arm that dead-letters. Branching on `exc.reason is FailureReason.NOT_MODIFIED` inside that arm would work and is wrong: `reason` is a wire string every `content.blobs` consumer branches on, so renaming it would silently change retry and DLQ behaviour here.
  - **No DLQ entry.** The DLQ is an operator surface and a successful no-change check is not operator-actionable; wherever conditional GET is in use it is the *common* outcome, so copying each one there would bury the entries that matter. This is the first close that leaves none, so "the DLQ is the complement of the fact" is now true of every *failed* close rather than every terminal one.
  - **The dedupe key is written.** Every other close acks without one, because it is discarding a command; this one completed it. Without the key a reclaim after a crash re-asks an origin that has just said nothing changed.
  - **The ordering is inverted and explicit.** `_close` cannot get fact-before-ack wrong because `dead_letter` acks inside itself; `_close_without_dlq` acks by hand, so it publishes, then writes the key, then acks. A fact published after the ack is lost outright on a crash, with no DLQ entry left to repair from.
  The cost lands on the fact stream: `fetch_failed` now carries a non-failure, so its volume stops being a failure signal — count `fetch_failed where reason != "not_modified"`.
- **A frame that fails to decode has no fields.** `from_wire` raises from *inside* `read`/`claim_stale`, so the anomaly carries `topic` + `message_id` only — but `dead_letter` XADDs the fields it is given and `XADD` rejects an empty map. Re-read the raw frame by id (`XRANGE topic id id`) and fall back to a synthesized record when the entry has been trimmed. `src/worker/loop.py::dead_letter_anomaly`.
- **`from_wire`'s dispatch table is global.** A `blob_available` frame XADDed to `content.fetch` decodes cleanly into the wrong model rather than raising — `isinstance`-check the payload before destructuring.
- **`claim_stale` is the retry path, not just crash recovery.** A transiently-failed message is left unacked and comes back through the same reclaim, so retry cadence = `REPLICATOR_CLAIM_MIN_IDLE_MS`. Call it with `count=1`: XAUTOCLAIM transfers ownership and resets the idle clock on every entry it returns *before* co-core decodes them, and it restarts at `0-0` each call, so a poison entry jams recovery permanently unless it is DLQ'd first.
- **Retry accounting is XPENDING's `times_delivered`**, not a side counter. It only advances on a reclaim.
- **A consumer appears in `XINFO CONSUMERS` only after its first *delivered* message.** An empty poll registers nothing, so an absent consumer entry is not evidence a worker is down — a liveness check built on it reports every idle worker as dead. Recovery is unaffected: `claim_stale` reclaims by group and idle time, not by a pre-existing consumer entry.
- **A consumer *group's* name is co-core's to define, and both defaults derive it (#77, cannobserv#384).** `<service>.<stream-suffix>[-<purpose>]` via `group_name(topic, service)`, so `content.fetch` → `replicator.fetch` and `content.replicate` → `replicator.replicate` — the names live on the broker, audited 2026-09-01. `src/core/config.py` calls the helper rather than writing the strings, because a literal here is a second copy of a rule this repo does not own, and the cluster's group naming has already drifted once. The failure that forecloses is the quiet one: a renamed convention spelled locally as a literal reaches `ensure_group`, which creates a **new empty group beside the real one**, and the worker then polls a stream nothing delivers to. `purpose` is omitted while a service runs one group per stream and disambiguates when it runs more. The floor is co-core `>=0.13.1`, and `uv.lock` plus `ExecStart`'s `--frozen --no-sync` mean a convention change cannot arrive on a deploy without a lockfile commit — which runs the tests that catch it. Config/state streams have no group at all: `group_name` raises on `content.fetch-policy`, and `stream_kind` is the machine-readable form of the three-kind taxonomy this file's rules are organised around.
- **A consumer's name is derived from its group, never from the host (#77).** `<group with dots dashed>-<slot>`, so `replicator.fetch` → `replicator-fetch-1` and `replicator.replicate` → `replicator-replicate-1`, one per group and computed at the wiring seam (`src/worker/main.py::consumer_name_for`). The rule is **stability**: a registration persists until an explicit `XGROUP DELCONSUMER` that nothing calls, so a name carrying anything that varies — a hostname, a pid — mints a fresh registration on each change and abandons the old one holding its PEL, reclaimable only by an `XAUTOCLAIM` at `min_idle_time`. Archiver reached seven registrations on the production broker, six dead (archiver#156); Replicator's hostname-derived spelling additionally read as the *Watcher* service's, this VM being named `watcher`. A stable name makes a restart reuse its registration, so the leak cannot recur — no periodic sweep, and no shutdown hook a `SIGKILL` would skip. Overrides (`REPLICATOR_CONSUMER_NAME`, `REPLICATOR_REPLICATE_CONSUMER_NAME`) are **per group** for the same reason the name is: one process-wide override put a `replicator-fetch-…` consumer inside `replicator.replicate`, a name that misstates its own group. The `-1` is a slot — a second member of a group takes `-2` upward, and must not share a name with the first, since Redis tracks pending entries per consumer name.
- **A failing *message* and a failing *cycle* are different.** `process_message` decides a message's fate; a broker refusing reads/acks/DLQ writes is `run_loop`'s problem — it backs off (`REPLICATOR_ERROR_BACKOFF_BASE_SECONDS` → `_MAX_SECONDS`) and retries, then re-raises after `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` so a permanently wrong `REPLICATOR_REDIS_URL` surfaces as a restart instead of a worker that looks alive while doing nothing. The unit's `StartLimitIntervalSec` is sized against that ceiling — change one, revisit the other.
- **Under a capped broker the consume path lives and the publish path retries (#79, broker#6).** Observed against a scratch `redis-server 7.0.15` at `maxmemory 1mb` / `noeviction`, not inferred from redis-py's class hierarchy as #20 was — `tests/worker/test_oom_integration.py` is the run, and it spawns its own broker because `maxmemory` is instance-wide and `co-broker` carries three services. What a full cap refuses is exactly the `denyoom` commands, which for this worker are `XADD` (every fact, and the dead-letter copy) and `SET` (the dedupe key). `XREADGROUP`, `XACK`, `XAUTOCLAIM`, `XPENDING`, `XRANGE`, `EXISTS` and `PING` are all admitted, so an OOM is a *publishing* incident here rather than a dead worker. Five things follow:
  - **The error is `redis.exceptions.OutOfMemoryError`**, a `ResponseError` subclass, message `command not allowed when used memory > 'maxmemory'.` (redis-py drops the wire's `OOM ` prefix). It is in `_TRANSIENT_ERRORS` and must stay there: everything else in that family means "this command will never work", and this one member is somebody else's incident.
  - **A refused fact is `Outcome.RETRY`, exempt from the delivery ceiling.** Nothing is acked, nothing is announced, nothing reaches `<topic>.dlq`, and the PEL keeps naming the command — which is the durable record of intent this service has instead of an outbox. Driven past `REPLICATOR_MAX_DELIVERY_ATTEMPTS` reclaims to prove the ceiling is never consulted. The command completes on the reclaim after the cap lifts, in the same worker, with no restart: store-then-publish means the bytes were already on disk and the re-run is the no-op content-addressed storage makes it.
  - **A refused *dead-letter* is a cycle failure, and it does not escalate to a restart.** The DLQ write escapes `process_message` (only the handler call is wrapped), so it lands in `run_loop`'s backoff — but `consecutive_failures` resets on any cycle that completes, and the cycle after a refused dead-letter finds the entry too young to reclaim and returns an empty read. So a capped broker produces a fail/idle alternation that never accumulates `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` in a row: the outcome is the same indefinite retry the byte path gets, with nothing acked. Read the cycle-failure bullet above with this exception in mind — the ceiling still fires against the failure it was written for, a broker that refuses the *read* as well.
  - **`XGROUP CREATE … MKSTREAM` is `denyoom`; `XGROUP CREATE` on an existing key is not.** A cold boot against a capped broker therefore succeeds wherever the stream already exists and fails at `ensure_group` where it does not — a first-boot hazard only, and one no live stream has. It is also the one refusal this service does **not** retry: `ensure_group` re-raises anything but `BUSYGROUP`, so it leaves `run()` and the unit restarts into the same wall until the cap lifts.
  - **The clearing edge is a duplicate, not a loss.** A fact that is accepted and a dedupe `SET` that is then refused leaves the command unacked with no key, so the reclaim re-runs it and publishes a second fact — distinguishable, because the envelope key is `command_id:occurred_at`, and exactly what contract MUST-4 requires issuers to tolerate.
  Never answer any of this with a client-level retry: `Redis.execute_command` wraps the send in `Retry.call_with_retry`, so a retried `XADD` the broker already applied publishes the entry twice, and `content.replicate` is the least duplicate-tolerant consumer on this bus. `BUS_RETRIES = 0` in `src/core/bus_client.py` is that decision.
- **An ACL denial is transient too, and for a sharper reason than the cap (#82, broker#2).** `NoPermissionError` is the second `ResponseError` subclass in `_TRANSIENT_ERRORS`. broker#1 puts per-service ACL users in front of the broker, so a missing `+xadd`, a `~` pattern that omits `content.blobs`, or a typo produces this against commands that are *valid* — and the unclassified arm would retry to `REPLICATOR_MAX_DELIVERY_ATTEMPTS` and then close them with a terminal `fetch_failed(handler_error)`, telling issuers their bytes are never coming about a fault one `ACL SETUSER` fixes, with any bytes already stored left as orphans no fact references. Three things worth keeping:
  - **The trade is deliberate.** A grant that is never fixed now retries forever instead of dead-lettering, so a permanent mistake stays out of `<topic>.dlq` and shows up in the journal at every reclaim. That is the side to be wrong on: a stuck PEL entry is repairable and a wrong terminal fact is not. Archiver made the same choice (archiver#193 Phase 1); watcher's loops classify by nothing, so they already back off.
  - **Replicator was the participant that needed it.** Classifying by exception type is what makes this a decision here at all — the other two consumers on the bus reach backoff by default.
  - **Both refusal shapes are exercised against a real broker**, not a raised exception: `tests/worker/test_oom_integration.py` denies a key pattern on a broker it owns and drives the real publish path through it.
- **A dead-letter is two commands: `XADD <topic>.dlq * …` then `XACK <topic> <group> <id>` (#79, broker#2).** Observed through `MONITOR` rather than read off `co_core_aio.bus.dead_letter`, because an ACL that omits a command nobody saw breaks the service at the moment it is already failing. No `MAXLEN`, no `NOMKSTREAM` — the DLQ stream is created on first use — and the anomaly route reaches the same pair after an `XRANGE <topic> <id> <id>` re-read, which a grant covering only the write would jam. All three routes are captured in `test_oom_integration.py`: `content.fetch`, `content.replicate` (one loop serves both, which is a claim about code the ACL is written from and so is captured rather than inferred), and the frame that never decoded.
- **Bus clients are injection-only** — the co-core driver never opens or closes the `redis.asyncio.Redis` client. The worker owns one for its lifetime.
- **Store, then publish — never the reverse.** A crash between the two must not leave a `blob_available` pointing at bytes that are not there: a consumer would read the fact, fail to open the blob, and have no way to ask again. The opposite gap (stored bytes, no fact) repairs itself — the message stays unacked and the reclaim re-runs a handler that content-addressed storage makes a no-op.
- **`occurred_at` is enforced tz-aware UTC on every payload** since co-core v0.7.2 (cannobserv#273). Naive is rejected fail-loud rather than assumed UTC; aware non-UTC is normalized. Load-bearing beyond tidiness — `isoformat()` is half `fetch_failed`'s envelope key, and a naive value would serialize without an offset. Issuer-visible: a naive `occurred_at` now fails `from_wire` and dead-letters as an anomaly.
- **`from_wire`'s topic and message_id are keyword-only** — `from_wire(fields, topic=..., message_id=...)`. The founding plan's API table showed them positionally.
- `sha256` lives at `co_core.pure.util.hashing`, not `co_core.pure.extract` (which carries `simhash`, `Chunk`, and the parsers). Import parsers from submodules — they are not re-exported from `__init__`.
- **The replicate loop writes for `gcs` (#29).** T4's create-if-absent, so a
  redelivery onto matching bytes re-emits the same `public_url` and differing bytes
  are a terminal conflict. `blob_uri` is **never resolved as a path** — fingerprint
  out, compared against `store.uri_for()`. Writers are keyed **by alias**, and every
  refusal happens before any credential is touched. Provider failures classify by
  HTTP status — 4xx closes the command, 5xx/408/429 and statusless errors leave it
  pending — because a transient failure is exempt from the delivery ceiling and
  publishes no fact at all, so misclassifying one strands the issuer forever
  (#29 CR #26, #27).
  The mechanism in full — the four outcomes, the guard order, why the writers are
  keyed by alias — is [STREAMS.md](STREAMS.md); this bullet is the rule
  an agent needs before touching the replicate path.
- **A 429 or a 503 escalates that host's spacing, and only those two (#25).** The
  adaptive politeness Watcher ran on its own fetch path and lost at the Phase 4
  cutover, re-homed here on the charter's second test: an origin's tolerance
  across commands is visible to nobody but the fetcher. `handler.py` reports the
  status to `HostPacer.report_rate_limited`, which multiplies the interval in
  force by `BACKOFF_MULTIPLIER` (×2, first step ≥ 2 s) up to
  `BACKOFF_MAX_HEADROOM` (60 s) **above that host's floor**, and drops it in one
  step once `BACKOFF_DECAY_SECONDS` (1800 s) pass without another refusal. Six
  rules an agent has to keep:
  - **The published number stays the floor.** Escalation may only ever raise the
    effective interval above it. The floor is re-resolved on every read rather
    than folded into the escalation, so a republished policy — or a revocation —
    takes effect on the next command instead of being shadowed by a number this
    service invented. A host that once 429'd can never come back less polite.
  - **The ceiling is headroom above that floor, not an absolute interval.**
    Watcher's was absolute (`BACKOFF_MAX_INTERVAL`) and could be: it had one
    global floor of 1 s and no per-host published numbers. Replicator has them
    (#19), so an absolute ceiling would make the mechanism silently inert for
    every host whose policy already exceeds it — the origins an issuer has
    already marked fragile, and therefore the likeliest to go on refusing. The
    constant is named for the shape (`BACKOFF_MAX_HEADROOM`) because the old name
    is what made the wrong reading plausible (CR #14).
  - **Narrower than transient.** Keyed on the *status*, not on the exception type:
    a 500 or a 504 is a `TransientFetchError` too, but it is an origin bug or a
    slow upstream rather than a statement about request rate, and slowing a host
    for a fault more requests would not have caused buys nothing.
  - **The quiet window is measured from the last refusal, not the last request.**
    Watcher's `Domain.last_request_at` reads like the latter and was written only
    on a 429. The distinction is the difference between working and not: a host
    fetched every minute never has a half-hour gap between requests, so a window
    measured from traffic would hold every escalation forever on exactly the hosts
    busy enough to earn one.
  - **`Retry-After` raises an escalation, never softens one.** Honoured on both
    statuses, in both wire forms (delta-seconds *and* HTTP-date, RFC 9110
    §10.2.3), clamped to the same headroom as any other escalation; a malformed,
    absent, or already-past value falls back to the multiplier rather than
    raising — `delay-seconds` is `1*DIGIT` and is checked as such, not left to
    `int()`, which also accepts `1_0` as ten. Applied
    as `max(multiplier step, header)` because an origin refusing while asking for
    a one-second delay is describing the cadence it is already refusing — reading
    that downward would let a small header disable escalation outright, and being
    more polite than asked violates nothing.
  - **No fact, no setting, no second map.** The escalated interval is transient
    mechanism state: nothing publishes it (a 429 is non-terminal and emits no
    fact at all), the constants are module constants rather than `REPLICATOR_*`
    settings — the numbers an issuer owns travel on `content.fetch-policy` and
    these are not those — and the state shares `HostPacer`'s existing bounded map
    so `MAX_TRACKED_HOSTS` still governs it. `_prune` keeps an entry whose window
    is open even once its interval has elapsed: reclaiming it would honour a
    memory bound by becoming less polite.

## The `replicator:cmd:*` keys

The only keys Replicator writes to the broker that are not streams, and the
question broker#1 carried open from the day it was filed (#80). A keyspace scan
found `replicator:cmd:fetch:<ULID>` strings alongside the ten streams on `db0` —
26 to 40 of them across the epic's life, TTL remaining observed between ~2,300 s
and ~83,000 s — and asked, reasonably, what a change bus was doing holding
another service's state. Four answers. Each is a claim about code, so
`tests/test_broker_keyspace.py` holds them to it: the section another repo's
inventory links to is the one that can rot into a plausible lie without anybody
here touching it.

**What they guard: an already-handled command, per command stream.**
`replicator:cmd:<stream>:<command_id>` — `replicator:cmd:fetch:<command_id>` and
`replicator:cmd:replicate:<command_id>`, namespaced by the spec's
`dedupe_segment` since #29. The un-segmented `replicator:cmd:<id>` form was never
read again after that release and, carrying the same day-long TTL, was gone within
a day of it — today's scan finds none, and a keyspace audit that turns one up is
looking at a restored snapshot. The value is the
`message_id` of the delivery that completed the command, which exists so an
operator can join a key back to a stream entry; no code reads it. Lifetime is
`REPLICATOR_DEDUPE_TTL_SECONDS`, default `86400` — the ~24 h window the audit
measured. No TTL is *provably* sufficient, because redelivery is bounded by the
PEL and the PEL is unbounded in principle; a day covers any realistic outage and
expiry degrades to a re-run.

Only the two closes that **complete** a command write one — the success path and
the completed-without-bytes path (#17) — and both write it *after* the handler.
A retry writes none, a dead-letter writes none, and the blank-`command_id`
refusal happens before the key is ever computed (which is the whole point of it:
an empty id would take `replicator:cmd:fetch:` and make every later blank-id
command a silent no-op, CR #6). One consequence is worth stating outright: a
command that failed permanently is **not** deduped, so an issuer re-publishing
the same `command_id` after a `fetch_failed` gets it handled again rather than
silently acked.

**What reads them: `EXISTS`, once, and nothing else.** `process_message` checks
existence before calling the handler and acks on a hit; existence is the entire
read. `NX` is therefore not the mechanism — it is there so a redelivery cannot
extend a window the first delivery opened. That makes the service's whole
non-stream command surface two commands, `SET key <message_id> NX EX <ttl>` and
`EXISTS key`. No `GET`, no `DEL`, no `TTL`: nothing in `src/` reads the value
back or reaps a key early, and the `SCAN` in [COMMANDS.md](COMMANDS.md) is an
*operator* command that the service never issues — granting any of them here
would widen a grant for a caller that does not exist.

For broker#2, that is two clauses rather than one, because Redis ACLs grant
commands and key patterns separately: `+set` and `+exists` among the commands,
`~replicator:cmd:*` among the key patterns. Neither is scoped by the other —
`+set` is a grant to run `SET` on *any* key the user's patterns already reach —
so a pattern list written to this service's real footprint is what keeps the
command grants narrow in effect.

**What a cold start does without them: re-work, never loss.** The
set-after-success ordering is what makes that true — the key can only ever
short-circuit work already known to have finished, so its absence costs the
short-circuit and nothing else. An empty namespace is reachable only by a
command that is *delivered again*: a PEL entry reclaimed across the restart, or
an issuer re-publishing an id. Each of those re-runs the handler, and the bill is
a re-fetch of the origin (`HostPacer` is in-memory, so its escalations are cold
too), a content-addressed re-store that is a no-op, and a second fact — which is
distinguishable, both envelope keys being per occurrence, and exactly what
contract MUST-4 already requires issuers to tolerate. On `content.replicate` the
re-run is a create-if-absent, so matching bytes re-emit the same `public_url`.
The one saving genuinely lost is the conditional GET: without a key, a reclaimed
304 re-asks an origin that has just said nothing changed.

The epic's fresh-start plan was worrying about the wrong horizon, and the shape
of the mistake outlives it. A `db0` that has lost these keys has lost the streams
and the consumer groups' **PELs** with them — and the PEL is this service's only
durable record of intent, there being no database and no outbox on the consume
path. Restoring an older snapshot brings back unacked entries and their keys
together. So on any future restore or `db0` incident, the dedupe keys are the
cheapest thing in the blast radius, and the exposure they represent is one TTL
window of re-fetches, not one TTL window of anything unguarded.

**One thing does lose them on their own, and it is a broker-side setting.** These
are the only keys Replicator writes with a TTL, and on a broker whose other
tenants write streams they may be the only volatile keys on `db0` at all — so a
`maxmemory-policy` of `volatile-lru` or `volatile-ttl` under pressure would evict
*precisely* this namespace and nothing else, leaving every PEL intact and every
issuer none the wiser. That is the one route to the state the epic's fresh-start
plan feared without a restore, it is reachable by a config change Replicator does
not own, and it is silent. `noeviction` forecloses it by refusing the write
instead (#79), which is why the cap behaves as a publishing incident here rather
than a data one. Observed 2026-09-09: `maxmemory 536870912`, `maxmemory-policy
noeviction`. Anyone changing that policy should read this paragraph first.

**Why they belong on the change bus.** Endorsed deliberately, not tolerated. This
is not application state: Replicator holds no domain vocabulary and no database
by charter ([contracts/replicator-boundaries.md](contracts/replicator-boundaries.md)),
and what these keys carry is per-command *bus* state whose lifetime is the bus's
— sitting beside the PEL entry it short-circuits and the stream that named the
command. Two properties settle where it goes: it must survive the restart that
redelivery follows, so an in-memory set is wrong; and it is safely lossy, so a
durable store is more than it earns — and standing one up is precisely the
"Replicator gets a database" step the charter refuses, reached one defensible
commit at a time. A second store would also be a second thing that can be down
while the broker is up, on the path of every command. The footprint is bounded by
construction — one key per *completed* command, TTL-capped, so the standing count
is the completion rate times the TTL, which is the 26–40 the audit saw and not a
number that grows with uptime. Measured on 2026-09-09: 27 keys, TTL remaining
across all of them spanning 534 s to 84,713 s — the ~24 h window, seen whole
rather than sampled — and every one under the `fetch` segment, `replicate`
completing nothing while no alias table is provisioned, so the second namespace
is empty rather than absent. Under a capped broker they behave like every other
write here: `SET` is `denyoom`, so it is refused, retried, and the clearing edge
is a duplicate fact rather than a loss (#79, above).
