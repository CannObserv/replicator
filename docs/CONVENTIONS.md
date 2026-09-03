# Bus & consume-path conventions

The co-core / Redis Streams rules that hold for **every** stream Replicator
touches, with the reasoning that makes each one non-negotiable. `AGENTS.md`
states them in one line each; what a *particular* stream carries, and why, is in
[STREAMS.md](STREAMS.md).

- **Consumers must be idempotent; producers own the outbox.** The cluster split (parent strategy, "Delivery + correctness") assigns the transactional outbox to producers with a DB system of record. Replicator has none — its durable record of intent is the consumer group's PEL, recovered via `claim_stale`. Do not add a Postgres outbox to the consume path.
- **Validation posture:** use the canonical `extra="ignore"` models; **branch on `schema_version` before destructuring**; tolerate additive producer fields. Never use the strict `*Emit` classes on the consume path.
- **Batch-poison caveat:** `AsyncBusConsumer.read(count>1)` raises `BusMessageAnomaly` on a malformed frame *before* returning the well-formed ones in the batch. Read `count=1`, or catch the anomaly and route via `dead_letter`. `from_wire` is deliberately fail-loud.
- **DLQ is a shipped seam, not a TODO:** `dead_letter(message_id, fields)` copies the frame to `<topic>.dlq` and acks the original. Deterministic failure ⇒ DLQ; transient failure ⇒ retry.
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
- **A consumer's name is derived from its group, never from the host (#77).** `<group with dots dashed>-<slot>`, so `replicator.fetch` → `replicator-fetch-1` and `replicator.replicate` → `replicator-replicate-1`, one per group and computed at the wiring seam (`src/worker/main.py::consumer_name_for`). The rule is **stability**: a registration persists until an explicit `XGROUP DELCONSUMER` that nothing calls, so a name carrying anything that varies — a hostname, a pid — mints a fresh registration on each change and abandons the old one holding its PEL, reclaimable only by an `XAUTOCLAIM` at `min_idle_time`. Archiver reached seven registrations on the production broker, six dead (archiver#156); Replicator's hostname-derived spelling additionally read as the *Watcher* service's, this VM being named `watcher`. A stable name makes a restart reuse its registration, so the leak cannot recur — no periodic sweep, and no shutdown hook a `SIGKILL` would skip. Overrides (`REPLICATOR_CONSUMER_NAME`, `REPLICATOR_REPLICATE_CONSUMER_NAME`) are **per group** for the same reason the name is: one process-wide override put a `replicator-fetch-…` consumer inside `replicator.replicate`, a name that misstates its own group. The `-1` is a slot — a second member of a group takes `-2` upward, and must not share a name with the first, since Redis tracks pending entries per consumer name.
- **A failing *message* and a failing *cycle* are different.** `process_message` decides a message's fate; a broker refusing reads/acks/DLQ writes is `run_loop`'s problem — it backs off (`REPLICATOR_ERROR_BACKOFF_BASE_SECONDS` → `_MAX_SECONDS`) and retries, then re-raises after `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` so a permanently wrong `REPLICATOR_REDIS_URL` surfaces as a restart instead of a worker that looks alive while doing nothing. The unit's `StartLimitIntervalSec` is sized against that ceiling — change one, revisit the other.
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
