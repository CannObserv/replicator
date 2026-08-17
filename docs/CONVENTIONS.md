# Bus & consume-path conventions

The co-core / Redis Streams rules that hold for **every** stream Replicator
touches, with the reasoning that makes each one non-negotiable. `AGENTS.md`
states them in one line each; what a *particular* stream carries, and why, is in
[ARCHITECTURE.md](ARCHITECTURE.md).

- **Consumers must be idempotent; producers own the outbox.** The cluster split (parent strategy, "Delivery + correctness") assigns the transactional outbox to producers with a DB system of record. Replicator has none — its durable record of intent is the consumer group's PEL, recovered via `claim_stale`. Do not add a Postgres outbox to the consume path.
- **Validation posture:** use the canonical `extra="ignore"` models; **branch on `schema_version` before destructuring**; tolerate additive producer fields. Never use the strict `*Emit` classes on the consume path.
- **Batch-poison caveat:** `AsyncBusConsumer.read(count>1)` raises `BusMessageAnomaly` on a malformed frame *before* returning the well-formed ones in the batch. Read `count=1`, or catch the anomaly and route via `dead_letter`. `from_wire` is deliberately fail-loud.
- **DLQ is a shipped seam, not a TODO:** `dead_letter(message_id, fields)` copies the frame to `<topic>.dlq` and acks the original. Deterministic failure ⇒ DLQ; transient failure ⇒ retry.
- **A frame that fails to decode has no fields.** `from_wire` raises from *inside* `read`/`claim_stale`, so the anomaly carries `topic` + `message_id` only — but `dead_letter` XADDs the fields it is given and `XADD` rejects an empty map. Re-read the raw frame by id (`XRANGE topic id id`) and fall back to a synthesized record when the entry has been trimmed. `src/worker/loop.py::dead_letter_anomaly`.
- **`from_wire`'s dispatch table is global.** A `blob_available` frame XADDed to `content.fetch` decodes cleanly into the wrong model rather than raising — `isinstance`-check the payload before destructuring.
- **`claim_stale` is the retry path, not just crash recovery.** A transiently-failed message is left unacked and comes back through the same reclaim, so retry cadence = `REPLICATOR_CLAIM_MIN_IDLE_MS`. Call it with `count=1`: XAUTOCLAIM transfers ownership and resets the idle clock on every entry it returns *before* co-core decodes them, and it restarts at `0-0` each call, so a poison entry jams recovery permanently unless it is DLQ'd first.
- **Retry accounting is XPENDING's `times_delivered`**, not a side counter. It only advances on a reclaim.
- **A consumer appears in `XINFO CONSUMERS` only after its first *delivered* message.** An empty poll registers nothing, so an absent consumer entry is not evidence a worker is down — a liveness check built on it reports every idle worker as dead. Recovery is unaffected: `claim_stale` reclaims by group and idle time, not by a pre-existing consumer entry.
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
  keyed by alias — is [ARCHITECTURE.md](ARCHITECTURE.md); this bullet is the rule
  an agent needs before touching the replicate path.
