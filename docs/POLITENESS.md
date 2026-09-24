# Per-host politeness

How Replicator spaces its requests to one origin: how a wait is spent, where the
numbers come from, and when a host's spacing escalates. The boundary underneath —
Replicator enforces, the issuer decides, the decision travels over the bus — and
what the policy stream's producer owes are the charter's
([replicator-boundaries.md](contracts/replicator-boundaries.md#worked-example--per-host-politeness));
this file is the consumer-side mechanism.

## Spending a wait

- **Politeness is enforced here and decided elsewhere, and since #19 the numbers actually arrive.** `src/worker/pacing.py` holds a host → last-request map in memory (derived, bounded, rebuildable — one of the three state shapes the boundaries charter permits) and reports a wait; `handler.py::_pace` spends it. **Two ways to spend it, split by duration, because neither works alone on a serial consume path**: a wait ≤ `REPLICATOR_READ_BLOCK_MS` is slept through, a longer one raises `TransientFetchError` and parks the command for `claim_stale`. Park-only was the obvious design and is wrong by 60× — a parked wait cannot be shorter than `REPLICATOR_CLAIM_MIN_IDLE_MS` (60 s) while the normal interval is 1 s, so every host would have been paced at 1/60th of today's rate, silently and in the safe direction. Sleep-only holds every *other* host's commands, and a SIGTERM, behind one origin's politeness. Transient in both directions so being polite can never burn the delivery ceiling. The pacer is built from settings when not injected — the seam fails **open**, and a byte path that quietly stopped pacing is indistinguishable from one that is working. Only a request that actually goes out calls `record()`: stamping a parked attempt would space the origin from requests it never received. **Keyed on the host asked for, not the host reached** — httpx follows redirects inside the driver, so URLs funnelling into one portal hit it at N× the intended rate; recorded as a known limitation in the charter rather than fixed here, because the fix breaks "one request, one record" — **#19 did not resolve it**, it only made the fix more defensible. Signal: `paced_seconds` on the byte path's success line (per-fetch, correlate with `duration_ms`), `tracked_hosts` on `_pace`'s own INFO line, which fires only when a wait was actually spent. The pacer resolves each host's interval through the `policy` seam — a bare callable, not the map, so `handler.py` stays ignorant of `content.fetch-policy` the way `loop.py` stays ignorant of `content.blobs` (#12, #19, watcher#245, cannobserv#285). **Adaptive upward since #25**: a 429 or 503 raises that host's interval above the published floor until a quiet window passes ([below](#escalation-on-429-and-503)).

### Why the wait splits by duration

**Enforcement mechanism:** when a host's bucket is dry, leave the message in the PEL and let
the reclaim bring it back. That is already the idiom for the disk ceiling — a policy check in
the handler, not new machinery — and it inherits the ceiling's safety property: the raise is a
`TransientFetchError`, which is exempt from the delivery ceiling, so a paced command cannot
DLQ for being paced.

**It also inherits the ceiling's granularity, and parking alone is therefore not a sufficient
mechanism.** A parked message returns via `claim_stale`, so the finest per-host spacing it can
express is `REPLICATOR_CLAIM_MIN_IDLE_MS` — **60 s by default**. Watcher's baseline today is
`DEFAULT_MIN_INTERVAL = 1.0` s (`watcher/src/core/rate_limiter.py` as it stood then; the
file was deleted with the cutover), backing off to `BACKOFF_MAX_INTERVAL = 60.0` s. So parking matches the *backoff* case almost exactly and
misses the *normal* case by 60×: implemented naively, every host would be paced at 1/60th of
the rate the cluster runs at now. The failure is silent and in the safe direction, which is
what makes it easy to ship.

The constraint, stated so a design has to answer it: **a serial consume path cannot both sleep
for a short wait and stay available to other hosts.** Sleeping in the handler blocks every
other command in the group — which is why parking exists — and parking cannot express a
sub-reclaim interval.

**Resolved by splitting the wait by duration**, and shipped with what the [charter](contracts/replicator-boundaries.md) now records as the fallback default (then the interim one):
a wait no longer than one poll window (`REPLICATOR_READ_BLOCK_MS`) is slept through in the
handler, and anything longer parks. The bound is derived from an existing setting rather than
given its own, because it is the same quantity — a wait shorter than a poll the loop already
performs adds nothing to the shutdown latency `TimeoutStopSec` is sized for. The stop event
cuts the sleep short, and an interrupted wait is not an elapsed one: the command parks rather
than fetching unpaced on the way out. `src/worker/pacing.py`, `handler.py::_pace`.

## The policy stream

- **`content.fetch-policy` is the third stream kind, and it is read groupless.** `src/worker/policy.py` replays it from `0-0` at boot and tails it thereafter; `FetchPolicyMap` is the state, `run_policy_reader` the poll loop, a peer of the consume loop and the retention sweep in `_run_until_first_exit`. Hyphen, not a third dot segment — `content.fetch.policy` collides with the `<topic>.dlq` derivation of the command stream, so use `streams.CONTENT_FETCH_POLICY`. **No consumer group**: every worker needs every message, and a group here grows a PEL nothing drains — hence no `ack` and no DLQ either, and a frame that will never decode is skipped by forcing the cursor (`seek`). **The tail reads `count=1`; the boot replay reads `REPLAY_COUNT` (#85).** `AsyncBusTailReader` advances its cursor only on a fully-decoded batch, so recovering from a poison frame at `count>1` means draining the well-formed prefix at `count=1` *before* seeking past it (`seek` only moves forward). The tail deletes that ordering rather than implementing it, and it costs nothing — the stream gains three entries every five minutes and the read blocks. The replay cannot: its round trips are the *length* of the stream, not the rate of the producer, and the charter's `MAXLEN` ask is the producer's to honour. Untrimmed at 29,770 entries the live stream took 950 seconds to replay one entry at a time — a sixteen-minute boot in which `ensure_group` had run but neither consume loop had reached a first read, which from the broker is one non-blocking `xread` connection and no blocked `xreadgroup` at all. So the replay batches and pays for the sequence: on a `BusMessageAnomaly` it degrades to `count=1`, drains the prefix, seeks past the poison, and restores the batch on the first read that succeeds **after** the seek. Both halves of that condition rule out one of the two simpler rules (#85 CR 7): restoring on the *skip* spends a whole batch rediscovering the next frame of a run of malformed ones, and restoring on any *success* re-raises on the same poison once per entry of the prefix still ahead of it — `2P + 2` reads rather than `P + 4`. **`AsyncBusTailReader.replay()` is unusable and `PolicyReader` deliberately omits it** (#19 CR #1) — why, under [Applying a policy](#applying-a-policy). Recovery is bounded (`MAX_POISON_SKIPS`, shared with `loop.py`) because an anomaly clears the outage counter, so a run of them would otherwise spin at broker round-trip speed with backoff permanently disarmed; and the boot replay takes the **stop event** so a SIGTERM during an untrimmed stream's replay is not ignored until it finishes. **Replay runs synchronously before the consume loop starts**, or the worker's opening commands are paced against an empty map; a failed replay is absorbed rather than fatal, because the cursor advanced only over what decoded and the tail drains the rest. It brackets itself in the journal — `replaying the fetch policy stream` before, `fetch policy replay complete` after — and **`worker ready` is logged after it**, so the line that names both groups and both consumer names cannot precede the loops by a replay's length (#85). **Applying a policy logs only when the value changes**, live or tombstoned: a cron republish of an unchanged set is the common case and an ungated line made the journal a function of the stream's length rather than of what happened. `tracked_hosts` on the summary line is the gauge that the map is populated; `applied a host fetch policy` is the event that one moved. The four ways to apply a message wrongly and silently, and the two frame hazards, are under [Applying a policy](#applying-a-policy). The reader **absorbs its own failures** like `run_sweeper` does: politeness is not load-bearing for correctness, and a broker that is genuinely gone surfaces through the consume loop, which has the delivery obligations. Its blocking read uses `REPLICATOR_READ_BLOCK_MS`, the same window the consume loop uses and concurrently with it, so `TimeoutStopSec` gains no term (#19, cannobserv#285 — v0.7.7).

### Applying a policy

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
rides the worker's stop event, because how long it runs is the producer's business: the
charter asks the producer to `MAXLEN`, and Replicator cannot enforce it.

## Escalation on 429 and 503

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
