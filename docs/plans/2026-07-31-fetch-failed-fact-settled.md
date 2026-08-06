# The `fetch_failed` fact: how the open question was closed

**Status:** resolved 2026-07-31, recorded so the resolution is not re-litigated as an oversight.
Relocated out of [`docs/contracts/content-fetch-issuer-contract.md`](../contracts/content-fetch-issuer-contract.md)
in #24 — the contract states the shipped rules; this states what the question was and what the
answer deliberately did not cover.

The current contract is MUST-6 and the guarantee/non-guarantee pair in that document.

---

**Resolved.** This document previously carried silence-as-failure as an open question, arguing the
fix needed "a consumer that wants it — Watcher, once Phase 4 has a real pending map to close out."
That consumer arrived ([CannObserv/watcher#241](https://github.com/CannObserv/watcher/issues/241)),
co-core shipped `FetchFailedEvent` in **v0.7.2**
([cannobserv#270](https://github.com/CannObserv/cannobserv/issues/270)), and Replicator publishes
it (#9).
[MUST-6](../contracts/content-fetch-issuer-contract.md#6-handle-fetch_failed-and-keep-a-reaper-anyway)
is the current contract; the tables are the current shape.

Two things the resolution deliberately did **not** do, recorded so they are not re-litigated as
oversights:

- **Non-terminal facts are deferred** (#9 §3). `content.blobs` is broadcast and nothing trims it,
  so emitting a fact per reclaim while an origin is down is unbounded growth on a stream nobody
  prunes. The cost is real and named in MUST-6: a 429 backing off is invisible while it retries,
  which is one of the four Watcher behaviours cannobserv#270 set out to enable. Reopen it on the
  consumer's tracker if the reaper turns out to re-issue under live retries in practice.
- **The rows with no usable `command_id` stay DLQ-only.** Not a scope cut. `FetchFailedEvent`'s
  `command_id` is required, so a fact naming no command closes nothing; and for a frame that
  decoded to a *foreign* payload, the `command_id` it carries belongs to a different command —
  reporting it would be worse than silence, not better. The reaper is the mechanism there,
  permanently.
