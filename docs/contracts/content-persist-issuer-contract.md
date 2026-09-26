# The `content.persist` issuer contract

**Status: shipped, and off until an operator turns it on.** `src/` runs a `content.persist` loop when
`REPLICATOR_PERSIST_ENABLED` is set. It is off by default on every host, because the loop creates its
consumer group at boot, and a broker that has not granted `content.persist` refuses that once and for
all. The grant is CannObserv/broker#64. The loop copies a blob into the permanent content-addressed
store and reports the outcome on `content.artifacts` (#114 step 6, cannobserv#493).

**Audience:** Archiver, the sole issuer ([archiver#276](https://github.com/CannObserv/archiver/issues/276)).

**Companions.** [`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md) supplies the
blobs this command keeps. [`content-replicate-issuer-contract.md`](content-replicate-issuer-contract.md)
supplies the source guard (T3a) this handler reuses, and the MUST-7 relaxation that lets a replicate
read from the permanent store. The wire models are co-core's `ContentPersistCommand`,
`BlobPersistedEvent` and `PersistFailedEvent` (0.19.6). Their docstrings and cannobserv
`docs/CHANGE_BUS.md` § "Archiver-issued persist" state the shape; this document states the obligations
and the refusal vocabulary.

---

## What the command means

> **"Keep the bytes whose sha256 is `content_fingerprint`, permanently."**

| Field | Meaning |
|---|---|
| `command_id` | The correlator. Both outcome facts carry it; nothing else is echoed. |
| `content_fingerprint` | The **raw-bytes** sha256, bare hex: `BlobAvailableEvent.content_fingerprint`, or `SourceRevisionObservedEvent.blob_fingerprint`. Never an extracted fingerprint. |
| `blob_uri` | Where to read the bytes. It is resolved by fingerprint and never parsed as a path (T3a). |
| `media_type` | Stored as object metadata on the first write. Replicator holds only bytes and cannot recover it. |

**The digest is the address.** The object lands at `gs://<permanent bucket>/blobs/<sha256>.bin`. The
success fact carries no URL, and a reader derives the location with co-core's
`blobstore.gcs_uri(bucket, digest)`, from the bucket it is configured with: `co-gcs-replicator` in
production. **Record the digest, never a URL.**

**No domain echo, on either fact.** A persisted blob belongs to every revision whose bytes hash to it,
so an echoed domain key would claim a one-to-one link that is false. Correlate on `command_id` and map
it to your own revision rows.

## What Replicator does

1. **T3a on the source.** `blob_uri` must be a URI one of this host's stores minted: the temp store, or
   the permanent store itself. It is matched exactly, by deriving the URI from the fingerprint, and the
   message's path is never read.
2. **The two digests must agree.** The fingerprint in `blob_uri` must equal `content_fingerprint`.
3. **Read, then create if absent,** into the permanent store. The store hashes the bytes it is about to
   write (cannobserv#492), so bytes that do not match their name never reach the tier nothing may
   delete from. Touch is off: a permanent object has no retention clock.
4. **Then `blob_persisted`**, with the digest and the stored size. A fact is never published for absent
   bytes.

**A second persist of the same bytes is a no-op success** that publishes `blob_persisted` again, and so
is a persist whose `blob_uri` already names the permanent store. So is one whose temp blob has expired
when the permanent store already holds its digest: the permanent store decides whether the bytes are
kept, and a temp blob gone *after* a persist is not a loss. An object already at the address holds
these bytes by construction, so no conflict outcome exists. A later persist under another `media_type`
is still a success and does not change the stored type.

## What the issuer must do

**P1 — Issue on receipt of the revision.** The temp tier keeps a blob for at least seven days from its
last fetch reference ([fetch contract MUST-7](content-fetch-issuer-contract.md#7-copy-the-bytes-before-the-blob-expires)),
and a persist that runs after that is refused `blob_expired`, unless an earlier persist already kept
the digest. `blob_expires_at` on the `blob_available`
fact is the value to schedule against. Persisting is what removes the clock: once `blob_persisted`
arrives, publish from the permanent URI, which a replicate command accepts with no expiry (replicate
contract, MUST-7).

**P2 — Correlate on `command_id`; expect more than one success.** Delivery is at-least-once, and a
redelivery re-emits `blob_persisted`. Every fact's envelope key is `command_id:occurred_at`, so
duplicates are distinguishable, never collapsed. **`occurred_at` is an upper bound on the write, not
the write time.** For the earliest time the bytes were kept, take the minimum over every fact for the
*digest*, not the first fact for one command.

**P3 — Branch on `terminal`, then treat an unknown `reason` as opaque.** Every fact this service emits
today is terminal. A failure that is still retrying publishes nothing, so silence means "still trying".
**Keep a reaper** that re-issues under a fresh `command_id`, as the fetch contract's MUST-6 requires.

**P4 — Send the raw-bytes digest and the URI the fetch fact gave you, verbatim.** Do not rebuild
`blob_uri` from a bucket name. The URI a store minted is the only one T3a accepts.

**P5 — Adopt co-core 0.19.6 before issuing.** An older co-core raises
`BusMessageUnknownEventTypeError` on `blob_persisted`, so your consumer must decode the new facts before
the first command it sends produces one.

## What Replicator refuses

Every refusal is terminal and carries a `reason` on `persist_failed`. The vocabulary is producer-owned:
co-core types `reason` as a plain `str`, and this table is the registry. The first two rows are fixed by
co-core's contract shape.

| Condition | `reason` | Issuer's remedy |
|---|---|---|
| `blob_uri` is not a URI this host's stores minted, `content_fingerprint` is malformed, or the two name different digests | `invalid_source` | Fix the plumbing; re-fetching fixes nothing |
| The bytes left the temp tier before the persist ran, and the permanent store does not hold the digest | `blob_expired` | A fresh fetch, then a persist of what it returns |
| The stored bytes do not hash to their own fingerprint, so the permanent store refused them | `source_corrupt` | A fresh fetch, as for `blob_expired`. It is a storage fault on Replicator's side, and its journal records it |
| The permanent store refused the write with a terminal status (403, 404) | `store_refused` | Wait for the operator, then re-issue under a fresh `command_id`. The host cannot write there |

The loop's own two apply to this stream as to the others: **`unsupported_schema_version`** (a command
this version cannot read) and **`handler_error`** (a defect on Replicator's side, after the delivery
ceiling). `content_fingerprint` is echoed on every failure verbatim, malformed or not.

**What stays open instead.** A store 5xx, 408 or 429, or a failure with no status at all, is retried with
no fact, exempt from the delivery ceiling. A failure *reading* the source with a terminal status is left
to the ceiling and closes as `handler_error`: no token here describes "this worker cannot read its own
temp store".

## Enabling it on a host

- **`REPLICATOR_PERMANENT_BUCKET`** names the permanent store, and must be set. Settings construction
  refuses persist without it.
- **`REPLICATOR_PERSIST_ENABLED=true`** starts the loop. Set it only after the broker grants
  `content.persist` to the `replicator` user and the `replicator.persist` group exists (broker#64).
  Before that, the boot-time group creation is refused and the worker does not start.
- The worker's identity needs `create` and `get` on the permanent bucket (`objectCreator` +
  `objectViewer`), and never `update` or `delete`. It also needs `get` on the temp bucket.
- `worker ready` reports `persist: enabled` with its group and consumer, or `persist: disabled`.

## Charter check

Against the three tests in [replicator-boundaries.md](replicator-boundaries.md#the-three-tests):

1. **No durable per-resource history here.** Replicator keeps no record of what it persisted. The
   bucket is a content-addressed store, written the way a replicate destination is, and **which**
   revisions to keep is the issuer's policy, decided by issuing the command. The issuer records the
   digests.
2. **No cross-command coordination** over a resource only the fetcher sees.
3. **No domain vocabulary**, read or carried. The payloads echo nothing, so `persist.py` and
   `persist_reporter.py` stay off the echo allowlist.
