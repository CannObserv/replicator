# Replicator MVP — Open Questions, Settled

**Date:** 2026-07-31
**Status:** Approved. Settles the three open questions left by the founding plan.
**Parent:** `docs/plans/2026-06-25-replicator-mvp-design.md` §"Open questions for the Replicator team"
**Feeds:** #4 (byte path — steps 4+5), #5 (blob retention), archiver#118 (writeback inversion)

---

## Goal

The founding plan left three questions open. Build-sequence steps 1–3 are shipped (#1, #2, #3);
step 4 cannot start until the first of them is answered. This doc settles all three.

| # | Question | Answer |
|---|---|---|
| 1 | Temp-storage `backend_uri` scheme + local-FS layout | `file://` URI, 2-level sharded content-addressed path |
| 2 | Command issuer during the MVP window | `scripts/seed_fetch.py` — an in-repo CLI publisher |
| 3 | Stop-at-fact vs archiver-writeback | **Stop at fact. Writeback is won't-do** — archiver reacts to `blob_available` itself |

---

## 1. `blob_uri` scheme and local-FS layout

### Decision

`file://` + absolute path, filename = the sha256 fingerprint with a constant `.bin` extension,
sharded two levels by the first four hex characters.

```
REPLICATOR_BLOB_DIR = /var/lib/replicator/blobs
fingerprint         = 9f2a7c1e…            (64 hex)

path                = /var/lib/replicator/blobs/9f/2a/9f2a7c1e….bin
blob_uri            = file:///var/lib/replicator/blobs/9f/2a/9f2a7c1e….bin
```

### Rationale

**The scheme is precedent, not invention.** Archiver's v2 architecture already defines
`content_cache_uri` as a `file://` URI (`file:///var/cache/archiver/<id>.bin`) and its dashboard
already handles non-`http(s)` URIs opaquely — the `open_button` macro deliberately renders nothing
for `file://`/`gs://`/`s3://`. Emitting `file://` means a downstream reader that already understands
archiver's cache field understands `blob_uri` unchanged.

**Sharding bounds directory fan-out.** A flat directory degrades on ext4 past roughly ten thousand
entries without `dir_index` tuning. Two levels of two hex characters gives 65,536 leaf directories
from a uniformly distributed hash — enough that the leaves stay small at any volume this cluster will
reach, and cheap enough that it is two string slices.

**The extension is a constant `.bin`, never derived from `media_type`.** Deriving it would break the
content-addressing invariant: the same octets can legitimately arrive under different `Content-Type`
headers, which would produce two paths for one fingerprint and defeat `exists()` as a short-circuit.
`media_type` travels on the fact, where it describes the fetch rather than the bytes.

**The layout is a local-backend detail, not part of the contract.** `blob_uri` is opaque to
consumers. A GCS backend returns `gs://<bucket>/<fingerprint>.bin` — no sharding, no change to the
consume loop. That is the swap property the founding plan asked for.

### Interface

```python
store(data: bytes, fingerprint: str, media_type: str) -> str   # returns backend_uri
open(fingerprint: str) -> bytes
exists(fingerprint: str) -> bool
```

Three implementation constraints follow from the decision:

- **`blob_dir` resolves to absolute before a URI is built.** `file://` requires an absolute path and
  the setting's default (`blobs`) is relative to the working directory. In production the systemd
  unit's `StateDirectory=replicator` puts it at `/var/lib/replicator/blobs`.
- **`store()` short-circuits on `exists()`** and returns the URI without rewriting. This is the
  idempotence the consume loop already assumes: the `command_id` dedupe key is written *after* the
  handler returns (`src/worker/loop.py`, "Written *after* the handler, deliberately"), so a re-run of
  an already-successful handler is an expected path, absorbed by content-addressed storage.
- **Writes go through a temporary file plus `os.replace`.** Readers key on *presence* at a
  content-addressed path, so a partially written file there is indistinguishable from a complete one.
  An atomic rename is what makes presence mean "complete".

### Rejected

- **Flat directory.** Simpler, and MVP volume is a seed harness — but the fan-out ceiling is a
  latent operational cliff on a shared VM, and the shard math is two slices.
- **Opaque `replicator://` scheme.** Maximum decoupling, but every consumer would need a resolver
  that calls back into Replicator, and it discards archiver's already-shipped `file://` handling for
  no gain in the MVP.

---

## 2. MVP command issuer

### Decision

`scripts/seed_fetch.py` — an in-repo CLI that publishes `ContentFetchCommand` frames to
`streams.CONTENT_FETCH` via `AsyncBusPublisher` + `to_wire`. Takes one or more URLs and mints a
`command_id` per URL.

### Rationale

The MVP's stated job is to prove the command → fetch → store → fact loop **standalone, without
requiring the Watcher cutover**. An issuer inside this repo is the only option that preserves that.
Both alternatives — an early Watcher hook, or an archiver-side trigger — bind the MVP's completion to
another repo's review, CI, and deploy cycle, and cross-repo work in this cluster is filed as an issue
rather than edited directly.

It is not throwaway scaffolding: the same script drives the build-sequence step 6 live integration
test against the Archiver-operated Redis, so it earns a permanent place in `scripts/` alongside
`sync_wheelhouse.py` and `check_redis_floor.sh`.

### Notes for implementation

- **`command_id` is a ULID.** `python-ulid` is already a direct dependency and ULIDs are the cluster's
  identifier convention (archiver depends on the same package).
- **The publisher opens its own Redis client.** Bus clients are injection-only, so the script owns one
  for its run and closes it — the same ownership rule `src/worker/main.py` follows.
- **Seed after the worker is running.** The `replicator.fetch` group already exists on the live broker
  with `start_id="$"`, so a frame XADDed while no worker is polling is still delivered on the next
  read — but a frame added before the group existed would not have been. The group exists; this is a
  note, not a constraint.
- **Integration tests confine themselves to scratch streams** (`replicator.itest.<uuid>`), so the
  script's topic must be overridable rather than hardcoded to `streams.CONTENT_FETCH`.

---

## 3. MVP boundary — stop at the fact; writeback is won't-do

### Decision

Replicator's terminal act is the `blob_available` XADD to `content.blobs`. **Archiver consumes that
fact and writes the SourceRevision itself.** Writeback moves from the founding plan's "MVP+" to
**not Replicator's job at all** — Replicator never calls archiver's HTTP API.

### Rationale

Replicator writing to the bus is the whole point of the boundary. Having it also POST a
SourceRevision would give one outcome two delivery paths — a bus fact and an HTTP call — with two
failure modes and no shared transaction, in a service that deliberately has no database and no
outbox. Archiver has the system of record; it should write to it from a fact it already receives.
This is also strictly less coupling: Replicator needs no archiver client, no archiver credentials,
and no knowledge of the SourceRevision shape.

### Consequence — an archiver contract gap, to be filed

Archiver's current model has **Watcher** POSTing the SourceRevision with `content_cache_uri` set.
Under this split archiver reacts to `blob_available` instead — but the fact deliberately carries
**no `info_source_id`**. cannobserv#266 kept the content contracts domain-agnostic: the fact carries
`content_fingerprint`, `blob_uri`, `size_bytes`, `media_type`, `url`, and the correlating
`command_id`, and the *issuer* is expected to keep its own `command_id → info_source` mapping.

So archiver cannot resolve a SourceRevision's owning InfoSource from the fact alone. Two shapes are
available and **the choice belongs to archiver and watcher, not to Replicator**:

1. Archiver maps on `url` — it already knows its InfoSources' target URLs.
2. Watcher stays in the loop: it consumes `blob_available`, re-associates on its own `command_id`
   mapping, and POSTs as it does today.

The archiver issue states the constraint and leaves the choice open. Nothing here blocks Replicator's
MVP — the fact is emitted either way.

### Rejected

- **Extending the MVP to include writeback.** Pulls an HTTP client, archiver auth, and a second
  failure mode into a phase whose stated job is proving the bus loop.

---

## 4. Retention — out of MVP scope, tracked

The MVP writes blobs and never reaps them. That is acceptable at seed-harness volume, and the
founding plan explicitly scope-cuts retention policy — but it is now a named risk rather than an
unexamined omission, because Replicator has taken on the producer role in archiver's temp-cache
protocol, where **the producer cleans up**.

Filed as its own issue. The constraint it must respect: blob lifetime has to exceed archiver's
consumption latency for the corresponding fact, so a TTL cannot be chosen independently of how
archiver reacts to `blob_available`.

---

## Out of scope

- Durable/permanent replication (GCS/GDrive/IA), RepSpec resolution, `path_template` — founding-plan
  scope cuts, unchanged.
- Watcher cutover (issuing `content.fetch`, consuming `blob_available`) — parent strategy Phase 4.
- Near-dup `simhash`. `co-core[extract]` is installed; the MVP loop needs `sha256` only.
- Blob GC/retention implementation — decided out, tracked separately (§4).

## Follow-on work

| Where | What |
|---|---|
| #4 | Byte path (steps 4+5) — carries the §1 decision |
| #5 | Blob retention / TTL sweeper (§4) |
| archiver#118 | Consume `blob_available` and write the SourceRevision; resolve the `info_source_id` gap (§3) |
| — | Build-sequence step 6: seed harness + live end-to-end integration test (§2) |
