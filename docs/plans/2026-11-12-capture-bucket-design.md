---
title: Captures in their own bucket, co-gcs-captures (archiver#380 D4)
date: 2026-11-12
status: approved 2026-11-12 (operator)
issue: "#189 (epic #154)"
---

# Captures in their own bucket

## Intent

Archiver will show viewport captures on its dashboard. It may never read raw fetched bytes: its
`content.blobs` boundary has no exceptions. Today a capture shares both of Replicator's stores, and the
`blobs/<sha256>.bin` key scheme, with raw blobs:

- **temp:** `gs://co-gcs-blobs/blobs/…`;
- **kept:** `gs://co-gcs-replicator/blobs/…`.

So no grant can admit captures without also admitting raw blobs. The operator decided D4 on
2026-11-11 ([archiver#380](https://github.com/CannObserv/archiver/issues/380#issuecomment-6809278639)):
one new bucket, `co-gcs-captures`, holds every capture byte, temporary and kept. Archiver gets
`objectViewer` on that bucket alone. This design doesn't reopen D4. It settles how Replicator lands
bytes there and what the operator provisions.

**This is a contract change** to the capture, persist and replicate contracts. Each gets the same
answer: where the bytes land changes, the wire doesn't.

### Requirements (#189 body)

1. Every capture byte, temp and kept, lives only in `gs://co-gcs-captures`, and no raw fetched blob
   ever does.
2. `capture_stored.blob_uri` names the temp object there, and `blob_expires_at` still bounds it.
3. A `content.persist` whose `blob_uri` is in `co-gcs-captures` keeps the image in `co-gcs-captures`,
   at `gcs_uri("co-gcs-captures", digest, prefix=P)`. `blob_persisted` doesn't change.
4. **No wire change.** `ContentCaptureCommand`, `CaptureStoredEvent`, `ContentPersistCommand` and
   `BlobPersistedEvent` stay as they are. No co-core or broker change.

### Non-goals

- **Migrating the two existing captures** (`9a1d0471…`, `a0b1c585…`). Measured 2026-11-12, each is
  in both `co-gcs-blobs` (with `customTime`) and `co-gcs-replicator` (without). They stay there, and
  Archiver shows them as metadata only.
- **Publishing a capture** ([archiver#411](https://github.com/CannObserv/archiver/issues/411)).
- **Archiver's reader identity**, `archiver-capture-reader`. It's tracked on archiver#380, and the
  operator provisions it beside this.

## Design

### The setting and the two prefixes (weigh 2)

One new setting, **`REPLICATOR_CAPTURE_BUCKET`**, default empty. Like the other two buckets, it's
host configuration with no compiled-in default. Two prefixes, both **constants**:

| Tier | Key | URI | Store |
|---|---|---|---|
| temp | `temp/<sha256>.bin` | `gs://co-gcs-captures/temp/<sha256>.bin` | `GcsBlobStore(bucket, prefix="temp", touch_on_rereference=True)` |
| kept | `blobs/<sha256>.bin` | `gcs_uri("co-gcs-captures", digest)` | `GcsBlobStore(bucket)`: co-core's `DEFAULT_PREFIX`, touch off |

They're constants and not settings because the provisioning carries each one somewhere this process
can't see:

- **`temp/`** is in the lifecycle rule's `matchesPrefix` and in the IAM condition.
- **`blobs/`** is in Archiver's derivation (`permanent_store.CAPTURES_KEPT_PREFIX`, archiver#380
  issuecomment-6809635925).

A knob would be a second place to disagree with each, which is `config.py:103-111`'s argument for the
permanent store, made twice. The kept prefix is the default, so the persist contract's rule holds
unchanged: a reader derives the location with `gcs_uri(bucket, digest)`, no prefix argument.

`temp/` and not `blobs/` for the temp tier, though `REPLICATOR_BLOB_PREFIX` defaults to `blobs`. In
one bucket the two tiers need different keys, and the kept tier owns the default.

### Which store each handler gets

| Handler | Writes to | Reads (T3a sources) |
|---|---|---|
| fetch (HTTP and `render="browser"`) | raw temp (`co-gcs-blobs`) | — |
| capture | **captures temp** | — |
| persist | the kept store **of the source's family** (below) | raw temp, raw kept, captures temp, captures kept |
| replicate | the alias destination | raw temp, raw kept; **a captures URI is refused** |

`run()` builds the captures stores beside the other two, in `_prepare_storage`'s pattern, and fails
the boot on a preflight failure, as the others do. It logs one line, greppable as the read-back:

```
storing captures in their own bucket  capture_bucket=co-gcs-captures capture_temp_prefix=temp capture_kept_prefix=blobs blob_ttl_seconds=…
```

**`render="browser"` stays a raw blob.** It is a fetch: its bytes are a document, announced on
`content.blobs` to consumers who aren't Archiver. Requirement 1's second half covers it.

### Persist routes by family (weigh 5, requirement 3)

A family is a temp store and its kept store: raw `(co-gcs-blobs, co-gcs-replicator)`, captures
`(co-gcs-captures/temp, co-gcs-captures/blobs)`. Persist picks the family whose store **minted**
`blob_uri`, using the same exact `uri_for(fingerprint)` comparison T3a already makes, with no parsing.
It then runs today's handler unchanged against that pair. That covers T3a, the digest check,
read-then-create, `blob_expired` resolved by the kept store, and the no-op when `blob_uri` already
names the kept store.

- **Neither family can reach the other's kept store.** The pair is chosen once, and every later step
  takes its stores from it. A test for each direction goes red first.
- **No family matches**, so the raw family handles it. Its refusals are today's: `invalid_source`,
  or `blob_expired` for a URI from the other backend. A stranger URI reads as it reads now.
- **A capture's old location** (`gs://co-gcs-blobs/blobs/<sha>.bin`) is still a raw-temp URI, so its
  persist is kept in `co-gcs-replicator`. That is what "no migration" implies, and it is Archiver's
  derivation exactly: a `blob_uri` outside `co-gcs-captures` is kept in `co-gcs-replicator`.
- **No new `reason` token.** The registry is unchanged.

**Refused at boot, not per command:**

- `REPLICATOR_CAPTURE_ENABLED` with the `gcs` backend needs `REPLICATOR_CAPTURE_BUCKET`. Captures would
  otherwise have nowhere to land.
- `REPLICATOR_CAPTURE_ENABLED` with `REPLICATOR_PERSIST_ENABLED` needs `REPLICATOR_CAPTURE_BUCKET` on
  any backend. Persist would otherwise have no kept store for a capture, and the only kept store is the
  raw one.
- `REPLICATOR_CAPTURE_BUCKET` may equal neither `REPLICATOR_BLOB_BUCKET` nor
  `REPLICATOR_PERMANENT_BUCKET`. Equal to either one, requirement 1 is false by configuration.

A host with persist on, capture off and no capture bucket has no captures family. A captures URI
sent to it is then `invalid_source`, which is true of that host.

### Replicate refuses a capture on purpose

Today a `content.replicate` naming a capture's temp URI passes T3a, because the URI is the temp
store's. After the split it isn't. Publishing captures is archiver#411, and is out of scope, so the
refusal is right, but it should be explicit rather than a side effect of the move.

`build_replicate_handler` gets the captures stores as **refused sources**. A `blob_uri` that one of
them minted is refused `invalid_source`, with the detail "names a capture; captures are not
replicated (archiver#411)", before any read and before `_names_another_backend`. The order matters on
a `local` host with no permanent store: without the check, a `gs://co-gcs-captures/…` URI would be
refused **`blob_expired`**, which tells the issuer to re-fetch. The replicate contract's
`invalid_source` row names captures.

The alias-table guard (`main.py:848`, `host_stores=`) adds the capture bucket. No alias may bind a
bucket this host keeps bytes in.

### The `local` backend (weigh 3)

Captures get **their own root, `<REPLICATOR_BLOB_DIR>/captures/`**, and aren't shared. Persist routes
by URI, and under a shared root a capture's `file://` URI and a fetched blob's are the same string.
Persist would then send a capture's kept copy to the raw kept store, which on a `local` host with
persist on is still `co-gcs-replicator`. A shared root makes requirement 1 depend on the backend. Its
own root keeps one invariant everywhere, and the routing tests run on local stores.

What it costs, and what is done about it:

- **The sweep has to cover it.** `sweep()` globs `??/??/*.bin` from `blob_dir`, so a nested
  `captures/` is invisible to it. `run_sweeper` sweeps both roots in one pass and sums the results
  into the one `BlobUsage`. The ceiling therefore still bounds one disk, which is what it is for.
  `captures` is eight characters, so the `??` shard globs can never match it.
- **The directory is made only when capture is on.** The store object always exists, and a URI
  under a missing root just doesn't exist.
- **No `warn_if_unreachable`.** Nothing on this host reads a capture's `file://` URI.

### The cross-kind dedupe goes away (weigh 4)

Today a fetched PNG and a capture with the same bytes are one object (`capture.py:121`'s
`exists`-then-`store` on the shared store). Afterwards they're two, in two buckets (two roots under
`local`), each with its own TTL clock and its own kept copy. That's correct under requirement 1, and
Archiver independently expects it: a re-capture after the split is owed its own copy in the new
bucket. A same-digest pair is rare: a raw fetch of a PNG equal byte-for-byte to a fresh render.

### The bucket (weigh 1: one bucket, two tiers)

The operator decided one bucket. It conflicts with the two-tier shape in three places, and each is
settled here.

**Soft delete: keep GCS's default 7 days.**

- **The kept tier keeps a 7-day undo** of an operator's mistaken delete, as `co-gcs-replicator` has.
  It is the tier that matters: a render records a moment, so a lost kept capture can't be recreated
  by re-capturing.
- **The temp tier loses nothing functionally, and pays for it.** Lifecycle deletes are soft-deleted
  too, so each reaped temp capture is billed up to 7 more days. That is the cost `co-gcs-blobs` turned
  soft delete off to avoid (`INFRASTRUCTURE.md:150`). Here it's ~0.45 MB per capture, at two captures
  to date: noise.
- **Revisit** if capture volume reaches the raw tier's.

**The touch grant: conditioned to `temp/`.** The temp store's `customTime` refresh is a `patch()`,
which needs `storage.objects.update`. The kept tier must not hold it, because a kept object has no
clock to move and its writer is append-only.

- The worker gets `objectCreator` + `objectViewer` on the whole bucket.
- The custom `replicatorTempBlobWriter` role (create, get, list, update) is bound **with an IAM
  Condition**: `resource.name.startsWith("projects/_/buckets/co-gcs-captures/objects/temp/")`.
  `update` therefore exists only under `temp/`.
- `list` is a bucket-level check, so the condition can't scope it, and doesn't need to: it comes
  unconditionally from `objectViewer`, which the boot preflight's one-object listing needs.
- Conditions need uniform bucket-level access, which the bucket has, and IAM policy version 3, which
  `gcloud` sets.
- **Delete is granted nowhere.** Expiry is the lifecycle rule's.

**The lifecycle rule: both rules scoped to `temp/`.**

- `daysSinceCustomTime: 8` (≥ `REPLICATOR_BLOB_TTL_SECONDS`' 7 days, as on `co-gcs-blobs`), with
  `matchesPrefix: ["temp/"]`.
- The `age: 365` cost backstop, also with `matchesPrefix: ["temp/"]`. **This is the one that could
  hurt.** An `age` rule matches every object, so unscoped it would delete each kept capture after a
  year.
- The `customTime` rule has defence in depth on top of its prefix. Kept objects are written with
  touch off and carry no `customTime`: measured on both production kept captures, and enforced by
  `gcs.py`'s `touch_on_rereference=False`. GCS documents that `daysSinceCustomTime` is
  [never satisfied for an object without one](https://cloud.google.com/storage/docs/lifecycle).

### Who reads the bucket (weigh 6)

| Identity | On `co-gcs-captures` |
|---|---|
| `co-gcs-replicator-writer` (the worker) | `objectCreator` + `objectViewer`; the temp role conditioned to `temp/` |
| `archiver-capture-reader` (Archiver, archiver#380) | `objectViewer`, and nothing else anywhere |
| `co-gcs-publication-writer` | **nothing**. Its `objectViewer` on `co-gcs-blobs` and `co-gcs-replicator` is for replicate's server-side copy, and replicate refuses captures. archiver#411 decides whether that changes |
| `co-gcs-blob-reader` (Watcher) | **nothing**. Watcher reads `content.blobs`, and a capture is never on it |

## Test plan

**Red first, on the routing:**

- A capture lands in the captures temp store, never the raw one, under `local` stores.
- A fetch (HTTP and `render="browser"`) never lands in the captures store.
- A persist whose `blob_uri` the captures temp store minted lands in the captures kept store, and the
  raw kept store is untouched. The mirror holds too: a raw source never reaches the captures kept
  store. Both are also tested from the kept URI and from an expired temp blob.
- A persist of a capture's **old** `co-gcs-blobs` URI goes to the raw kept store.
- Replicate refuses a captures URI `invalid_source`, including on a `local` host with no permanent
  store, which today answers `blob_expired`.

**Settings and wiring:**

- the three validators;
- the alias guard covering the capture bucket;
- `run()` wiring each handler to its store, with the boot line;
- the sweep covering both local roots into one usage figure;
- **a third derived scrub guard**: every `*_bucket` setting is in `PRODUCTION_ENV`. #182's guard
  derives only `bool` fields and #187's only credentials, so the new variable is caught by derivation
  and not by hand.

**`-m gcs`: no new test twin.** One added test, beside `test_persist_gcs.py`, uses the permanent twin
`co-gcs-test-replicator` as the captures bucket. It stores at `temp/` with touch on, persists into
`blobs/` with touch off, asserts both keys and their `customTime` presence and absence, then deletes
both. The test SA holds `objectAdmin` there, so a twin can't test the IAM condition anyway. That proof
is the host read-back's object-level probe (step 5 below), the only place the conditional grant
exists.

**Gates:** the default suite (baseline 1961 passed / 2 skipped), `-m gcs`, `-m browser`, `tests/ci`,
ruff, and CI green by job.

## Docs

- **Capture contract:** where `blob_uri` lands, the Status line (also folded into #184, and whichever
  lands second rebases), and C3's "persist's source guard accepts it".
- **Persist contract:** sources, the two families and their destinations, enabling.
- **Replicate contract:** `invalid_source` for a capture.
- **Others:** `INFRASTRUCTURE.md` (bucket table, the captures bucket's section, the provisioning
  record), `STORAGE.md` (the local captures root, both prefixes), `ENVIRONMENT.md`
  (`REPLICATOR_CAPTURE_BUCKET`).

## Operator provisioning (before the restart)

Run from a workstation as project owner, in project `co-gcs`, in this order. The commands are for
review here; the issue carries the final copy.

**1. The bucket.** Uniform access, public access prevented, 7-day soft delete (the default, stated
explicitly), and the lifecycle rule:

```bash
cat > /tmp/co-gcs-captures-lifecycle.json <<'JSON'
{"rule": [
  {"action": {"type": "Delete"}, "condition": {"daysSinceCustomTime": 8, "matchesPrefix": ["temp/"]}},
  {"action": {"type": "Delete"}, "condition": {"age": 365, "matchesPrefix": ["temp/"]}}
]}
JSON
gcloud storage buckets create gs://co-gcs-captures --project=co-gcs \
  --location=US-WEST1 --default-storage-class=STANDARD \
  --uniform-bucket-level-access --public-access-prevention \
  --soft-delete-duration=7d \
  --lifecycle-file=/tmp/co-gcs-captures-lifecycle.json
```

**2. The worker's grants.**

```bash
SA=serviceAccount:co-gcs-replicator-writer@co-gcs.iam.gserviceaccount.com
gcloud storage buckets add-iam-policy-binding gs://co-gcs-captures --member="$SA" --role=roles/storage.objectCreator
gcloud storage buckets add-iam-policy-binding gs://co-gcs-captures --member="$SA" --role=roles/storage.objectViewer
gcloud storage buckets add-iam-policy-binding gs://co-gcs-captures --member="$SA" \
  --role=projects/co-gcs/roles/replicatorTempBlobWriter \
  --condition='expression=resource.name.startsWith("projects/_/buckets/co-gcs-captures/objects/temp/"),title=temp-tier-only,description=customTime touch on temp/ only (replicator#189)'
```

**3. Read back** without `--format`, because a wrong key in a projection prints nothing
(`INFRASTRUCTURE.md`):

```bash
gcloud storage buckets describe gs://co-gcs-captures
gcloud storage buckets get-iam-policy gs://co-gcs-captures
```

Expect:

- `location: US-WEST1`, `uniform_bucket_level_access: true`, `public_access_prevention: enforced`;
- `soft_delete_policy.retentionDurationSeconds: '604800'`;
- both lifecycle rules, each with `matchesPrefix: [temp/]`;
- three bindings for the writer, the third with the condition;
- no binding for `co-gcs-publication-writer` or `co-gcs-blob-reader`.

**4. The env line**, in `/etc/replicator/.env`, before the deploy:

```
REPLICATOR_CAPTURE_BUCKET=co-gcs-captures
```

Writing the line before the deploy is harmless: today's code ignores an unknown variable
(`extra="ignore"`). Deploying the code first, without it, fails the boot: production has capture on
and the `gcs` backend, so the new validator refuses the configuration. That is why provisioning and
`.env` come first.

**5. After the restart**, the object-level probe for the conditional grant. I can run it on the VM as
the worker's key, and leave the cleanup of its two objects to the operator, since the worker holds no
`delete`:

- a create plus a `customTime` patch under `temp/` succeeds;
- the same patch under `probe/` is refused **403**.

Full ordered host steps are posted on #189.

## Risks and accepted limits

- **Nothing keeps the lifecycle rule, the IAM condition and the `temp` constant in step.** Only the
  boot line and the provisioning record name them together. That is the same limit as the raw temp
  tier's rule against `REPLICATOR_BLOB_TTL_SECONDS`.
- **No fact names the old bucket after the restart.** A capture still in the PEL wasn't published,
  so its redelivery renders again under the new code and lands in `co-gcs-captures`. One that did
  publish was already acked. What stays old is a persist of a pre-split URI: it is kept in
  `co-gcs-replicator`, which is Archiver's derivation (corrected in CR 4).
- **Archiver's reader is verified on Archiver's side.** Nothing here can see the
  `archiver-capture-reader` grant.
