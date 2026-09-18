# Replicator Infrastructure

Where this service runs and what it talks to: the dedicated VM, the broker it
consumes, and the GCS buckets on both sides of the test/production boundary.
The unit's own lifecycle — how it starts, what guards it, what it reports when
it fails — is in [DEPLOYMENT.md](DEPLOYMENT.md).

Own exe.dev VM, **`co-replicator`** (`pdx`), tailnet **`replicator`** — off the shared
`watcher` VM since 2026-09-11 (#88), and also the dev workspace. The node:
[reference/tailscale.md](reference/tailscale.md).

| Service | Framework | Port | Managed by |
|---|---|---|---|
| Worker (live) | asyncio bus consumer | — | `systemctl` (`replicator.service`) |
| API (dev) | FastAPI | 8001 | manual uvicorn |

The worker binds no port; 8000 is reserved for the API, 8001 is dev. No tailnet rule reaches either; the dev server is `https://co-replicator.exe.xyz:8001/` behind the exe.dev proxy's login gate.

## The broker is `co-broker` — Replicator connects, it does not run one

The change bus runs on `co-broker` (tailnet `broker`), operated from CannObserv/broker (broker#1). Replicator is a **client** — the `replicator` ACL user (broker#2) — and never ships a broker. The `redis-server` here is a binary for tests that spawn their own; its service is masked.

**Redis ≥ 7.0 is Replicator-critical.** Replicator is the cluster's first user of `AsyncBusConsumer.claim_stale`, which reads `XAUTOCLAIM`'s three-element reply — the deleted-ids element added in Redis **server** 7.0. Below that, the crash-recovery path raises. `scripts/check_redis_floor.sh` guards this as an `ExecStartPre`. On a cold boot `After=tailscaled.service` is not enough — MagicDNS answers `broker` with no address for a moment after tailscaled starts — so the check retries an unreachable broker for `REPLICATOR_REDIS_FLOOR_WAIT` (#88). (The broker runs 7.0.15.)

The **redis-py client** resolves `>=5,<8` transitively via `co-core-aio[bus]`. Don't re-pin it narrower.

## The semantic index is `co-index` — a store this repo is a client of (#92)

The cohort's shared Qdrant runs on a fifth VM, **`co-index`** (tailnet `index`),
built and operated from CannObserv/notifier (notifier#57). Replicator is a
**client**: no Qdrant, no Ollama, no Docker image here. `docker.service` and
`docker.socket` are disabled on this VM, and `/var/lib/docker` has never
existed.

| | Value | Why that form |
|---|---|---|
| Qdrant | `https://index.taild0fb76.ts.net:6333` | **The full MagicDNS name** — `index` alone is not in the certificate's SAN. TLS is not optional: upstream refuses to send `QDRANT_API_KEY` over a non-TLS, non-loopback connection, which is why the store serves TLS at all. |
| Ollama | `http://index:11434` | `nomic-embed-text`, 768 dimensions — the store's, not this repo's choice. **Unauthenticated**: any node the ACL admits can reach it. Tailnet reachability is not authorization. |
| Collections | `codebase_replicator`, `context_replicator`, `replicator_symgraph_{file,index,meta}` | Named by `projectId` in `.socraticode.json`. Without that file the id is `sha256(<absolute path>)[:12]` — `2a818eb7302d` for this checkout — which is not even per-host. |
| Key | `QDRANT_API_KEY`, `.claude/settings.local.json` | See [ENVIRONMENT.md](ENVIRONMENT.md). |

Both endpoints are reached over the tailnet, so the ACL must admit this VM to
`tag:index:6333,11434`. **A missing ACL rule presents as a DNS failure, not a
permission denial** — the same shape as #88 and archiver#193.

**One host indexes a `projectId`, and for `replicator` that host is this one.**
Nothing enforces it: a clone elsewhere that holds the key writes to the same
collections, because a session's startup auto-resume updates them and its status
and query calls start the file watcher. A checkout that must exist on another
machine opts out in its own git-ignored `.claude/settings.local.json` with
`SOCRATICODE_AUTO_RESUME=off` and `SOCRATICODE_WATCHER=off`. It can still search.

## The temp-blob buckets — live since 2026-08-20 (#7)

The `gcs` blob backend is **what this VM runs**: `/etc/replicator/.env` sets
`REPLICATOR_BLOB_BACKEND=gcs` / `REPLICATOR_BLOB_BUCKET=co-gcs-blobs` (flipped
2026-08-20, after watcher#275 deployed `gs://` support). The compiled-in default
stays `local` — permanently, see ENVIRONMENT.md — so a fresh clone, a test run,
and any host without this env still get the filesystem backend.

| | Production temp store | Test temp store |
|---|---|---|
| Bucket | `gs://co-gcs-blobs` | `gs://co-gcs-test-blobs` |
| Location / class | `US-WEST1` / `STANDARD` | same |
| Lifecycle | `daysSinceCustomTime: 8`, plus an `age: 365` cost backstop | — |
| Soft delete | disabled (`--soft-delete-duration=0`) | disabled |
| Public access | prevented; UBLA on | same |
| Writer | `co-gcs-replicator` via the custom role below | `co-gcs-test-replicator`, `roles/storage.objectAdmin` |
| Reader | `co-gcs-blob-reader@co-gcs.iam.gserviceaccount.com`, `roles/storage.objectViewer` | — |
| Key on the VM | — | `/etc/replicator/co-gcs-test-replicator.json` |
| Consumer key | `/etc/watcher/co-gcs-blob-reader.json`, named by `GCS_BLOB_CREDENTIALS` | — |

**The worker's grant is a custom role, because no predefined one fits.**
`GcsBlobStore._touch` moves `customTime` on every re-reference, which needs
`storage.objects.update` — and every predefined role carrying `update`
(`objectUser`, `objectAdmin`) also carries `delete`. The worker must never hold
`delete`: expiry is the lifecycle rule's job, and "this identity cannot delete
anything, anywhere" is the same property that makes the permanent writer's grant
correct. So `projects/co-gcs/roles/replicatorTempBlobWriter` grants exactly
`storage.objects.{create,get,list,update}` and nothing else — not even
`storage.buckets.get`, which is why the lifecycle rule is verifiable only from a
workstation.

**`--soft-delete-duration=0` is the flag**, not `--clear-soft-delete-policy`. The
GCS default is 7 days of soft-delete retention, which on a bucket whose entire
purpose is expiry means paying to store every expired blob for a week after it
expired.

Order still mattered more than any single step, and the two things this bucket
needs from an operator — a lifecycle rule and a consumer-side grant — remain the
two things no test in this repo can check.

**The flip sequence, as executed** — kept because its ordering argument is the
template for any future backend change:

1. ~~CannObserv/watcher#275 ships~~ — `gs://` support *and* the re-issue cap,
   deployed 2026-08-20. The ordering was the whole point: a worker announcing
   `gs://` to a Watcher that could not read it would have put every watched item
   into re-fetch-until-capped against live origins.
2. **CannObserv/archiver#175 answers** — the availability window. Still open,
   and deliberately never blocking: the rule is provisioned at 8 days and is one
   `buckets update` away from whatever number comes back.
3. ~~Provision the buckets, the lifecycle rule, and both grants~~ — done
   2026-08-20, verified per identity with `testIamPermissions`.
4. ~~Set the env and restart~~ — done 2026-08-20 19:45Z. The boot line confirmed
   bucket, prefix, and the 7-day published horizon against the 8-day rule.
   Commands in the PEL naming `file://` blobs are refused `blob_expired`, not
   `invalid_source` — the issuer is told to fetch again, which is the truth
   after a flip.
5. ~~`rm -rf /var/lib/replicator/blobs`~~ — held until the pre-flip `file://`
   horizon passed (2026-08-27), since nothing reclaims it under `gcs`, then done
   2026-09-12 by #88's decommission of the watcher VM, which held the only tree
   (~2 MB). `co-replicator` never had one.

What has to exist, and why each part:

| Thing | Why it is not optional |
|---|---|
| A **separate** bucket from `co-gcs-replication` | These are arbitrary bytes from arbitrary origins. The permanent-artifact bucket's whole grant design withholds `delete` so nothing can erase it; a temp store exists to expire. One bucket cannot be both |
| Uniform bucket-level access, no public access | Fetched content is not published content. Nothing here should be reachable without a grant |
| Same region as the VM | In-region reads **from a GCE instance** are not egress-billed. This VM shows no GCE DMI signature, so consumer reads may well be billed as internet egress whatever the region — co-locating still minimises latency and cost, but do not plan on the traffic being free. At the volumes seen so far (~2 MB of live blobs) the distinction is rounding error either way |
| A lifecycle rule on **`daysSinceCustomTime`** | Not `age`. The store stamps `customTime` on every re-reference, which is what makes "TTL since last referenced" expressible — an age rule would reap a blob announced moments ago, invisibly, because re-fetching unchanged bytes never rewrites the object |
| The rule's day count ≥ `REPLICATOR_BLOB_TTL_SECONDS` | The two are configured in different places and nothing keeps them in step. A rule shorter than the published horizon announces a window the bucket will not honour |
| `objectAdmin`-equivalent for the worker's SA | It creates objects, reads them back, and **lists** — the boot preflight is a one-object listing, because an existence check cannot detect a missing bucket (the SDK swallows the 404). `storage.objects.list` is in both `objectViewer` and `objectAdmin`, so this widens nothing. It needs no `delete`: expiry is the lifecycle rule's job, which is also why the preflight is a read rather than a write-and-clean-up |
| `objectViewer` for **each consumer's** SA | This is the grant that replaces the filesystem coupling, and the one thing the worker cannot verify at boot. Watcher's SA is the one that matters today — it is the service that opens the bytes |

Lifecycle granularity is **one day** and enforcement is asynchronous, so a blob
may outlive its rule by a day or more. That direction is safe and is stated to
consumers as such: `blob_expires_at` becomes a floor rather than an exact
horizon. See **Retention** in [STORAGE.md](STORAGE.md).

## The GCS test bucket — the opposite grant, on purpose (#38, #50)

Production `co-gcs-replication` can never be a test target. Its writer holds `storage.objects.{create,get,list}` and **no `delete`, no `update`** — the property that enforces T4's "never overwrite, never delete" at IAM rather than only in our code, and therefore the property that makes a conflict fixture unable to reset itself. Every verification run against it would be permanent litter, which is why the hand-run T4 e2e was never committed as a test.

Provisioned 2026-08-18 in project `co-gcs`:

| | |
|---|---|
| Bucket | `gs://co-gcs-test-replication` |
| Service account | `co-gcs-test-replicator@co-gcs.iam.gserviceaccount.com` |
| Grant | `roles/storage.objectAdmin` on that bucket **only** |
| Key on the VM | `/etc/replicator/co-gcs-test-replicator.json` (`root:exedev`, `0640`) |
| CI identity | the same SA, keyless, via `principalSet://iam.googleapis.com/projects/912903030445/locations/global/workloadIdentityPools/github/attribute.repository/CannObserv/replicator` |

**`test` is infixed in both names, never suffixed.** `co-gcs-replication-test` would contain the production bucket name as a substring, and `co-gcs-replication-test` likewise for the SA — which would make `tests/test_destinations.py`'s literal scan refuse the very names it exists to steer traffic towards, or force it to carry a negative lookahead nobody maintains. Renaming either resource means revisiting that scan.

What the bucket differs from production in, and why each one:

| Property | Test bucket | Why |
|---|---|---|
| `delete` / `update` on objects | granted | a conflict fixture must reset itself, or the same test cannot run twice |
| soft-delete policy | **cleared** | the GCS default is 7 days; deleted fixtures would linger and "absent" assertions would read against a bucket that still remembers. That ambiguity is what made #38's investigation need a second pass |
| lifecycle | delete at age 1 day | litter insurance only. It is asynchronous with 24h+ latency and is **not** the fixture reset — the SA's `delete` is |
| public access | prevented | production is public; nothing here should be |
| location, storage class, versioning (off) | matched | a test only predicts production behaviour to the extent the destinations agree |

**The test SA cannot read the bucket's own metadata**, because `objectAdmin` does not include `storage.buckets.get` — the same blindness production's writer has. So the lifecycle rule and the location are verifiable only from a workstation with `roles/storage.admin`:

```bash
gcloud storage buckets describe gs://co-gcs-test-replication \
  --format="yaml(location, default_storage_class, versioning_enabled, lifecycle_config,
                 soft_delete_policy, uniform_bucket_level_access, public_access_prevention)"
```

**Those are `gcloud storage`'s key names, not the JSON API's**, and the difference
is silent: an unknown key in a `yaml()` projection prints *nothing* rather than
erroring, so the earlier spelling of this command (`storageClass`, `lifecycle`,
`softDeletePolicy`, `iamConfiguration`) returned `location:` alone and read
exactly like a bucket with no lifecycle rule configured. It cost a round trip
during #7's provisioning. Either drop `--format` entirely — the bare output
cannot be wrong, and is the one to trust when a field comes back missing — or
pass `--raw`, which switches the resource to the API's own camelCase
representation and makes the old spelling correct again.

Everything else was verified from the VM as the test SA on 2026-08-18: `create` with `ifGenerationMatch: 0` succeeds, a second create raises `PreconditionFailed`, the confirming `get` finds the object, `delete` removes it, and a soft-deleted listing is refused `400 Soft delete policy is required to list soft-deleted versions` — which is the policy-cleared confirmation. On production the same identity holds **no write permission of any kind**; the `get`/`list` it does report there belong to `allUsers`, since that bucket is public, and are not a grant to this SA — an anonymous client reports the identical pair.

Usage, the marker, and the variables: **Testing the write path** in [TESTING.md](TESTING.md).

**#7 adds a second test bucket, provisioned 2026-08-20.**
`REPLICATOR_TEST_BLOB_BUCKET=co-gcs-test-blobs` names the temp-store destination
for `@pytest.mark.gcs`, and it must not be the replicate one: those tests create
objects and delete them afterwards, which needs exactly the `delete` the
replicate grant withholds. Its first run found a test asserting behaviour a
review had already changed — see **the marked suite** in [TESTING.md](TESTING.md).
