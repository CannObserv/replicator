# Replicator Deployment

Single-VM topology, the systemd unit's lifecycle and the guards it starts
behind, and the co-core pin. `AGENTS.md` keeps the two-env-file boundary and the
restart command; the reasoning behind each of them is here. The variables
themselves — every one either env file carries — are in
[ENVIRONMENT.md](ENVIRONMENT.md).

## Infrastructure

Replicator shares the VM with archiver, watcher, and notifier:

| Service | Framework | Port | Managed by |
|---|---|---|---|
| Worker (live) | asyncio bus consumer | — | `systemctl` (`replicator.service`) |
| API (dev) | FastAPI | 8041 | manual uvicorn |

The worker binds no port. Port 8040 is reserved for Replicator's API should it ever be deployed; 8041 is the dev port. Neighbours: watcher 8000/8001, archiver 8020/8021, notifier 9000/9001. The exe.dev proxy transparently forwards ports 3000–9999; the dev server is reachable at `https://replicator.exe.xyz:8041/`.

### Redis is Archiver-operated — Replicator connects, it does not run its own

The Redis change bus is Archiver-operated cluster infrastructure (the shared VM's `redis-server.service`). Replicator is a **client**: never ship a broker, never claim ownership.

**Redis ≥ 7.0 is Replicator-critical.** Replicator is the cluster's first user of `AsyncBusConsumer.claim_stale`, which reads `XAUTOCLAIM`'s three-element reply — the deleted-ids element added in Redis **server** 7.0. Below that, the crash-recovery path raises. `scripts/check_redis_floor.sh` guards this as an `ExecStartPre`. (The VM runs 7.0.15.)

The **redis-py client** resolves `>=5,<8` transitively via `co-core-aio[bus]`. Don't re-pin it narrower.

### The temp-blob buckets — live since 2026-08-20 (#7)

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
template for any future backend change, and because one step is still pending:

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
5. **Pending, dated:** `rm -rf /var/lib/replicator/blobs` — **not before
   2026-08-27**. Pre-flip `blob_available` facts promise `file://` URIs for up
   to the 7-day window, and Watcher reads those straight off this tree; deleting
   it early would break reads the contract still guarantees. After the horizon
   passes nothing can legitimately reference it, and **nothing will ever
   reclaim it otherwise** — the sweep does not run under `gcs`. ~2 MB.

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

### The GCS test bucket — the opposite grant, on purpose (#38, #50)

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

## Server Lifecycle

**`replicator.service` runs the worker.** It binds no port, so there is no port to conflict over — but only one process should hold a given consumer name at a time.

| Situation | Action |
|---|---|
| Code merged to main **on GitHub** (the usual path) | `git pull --ff-only && uv sync --frozen && sudo systemctl restart replicator` |
| Code merged to main **locally** | `git push && uv sync --frozen && sudo systemctl restart replicator` |
| Testing a worktree/branch | `uv run python -m src.worker.main` (set distinct `REPLICATOR_CONSUMER_NAME` **and** `REPLICATOR_REPLICATE_CONSUMER_NAME` — **required** while the service runs, since #77 makes both loops derive the names the unit registers under) |
| Debugging the live service | `sudo journalctl -u replicator -f` |
| After editing `deploy/replicator.service` | `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart replicator` |
| After a co-core version bump | re-run `sync_wheelhouse.py`, then `uv sync` |

`ExecStart` uses `--frozen --no-sync`, so dependency sync is a deploy step, not a service-start side effect.

**`/etc/systemd/system/replicator.service` is a *copy*, not a symlink to `deploy/`.** So the `cp` above is load-bearing and `daemon-reload` alone silently does nothing — systemd re-reads the installed file, which is still the old one. The failure has no symptom at restart: the worker comes up on the new code under the *old* unit, and the mismatch only surfaces the first time a directive actually matters. Nothing guards it, either — `tests/test_deploy.py` reads the repo file, which is exactly the copy that is still correct. Diff the two when a restart follows a unit edit (#11 deploy).

The copy is deliberate, for the same reason `/etc/replicator/.env` is not read from the repo: the live unit must survive a repo reset, a worktree switch, or a branch checkout that happens to be mid-edit.

### The co-core pin, and why the patch floor is load-bearing

`co-core` and `co-core-aio` come from the private GCS index `gs://co-gcs-pypi`,
mirrored into `./.wheelhouse` by `scripts/sync_wheelhouse.py` and resolved
through `[tool.uv] find-links` — never from PyPI.

Auth is ADC: on the VM the SA key at `GOOGLE_APPLICATION_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`), in CI a keyless WIF token. Pin the current minor — `>=0.10,<0.11`. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead — a ValidationError at construction (0.8.0 requires `info_source_id` on all three fetch payloads, #19/#28) or an ImportError at load, never reaching a running worker (0.9.4 cuts the replicate contracts, #29).

### The checkout guard — the service refuses to start off `main` (#37), or off unpushed commits (#48)

"Code committed to main is the deployed code" was an invariant AGENTS.md asserted and nothing enforced. On 2026-08-14, during #29, a restart to verify a credential change deployed branch `29-replicate-refusals` as build `7d6f195` while `main` was at `b69771a`. The blast radius was small — the replicate loop had nothing provisioned and refused everything — but the service ran unmerged code, and it was noticed only because someone read the build stamp carefully. That is the point: a branch deploy stamps `BUILD_ID` with the branch commit, so the journal *looks* correct while describing code that is on no shared branch.

`scripts/check_main_checkout.sh` is now a fatal `ExecStartPre` (no `-` prefix), placed **before** the `BUILD_ID` stamp so a refused start cannot leave a misleading build id in `/run/replicator/build-id`, which outlives the failed start. It checks the unit's `WorkingDirectory` rather than a hardcoded path, so the guard, the stamp, and `ExecStart` can never disagree about which tree is under test.

| Condition | Verdict |
|---|---|
| HEAD on `main`, clean, in sync | start |
| HEAD on any other branch | **refuse** — the case that motivated it |
| detached HEAD | **refuse**, named as such rather than as "on 'HEAD'" |
| unborn HEAD, or not a git work tree | **refuse** — no evidence to check, and soft-passing would make `rm -rf .git` a silent bypass |
| `main` ahead of `origin/main` | **refuse** (#48) — unpushed commits are the same "on no shared branch" case, and unlike *behind* the verdict does not depend on the ref being fresh (below). `git push`, or `git reset --hard origin/main` |
| `main` behind `origin/main` | warn — a stale-but-shared commit is a different problem from an unshared one, and `origin/main` is only as fresh as the last fetch, so refusing would make the service unstartable during a network outage. The guard never fetches |
| no `origin/main` ref at all | warn — absence of evidence, not evidence of an unshared commit: HEAD is already proven to be `main`. Named out loud only because ahead now refuses, so silence would make `git remote remove origin` a quiet bypass |
| dirty working tree (tracked files) | warn — refusing would block an operator mid-incident. Untracked files **and submodule state** are ignored, on the same reasoning both times: scratch files and a `skills-vendor/` refresh would otherwise keep this warning permanently lit over things the worker never loads, and a warning nobody reads is no warning |

**Why *ahead* refuses while *behind* warns, on the same cached ref.** `origin/main` is updated by `git fetch` **and** by a successful `git push` from this repository. A never-fetched ref can therefore hide *behind*-ness — remote commits this checkout cannot see, which is why refusing there would fail an operator whose only sin is a network outage — but it cannot manufacture *ahead*-ness for commits this checkout pushed, because the push would have moved the ref. "Ahead" is **local** evidence: the commits are here and nothing here published them, assertable without the network call an `ExecStartPre` must not make. The one false positive needs someone to publish the identical SHAs from another clone; through a PR merge the SHAs differ, so the tree reads as *diverged* — the refusal still fires, and the behind warning prints alongside it so both sides get named.

The practical cost is one `git push` before the restart, in a flow that already meant to push — which is why the lifecycle table above splits the two merge paths. Merging on GitHub leaves this checkout *behind* rather than ahead, so that path pulls and never pushes; a deploy line whose first command routinely prints `Everything up-to-date` is one an operator learns to skip, and skipping it is the whole failure this guard now refuses. The network-partition case (a hotfix committed while the remote is unreachable) is what `REPLICATOR_ALLOW_ANY_CHECKOUT=1` is for, and that is a documented use of the override rather than an erosion of the guard.

Verify it by hand with `bash scripts/check_main_checkout.sh` (exit 0 starts, non-zero refuses). `REPLICATOR_ALLOW_ANY_CHECKOUT=1` is the escape hatch — see **Environment Variables**.

**The dev worker asks the same question, at the writer (#52).** The `ExecStartPre` covers the service and nothing else; the documented way to test a branch here — `uv run python -m src.worker.main` under distinct `REPLICATOR_CONSUMER_NAME` and `REPLICATOR_REPLICATE_CONSUMER_NAME` — involves no systemd, so it runs no guard at all. That was free while replication was unprovisioned and every command refused. It stops being free once an alias table exists: the shell snippet under **Environment Variables** loads `/etc/replicator/.env`, so a worker started from a feature branch inherits the production ADC *and* the production alias table, and acquires a write identity against a bucket whose objects it cannot delete (#38).

So `build_writers` calls `src/worker/checkout.py`, which runs the same script and takes its verdict wholesale — same seven conditions, same override, one source of truth. A refused checkout builds **no provider writer**, logs `refusing to build a provider writer` at `error`, and changes nothing else: the fetch path runs, the alias table is still read, and commands naming the alias are refused `provider_disabled` — accurate, and the reason whose remedy is the operator act that fixes it. It is asked only when there is a `gcs` binding to build, so a worker that replicates nothing (every worker on this VM today) pays no subprocess. `REPLICATOR_ALLOW_ANY_CHECKOUT=1` builds the writers anyway, which is how a branch is tested against the **test** bucket (#50).

**The guard reaches the live service only after the `cp`.** It ships in `deploy/replicator.service`, and the installed unit is a copy — so until `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload`, the running service is still ungated. A guard that exists only in the repo's copy of the unit is the same failure mode one level up.

### The co-core 0.8.0 cutover is a two-repo deploy, streams flushed between (#28)

`schema_version` stays 1, and that is a decision rather than an oversight: bumping to 2 would imply
a v1 consumers must branch on, when the correct operation is to discard the v1 messages. The wire
is pre-production, so it is discardable.

**Flushing is a prerequisite step, not cleanup.** `content.fetch`, `content.blobs`, and **both
`.dlq` streams** — a v1 dead-letter cannot be replayed under 0.8.0, so leaving it is leaving a trap
for whoever triages next. Add `replicator:cmd:*` if any `command_id` will be reused across the
flush. **Not** `content.fetch-policy`: it is a groupless state stream, and flushing it leaves every
worker with an empty policy map until the next republish.

**Ship with [CannObserv/watcher#252](https://github.com/CannObserv/watcher/issues/252).** Required
fields mean a half-deployed cluster does not degrade, it dead-letters: a Replicator on 0.8.0 fails
`from_wire` on any command from a Watcher still on 0.7.x, and that failure destroys the
`command_id` correlator before any fact can name it. The two may be worked in parallel; they must
land together.

### The dedupe keys gain a stream segment (#29) — no action, but know why

Keys move from `replicator:cmd:<id>` to `replicator:cmd:fetch:<id>`, so a second command stream can
never dedupe a command against the other stream's. **Nothing to do at deploy time.** Old-format keys
are simply never read again and expire on their own `REPLICATOR_DEDUPE_TTL_SECONDS`; a command that
was already handled and is redelivered across the restart re-runs its handler instead of
short-circuiting.

That re-run is safe by the same property the key's set-after-success ordering already relies on:
storage is content-addressed, so re-storing identical bytes is a no-op that republishes the fact,
and the key is a cheap short-circuit rather than the correctness mechanism. Flush `replicator:cmd:*`
only if you would rather not pay the handful of re-fetches.

**Dev server workflow** (the `/health` app, port 8041 so a future live service stays up):

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload --log-config src/core/log_config.json
```

## Environment Variables

Every variable either env file carries, with the reasoning behind each default,
and the boundary between them: [ENVIRONMENT.md](ENVIRONMENT.md).
