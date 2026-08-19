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
  --format="yaml(location, storageClass, versioning, lifecycle, softDeletePolicy, iamConfiguration)"
```

Everything else was verified from the VM as the test SA on 2026-08-18: `create` with `ifGenerationMatch: 0` succeeds, a second create raises `PreconditionFailed`, the confirming `get` finds the object, `delete` removes it, and a soft-deleted listing is refused `400 Soft delete policy is required to list soft-deleted versions` — which is the policy-cleared confirmation. On production the same identity holds **no write permission of any kind**; the `get`/`list` it does report there belong to `allUsers`, since that bucket is public, and are not a grant to this SA — an anonymous client reports the identical pair.

Usage, the marker, and the two variables: **Testing the write path** in [TESTING.md](TESTING.md).

## Server Lifecycle

**`replicator.service` runs the worker.** It binds no port, so there is no port to conflict over — but only one process should hold a given consumer name at a time.

| Situation | Action |
|---|---|
| Code merged to main **on GitHub** (the usual path) | `git pull --ff-only && uv sync --frozen && sudo systemctl restart replicator` |
| Code merged to main **locally** | `git push && uv sync --frozen && sudo systemctl restart replicator` |
| Testing a worktree/branch | `uv run python -m src.worker.main` (set a distinct `REPLICATOR_CONSUMER_NAME`) |
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

**The dev worker asks the same question, at the writer (#52).** The `ExecStartPre` covers the service and nothing else; the documented way to test a branch here — `uv run python -m src.worker.main` under a distinct `REPLICATOR_CONSUMER_NAME` — involves no systemd, so it runs no guard at all. That was free while replication was unprovisioned and every command refused. It stops being free once an alias table exists: the shell snippet under **Environment Variables** loads `/etc/replicator/.env`, so a worker started from a feature branch inherits the production ADC *and* the production alias table, and acquires a write identity against a bucket whose objects it cannot delete (#38).

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
