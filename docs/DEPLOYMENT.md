# Replicator Deployment

The systemd unit's lifecycle, the guards it starts behind, and the co-core pin.
What it reports when it *fails* is in
[FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md); the host's memory tunables
are in [MEMORY-PROTECTION.md](MEMORY-PROTECTION.md).
`AGENTS.md` keeps the two-env-file boundary and
the restart command; the reasoning behind each of them is here. The variables
themselves are in [ENVIRONMENT.md](ENVIRONMENT.md), which indexes every one and
sends the blob and temp-store settings on to [STORAGE.md](STORAGE.md); the VM,
broker and bucket topology this unit runs on is in
[INFRASTRUCTURE.md](INFRASTRUCTURE.md).

## Server Lifecycle

**`replicator.service` runs the worker.** It binds no port, so there is no port to conflict over — but only one process should hold a given consumer name at a time.

| Situation | Action |
|---|---|
| Code merged to main **on GitHub** (the usual path) | `git pull --ff-only && uv sync --frozen && sudo systemctl restart replicator` |
| Code merged to main **locally** | `git push && uv sync --frozen && sudo systemctl restart replicator` |
| Testing a worktree/branch | `uv run python -m src.worker.main` (set distinct `REPLICATOR_CONSUMER_NAME` **and** `REPLICATOR_REPLICATE_CONSUMER_NAME` — **required** while the service runs, since #77 makes both loops derive the names the unit registers under) |
| Debugging the live service | `sudo journalctl -u replicator -f` |
| After editing `deploy/replicator.service` | `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart replicator` |
| After editing `deploy/replicator-failure-notify@.service` | `sudo cp 'deploy/replicator-failure-notify@.service' /etc/systemd/system/ && sudo systemctl daemon-reload` — **no restart**, it is a template nothing runs until a unit fails |
| After editing `deploy/99-co-replicator-memory.conf` | `sudo cp deploy/99-co-replicator-memory.conf /etc/sysctl.d/ && sudo sysctl --system` — **`sysctl --system`, not `daemon-reload`**, which does not read this file |
| After editing `deploy/tailscaled.service.d/memory.conf` | `sudo install -D -m 644 deploy/tailscaled.service.d/memory.conf /etc/systemd/system/tailscaled.service.d/memory.conf && sudo systemctl daemon-reload && sudo systemctl restart tailscaled` — the **restart** is for `OOMScoreAdjust=`, which applies at exec; read `/proc/$(systemctl show -p MainPID --value tailscaled)/oom_score_adj`, never `systemctl show -p OOMScoreAdjust`. It drops the tailnet briefly — 3 s on 2026-09-24, and the worker logged nothing |
| After editing `deploy/system.slice.d/replicator-memory.conf` | `sudo install -D -m 644 deploy/system.slice.d/replicator-memory.conf /etc/systemd/system/system.slice.d/replicator-memory.conf && sudo systemctl daemon-reload` — **no restart**; read `/sys/fs/cgroup/system.slice/memory.low` |
| After editing `deploy/apt/preferences.d/tailscale.pref` | `sudo install -D -m 644 deploy/apt/preferences.d/tailscale.pref /etc/apt/preferences.d/tailscale.pref` — **no reload**; `apt-cache policy tailscale` showing 600 over the site's 100 proves it. It scopes the Tailscale origin the `patching-hosts` maintenance lane follows (#131) |
| After editing `deploy/needrestart.conf.d/replicator.conf` | `sudo install -D -m 644 deploy/needrestart.conf.d/replicator.conf /etc/needrestart/conf.d/replicator.conf` — **no reload**, apt's hook reads it at the next dpkg run; `sudo needrestart -m u -r l -b` printing `Disabling Ubuntu mode` proves it loads. Without it a security apply restarts the worker mid-apply, outside any window (#122) |
| Reading what the failure handler reported | `journalctl -t replicator-failure` — **not** `journalctl -u`; why, in [FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md) |
| After a co-core version bump | re-run `sync_wheelhouse.py`, then `uv sync` |
| `Start request repeated too quickly` | `sudo systemctl reset-failed replicator && sudo systemctl start replicator` — the rate limit, not a broken build |

`ExecStart` uses `--frozen --no-sync`, so dependency sync is a deploy step, not a service-start side effect.

**`/etc/systemd/system/replicator.service` is a *copy*, not a symlink to `deploy/`.** So the `cp` above is load-bearing and `daemon-reload` alone silently does nothing — systemd re-reads the installed file, which is still the old one. The failure has no symptom at restart: the worker comes up on the new code under the *old* unit, and the mismatch only surfaces the first time a directive actually matters. Little guards it, either — `tests/test_deploy.py` reads the repo file, which is exactly the copy that is still correct; only its live checks of `OOMScoreAdjust=` and `MemoryLow=` (#113) read what the host holds. Diff the two when a restart follows a unit edit (#11 deploy).

The copy is deliberate, for the same reason `/etc/replicator/.env` is not read from the repo: the live unit must survive a repo reset, a worktree switch, or a branch checkout that happens to be mid-edit.

**Two unit files now, and the second one is easy to forget.** `deploy/replicator-failure-notify@.service` is the `OnFailure=` handler, and it is a copy under `/etc/systemd/system/` exactly like the worker's unit — with one difference that makes its absence quieter: nothing runs it until something fails, so a missed `cp` is invisible until the first incident, which is the one moment it was supposed to help. `systemctl status replicator-failure-notify@replicator.service.service` answering `Unit ... not found` is how that looks. There is no restart to pair with the copy.

**Eight starts in two hours, and an iterative session will spend them.** `StartLimitIntervalSec=7200` with `StartLimitBurst=8` is sized against `worst_case_outage_seconds` so a permanently unreachable Redis surfaces as a *stopped unit* rather than a hot restart loop. The cost is that a ninth `systemctl restart` inside two hours — ordinary when shipping several commits in one sitting — fails with `Start request repeated too quickly` and `Result: start-limit-hit`, which reads as a broken deploy and is not one: the previous instance stops cleanly and logs `worker stopped` on its way out. `sudo systemctl reset-failed replicator` clears the counter; then `start` as normal. Check `systemctl status` for `start-limit-hit` before debugging the build — #77 hit this twice in one session.

**Why eight and not three (#94, #134).** Three starts absorbed 30 minutes of broker outage. On 2026-09-16 the broker was away for 58, and the only reason this unit was not asked to survive the whole of it is that the network degraded from ~14:28 while the worker's cycles did not fail *continuously* until ~15:00. #94 raised it to six, sized at `worst_case_outage_seconds` (~10 min) a start — an upper bound. Against a broker that refuses instantly a start absorbs only its backoff waits, ~7.5 min, with ~35 s of `RestartSec` and floor-check wait between starts. The burst counts every start, the deploy that started the running worker included, so an outage within two hours of a deploy gets one restart fewer: six covered ~48 min at that floor, seven ~56, eight ~64 (#134). `tests/test_deploy.py` pins that floor, not the ceiling, against `WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS` — the worst outage this cluster has actually had — rather than against internal consistency alone. Raise that constant when a worse one happens and the test will tell you whether the unit still covers it. Before #133, later starts exited on their first `ensure_group` (six absorbed ~12 min). Widening the fuse is safer than it was, because the other half finally exists: before #94 a unit that gave up was discovered by whoever next looked, and a longer fuse on a silent failure would be the wrong trade.

Rehearse reconnection before changing the start budget — the loop's half in pytest, systemd's in `scripts/rehearse_reconnect.sh`: [TESTING.md](TESTING.md#rehearsing-reconnection).

**Dev server workflow** (the `/health` app, port 8001 so a future live service on 8000 stays up):

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8001 --reload --log-config src/core/log_config.json
```

### When the worker fails, who is told

The `OnFailure=` handler, the six `REPLICATOR_NOTIFY_*` variables it reads,
notifier mode and its delivery scoring:
[FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md).

### Memory protection — the worker outranks the dev session that shares this VM

`OOMScoreAdjust=` on both units and `tailscaled`, the host's swap and
`vm.min_free_kbytes`, the `MemoryLow=` slice grant, and why `earlyoom` is
declined: [MEMORY-PROTECTION.md](MEMORY-PROTECTION.md).

### The co-core pin, and why the patch floor is load-bearing

`co-core`, `co-core-aio` and `co-core-sync` come from the private GCS index `gs://co-gcs-pypi`,
mirrored into `./.wheelhouse` by `scripts/sync_wheelhouse.py` and resolved
through `[tool.uv] find-links` — never from PyPI.

Auth is ADC, with one substitution: on the VM the sync uses the read-only key named by `REPLICATOR_WHEELHOUSE_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`) rather than the worker's own `GOOGLE_APPLICATION_CREDENTIALS`, which is the writer (`/etc/replicator/co-gcs-replicator-writer.json`, #29, #114); in CI it is a keyless WIF token. Pin the current minor — `>=0.37.0,<0.38`, on all three. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead — a ValidationError at construction (0.8.0 requires `info_source_id` on all three fetch payloads, #19/#28) or an ImportError at load, never reaching a running worker (0.9.4 cuts the replicate contracts, #29; 0.19.3 ships the shared `BlobStore`, #114; 0.19.6 cuts the persist contract, #114 step 6).

**The 0.10 → 0.13 jump (#77) was the first departure from "pin the current minor", and what justified it is evidence rather than judgement.** The hazard that rule guards is payload fields moving under `extra="ignore"`; every module this service imports proved byte-identical between the two wheels — `pure/models/changes.py`, `pure/adapters/bus/{envelope,exceptions}.py`, `effects/{bus,fetch,gcs}.py`, `pure/util/{hashing,gcs}.py`, and all of `co_core_aio` bar its `__version__`. Only `streams.py` gained anything (`group_name`, `stream_kind` — cannobserv#384), and Replicator imports no `co_v1` adapter, which is where 0.13.2's other changes landed. **Diff the wheels before any future multi-minor bump**; the version distance is not the risk, the moved surface is. Unzip both from `.wheelhouse/` and `diff -rq` them — the whole check took a minute and turned a deferred change into a safe one. **Diff the import closure, not just the import lines.** At 0.19 → 0.36 (#140), `changes.py`'s import of `extract.canonical` brought in the `pure/extract` package, which had changed (cannobserv#523), and no import line in Replicator names it.

**Sync the wheelhouse before concluding a version is unavailable.** The #77 review reported 0.13.1 "not mirrored" from a local `ls` while the private index already carried it; the mirror had simply not been synced, and what silently fixed it was `replicator.service`'s own non-fatal `ExecStartPre` running `sync_wheelhouse.py` on the next restart. A local listing is a cache, not the index.

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

**The dev worker asks the same question, at the writer (#52).** The `ExecStartPre` covers the service and nothing else; the documented way to test a branch here — `uv run python -m src.worker.main` under distinct `REPLICATOR_CONSUMER_NAME` and `REPLICATOR_REPLICATE_CONSUMER_NAME` — involves no systemd, so it runs no guard at all. That was free while replication was unprovisioned and every command refused. **It stopped being free on 2026-09-10**, when `primary` was provisioned here (#86): the shell snippet under **Environment Variables** loads `/etc/replicator/.env`, so a worker started from a feature branch inherits the production ADC *and* the production alias table, and acquires a write identity against a bucket whose objects it cannot delete (#38). That is today's condition on this VM, not a future one — a dev worker testing a replicate branch here reaches the production bucket unless it is pointed at the test one.

So `build_writers` calls `src/worker/checkout.py`, which runs the same script and takes its verdict wholesale — same seven conditions, same override, one source of truth. A refused checkout builds **no provider writer**, logs `refusing to build a provider writer` at `error`, and changes nothing else: the fetch path runs, the alias table is still read, and commands naming the alias are refused `provider_disabled` — accurate, and the reason whose remedy is the operator act that fixes it. It is asked only when there is a `gcs` binding to build, so a worker that replicates nothing (this VM's, until #86 provisioned `primary` on 2026-09-10) pays no subprocess. `REPLICATOR_ALLOW_ANY_CHECKOUT=1` builds the writers anyway, which is how a branch is tested against the **test** bucket (#50).

**The guard reaches the live service only after the `cp`.** It ships in `deploy/replicator.service`, and the installed unit is a copy — so until `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload`, the running service is still ungated. A guard that exists only in the repo's copy of the unit is the same failure mode one level up.

### Required fields make a co-core bump a two-repo deploy

A half-deployed cluster does not degrade, it dead-letters: a Replicator on the
new wheel fails `from_wire` on any command from an issuer still on the old one,
and that failure destroys the `command_id` correlator before any fact can name
it. The two sides may be worked in parallel; they must land together.

0.8.0 did this with `info_source_id` (#28), shipped with
[CannObserv/watcher#252](https://github.com/CannObserv/watcher/issues/252). The
wire was pre-production then, so `schema_version` stayed 1 and the v1 messages
were discarded rather than branched on: `content.fetch`, `content.blobs` and
**both `.dlq` streams** were flushed first — a v1 dead-letter cannot be replayed
under 0.8.0 — plus `replicator:cmd:*` where a `command_id` would be reused.
**Not** `content.fetch-policy`: a groupless state stream, whose flush leaves every
worker with an empty policy map until the next republish.

### Dedupe keys carry a stream segment

`replicator:cmd:fetch:<id>` since #29, where they were `replicator:cmd:<id>`, so a
second command stream can never dedupe a command against the other's. The old
keys expired on `REPLICATOR_DEDUPE_TTL_SECONDS` and nothing reads them. Why
losing any `replicator:cmd:*` key costs a re-fetch rather than correctness,
including an empty namespace after a flush:
[CONVENTIONS.md](CONVENTIONS.md#the-replicatorcmd-keys).

## Environment Variables

Every variable either env file carries, with the reasoning behind each default,
and the boundary between them: [ENVIRONMENT.md](ENVIRONMENT.md).

The variables the unit's start guards read are kept here, beside the guards they set — the
Redis floor check's timeout and wait, and the checkout guard's override. They live in `/etc/replicator/.env`
like the rest (#146):

- `REPLICATOR_REDIS_FLOOR_TIMEOUT` — seconds one `check_redis_floor.sh` probe may run before it is killed; default `5`. Read by the unit's `ExecStartPre` only, never by the worker. Exists because `redis-cli` has no connect timeout, and a `rediss://` URL against a plaintext endpoint hangs on the TLS handshake
- `REPLICATOR_REDIS_FLOOR_WAIT` — how long `check_redis_floor.sh` keeps retrying a broker it cannot reach before reporting the floor `UNVERIFIED`; default `30`, and `0` probes once. Exists because on a cold boot MagicDNS answers `broker` with no address for a moment after tailscaled starts, which `After=tailscaled.service` cannot see (#88; a wait for the tailnet *address* was tried and did not help). Only `unreachable` is retried — an auth refusal is an answer, and a silent failure has already spent the timeout. `WAIT + TIMEOUT` is held to half the unit's 90 s start budget by `tests/test_deploy.py`, because every `ExecStartPre` shares it. A non-integer is named at start and the default used
- `REPLICATOR_ALLOW_ANY_CHECKOUT` — bypasses the `main`-checkout guard; **unset by default, which is the enforcing posture**. `scripts/check_main_checkout.sh` runs as the unit's first non-privileged `ExecStartPre` and refuses to start on any branch but `main` — see [**The checkout guard**](DEPLOYMENT.md#the-checkout-guard--the-service-refuses-to-start-off-main-37-or-off-unpushed-commits-48). Only the literal `1` bypasses it; anything else non-empty logs that it is *not* bypassing rather than silently failing to, because an override that quietly does nothing is worse than none. Exists at all because a guard with no override gets removed rather than overridden — but it is not the way to test a branch on this VM. That is `uv run python -m src.worker.main` under distinct `REPLICATOR_CONSUMER_NAME` and `REPLICATOR_REPLICATE_CONSUMER_NAME`, which never touches the unit and never needs this. Set it only when the thing under test *is* the unit — or when `main` carries a commit that cannot be pushed because the remote is unreachable, which is the one incident-time use the ahead refusal leaves (#37, #48). It has a second reader since #52: `build_writers` refuses to build a provider writer on a checkout the same script rejects, so this is also what a dev worker needs before it can write to the test bucket from a branch
