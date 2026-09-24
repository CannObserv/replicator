# Replicator Deployment

The systemd unit's lifecycle, the guards it starts behind, the host's memory
tunables, and the co-core pin. What it reports when it *fails* is in
[FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md).
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
| Reading what the failure handler reported | `journalctl -t replicator-failure` — **not** `journalctl -u`; why, in [FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md) |
| After a co-core version bump | re-run `sync_wheelhouse.py`, then `uv sync` |
| `Start request repeated too quickly` | `sudo systemctl reset-failed replicator && sudo systemctl start replicator` — the rate limit, not a broken build |

`ExecStart` uses `--frozen --no-sync`, so dependency sync is a deploy step, not a service-start side effect.

**Six starts in two hours, and an iterative session will spend them.** `StartLimitIntervalSec=7200` with `StartLimitBurst=6` is sized against `worst_case_outage_seconds` so a permanently unreachable Redis surfaces as a *stopped unit* rather than a hot restart loop. The cost is that a fourth `systemctl restart` inside an hour — ordinary when shipping several commits in one sitting — fails with `Start request repeated too quickly` and `Result: start-limit-hit`, which reads as a broken deploy and is not one: the previous instance stops cleanly and logs `worker stopped` on its way out. `sudo systemctl reset-failed replicator` clears the counter; then `start` as normal. Check `systemctl status` for `start-limit-hit` before debugging the build — #77 hit this twice in one session.

**Why six and not three (#94).** Three starts absorbed 30 minutes of broker outage. On 2026-09-16 the broker was away for 58, and the only reason this unit was not asked to survive the whole of it is that the network degraded from ~14:28 while the worker's cycles did not fail *continuously* until ~15:00. Six starts absorb an hour, and `tests/test_deploy.py` pins that against `WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS` — the worst outage this cluster has actually had — rather than against internal consistency alone. Raise that constant when a worse one happens and the test will tell you whether the unit still covers it. Widening the fuse is safer than it was, because the other half finally exists: before #94 a unit that gave up was discovered by whoever next looked, and a longer fuse on a silent failure would be the wrong trade.

### Rehearsing reconnection (#94)

Two halves, because neither mechanism can test the other:

```bash
uv run pytest --no-cov -m integration tests/worker/test_reconnect_integration.py
sudo bash scripts/rehearse_reconnect.sh
```

The pytest half stops and restarts a real `redis-server` it spawns and asserts the in-process property: the loop rides out a survivable outage, gives up on a sustained one, and a worker started fresh against a recovered broker picks up what was stranded. It runs with the AOF on, because the incident being modelled had the group survive; `--appendonly no` would model `NOGROUP` instead.

The script half drives what a pytest cannot — systemd's restart semantics — against a scratch unit under `/run/systemd/system` and a broker it owns. It asserts the worker exits, that systemd restarts it, **that the start budget survives the outage**, and that consumption resumes with no human step. That third assertion is the one #94 turns on: on 2026-09-16 the unit was `failed` sixteen minutes before the broker came back. Set `StartLimitBurst=1` in `deploy/` and the script reproduces that failure by name.

Neither touches `co-broker` or `replicator.service`: the script checks the spawned broker's own reported pid before driving it, and refuses a port answered by anything else.

**`/etc/systemd/system/replicator.service` is a *copy*, not a symlink to `deploy/`.** So the `cp` above is load-bearing and `daemon-reload` alone silently does nothing — systemd re-reads the installed file, which is still the old one. The failure has no symptom at restart: the worker comes up on the new code under the *old* unit, and the mismatch only surfaces the first time a directive actually matters. Little guards it, either — `tests/test_deploy.py` reads the repo file, which is exactly the copy that is still correct; only its live checks of `OOMScoreAdjust=` and `MemoryLow=` (#113) read what the host holds. Diff the two when a restart follows a unit edit (#11 deploy).

The copy is deliberate, for the same reason `/etc/replicator/.env` is not read from the repo: the live unit must survive a repo reset, a worktree switch, or a branch checkout that happens to be mid-edit.

**Two unit files now, and the second one is easy to forget.** `deploy/replicator-failure-notify@.service` is the `OnFailure=` handler, and it is a copy under `/etc/systemd/system/` exactly like the worker's unit — with one difference that makes its absence quieter: nothing runs it until something fails, so a missed `cp` is invisible until the first incident, which is the one moment it was supposed to help. `systemctl status replicator-failure-notify@replicator.service.service` answering `Unit ... not found` is how that looks. There is no restart to pair with the copy.

### When the worker fails, who is told

The `OnFailure=` handler, the six `REPLICATOR_NOTIFY_*` variables it reads,
notifier mode and its delivery scoring:
[FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md).

What belongs here rather than there: the handler unit is a copy under
`/etc/systemd/system/` like the worker's, so it needs the `cp` from the table
above and has no restart to pair with it.

### Memory protection — the worker outranks the dev session that shares this VM

Both units set `OOMScoreAdjust=-900`. The reason is a property of exe.dev, not
of this service: **every process descended from a session is exempt from the OOM
killer.** VSCode Server, Claude Code, and any MCP server they start inherit
`oom_score_adj=-1000` from `exe-init` and `sshd`. Measured on co-replicator
while adopting the shared SocratiCode index:

```bash
cat /proc/self/oom_score_adj                                          # -1000, from a session shell
cat /proc/$(systemctl show -p MainPID --value replicator)/oom_score   # 72 at adj -900; 670 before
```

**`-1000` is not a low score, it is ineligibility** — the OOM killer skips such a
process entirely. 28 processes here hold it. So the directive was never going to
win a comparison against them; what it changes is the worker's rank among the
processes that *can* be chosen, and there it is decisive: 670 put the worker
second from the top of that list, and 72 puts it at the bottom. co-replicator is
also the dev workspace, so this is not a remote condition.

**Resized 2026-09-23 (#99): 8 GiB + 4 G swap, where the scores above were
measured at 3.9 GB with none.** The scores stand — `oom_score_adj` is a rank,
not a threshold — and the margin widened rather than closed: broker was already
8 GiB when it lost the bus for 57m48s. Swap is what makes a ceiling a
survivable reclaim instead of failed atomic allocations; it and
`vm.min_free_kbytes`, which had rescaled to only ~11 MB, are pinned in
[`deploy/99-co-replicator-memory.conf`](../deploy/99-co-replicator-memory.conf).

**At the top of the eligible list: the user manager, `systemd --user` and
`(sd-pam)` at 733 (adj +100), then — until #113 — `tailscaled` at 670.**
Measured 2026-09-24 at 8 GiB (#112); the 675 recorded on 2026-09-18 was at
3.9 GB. Read tailscaled's place against CannObserv/broker#17, where the failure
*was* the tailnet: killing tailscaled takes the bus away exactly as effectively
as killing this worker. So
[`deploy/tailscaled.service.d/memory.conf`](../deploy/tailscaled.service.d/memory.conf)
gives it the worker's own -900, as CannObserv/broker#21 does, and the network
path and its only consumer now rank together at the bottom: 69 and 68 after the
restart. The same pass found a session `dbus-daemon` above the user manager
(adj +200, 800), started under `user@1000` at 04:04.
CannObserv/watcher#309 chose -400 because its dashboard does not use the
tailnet; this worker does.

CannObserv/broker#17 is what it costs when it fires, and it fires in a shape
worth recognising: launching a SocratiCode server on the broker's VM took the
bus out for **57m48s with nothing OOM-killed at all**. The kernel failed
*atomic* allocations in `tailscaled` and `ksoftirqd`, so the network path
degraded while every process stayed alive — and this worker did not reconnect on
its own, which is #94.

**`MemoryLow=` — adopted, and real only through the slice grant (#113).** The
worker and `tailscaled` each reserve 128M, against what each cgroup is charged
(measured 2026-09-24: the worker 67 MiB at peak; tailscaled 81 MiB, 60 MiB of it
RSS, with a 123 MiB peak), and
[`deploy/system.slice.d/replicator-memory.conf`](../deploy/system.slice.d/replicator-memory.conf)
grants `system.slice` their sum, 256M. The grant is the load-bearing file:
cgroup2 here is mounted without `memory_recursiveprot`, so a unit keeps no more
`memory.low` than its slice grants, and `system.slice` defaults to 0 — without
it both reservations are reported by `systemctl show` and protect nothing. That
is what #99 and #112 called inert: inert *as configured*, not structurally.

Under reclaim it keeps both working sets resident and moves the pressure onto
`init.scope` — the sessions, a root-level sibling holding ~4 GiB — and the
unreserved daemons. It is a reclaim priority, not a guarantee, and does nothing
for *atomic* allocations; `vm.min_free_kbytes` covers those. The grant tracks
the sum and no more, because without `memory_recursiveprot` an unclaimed grant
is never handed down; the slice is the host's, so a unit that later claims a
share here needs it added. `tests/test_deploy.py` checks the sum, and on this
host reads every claim under the slice back from `/sys/fs/cgroup`, as
`init-socraticode`'s `preflight.sh --check` does.

What this is not:

- **Not a substitute for capping the launch.** A cgroup cap on a process at adj
  -1000 *stalls* it rather than killing it, so the two halves are separate: this
  is the unit's half, and the capped invocation for anything that starts a
  SocratiCode server is in [COMMANDS.md](COMMANDS.md).
- **Not reachable with `earlyoom` (#112).** Measured 2026-09-24 against the
  packaged 1.7-2 in `--dryrun`: it skips `oom_score_adj` -1000 exactly as the
  kernel does (`kill.c`), `--prefer` or not — a preferred `sshd` prints badness
  300 and is passed over. So its order is the kernel's: `systemd --user` 733,
  `tailscaled` 670 until #113, the adj-0 daemons 666, journald 501, the worker
  71. It would shed small daemons, freeing little, while the ~1.7 GiB
  held at -1000 stays out of reach. The one session process it *can* take is
  one under `choom -n 500`, and the capped launch already bounds that. broker
  runs it on a "300 floor" reading of the same dry run — the score printed
  *before* the skip (CannObserv/broker#58) — and watcher on its sessions'
  `node` sitting at adj 0, which on that -1000 host it does not
  (CannObserv/watcher#323). What would change the answer: sessions leaving
  -1000, notifier's shape (CannObserv/notifier#74), which
  `tests/test_deploy.py` pins live; and, if it is ever installed here,
  `-s 100` — with 4 G of swap its default waits for swap to fall to 10% free.
- **Not `-1000`.** That is the exemption above, and an exempt worker that leaks
  is unreclaimable — the kernel would work through everything else on the box
  first. `-900` is the cohort's value (CannObserv/broker#25): last of the
  eligible, not exempt. `tests/test_deploy.py` pins both bounds, for both units
  and tailscaled's drop-in.

The handler unit carries it for a sharper reason than the worker does: memory
exhaustion is one of the conditions that *fires* it, so the moment it is most
likely to run is the moment an unprotected process is most likely to be killed.

All four files are copies under `/etc/systemd/system/`, so each needs its line
from the table above — `daemon-reload` alone silently keeps the old values, and
tailscaled keeps its old adj until it restarts.

### The co-core pin, and why the patch floor is load-bearing

`co-core` and `co-core-aio` come from the private GCS index `gs://co-gcs-pypi`,
mirrored into `./.wheelhouse` by `scripts/sync_wheelhouse.py` and resolved
through `[tool.uv] find-links` — never from PyPI.

Auth is ADC, with one substitution: on the VM the sync uses the read-only key named by `REPLICATOR_WHEELHOUSE_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`) rather than the worker's own `GOOGLE_APPLICATION_CREDENTIALS`, which is the replication writer (`/etc/replicator/co-gcs-replicator.json`, #29); in CI it is a keyless WIF token. Pin the current minor — `>=0.19.1,<0.20`. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead — a ValidationError at construction (0.8.0 requires `info_source_id` on all three fetch payloads, #19/#28) or an ImportError at load, never reaching a running worker (0.9.4 cuts the replicate contracts, #29).

**The 0.10 → 0.13 jump (#77) is the one departure from "pin the current minor", and what justified it is evidence rather than judgement.** The hazard that rule guards is payload fields moving under `extra="ignore"`; every module this service imports proved byte-identical between the two wheels — `pure/models/changes.py`, `pure/adapters/bus/{envelope,exceptions}.py`, `effects/{bus,fetch,gcs}.py`, `pure/util/{hashing,gcs}.py`, and all of `co_core_aio` bar its `__version__`. Only `streams.py` gained anything (`group_name`, `stream_kind` — cannobserv#384), and Replicator imports no `co_v1` adapter, which is where 0.13.2's other changes landed. **Diff the wheels before any future multi-minor bump**; the version distance is not the risk, the moved surface is. Unzip both from `.wheelhouse/` and `diff -rq` them — the whole check took a minute and turned a deferred change into a safe one.

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

**Dev server workflow** (the `/health` app, port 8001 so a future live service on 8000 stays up):

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8001 --reload --log-config src/core/log_config.json
```

## Environment Variables

Every variable either env file carries, with the reasoning behind each default,
and the boundary between them: [ENVIRONMENT.md](ENVIRONMENT.md).
