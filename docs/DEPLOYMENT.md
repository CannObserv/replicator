# Replicator Deployment

The systemd unit's lifecycle, the guards it starts behind, what it reports when
it fails, and the co-core pin. `AGENTS.md` keeps the two-env-file boundary and
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
| Reading what the failure handler reported | `journalctl -t replicator-failure` — **not** `journalctl -u`, see below |
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

**`/etc/systemd/system/replicator.service` is a *copy*, not a symlink to `deploy/`.** So the `cp` above is load-bearing and `daemon-reload` alone silently does nothing — systemd re-reads the installed file, which is still the old one. The failure has no symptom at restart: the worker comes up on the new code under the *old* unit, and the mismatch only surfaces the first time a directive actually matters. Nothing guards it, either — `tests/test_deploy.py` reads the repo file, which is exactly the copy that is still correct. Diff the two when a restart follows a unit edit (#11 deploy).

The copy is deliberate, for the same reason `/etc/replicator/.env` is not read from the repo: the live unit must survive a repo reset, a worktree switch, or a branch checkout that happens to be mid-edit.

**Two unit files now, and the second one is easy to forget.** `deploy/replicator-failure-notify@.service` is the `OnFailure=` handler, and it is a copy under `/etc/systemd/system/` exactly like the worker's unit — with one difference that makes its absence quieter: nothing runs it until something fails, so a missed `cp` is invisible until the first incident, which is the one moment it was supposed to help. `systemctl status replicator-failure-notify@replicator.service.service` answering `Unit ... not found` is how that looks. There is no restart to pair with the copy.

### When the worker fails, who is told

The unit bounds its restart loops and then stays `failed` on purpose — that is the design, and it stays. What was missing was the other half. `deploy/replicator.service` claimed in its own comments that a failure was visible "in `systemctl status` + `OnFailure=`" while the ini file carried **no `OnFailure=` directive at all**. On 2026-09-16 the unit sat `failed` for 56 minutes and what noticed was a *sibling repo* reading the broker from another VM, not this host.

`OnFailure=replicator-failure-notify@%n.service` closes it. The handler writes a `CRITICAL` journal record naming the unit, the host and the build, then POSTs the same incident to `REPLICATOR_NOTIFY_URL` when one is configured. With that variable unset — how it ships — the record is the whole behaviour, which is deliberate: the wiring did not have to wait on a notifier channel, and enabling delivery later is a line in `/etc/replicator/.env`, not a code change.

**Read it with `journalctl -t replicator-failure`, not `journalctl -u`.** `%n` expands to the full unit name *including* its suffix, so the handler instantiates as `replicator-failure-notify@replicator.service.service` — a doubled suffix. That is the canonical systemd idiom and it is kept, because it is what makes the record name the failed unit precisely; the price is that the obvious `journalctl -u replicator-failure-notify@replicator.service` returns nothing at all. Measured in the #94 rehearsal, which is a bad place to learn it. The `SyslogIdentifier=replicator-failure` in the handler exists to pay that price off, and `tests/test_deploy.py` pins its presence.

The handler cannot make an incident worse, by construction: it is `Type=oneshot`, it carries no `OnFailure=` of its own (systemd honours the directive on handler units too, so one that could fail into itself would chain), and `scripts/notify_failure.sh` exits `0` on every path — unconfigured, no `curl`, unreachable notifier, malformed URL, 5xx, or timeout. A notifier outage correlates with the broker outages that fire this, so an undeliverable dispatch is the expected case and degrades to the journal record rather than to a second failed unit.

#### The variables the handler reads

All of them live in `/etc/replicator/.env` and are read by the `OnFailure=` handler **only** — never by the worker, which is why they are here rather than in [ENVIRONMENT.md](ENVIRONMENT.md) with the settings `src/core/config.py` parses.

- **`REPLICATOR_NOTIFY_URL`** — where the incident is POSTed. **Unset by default, and that is a working posture, not a broken one**: the handler writes its `CRITICAL` journal record and dispatches nothing, which is how this shipped, so the `OnFailure=` wiring did not have to wait on a notifier channel being provisioned. The payload is a self-describing incident object (`level`, `event`, `unit`, `host`, `build`, `message`, `timestamp`), sent as-is in webhook mode, which is what lets the handler serve a plain webhook. For the cohort notifier set `REPLICATOR_NOTIFY_MODE=notifier` and point this at `http://notifier:9000/api/v1/dispatch`. See below. A dispatch that fails is logged and dropped, never retried: systemd is holding no queue, and the journal record already survived.
- **`REPLICATOR_NOTIFY_TOKEN`** — sent as `Authorization: Bearer` in webhook mode and as `X-API-Key` in notifier mode (the tenant's `nk_…` key). Unset means **no header at all**, not an empty bearer, which would read as a configured credential that is merely wrong. Passed to `curl` through a `--config` on **stdin, never argv**, so it does not appear in `ps` or `/proc/<pid>/cmdline` for the life of the dispatch — AGENTS.md treats this env boundary as a security boundary, and argv is the usual way a secret crosses one.
- **`REPLICATOR_NOTIFY_TIMEOUT_SECONDS`** — ceiling on one dispatch; default `10`, well inside the handler unit's own `TimeoutStartSec=60`. That gap is deliberate: the hung-notifier case is the *expected* one here, since the outages that fire this handler are the ones that degrade the tailnet both VMs sit on, and blowing the outer timeout would turn a notification into a second failed unit. A value that is not a positive integer is **named in the journal and replaced by the default** rather than handed to `curl` — unvalidated it came back as a bare `curl_exit: 2`, indistinguishable mid-incident from the notifier being down, so an operator would chase the wrong VM instead of their own typo.

- **`REPLICATOR_NOTIFY_MODE`** — `webhook` (default) or `notifier` (#108). Any other value is **named in the journal and dispatches nothing**. The likeliest unknown value is a misspelt `notifier`, and sending the flat payload to `/dispatch` would come back as a 422 that looks like notifier's fault.
- **`REPLICATOR_NOTIFY_TEMPLATE_ID`** — notifier mode: the ULID notifier assigned when the operator POSTed [`deploy/notifier-template.json`](../deploy/notifier-template.json) to `/api/v1/templates`. Not stable across re-creation.
- **`REPLICATOR_NOTIFY_CHANNEL_IDS`** — notifier mode: comma-separated channel ULIDs; whitespace is trimmed. Notifier requires at least one even with a template, so notifier mode without this, or without the template id, **records locally and dispatches nothing**. The same goes for a template id or channel entry that is not a ULID. It is named in the journal before any request is sent, so a typo, or a pasted `<placeholder>` (which cost a 422 in the #108 smoke test), costs a log line and not an alert.

**Notifier mode** (agreed on CannObserv/notifier#70, pinned against `DispatchRequest`/`DispatchOut` in notifier's unauthenticated `http://notifier:9000/openapi.json`) sends `{template_id, channel_ids, variables, idempotency_key, metadata: {event}}`, where `variables` is the same seven-field incident. Three things differ from webhook mode:

- **`idempotency_key` is `<unit>:$MONITOR_INVOCATION_ID`**, the failed run's InvocationID, which systemd ≥251 hands an `OnFailure=` handler (co-replicator runs 255). It is null when systemd supplied none. When `<unit>:<id>` would exceed notifier's 200-character cap, the key is the id alone, which is still unique per failure. **Never reuse a key to retry:** notifier answers a replayed key with the *prior* record, `failed` included, and makes no new delivery attempt. The handler makes one POST per failure and no retries.
- **Delivery is scored on the 202 body's `status`**, not the 202 itself: `succeeded` → `INFO unit_failed_notified`, `partial` (one of several channels failed) → `WARNING unit_failed_notify_degraded`, `failed` → `ERROR unit_failed_notify_undelivered`. A body without a readable `status` (or no `jq` on the host) → `WARNING unit_failed_notify_unconfirmed`, never counted as delivered. Each record carries `delivery_status`.
- **The template's schema is deliberately loose.** The seven fields are required strings, with no enums and extra properties allowed. On an alert path a 422 loses the alert, so a new level or an eighth field has to degrade to "rendered", not "rejected". `tests/test_notify_failure.py` pins the template against what the script emits.

Smoke-test with a throwaway oneshot that exits 1 and carries `OnFailure=replicator-failure-notify@%n.service`, never by failing `replicator.service`. The handler takes the unit name from `%i`, so nothing in it is specific to the worker.

**`build` is the worker's build, so only `replicator.service` carries it.** The handler reads `/run/replicator/build-id` whichever unit failed. Any other unit sent through this handler (the `notify-smoke-test.service` smoke test, for one) records `build: "<n/a>"`, and its message leaves the build out. Otherwise the alert would claim a build that unit never had.

A failed dispatch records `reason` alongside `curl_exit`, mapped from curl's exit code (`6` unresolvable, `7` refused, `28` timed out, `35`/`60` TLS, …), because curl's own error sentence is discarded with its stderr. It also records **`response`, the first 512 bytes of whatever the far end answered**, reduced to printable ASCII so any body leaves one parseable JSON line. Notifier's 422 detail names the offending field, and without the excerpt the journal said only `422`. The unconfirmed-delivery record carries the same excerpt. The token travels in a header, and notifier's 422 echoes only the request body, so it cannot surface there. A custom webhook that echoes headers back into its error page would be a reason to leave this handler in record-only mode.

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

**What now sits at the top of the eligible list is `tailscaled`, at 675.** Read
that against CannObserv/broker#17, where the failure *was* the tailnet: killing
tailscaled takes the bus away exactly as effectively as killing this worker, and
no directive of ours reaches a system unit. Measured 2026-09-18, filed as an
observation rather than fixed here.

CannObserv/broker#17 is what it costs when it fires, and it fires in a shape
worth recognising: launching a SocratiCode server on the broker's VM took the
bus out for **57m48s with nothing OOM-killed at all**. The kernel failed
*atomic* allocations in `tailscaled` and `ksoftirqd`, so the network path
degraded while every process stayed alive — and this worker did not reconnect on
its own, which is #94.

Three things this is not:

- **Not a substitute for capping the launch.** A cgroup cap on a process at adj
  -1000 *stalls* it rather than killing it, so the two halves are separate: this
  is the unit's half, and the capped invocation for anything that starts a
  SocratiCode server is in [COMMANDS.md](COMMANDS.md).
- **Not reachable with `earlyoom`.** It floors a `--prefer` match at 300, while
  a service at adj 0 reads ~670 on this kernel — it would choose the worker too.
- **Not `MemoryLow=`.** The obvious next reach, and it is inert on this host:
  cgroup2 is mounted without `memory_recursiveprot` and no slice above grants
  one, so a reservation on either unit would be silently ineffective. #99 step 3
  prescribes it generically; `init-socraticode`'s `preflight.sh --check` reports
  the mount state and is the fastest way to re-confirm it.
- **Not `-1000`.** That is the exemption above, and an exempt worker that leaks
  is unreclaimable — the kernel would work through everything else on the box
  first. `-900` is the cohort's value (CannObserv/broker#25): last of the
  eligible, not exempt. `tests/test_deploy.py` pins both bounds, for both units.

The handler unit carries it for a sharper reason than the worker does: memory
exhaustion is one of the conditions that *fires* it, so the moment it is most
likely to run is the moment an unprotected process is most likely to be killed.

Both files are copies under `/etc/systemd/system/`, so this needs the `cp` pair
from the table above — and `daemon-reload` alone will silently keep the old
values.

### The co-core pin, and why the patch floor is load-bearing

`co-core` and `co-core-aio` come from the private GCS index `gs://co-gcs-pypi`,
mirrored into `./.wheelhouse` by `scripts/sync_wheelhouse.py` and resolved
through `[tool.uv] find-links` — never from PyPI.

Auth is ADC, with one substitution: on the VM the sync uses the read-only key named by `REPLICATOR_WHEELHOUSE_CREDENTIALS` (`/etc/replicator/co-pypi-reader.json`) rather than the worker's own `GOOGLE_APPLICATION_CREDENTIALS`, which is the replication writer (`/etc/replicator/co-gcs-replicator.json`, #29); in CI it is a keyless WIF token. Pin the current minor — `>=0.13.1,<0.14`. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead — a ValidationError at construction (0.8.0 requires `info_source_id` on all three fetch payloads, #19/#28) or an ImportError at load, never reaching a running worker (0.9.4 cuts the replicate contracts, #29).

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
