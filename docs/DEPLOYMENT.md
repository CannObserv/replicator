# Replicator Deployment

How code reaches `replicator.service`: the release deploy, the unit's lifecycle and
the guards it starts behind. Where co-core comes from, and its pin, are in
[DEPENDENCIES.md](DEPENDENCIES.md).
What it reports when it *fails* is in
[FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md); the host's memory tunables
are in [MEMORY-PROTECTION.md](MEMORY-PROTECTION.md).
`AGENTS.md` keeps the two-env-file boundary and
the deploy command; the reasoning behind each of them is here. The variables
themselves are in [ENVIRONMENT.md](ENVIRONMENT.md), which indexes every one and
sends the blob and temp-store settings on to [STORAGE.md](STORAGE.md); the VM,
broker and bucket topology this unit runs on is in
[INFRASTRUCTURE.md](INFRASTRUCTURE.md).
The start guards' own variables are the exception, kept [below](#environment-variables) (#146).
The design, its decisions and the measured disk budget: [the #151 design doc](plans/2026-11-14-releases-design.md).

## Releases (#151)

Both units run `/srv/replicator/live`, a link into `releases/<sha12>`: a read-only,
root-owned `git archive` of one commit on `origin/main`, with its own venv
(`uv sync --locked --no-dev`, through `scripts/co-pypi.sh`). **Nothing done in the
checkout reaches the service** until `scripts/deploy.sh` puts it there: not its
branch, not its venv, not an unpushed commit. The script is the deploying-releases
core (gregoryfoster/skills#372), installed, **never edited here** —
`tests/deploy/test_deploy_contract.py` checks its sha256 marker. This repo's half is
`scripts/deploy_hooks.sh`; a change the hooks can't express is raised on
gregoryfoster/skills.

### Deploying

From `/home/exedev/replicator`, as `exedev`. `shipping-work` runs the first form
itself (`.skills/deploy_command`):

```bash
scripts/deploy.sh                   # origin/main, once its CI passed
scripts/deploy.sh <sha12>           # an older build: this is how to roll back
scripts/deploy.sh --skip-ci         # GitHub can't answer: an emergency, journaled
scripts/deploy.sh --build-only      # build releases/<sha12>, switch nothing
scripts/deploy.sh --verify <sha12>  # restart and verify every unit on a build already live
journalctl -t replicator-deploy -n 20
```

Exit 0: deployed and verified. 1: refused, or failed and switched back to a build
that answers. 4: left on a build that does not answer; the message says what to run.

**What one deploy does**, in order: waits for CI on the commit (all four jobs,
`CI_JOBS`); builds `releases/<sha12>` and imports `src.worker.main` from it;
`post_build_check` asks the release's own writer guard (#52, below), refusing a
release that would build no writer; switches `live`; installs whichever unit
differs (keeping the old copy for a switch-back); restarts the worker, which
drains for up to `TimeoutStopSec=180`; and verifies it: a new main PID whose cwd
is the release, then its own `"worker ready"` line naming the build. A failed
verify switches back and restarts the old build. The restart counts toward
`StartLimitBurst=8`, as a manual one always did.

**Not the weekly measurement commit.** `github-actions[bot]` pushes get no CI run,
so the core refuses them: deploy the commit under it.

**Only commits from #151's merge on are deployable.** An older commit's units name
no `/srv/replicator/live`, and the core refuses it before switching anything.

**Two releases on disk, three for the minute a build takes.** `KEEP=0`: the core
always keeps the live release and the one it replaced, so a one-step rollback needs
no spare. Each is ~259 MB, under 1% of the 30 GiB root (#204): two put root near
54%, against the patching run's 84.0% ceiling. `KEEP=0` was chosen at 20 GiB, by
the design doc's *Disk* table.

### Once, before the first deploy (the operator)

1. The root, root's alone: `sudo install -d -o root -g root -m 755 /srv/replicator`.
2. Nothing else is new at #151. `/etc/replicator/.env`, the keys beside it and
   `co-pypi-reader.json` stay where they are; the hooks name the reader key's path.
   The service user came later ([below](#the-service-user-202)).
3. The cutover: `scripts/deploy.sh --no-restart <sha12>`, then
   `scripts/deploy.sh --verify <sha12>`. The runbook is on #151.

The deploy user needs `sudo -n` for `install`, `ln`, `mv`, `chmod`, `chown`, `tee`,
`touch`, `rm` under `/srv/replicator` and `/etc/systemd/system`, `readlink` under
`/proc`, and `systemctl`. `exedev` has it. The deploy user is not the service user.

### The service user (#202)

Both units run as `replicator`, a system user with no home, no shell, no sudo and no
docker. Until #202 they ran as `exedev`, whose passwordless sudo made a compromised
fetch or render root on the host, with `/home/exedev/replicator/.env`'s org-wide PATs in
reach. The account owns `/var/lib/replicator` (`StateDirectory=`, which systemd chowns to
`User=` at start) and nothing else.

**One group, shared.** `/etc/replicator` is `root:replicator 750` and its files
`root:replicator 640`. `exedev` is a member, so `scripts/load_env.sh`, the `gcs` tests,
`scripts/co-pypi.sh` and the deploy read it as before, and `exedev` can read nothing it
couldn't before. **A process holds the groups it started with:** a session older than
`usermod -aG` can't read the directory. Log in again, restart the VS Code server, or bridge
with `sg replicator -c '<command>'`. `sg replicator -c 'id -nG'` names `replicator` when it
works. `sg` runs `sh`, so a bash-only line goes through `bash -c` inside it:
`sg replicator -c 'bash -c ". scripts/load_env.sh && …"'`. A script with a bash
shebang, `scripts/deploy.sh` among them, needs nothing extra (measured 2026-11-15).

**Confinement.** Both units set `NoNewPrivileges=yes`, `ProtectSystem=strict`,
`ProtectHome=yes` and `PrivateTmp=yes`. That makes `/home` invisible, the system
read-only, and `/tmp` and `/var/tmp` private. Each was proven before it landed: `-m browser`
in a transient system service with all four, as a uid with no home, a live public render
and browser fetch, and the handler delivering a webhook. `RestrictNamespaces=` is ruled out
by measurement: Chromium's sandbox is user namespaces, and with them restricted every launch
fails `No usable sandbox!` (`tests/test_deploy.py`). Commands and logs are on #202.

**Making the account** comes before the first deploy that names it. The core's
`check_users` refuses a release whose `User=` doesn't exist, with nothing switched. As
root, once:

```bash
useradd --system --user-group --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin replicator
usermod -aG replicator exedev
chgrp -R replicator /etc/replicator   # modes unchanged: 750 and 640
```

Then deploy from a session that has the group: `sg replicator -c 'scripts/deploy.sh'`. Its
build reads `/etc/replicator/co-pypi-reader.json`.

**Rolling back past #202** (`scripts/deploy.sh <sha12>` of an older build) puts
`User=exedev` back. `exedev` lost nothing, because only the group changed, and systemd
chowns the state directory back at start. The runtime stamp moved from `build-id` to
`build-id.env` for exactly this. Root writes it, under a name the older unit never
truncates, and removes the old name for the older unit to recreate as its own.

**A Playwright bump's shell** goes into the state directory as its owner, from the live
release's venv:

```bash
sudo -u replicator env PLAYWRIGHT_BROWSERS_PATH=/var/lib/replicator/ms-playwright \
  /srv/replicator/live/.venv/bin/playwright install chromium-headless-shell
```

The capture runbook's `exedev` install predates the account. Everything the shell needs is
world-readable, so `-m browser` still runs as `exedev`.

### What a deploy never does

- Install a host config. The five in `deploy/` (`HOST_CONFIGS` in the hooks) are
  compared after every deploy and named when they differ; the operator installs
  them, with the commands below.
- Enable a new unit (`ENABLE_NEW` is empty), or remove one the release lacks.
- Restart anything under `--no-restart`.

## Server Lifecycle

**`replicator.service` runs the worker.** It binds no port, so there is no port to conflict over — but only one process should hold a given consumer name at a time.

| Situation | Action |
|---|---|
| Code merged to main | `scripts/deploy.sh` — or `shipping-work`, which runs it. No `uv sync`, no `cp`, no restart by hand |
| Rolling back | `scripts/deploy.sh <sha12>` of the build to return to (`ls /srv/replicator/releases`) |
| Which build is live | `readlink /srv/replicator/live`, `cat /run/replicator/build-id.env`, or the `"worker ready"` line's `build` |
| Testing a worktree/branch | `uv run python -m src.worker.main` (set distinct `REPLICATOR_CONSUMER_NAME` **and** `REPLICATOR_REPLICATE_CONSUMER_NAME` — **required** while the service runs, since #77 makes both loops derive the names the unit registers under). It builds no provider writer: see [the release check](#the-release-check--only-a-release-builds-a-writer-52-151) |
| Debugging the live service | `journalctl -u replicator -f` (`exedev` reads the system journal through `adm`) |
| After editing either unit in `deploy/` | Merge it and deploy: the core installs every unit that differs, `daemon-reload`s, and restarts the worker (never the handler, a template nothing runs until a unit fails) |
| After editing `deploy/99-co-replicator-memory.conf` | `sudo cp deploy/99-co-replicator-memory.conf /etc/sysctl.d/ && sudo sysctl --system` — **`sysctl --system`, not `daemon-reload`**, which does not read this file |
| After editing `deploy/tailscaled.service.d/memory.conf` | `sudo install -D -m 644 deploy/tailscaled.service.d/memory.conf /etc/systemd/system/tailscaled.service.d/memory.conf && sudo systemctl daemon-reload && sudo systemctl restart tailscaled` — the **restart** is for `OOMScoreAdjust=`, which applies at exec; read `/proc/$(systemctl show -p MainPID --value tailscaled)/oom_score_adj`, never `systemctl show -p OOMScoreAdjust`. It drops the tailnet briefly — 3 s on 2026-09-24, and the worker logged nothing |
| After editing `deploy/system.slice.d/replicator-memory.conf` | `sudo install -D -m 644 deploy/system.slice.d/replicator-memory.conf /etc/systemd/system/system.slice.d/replicator-memory.conf && sudo systemctl daemon-reload` — **no restart**; read `/sys/fs/cgroup/system.slice/memory.low` |
| After editing `deploy/apt/preferences.d/tailscale.pref` | `sudo install -D -m 644 deploy/apt/preferences.d/tailscale.pref /etc/apt/preferences.d/tailscale.pref` — **no reload**; `apt-cache policy tailscale` showing 600 over the site's 100 proves it. It scopes the Tailscale origin the `patching-hosts` maintenance lane follows (#131) |
| After editing `deploy/needrestart.conf.d/replicator.conf` | `sudo install -D -m 644 deploy/needrestart.conf.d/replicator.conf /etc/needrestart/conf.d/replicator.conf` — **no reload**, apt's hook reads it at the next dpkg run; `sudo needrestart -m u -r l -b` printing `Disabling Ubuntu mode` proves it loads. Without it a security apply restarts the worker mid-apply, outside any window (#122) |
| Reading what the failure handler reported | `journalctl -t replicator-failure` — **not** `journalctl -u`; why, in [FAILURE-NOTIFICATION.md](FAILURE-NOTIFICATION.md) |
| After a co-core pin move | the procedure in [DEPENDENCIES.md](DEPENDENCIES.md#moving-the-pin), then deploy |
| `Start request repeated too quickly` | `sudo systemctl reset-failed replicator && sudo systemctl start replicator` — the rate limit, not a broken build |

**A start syncs nothing.** `ExecStart` runs the release's own `.venv`, built at deploy
time, so no step at start reaches the package index or needs its token (#183,
`tests/test_deploy.py`). It resolves `live` before `exec`ing the interpreter: Python
reads `sys.prefix` from the path it was started by, so a worker started through the
link would, once a deploy moved it, import the next release's packages while it
drains.

**The build id is the release's `REVISION`**, a 12-character sha since #151 (a
7-character `git rev-parse --short` before). The unit stamps it into
`/run/replicator/build-id.env` (`build-id` before #202, [above](#the-service-user-202)),
which outlives a failed start so the failure handler can still name the build.

**The installed units are copies the deploy keeps in step.** Off the host, nothing
compares them; on it, `tests/deploy/test_deploy_units.py` checks each installed unit
against the **live release's** copy, never the working tree's, so a merge not yet
deployed is not drift. A hand edit under `/etc/systemd/system` is.

**Eight starts in two hours, and an iterative session will spend them.** `StartLimitIntervalSec=7200` with `StartLimitBurst=8` is sized against `worst_case_outage_seconds` so a permanently unreachable Redis surfaces as a *stopped unit* rather than a hot restart loop. The cost is that a ninth `systemctl restart` inside two hours — ordinary when shipping several commits in one sitting — fails with `Start request repeated too quickly` and `Result: start-limit-hit`, which reads as a broken deploy and is not one: the previous instance stops cleanly and logs `worker stopped` on its way out. `sudo systemctl reset-failed replicator` clears the counter; then `start` as normal. Check `systemctl status` for `start-limit-hit` before debugging the build — #77 hit this twice in one session.

**Why eight and not three (#94, #134).** Three starts absorbed 30 minutes of broker outage. On 2026-09-16 the broker was away for 58, and the only reason this unit was not asked to survive the whole of it is that the network degraded from ~14:28 while the worker's cycles did not fail *continuously* until ~15:00. #94 raised it to six, sized at `worst_case_outage_seconds` (~10 min) a start — an upper bound. Against a broker that refuses instantly a start absorbs only its backoff waits, ~7.5 min, with ~35 s of `RestartSec` and floor-check wait between starts. The burst counts every start, the deploy that started the running worker included, so an outage within two hours of a deploy gets one restart fewer: six covered ~48 min at that floor, seven ~56, eight ~64 (#134). `tests/test_deploy.py` pins that floor, not the ceiling, against `WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS` — the worst outage this cluster has actually had — rather than against internal consistency alone. Raise that constant when a worse one happens and the test will tell you whether the unit still covers it. Before #133, later starts exited on their first `ensure_group` (six absorbed ~12 min). Widening the fuse is safer than it was, because the other half finally exists: before #94 a unit that gave up was discovered by whoever next looked, and a longer fuse on a silent failure would be the wrong trade.

Rehearse reconnection before changing the start budget — the loop's half in pytest, systemd's in `scripts/rehearse_reconnect.sh`: [TESTING.md](TESTING.md#rehearsing-reconnection).

**Dev server workflow** (the `/health` app, port 8001 so a future live service on 8000 stays up):

```bash
. scripts/load_env.sh
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

### The release check — only a release builds a writer (#52, #151)

A dev worker started from a checkout (the *Testing a worktree/branch* row) inherits,
through the shell snippet under **Environment Variables**, the production ADC *and*
the production alias table, and would acquire a write identity against a bucket
whose objects it cannot delete (#38). So `build_writers` asks
`src/worker/checkout.py` first: **a release is a tree whose `REVISION` is a regular
file owned by root**, which only the deploy writes, through `sudo`, after its CI
gate. Anything else — a checkout on any branch, `main` included, or a `REVISION`
someone wrote by hand — builds **no provider writer**, logs `refusing to build a
provider writer — this code is not a release` at `error`, and changes nothing else:
the fetch path runs, the alias table is still read, and commands naming the alias
are refused `provider_disabled`. `REPLICATOR_ALLOW_ANY_CHECKOUT=1` builds the writers
anyway, which is how a branch is tested against the **test** bucket (#50).

The deploy asks the same function from each release before switching to it
(`post_build_check`), so a release that would refuse its own writers never goes live.

**What it replaced.** Until #151 the service ran the checkout, and
`scripts/check_main_checkout.sh` (#37, #48) refused to start it off `main` or off
unpushed commits, after a 2026-08-14 restart deployed a feature branch. That guard
turned a mis-deploy into a failed start, which is still an outage (#94). It was
deleted, not ported: the release is the evidence now, and the git verdicts it drew
could not be asked of a tree with no `.git`.

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

### Dependabot proposes the action bumps (#76)

`.github/dependabot.yml` proposes **action bumps only**, weekly. Every `uses:` in `ci.yml` pins a commit SHA with a `# vX.Y.Z` comment, and Dependabot moves both. `context-cadence.yml` is excluded, because the installer re-renders it (gregoryfoster/skills#373). `uv` stays parked under #169, until an org owner admits Dependabot to the co-pypi index (CannObserv/cannobserv#615). Guarded by `tests/ci/`.

A bot PR touches only `.github/`, so it needs **no deploy and no restart**. Merging it on GitHub puts `origin/main` ahead of this checkout: `git pull --ff-only` before the next local commit. If a bot PR is red at the WIF auth step, suspect the Dependabot run's OIDC token before the bump (the verdict table on #76). Read the action's release notes, not the diff: setup-uv v9 flipped `prune-cache` silently. A bump that changes behaviour takes fix commits pushed onto its branch.

## Environment Variables

Every variable either env file carries, with the reasoning behind each default,
and the boundary between them: [ENVIRONMENT.md](ENVIRONMENT.md).

The variables the unit's start guards read are the exception, kept here with the unit they gate
and indexed there by name (#146): the Redis floor check's timeout and wait — the floor itself is
[INFRASTRUCTURE.md](INFRASTRUCTURE.md#the-broker-is-co-broker--replicator-connects-it-does-not-run-one)'s
— and the release check's override. They live in `/etc/replicator/.env` like the rest:

- `REPLICATOR_REDIS_FLOOR_TIMEOUT` — seconds one `check_redis_floor.sh` probe may run before it is killed; default `5`. Read by the unit's `ExecStartPre` only, never by the worker. Exists because `redis-cli` has no connect timeout, and a `rediss://` URL against a plaintext endpoint hangs on the TLS handshake
- `REPLICATOR_REDIS_FLOOR_WAIT` — how long `check_redis_floor.sh` keeps retrying a broker it cannot reach before reporting the floor `UNVERIFIED`; default `30`, and `0` probes once. Exists because on a cold boot MagicDNS answers `broker` with no address for a moment after tailscaled starts, which `After=tailscaled.service` cannot see (#88; a wait for the tailnet *address* was tried and did not help). Only `unreachable` is retried — an auth refusal is an answer, and a silent failure has already spent the timeout. `WAIT + TIMEOUT` is held to half the unit's 90 s start budget by `tests/test_deploy.py`, because every `ExecStartPre` shares it. A non-integer is named at start and the default used
- `REPLICATOR_ALLOW_ANY_CHECKOUT` — builds provider writers from code that is not a release; **unset by default, which is the enforcing posture**. See [**The release check**](#the-release-check--only-a-release-builds-a-writer-52-151). Only the literal `1` overrides, and the worker logs a warning when it does. Production never sets it: the service runs a release, which needs none. A dev worker needs it before it can write to the test bucket from a branch (#50, #52)
