---
title: replicator runs releases, not its checkout
date: 2026-11-14
status: approved 2026-11-14 (all six operator decisions as recommended)
---

# replicator on releases

This note covers adopting the deploying-releases core (gregoryfoster/skills#372) under #151. The core's contract is in deploying-releases' `references/hook-contract.md`, vendored at `55ede55f`. This note records the inventory, and every decision where replicator differs from the core.

Inputs:
- the brief on #151 (the 20:21Z and 02:20Z comments);
- its survey (2026-10-23);
- the pickup review ([#151 comment](https://github.com/CannObserv/replicator/issues/151#issuecomment-6883786829)), which corrects the brief.

[measured] means read on `co-replicator` on 2026-11-14. Line numbers are from `91e87dc`.

## Inventory

| What | Found | Becomes |
|---|---|---|
| **Units** | `replicator.service` (long-running, `User=exedev`, `WorkingDirectory=/home/exedev/replicator` `:78`, `ExecStart=uv run --frozen --no-sync python -m src.worker.main` `:189`). `replicator-failure-notify@.service` (oneshot template, `WorkingDirectory=` `:17`, `ExecStart=` runs `scripts/notify_failure.sh` by absolute path `:62`). No timers. Both installed copies are byte-identical to `deploy/` [measured]. Nothing else in `/etc/systemd/system` is replicator's. | Both on `/srv/replicator/live`, installed by the deploy (D5). The handler is a template, so the core installs it with live and never restarts or verifies it. |
| **Other files in `deploy/`** | Five host configs. All match their installed copies except `99-co-replicator-memory.conf`, whose installed copy predates #112's comment edit. Its values are the same; only the comment line differs [measured]. Also `notifier-template.json`, which is neither a unit nor a host config. | The five host configs become `HOST_CONFIGS`: compared after every deploy, never installed. The first deploy names the sysctl comment drift. `notifier-template.json` moves to `docs/reference/` (decision 4). |
| **`EnvironmentFile=`s, and the secrets each unit reads** | Both units read `-/run/replicator/build-id` and `/etc/replicator/.env` (root:exedev 0640). The worker's file includes `/etc/replicator/.env`, which holds the broker URL with its password and the notify token, plus key paths under `/etc/replicator/`. The handler reads the same file. No unit reads the repo `.env`. `Settings` declares no `env_file` (`config.py:57`), so observo#695 does not apply. | Unchanged. The core reads no env file. |
| **Paths written under `WorkingDirectory`** | `src/**/__pycache__/` (the first import after a change), `.git/index` (the guard's `git status`), and `blobs/` only when `REPLICATOR_BLOB_DIR` is unset (production sets `/var/lib/replicator/blobs`). The browser lives in `/var/lib/replicator/ms-playwright`; render homes are `tempfile` in `/tmp`. No other write is relative to the working directory (grep of `src/`, `scripts/`). | Bytecode is compiled at build. `.git` is gone. The `blob_dir` default becomes absolute (decision 2). |
| **`ExecStartPre=` lines (integrity or health)** | `:135` `+mkdir /run/replicator` (runtime). `:162` `check_main_checkout.sh` (integrity). `:165` the `git rev-parse` `BUILD_ID` stamp. `:183` `check_redis_floor.sh` (health: redis-cli, no Python). | `:135` stays. `:162` is **deleted, not ported**. `:165` stamps from `REVISION` and fails the start if it is missing. `:183` stays, run from the release. |
| **`uv` sources by kind** | Registry only: PyPI, plus the private co-pypi index for `co-core*` (#183). No git, path or find-links source, and no `.wheelhouse`. | `uv sync --locked --no-dev` through `UV_WRAPPER=scripts/co-pypi.sh`. The repo's own wrapper stays until #201 (D5). |
| **Hand-run production steps in the docs** | AGENTS.md *Server Lifecycle* (`git pull --ff-only && co-pypi.sh uv sync --frozen && systemctl restart`). `docs/DEPLOYMENT.md` (the guard's verdict table, every deploy situation). The `cp` of every `deploy/` unit. `.skills/patching-hosts`' `inflight` and `health` (`cd /home/exedev/replicator && .venv/bin/python -m scripts.inflight`). | `scripts/deploy.sh` (`.skills/deploy_command`). `inflight` and `health` run from `/srv/replicator/live`. Units are no longer copied by hand. Host configs still are, and the deploy names any that drift. |
| **Checkout guards in `src/`** | `src/worker/checkout.py:74` `checkout_refusal()` runs `check_main_checkout.sh`, which `build_writers` asks before building any provider writer (#52). From a `git archive` release there is no script and no `.git`, so it refuses, and every replicate and persist command would close `provider_disabled`. Today's start record shows `replication_aliases: ["gcs-publication"]` live [measured]. | Changes in the same commit as the units (decision 1). `post_build_check` proves each release passes its own guard. |
| **The oldest deployable commit** | — | **This branch's merge.** An older commit's units name no `/srv/replicator/live`, so the core refuses it with "nothing switched". The rollback drill therefore needs a second release on the new shape (Cutover, step 7). |

## Decisions

The rows answer CannObserv/broker#22's design questions. "As the core" means its answer is adopted unchanged.

| # | Question | Decision |
|---|---|---|
| D1 | Location and ownership | As the core: `/srv/replicator`, with releases owned by root. Service user: `exedev` to start; a dedicated user is a follow-up issue, filed at §6 cleanup. |
| D2 | Atomicity and rollback | As the core: `releases/<sha12>`, and a link switched by `rename(2)`. A rollback is a deploy of the older build. **`KEEP=0`**: the core always keeps the live release and the one it replaced, so a one-step rollback needs no spare. See *Disk* below. |
| D3 | Git source | As the core. Only a commit on `origin/main` deploys to live. The weekly `github-actions[bot]` measurement commit has no CI run, and the core refuses it by design: deploy the commit under it. |
| D4 | Who deploys, and how | As the core: `scripts/deploy.sh` from `/home/exedev/replicator`, as `exedev`, with `sudo -n` (which works here [measured]). `.skills/deploy_command` is `scripts/deploy.sh`. |
| D5 | Dependencies and private wheels | `uv sync --locked --no-dev` per release, through `UV_WRAPPER=scripts/co-pypi.sh`: the repo's own wrapper, kept as it is. **Its swap to `install.sh --co-pypi --replace` is deferred to #201** (the operator, 2026-11-14): the shared wrapper has no `wheels` mode, and every pin move's closure diff uses it. This wrapper has no default key path, so the hooks export `REPLICATOR_CO_PYPI_CREDENTIALS` with a default of `/etc/replicator/co-pypi-reader.json`. That export goes with #201. |
| D6 | Env and state | `/etc/replicator/` only, unchanged. State: `StateDirectory=replicator` (`/var/lib/replicator`) and `/run/replicator`, as today. `Settings.blob_dir` defaults to `/var/lib/replicator/blobs` (decision 2). |
| D7 | Restart semantics | As the core: the worker is restarted when live moves and drained by `TimeoutStopSec=180`. **The restart window is a deploy's**, as today's manual restart is: one SIGTERM drain, then a start that counts toward `StartLimitBurst=8`. The start resolves the release before `exec` (decision 3), so a draining worker never loads the next release's packages. |
| D8 | Guards, relocated | `check_main_checkout.sh` is deleted, along with its unit line, its tests and DEPLOYMENT.md's verdict table. The #52 writer guard asks "is this a release?" (decision 1). `post_build_check` runs it from the release, so a release that would refuse its own writers is refused before the switch. |
| D9 | Build id and drift | `REVISION`, stamped by the unit into `/run/replicator/build-id` as `BUILD_ID=<sha12>` (an `EnvironmentFile=`, so the handler still reads it after a failed start). **The build id grows from 7 to 12 characters.** `/health` (dev only), the start record and the failure notice carry it, and none of them parses it. Drift: the consumer test `test_the_installed_unit_is_the_live_releases` on the host, plus the core's `HOST_CONFIGS` compare. A drift timer like broker's is a follow-up issue. |
| D10 | Failure visibility | `OnFailure=replicator-failure-notify@%n.service`, as today, with the handler run from the release. A failed verify switches back and is journaled under `replicator-deploy`. The switch-back restart counts toward the burst. |
| D11 | Dev services on the host | There is no dev target and no dev unit. **One named exception:** a hand-run dev worker (`uv run python -m src.worker.main` under distinct consumer names, `docs/COMMANDS.md`). It runs a checkout by design and builds no provider writer unless `REPLICATOR_ALLOW_ANY_CHECKOUT=1` is set (decision 1). |
| D12 | Tests | The core's consumer tests go in `tests/deploy/`. Plus: `tests/test_deploy.py` loses the guard tests (`:327`–`:369`) and checks the new `ExecStart`/stamp. `tests/worker/test_checkout.py` is rewritten for the release check. `tests/test_check_main_checkout.py` is deleted. A test holds `HOST_CONFIGS`' installed paths to the paths `test_deploy.py` already reads (needrestart, the tailscale pin). |
| D13 | Packaging | The core, installed by `install.sh` (a copy with its marker, never a symlink). The hooks are in `scripts/deploy_hooks.sh`. The skill is linked as the cohort's others are: `skills/deploying-releases` and `.claude/skills/deploying-releases`, plus a docs/SKILLS.md row. |
| D14 | Rollout | This repo, first adopter, under #151. Problems with the core are raised on gregoryfoster/skills with the operator's consent, never patched into the installed copy. |

### Decisions for the operator

1. **The #52 guard asks "is this a release?"** `checkout_refusal()` keeps its name and caller, and its verdict becomes:
   - none when the code root holds a `REVISION` owned by root. Only the core writes one, through `sudo`, after the CI gate.
   - otherwise, none when `REPLICATOR_ALLOW_ANY_CHECKOUT=1`, now read in-process because the script is gone.
   - otherwise, a refusal.

   **Behaviour change:** a dev worker in the checkout on `main` used to build writers, and now it doesn't without the override. That is stricter, and correct, because the checkout is no longer what production runs. *Recommended.* The alternative, porting the script's git verdicts, would keep a git dependency that the release can't satisfy.
2. **`blob_dir` defaults to `/var/lib/replicator/blobs`** (the unit's `StateDirectory=`, and production's own setting), not the relative `blobs`. Today a relative path would resolve inside a read-only release, and the first local-backend write would fail as `EACCES` at runtime. Production is unaffected: it sets the variable and runs the `gcs` backend. *Recommended* over a validation refusal, which would break every dev run that leaves the variable unset.
3. **The start resolves the link** [measured, 2026-11-14]:
   - A venv run as `/srv/replicator/live/.venv/bin/python` keeps `sys.prefix`, and so site-packages, **through the `live` link**, while `sys.path[0]` is the resolved cwd.
   - So between the core's switch and the old worker's exit (unit install, `daemon-reload`, up to 180 s of drain), a lazy third-party import in the old process loads the **new** release's package. Playwright, for one, launches its Node driver from its package directory. `src/` itself has no function-level import.
   - The fix: `ExecStart=/bin/sh -c 'exec "$(pwd -P)/.venv/bin/python" -m src.worker.main'`. `exec` keeps the Python process as the main PID, which `KillMode=mixed` and `verify_started_on` both need. *Recommended.*
   - The core's own unit template has the same property for every adopter, so this is raised upstream, with consent.
4. **`notifier-template.json` moves to `docs/reference/`.** It is POSTed to notifier once by the operator, never installed on the host. The other three files that name it follow the move.
5. **Disk headroom** (below): accept `KEEP=0` at the figures given there, or reclaim space first.
6. **The branch-switch proof** (retrofit.md §5.5) would change the main checkout's branch, which the operator's standing instruction forbids. Instead: with the worker running, `/proc/<pid>/cwd` is the release, and nothing it has open or mapped is under `/home` (`ls -l /proc/<pid>/{cwd,exe}`, `sudo grep -c /home/ /proc/<pid>/maps`). Plus `test_no_unit_names_home`. *Recommended.* Alternatively, the operator switches the branch themselves at the cutover.

### Disk

Measured 2026-11-14 ~20:40Z.

**Today:** root is 16.29 GB used of 19.93 GB usable, **81.7%**; df shows 82%.

**A release is ~259 MB (247 MiB), 260 MB on disk in the branch's trial build:**
- the venv is 243 MiB, 140 MiB of it playwright;
- the tree is 4.2 MiB.

The core builds `--no-dev`. The dev venv's 309 MiB is not the figure.

**What the core keeps:**
- **steady:** the live release, the one it replaced, and `KEEP` spares;
- **mid-build:** one more.

**The thresholds:**
- the patching run refuses at **84.0%** (its sensor rounds up);
- the headroom sensor alerts at **85%** and clears at 80% (`sensor.json`).

| After the worktree is removed (−321 MB) | Releases | Root |
|---|---|---|
| Before the cutover | 0 | 80.1% |
| `KEEP=0`, steady | 2 | **82.7%** |
| `KEEP=0`, mid-build | 3 | **84.0%** (the pickup comment said ~83.9%) |
| `KEEP=1`, steady | 3 | 84.0% |
| `KEEP=2`, steady | 4 | 85.3% |

So `KEEP=0`. Even then, root sits at the patching ceiling for the minute a build takes, and above the sensor's clear line for good.

**The nightly disk GC** already runs. Its last pass freed 197.5 MB and kept two things on policy:
- a 675 MB staged VS Code server ("the newest");
- the 450 MB pinned `npx` SocratiCode.

**Options:** the operator's choice. (a) and (b) together are *recommended*.
- **(a)** Accept the figures above.
- **(b)** Raise upstream (with consent): hardlink-dedupe identical files between root-owned releases. Both are read-only and root's, so status#14's cache-inode hazard doesn't apply. A second release would then cost a few MB plus whatever packages changed.
- **(c)** Ask Provisioner (#148) whether a staged VS Code build the client never switches to is worth keeping.

## Hooks

`scripts/deploy_hooks.sh`:

```bash
PROJECT=replicator
CI=github                 # public repo: anonymous
CI_AUTH=anonymous
GITHUB_REPO=CannObserv/replicator
CI_JOBS=(lint test
  "browser (the renderer's confinements, pinned headless shell)"
  "gcs (T4 against the test bucket)")
UV_WRAPPER=scripts/co-pypi.sh        # this repo's own until #201
# The repo's wrapper has no default key path (co_pypi_token.py), and a deploy's
# shell loads no env file. Goes with #201, whose wrapper defaults to this path.
export REPLICATOR_CO_PYPI_CREDENTIALS="${REPLICATOR_CO_PYPI_CREDENTIALS:-/etc/replicator/co-pypi-reader.json}"
SOURCE_DIRS=(src scripts)            # scripts.inflight runs from live (patching-hosts)
IMPORT_MODULES=(src.worker.main scripts.inflight)
HOST_CONFIGS=(
  "99-co-replicator-memory.conf=/etc/sysctl.d/99-co-replicator-memory.conf"
  "apt/preferences.d/tailscale.pref=/etc/apt/preferences.d/tailscale.pref"
  "needrestart.conf.d/replicator.conf=/etc/needrestart/conf.d/replicator.conf"
  "system.slice.d/replicator-memory.conf=/etc/systemd/system/system.slice.d/replicator-memory.conf"
  "tailscaled.service.d/memory.conf=/etc/systemd/system/tailscaled.service.d/memory.conf"
)
KEEP=0                                # see Disk

post_build_check() {   # the release passes its own #52 writer guard
  .venv/bin/python -I -B -c 'import sys
from src.worker.checkout import checkout_refusal
why = checkout_refusal()
sys.exit(why)' # None exits 0; a reason exits 1 and prints itself
}

verify() { # <unit> <want>
  case "$1" in
    replicator.service)
      # The new PID runs from the release, then its own start record names the
      # build: "worker ready" comes after the broker, the groups and the
      # writers, and its "build" is BUILD_ID as the unit stamped it.
      verify_started_on "$1" "$2" || return 1
      ...poll `journalctl _PID=<MainPID> -o cat` for a JSON line with
         .message == "worker ready" and .build == $want, within VERIFY_SECONDS...
      ;;
    *) return 2 ;;
  esac
}
```

The rest:
- **`build_extra`, `pre_switch_check`, `migrate`, `post_verify`:** deleted. There is no database, frontend or timer.
- **`ENABLE_NEW`:** empty. Both units already exist, and the worker is enabled.
- **The consumer test forbids helper functions** in the hooks file, so `verify`'s poll is inline.

## Units after the change

The `replicator.service` diff, in outline. Every other directive and comment stays:

```ini
WorkingDirectory=/srv/replicator/live
ExecStartPre=+/bin/bash -c 'mkdir -p /run/replicator && chown exedev:exedev /run/replicator'
ExecStartPre=/bin/bash -ec 'build=$(cat REVISION); echo "BUILD_ID=$build" > /run/replicator/build-id'
EnvironmentFile=-/run/replicator/build-id
EnvironmentFile=/etc/replicator/.env
ExecStartPre=/bin/bash /srv/replicator/live/scripts/check_redis_floor.sh
ExecStart=/bin/sh -c 'exec "$(pwd -P)/.venv/bin/python" -m src.worker.main'
```

The comments about `KillMode=mixed` ("uv forwards it") and the guard's ordering are rewritten to match. In the handler, `WorkingDirectory=/srv/replicator/live` and `ExecStart=/bin/bash /srv/replicator/live/scripts/notify_failure.sh %i`.

## The live-tree sweep

These are rewritten in the same branch:
- AGENTS.md: *Server Lifecycle* and the guard bullet;
- `docs/`: `DEPLOYMENT.md` (rebuilt on the skill's `DEPLOYMENT.md.tmpl`), `ARCHITECTURE`, `COMMANDS`, `ENVIRONMENT`, `SKILLS`, `STYLE` and `FAILURE-NOTIFICATION`;
- `scripts/inflight.py`'s usage line, and `scripts/rehearse_reconnect.sh` (it reads `deploy/replicator.service` from the checkout, which is correct for a rehearsal: no change beyond any path text);
- `.skills/patching-hosts`;
- the SocratiCode manifest, with `codebase_update` in the same change.

Left alone:
- `.skills/context-metrics.jsonl`, which is append-only history;
- the seven `docs/plans/` files that name the old path, which are history too;
- `.skills/context-loss-ok` and `context-seams-ok`, which the next curation run re-acks.

## Cutover

retrofit.md §5, as the runbook posted on #151 before it runs. It runs in the host's own session, and every root write is the operator's (the deploy's own `sudo -n` writes included).

**Order matters, for one reason measured on the branch.** The *installed* unit runs `ExecStartPre=/bin/bash /home/exedev/replicator/scripts/check_main_checkout.sh`, which this branch deletes. If the main checkout is fast-forwarded to `M` before the new units are installed, any `Restart=on-failure` in that window fails to start: the script is gone, and the step is fatal. And a failed cutover's switch-back restores the old units, which need the old tree. So:
- `M` reaches `origin/main` from the worktree;
- the deploy runs from the worktree, which holds the same `M`;
- the main checkout moves only once the worker is verified on the release.

0. **Preconditions.**
   - `M` is pushed (`git push origin 151-releases:main`, a fast-forward, after a fetch: the weekly bot can race it).
   - CI is green on `M` as its own push.
   - Root is ≤ 82% (~83.0% during the build, with the worktree's venv still present).
1. **Root.**
   - `sudo install -d -o root -g root -m 755 /srv/replicator`.
   - No credential lives in the old tree [measured: every key path is under `/etc/replicator/`], so §5.2 has nothing to move.
2. **`scripts/deploy.sh --no-restart M`, from the worktree.**
   - It builds the release, switches `live`, and installs both units with one `daemon-reload`.
   - The running worker is untouched. From here on, any restart runs the release.
   - Expect a `HOST_CONFIGS` note for the sysctl drop-in's comment drift.
3. **`scripts/deploy.sh --verify <M sha12>`.** It restarts the worker onto the release (a drain of up to 180 s), then runs `verify_started_on` and checks the `"worker ready"` record names `M`.
4. **Prove it.**
   - `/proc/<pid>/cwd` → `/srv/replicator/releases/<M>`.
   - `sudo grep -c /home/ /proc/<pid>/maps` → 0.
   - `/run/replicator/build-id` → `BUILD_ID=<M>`.
   - `"worker ready"` lists the same `replication_aliases` as before, with no `refusing to build a provider writer`.
   - This replaces §5.5's branch switch (decision 6).
5. **Move the main checkout.**
   - `git -C /home/exedev/replicator merge --ff-only origin/main`, then the dev venv's `uv sync`.
   - Remove the worktree (−321 MB).
6. **The rollback drill,** on the §6 cleanup commit `C`, pushed alone and green.
   - `scripts/deploy.sh` (→ `C`), then `scripts/deploy.sh <M>` (back), then `scripts/deploy.sh <C>` (forward).
   - Each step reuses its release: no rebuild.
   - Step 4's checks are repeated on the last one.
   - Afterwards, `/srv/replicator/releases` holds exactly `C` and `M`.

**If step 3 fails** and the core's own switch-back reports exit 4:
1. Put back the units from `M^` with `git show <M^>:deploy/<unit> | sudo tee /etc/systemd/system/<unit>`.
2. `sudo systemctl daemon-reload && sudo systemctl restart replicator`.

The main checkout is still on the old tree at that point, so it starts as before.

### §6 cleanup

These go in commit `C`, after the cutover:
- set `.skills/deploy_command` to `scripts/deploy.sh`. (`.skills/worktree_venv` does not exist here, so there is nothing to delete.)
- file the follow-ups, with consent where they go upstream:
  - the dedicated service user;
  - a drift timer;
  - decision 3's template issue on gregoryfoster/skills;
  - Disk option (b) on gregoryfoster/skills;
  - Disk option (c) on #148, if the operator wants it.

**Agent memory that stops being true at the cutover.** Named here so the coordinator can retire it. The memory directory is `~/.claude/projects/-home-exedev-replicator/memory/`.

- `MEMORY.md:3`, the *Develop in worktrees* index line: "the service deploys whatever HEAD the main checkout is on; never leave it on a branch, and push before restarting (the guard refuses unpushed commits)".
- `develop-in-worktrees.md`:
  - `:13`–`:21`, the **Why:** paragraph: `WorkingDirectory=/home/exedev/replicator`, the `git rev-parse` stamp, the 2026-08-14 mis-deploy, the #37/#48 guard, and "push before restarting".
  - `:27`–`:34`: the skills-refresh hook "stranded the service". After the cutover, an unpushed commit can't strand the service, because nothing starts from the checkout.
  - `:36`: "**How to apply:** … push before restarting (the guard refuses unpushed commits)".
  - `:72`–`:81`: "`.venv` is what `replicator.service` imports from", "`uv sync --frozen` in the main checkout is the deploy step", and "Harmless to the service — `python -m src.worker.main` puts main's cwd first". The shared-venv warning still holds for dev; the service half doesn't.
- `production-writes-are-handed-to-the-operator.md:12`: "`sudo systemctl restart replicator` after a pushed main" is the old deploy. A deploy is now `scripts/deploy.sh`, whose `sudo -n` writes go under `/srv/replicator` and `/etc/systemd/system`.
- `patching-hosts-on-co-replicator.md`: nothing names the checkout. The knob's `inflight`/`health` commands move with `.skills/patching-hosts` in this branch.
