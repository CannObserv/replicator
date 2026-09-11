# Replicator Moves to Its Own VM — `co-replicator`

**Date:** 2026-09-11
**Status:** Approved (brainstorming, 2026-09-11).
**Issue:** #88
**Cohort:** CannObserv/notifier#43 (the pattern) → CannObserv/broker#1 (the neutral broker) →
CannObserv/archiver#193 (the move this one most resembles) → this. Framing from
CannObserv/watcher#282: watcher keeps the shared VM; everyone else leaves.

---

## Goal

Move `replicator.service` **and** Replicator's development workspace off the shared `watcher` VM
(`lax`) onto a dedicated exe.dev VM, `co-replicator` (`pdx`), joined to the
`cannobserv.org.github` tailnet as `replicator` / `tag:replicator`. When it's done, nothing named
`replicator` runs on the watcher VM, and `tag:watcher` describes exactly one service.

## Why this is the smallest move of the four

| Precondition | Already true because |
|---|---|
| No data to port | No database. Durable state is the PEL and `replicator:cmd:*` keys **on the broker**, and bytes **in GCS** |
| A host change strands no PEL | Consumer names derive from the group, not the host (#77): `replicator-fetch-1`, `replicator-replicate-1` |
| Consumers don't read our disk | The blob store is `gcs` since 2026-08-20 (#7). `file://` URIs would break across hosts |
| A remote broker has a failure policy | `src/core/bus_client.py` (broker#1 R7) |
| A capped or mis-granted broker is survivable | OOM and NOPERM are transient (#79, #82) |
| Nothing calls in | The worker binds no port; the API is a dev-only `/health` |

**The payoff is the bus path.** Today watcher (`lax`) → broker (`pdx`) goes through DERP(sea) at
36–40 ms and never goes direct. `pdx` → `pdx` measured 1 ms direct (archiver#193). The GCS
buckets are `US-WEST1`, next to `pdx`.

## Decisions

| # | Decision | Rationale |
|---|---|---|
| **D1** | **exe.dev VM `co-replicator`, tailnet hostname `replicator`, `tag:replicator`**, `pdx`, 2 vCPU / 4 GB / **20 GB**, proxy `private`. | The cohort's `co-<name>` pattern (`co-broker`; `co-registrar` for archiver after exe.dev rejected every `archiver*` name). The tailnet name stays bare, so every broker URL and ACL rule reads naturally. Region comes from the account default: **never run `set-region`**, because `pdx` isn't one of its values and any value moves us out. 20 GB, not archiver's 10, because the dev workspace travels (D2) and the plan pools by actual usage. If exe.dev refuses the name, stop and ask. |
| **D2** | **The dev/agent workspace moves with the service.** `co-replicator` is both the deploy host and where Claude sessions, worktrees, `gh`, and dev workers run, as the watcher VM is today. | Archiver never decided this, and every commit in its epic came from the host it decommissioned (archiver#193 closing note). Leaving dev on the watcher VM would need a permanent watcher→replicator admin edge and keep Replicator's footprint on watcher's host, the opposite of the cohort's goal. |
| **D3** | **The SA keys are copied, not re-minted.** `co-gcs-replicator`, `co-gcs-test-replicator`, and `co-pypi-reader` travel by Tailscale SSH. **`co-pypi-reader` is shared with sibling services and is never deleted anywhere.** Decommission deletes Replicator-only key files and revokes nothing. | Operator's call: the residual risk of old copies staying valid is accepted. |
| **D4** | **Cutover is build-then-swap under an at-most-one-can-start invariant.** Build and rehearse on `co-replicator` with the unit installed and disabled. Then `disable --now` + **`mask`** on the old host, and `enable --now` on the new one, under the **same derived consumer names**. | Rejected: an overlap on slot `-2`. The pacer and its 429 escalation live in memory (`src/worker/pacing.py`), so two workers double the per-host rate against origins. It also strands a dead `-1` registration that our ACL can't `DELCONSUMER` (#77's leak). Rejected: a cohort window with the issuers paused. There is nothing on this host to port, and the streams buffer. `mask` because a habitual `systemctl start` on the watcher VM would otherwise bring up a second `replicator-fetch-1`. |
| **D5** | **No service port is reachable on `tag:replicator`.** The only inbound rule is a **temporary** `tag:watcher → tag:replicator:22` admin edge for the build phase, removed at decommission. `tag:replicator → tag:broker:6379` is already live. | Replicator is a pure broker client plus internet egress. Tailscale SSH needs **both** an `acls` rule and an `ssh` block: without the network rule the node never shows up to peers, and the `ssh` rule is never consulted (archiver#193, Phase 4). |
| **D6** | **Ports 8040/8041 → 8000/8001.** | 8040/8041 existed only to avoid neighbours on a shared box. Matches archiver's D2. Only docs reference them, no `src/` or `tests/` file. |
| **D7** | **`deploy/replicator.service` gains `After=tailscaled.service`**: ordering only, **no `Wants=`**. A wait-for-tailnet-address step is added **only if** the reboot test shows `check_redis_floor.sh` reporting `UNVERIFIED` on a cold boot. | The worker's backoff already absorbs tailnet lag (20 cycles, ~8 min, before it exits). What's at risk is the floor guard going blind at boot. The unit's existing comment already explains why `Wants=` is a dependency trap. Evidence decides the wait; if it's needed, the pattern is broker's `deploy/wait-for-tailnet-addr.sh` (`/proc/net/fib_trie`, never `ip addr`, per observo#479). |
| **D8** | **The fetch contract's *Provenance and trust* section is restated, not escalated.** The boundary is now per-service Redis ACL users (broker#2) plus the Tailscale ACL, not "localhost on a single trusted VM". "The cluster VM" becomes "Replicator's host". **No obligation changes.** The signing / allowlist / private-range conversation, whose trigger ("the moment the bus spans hosts") fired at broker#1's cutover, is **filed as its own issue**. | A VM move shouldn't carry a fetch-path capability change. A normative doc shouldn't describe a topology that no longer exists. Non-breaking; watcher and archiver, the issuers, are told through the follow-up issue. |
| **D9** | **SocratiCode is deferred to CannObserv/notifier#57** (shared Qdrant). No Docker, Node, or index on `co-replicator` in this epic. Sibling repos are **read-only HTTPS clones with no venvs**, cloned with their `GH_TOKEN_*` and **not** their deploy keys. | Other repos' write keys have no business on the host that fetches public URLs. The clones exist for cross-repo reading while filing issues. |
| **D10** | **Nothing Replicator-owned stays on the watcher VM** except `/etc/replicator/co-pypi-reader.json` (D3) and a root-only decommission archive. | Unlike archiver's D9, nothing depends on this checkout. Watcher's `SOCRATICODE_LINKED_PROJECTS` is `notifier` only, and watcher has no `REPLICATOR_REPO_PATH`. Re-check at decommission time. |

## What the move does not change

- **Two env files, and the boundary between them.** `/etc/replicator/.env` is the only file the
  unit reads; the repo `.env` (PATs) never reaches the service. Same host as today under D2.
- **The dev worker still inherits prod credentials** when it sources `/etc/replicator/.env`
  (#52). Carried as-is.
- **The checkout guard, the build stamp, `WorkingDirectory=/home/exedev/replicator`.** Same paths
  on the new host, so the unit file needs nothing beyond D7.
- **`REPLICATOR_BLOB_BACKEND=local` stays the compiled-in default.** On any production host it's
  `gcs`, and now it has to be: `file://` means nothing across hosts.
- **Integration tests never touch the production broker.** Spawned or scratch redis, plus
  broker#5's `databases 1` and broker#2's `citest` user.

## Prerequisites (operator)

- [ ] `EXE_API_TOKEN` in this repo's `.env`, scoped to at least `new` and `whoami`. Neither is there today.
- [ ] `TAILSCALE_KEY_REPLICATOR` in this repo's `.env`: single-tag `tag:replicator`,
      pre-approved, non-ephemeral, minted **before** first join. A tagged key applies its tag set
      wholesale, and retagging costs a `logout` plus a fresh `up` (notifier#43 F1).
- [ ] Tailscale policy: confirm `tag:replicator` is declared (broker#1's broker rule lists it as a
      `src`, which implies it). Add the D5 admin edge:

  ```jsonc
  "acls": [ { "action": "accept", "src": ["tag:watcher"], "dst": ["tag:replicator:22"] } ],  // temporary: removed at decommission
  "ssh":  [ { "action": "accept", "src": ["tag:watcher", "autogroup:member"],
              "dst": ["tag:replicator"], "users": ["exedev", "root"] } ]
  ```

- [ ] An explicit go before `new` runs. It creates a billable resource.

## Plan

Phases 1–3 run from a session on the watcher VM. From the end of Phase 3, sessions run on
`co-replicator`. Every sudo write is prepared as exact commands for the operator to run.

### Phase 1: Unit ordering *(repo; lands first)*

1. TDD: `tests/test_deploy.py` pins `After=tailscaled.service` and the absence of `Wants=` on it.
   Then `deploy/replicator.service`, with a comment block giving the reasoning.
2. `cp` it into place on the watcher VM too. It's harmless there (tailscaled runs), and an
   installed copy that differs from the repo is the trap `docs/DEPLOYMENT.md` names.

### Phase 2: Provision `co-replicator`

3. `new --name co-replicator --cpu 2 --memory 4GB --disk 20GB --setup-script …` through
   `POST https://exe.dev/exec`.
4. The setup script (sketch; its shape is the requirement):

   ```bash
   #!/usr/bin/env bash
   set -euo pipefail
   exec >>"$HOME/setup.log" 2>&1            # a path exedev can write; /var/log aborted notifier's
   KEY="$HOME/.ts-authkey"; umask 077
   trap 'shred -u "$KEY" 2>/dev/null || true; sudo shred -u /exe.dev/setup 2>/dev/null || true' EXIT
   printf '%s' '<TAILSCALE_KEY_REPLICATOR>' >"$KEY"
   sudo systemctl enable --now tailscaled   # exeuntu ships it; no curl | sh
   sudo tailscale up --auth-key=file:"$KEY" --hostname=replicator --ssh --accept-dns=true
   ```

   The shred sits in the `trap`, not at the end: under `set -e`, archiver's script aborted at
   `tailscale up` and its key survived in the journal.
5. Verify from the watcher VM:
   - `tailscale ssh exedev@replicator hostname` returns `co-replicator`;
   - `tailscale ping broker` from `replicator` goes **direct**;
   - `nc broker 6379` answers `-NOAUTH`;
   - `CorpDNS: true` in `tailscale debug prefs`;
   - the broker cannot reach `replicator`.
6. Reboot test before any traffic: same node ID, same IPs, same tag, `tailscaled` active.

### Phase 3: Build the host *(the live service keeps running on the watcher VM)*

7. Packages:
   - `redis-tools`, for `redis-cli`. Without it the floor check skips silently, as it did on `co-registrar`.
   - `redis-server` as a binary only, with the service **masked**. The integration tests spawn their own capped broker, and this node never runs one.
   - `jq` and `gh`. uv, Python 3.12 and git ship with exeuntu.
8. Service half:
   - install the `github-replicator` deploy key and `~/.ssh/config` alias;
   - clone `/home/exedev/replicator` on `main`, in sync with origin;
   - `/etc/replicator/` at `root:exedev 0750`, with the **live** `.env` copied verbatim (none of the four backups), the three SA keys, and `replication-aliases.json`, each at `0640`;
   - diff the variable names against the watcher VM's, and confirm `REPLICATOR_REDIS_URL` names `broker` by MagicDNS with the user explicit (archiver#195);
   - `sync_wheelhouse.py`, then `uv sync --frozen`;
   - `cp` the unit and `daemon-reload`. **The unit stays disabled and stopped.**
9. Dev half:
   - the repo `.env`: `GH_TOKEN_*`, `ANTHROPIC_API_KEY`, and the `REPLICATOR_TEST_*` variables;
   - `~/.gitconfig`;
   - `~/.claude/projects/-home-exedev-replicator/memory/`. The path is the same, so the project key matches;
   - `.claude/settings.local.json`;
   - `git submodule update --init`;
   - read-only HTTPS clones of archiver, watcher, notifier and broker (D9).
10. **The gate.** All of these, on `co-replicator`:
    - `uv run pytest` with the coverage gate, `-m integration`, `-m gcs` against both test buckets, and `ruff check`;
    - `check_main_checkout.sh` exits 0;
    - `check_redis_floor.sh` reports 7.x, not `UNVERIFIED`;
    - `redis-cli PING` succeeds as `replicator@broker`;
    - the worker's key lists one object in `co-gcs-blobs`.

    **No process joins a production consumer group or creates a group on a production stream**:
    either one fetches real commands.

### Phase 4: Cutover *(one restart's worth of outage)*

11. Before the window:
    - both checkouts on the same pushed `main` SHA, and `git status -sb` clean on both (the skills-refresh hook commits without pushing, and now runs on both hosts);
    - the broker operator snapshots the PEL of `replicator.fetch` and `replicator.replicate` from `co-broker`. Our ACL denies `XPENDING` and `XINFO`.
12. Old host: `sudo systemctl disable --now replicator`, then move the installed unit out of
    `/etc/systemd/system/` and `mask`. Confirm `worker stopped` in the journal.

    *Corrected at the cutover:* `mask` works by putting a `/dev/null` symlink at
    `/etc/systemd/system/replicator.service`, and that path is where the installed copy lives,
    so a bare `mask` fails with `File … already exists`. The file is byte-identical to
    `deploy/replicator.service`, so moving it aside loses nothing:
    `sudo mv /etc/systemd/system/replicator.service /var/backups/replicator-decommission/replicator.service.installed && sudo systemctl daemon-reload && sudo systemctl mask replicator`.
13. New host: `sudo systemctl enable --now replicator`. Then check the boot lines:
    - the checkout guard passes;
    - `BUILD_ID` shows the same SHA;
    - the floor check reports 7.x;
    - `alias table loaded` with `[primary]`;
    - the `co-gcs-blobs` preflight passes;
    - `worker ready` with `replicator-fetch-1` and `replicator-replicate-1`;
    - the fetch-policy replay is applied.
14. Anything pending under those names is reclaimed by `claim_stale` after
    `REPLICATOR_CLAIM_MIN_IDLE_MS`. The operator confirms the PEL returns to 0.
15. **Functional gate:** the next **real** `content.fetch` produces `blob_available`, and
    `watcher.blobs` consumes it. No manufactured production frames. For `content.replicate` the
    gate is the writer for `primary` being built, not an actual write.
16. Measure cold latency (fresh TCP + `AUTH` + `PING`) and warm `PING`, each **with its path**.
17. Reboot `co-replicator` with the service enabled. Identity is unchanged, `NRestarts=0`, and the
    floor-check result is recorded, which settles D7's wait question.
18. **Rollback**, until Phase 7: new host `disable --now`; then on the old host `unmask`, `cp`
    `deploy/replicator.service` back into `/etc/systemd/system/`, `daemon-reload`, and
    `enable --now`. The consumer names are the same, so the PEL follows. Triggers:
    - the worker never reaches `worker ready`;
    - the GCS preflight fails;
    - fetch failures appear that the old host never produced.

### Phase 5: Docs *(lands right after step 15 passes)*

Present-tense claims only. History keeps its facts: archiver's lesson is that renaming a port in
an incident narrative falsifies it.

19. `AGENTS.md`: Project Overview, Infrastructure and Server Lifecycle. "Single-VM setup … shared
    with archiver, watcher, and notifier" becomes the dedicated `co-replicator`. "Redis is
    Archiver-operated" becomes CannObserv/broker on `co-broker`, which has been wrong since
    2026-09-08. Ports per D6. Keep it net-neutral against the context budget.
20. `docs/DEPLOYMENT.md`: the topology table, the neighbours line, the Redis section, and the
    pending `rm -rf /var/lib/replicator/blobs` step (it closes in Phase 7). Ports.
21. `docs/TESTING.md` and the `tests/conftest.py` docstring: "on this VM is the Archiver-operated
    broker" is stale. `docs/ENVIRONMENT.md`: the prefix rationale moves from "shared VM" to cohort
    convention. `docs/COMMANDS.md` and `README.md`: ports.
22. Comments: `src/core/config.py:10`, `:358`; `src/worker/handler.py:222`;
    `scripts/check_main_checkout.sh:100`. `src/worker/main.py:166` is #77's history and stays.
23. New `docs/reference/tailscale.md` for this node, deferring the primer to observo's copy. It
    carries:
    - the two-names/two-resolvers trap (`replicator` → `100.x`, `co-replicator` → `10.42.x`);
    - `CorpDNS`;
    - the shred-in-a-`trap` lesson;
    - that peers see a node only through `acls` rules, not `ssh`;
    - the latency table from step 16.
24. That this host has no SocratiCode install until notifier#57 is recorded in
    `docs/reference/tailscale.md`, not `docs/SOCRATICODE.md`: the latter is generated by
    `init-socraticode` and marked do-not-hand-edit.
    `.socraticodecontextartifacts.json` descriptions that name ports or the shared VM are updated.
    Re-indexing waits for notifier#57, which starts from a fresh index.
25. D8: restate `docs/contracts/content-fetch-issuer-reference.md` → *Provenance and trust*.
    `docs/contracts/replicator-boundaries.md:105` ("the shared VM") is edited only after
    confirming `tests/test_boundaries.py` doesn't pin that text. Otherwise the charter and its
    test change together.

### Phase 6: Soak

26. At least 24 hours on `co-replicator`, including step 17's reboot, before any Phase 7 step.

### Phase 7: Decommission on the watcher VM *(explicit go; a script the operator runs there)*

`tag:replicator` has no route to the watcher VM, so the operator runs this on that host, from a
session outside the checkout being removed.

27. Unit: `unmask` (the installed copy already left at step 12, into
    `/var/backups/replicator-decommission/`), `daemon-reload`, `reset-failed`.
28. Remove `/run/replicator/` and `/var/lib/replicator/`, including `blobs` (~2 MB; its horizon
    passed 2026-08-27).
29. `/etc/replicator/`:
    - archive `.env` and the unit to root-only `/var/backups/replicator-decommission/`;
    - delete the four `.env` backups, `co-gcs-replicator.json`, `co-gcs-test-replicator.json` and `replication-aliases.json`;
    - **keep `co-pypi-reader.json`** (D3).
30. Home: remove `~/co-gcs-replicator.json` and `~/co-gcs-test-replicator.json`. **Keep**
    `~/co-pypi-reader.json`, and watcher's `~/co-gcs-blob-reader.json`.
31. Re-run the dependency check (D10). Then remove:
    - the checkout, with its `.venv` (181 MB) and `.wheelhouse` (33 MB);
    - `~/replicator-wt/`;
    - the repo `.env`;
    - the `github-replicator` alias and key;
    - `~/.claude/projects/-home-exedev-replicator`, **only after** its copy is verified on the new host.
32. Tailscale: remove the D5 `:22` edge, and `tag:watcher` from the `ssh` source list.
33. Record: zero units matching `replicator` on the watcher VM, and the reclaim, honestly (~215 MB).

### Cross-repo follow-ups *(filed with each repo's own token, never committed)*

- **watcher#282:** replicator has left, and its Phase 4 step 17 is done by us. "Retain both
  checkouts" doesn't apply to this one. `tag:watcher` now describes one service.
- **broker#8:** replicator's latency and path, from step 16.
- **notifier#57:** replicator's checkout now lives on `co-replicator`, which bears on its Q4.
- **A new replicator issue:** the D8 escalation (signing / URL allowlist / private-range refusal).

## Risks

1. **Two workers at once.** The one real correctness hazard: a shared PEL and double origin rate.
   The at-most-one-can-start invariant plus `mask` guard against it (D4).
2. **The setup script aborts and leaves the auth key behind.** The shred sits in a `trap`
   (step 4). The key is single-use, and `KeyExpiry: None` means the node never consults it again.
3. **exe.dev refuses `co-replicator`.** Stop and ask; don't improvise a name.
4. **Keys valid on two hosts through the soak, and after.** Accepted (D3).
5. **Unpushed hook commits refuse a start**, now on two hosts. `git status -sb` before every start.
6. **`CorpDNS` false, so `broker` doesn't resolve.** The worker backs off for ~8 min and exits;
   three strikes in an hour leave the unit stopped. Verified at step 5 and again at step 17.
7. **The dev worker holds prod credentials (#52).** Unchanged from today.
8. **A new egress IP, and 429 escalation lost at the swap.** Same as any restart: a cold worker
   is polite from scratch, at the published floor.
9. **A gap in the agent workspace.** Memory and settings are copied in step 9 and verified before
   step 31 removes the originals.

## Success criteria

- [ ] `co-replicator` in `pdx`, tailnet `replicator` / `tag:replicator`, non-expiring, survives a
      reboot with the same identity and `NRestarts=0`, with the service enabled
- [ ] No service port reachable on `tag:replicator`, and the temporary `:22` edge removed
- [ ] Full suite green on `co-replicator`: default, `-m integration`, `-m gcs`, and ruff
- [ ] `check_redis_floor.sh` reports the broker version at boot, not `UNVERIFIED`
- [ ] A real `content.fetch` → `blob_available` → `watcher.blobs` round trip, on the new host
- [ ] Cold and warm broker latency recorded **with path**; the path to `broker` is direct
- [ ] No reference to 8040/8041 or to a shared VM as a present-tense claim, outside `docs/plans/`
- [ ] The fetch contract's trust section describes the real boundary; the escalation issue is filed
- [ ] Nothing named `replicator` remains on the watcher VM except `co-pypi-reader.json` and the
      root-only archive; reclaim recorded
- [ ] Development happens on `co-replicator`: agent memory, `gh` and worktrees all work there

## Out of scope

- The fetch-destination guard and message signing: D8's follow-up issue.
- SocratiCode on `co-replicator`: notifier#57.
- The headless-browser fetch path (#63) and any resize it would need.
- Key rotation or revocation (D3).
