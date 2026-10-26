---
title: Operator runbook for viewport capture — the browser runtime and the delegated unit
date: 2026-10-25
status: phase A ready with PR 1; phase B lands with PR 2
plan: 2026-10-25-viewport-capture-design.md
---

# Operator runbook — viewport capture (#138)

On `co-replicator`, as `exedev`, from the main checkout (`/home/exedev/replicator`). Phase A
changes production even though nothing renders yet: the worker moves into `<unit>/worker`.
Phase B turns capture on. Each phase says what to send back before the next.

## Phase A — delegation and the runtime (after PR 1 merges)

### A1. Deploy the delegated unit

```bash
cd /home/exedev/replicator
git pull --ff-only && uv sync --frozen
sudo cp deploy/replicator.service /etc/systemd/system/replicator.service
sudo systemctl daemon-reload && sudo systemctl restart replicator
```

Verify. All three must hold:

```bash
S=/sys/fs/cgroup/system.slice/replicator.service
cat $S/cgroup.subtree_control                  # memory pids
cat $S/memory.low $S/worker/memory.low         # 134217728 twice
journalctl -u replicator -n 50 -o cat | grep '"worker ready"' | tail -1   # "worker_memory_low": 134217728
uv run pytest --no-cov tests/test_deploy.py -k 'reservation or worker_leaf or adjust' -rs
```

`worker_memory_low` reads `"not delegated"` when the unit wasn't copied, and `"claim failed"`
when the copy was denied (the ERROR line before it names the path). Either way the worker is
running but unprotected from reclaim. Fix it before A2.

### A2. Install the pinned shell

```bash
df -h /                                         # needs ~450M free
PLAYWRIGHT_BROWSERS_PATH=/var/lib/replicator/ms-playwright \
  uv run playwright install chromium-headless-shell
ldd /var/lib/replicator/ms-playwright/chromium_headless_shell-*/chrome-headless-shell-linux64/chrome-headless-shell \
  | grep 'not found'                            # expect nothing (0 lines on 2026-10-26)
```

**Install no system packages unless `ldd` names a missing library**, and then only the package
that provides it. Don't follow `playwright install-deps --dry-run`: it lists 21 packages here (installs and upgrades of ones already present),
including `xvfb`, `xserver-common` and font sets, which a headless shell doesn't need. **Fonts
are a decision, not a dependency.** Pages render with the fonts already installed, and adding a
font package changes how captures look and every digest after it, so it's a change of its own,
not part of this step.

`StateDirectory=replicator` owns `/var/lib/replicator`, so no sudo is needed for the install. Expect
`chromium_headless_shell-<rev>` (~273M) and `ffmpeg-<rev>` (~5M). The Python package and its
bundled Node (~138M) are already in `.venv` from `uv sync`.

Verify against the real shell, in a delegated scope like the unit's:

```bash
REPLICATOR_PLAYWRIGHT_BROWSERS_PATH=/var/lib/replicator/ms-playwright \
  systemd-run --user --scope -q -p Delegate=yes -E PATH -E REPLICATOR_PLAYWRIGHT_BROWSERS_PATH \
  -- uv run pytest --no-cov -m browser -rs
```

It should pass in full: about 18 tests, around 35 s. It must not skip anything; a missing
runtime or an undelegated scope fails it.

**Send back:** the `worker ready` line, the two `memory.low` values, `df -h /` before and after,
and the `-m browser` summary line.

### Rollback (phase A)

```bash
git -C /home/exedev/replicator show <previous-main-sha>:deploy/replicator.service \
  | sudo tee /etc/systemd/system/replicator.service >/dev/null
sudo systemctl daemon-reload && sudo systemctl restart replicator
rm -rf /var/lib/replicator/ms-playwright
```

The code tolerates the old unit: `worker ready` says `"not delegated"`, and the unit's own
`MemoryLow=` protects the worker again, as before #138.

## Phase B — turn capture on (after PR 2 merges)

Written with PR 2: the operator adds `REPLICATOR_CAPTURE_ENABLED=true` to
`/etc/replicator/.env`, restarts, checks `worker ready` for `capture: enabled`, and then asks for
broker's `acladmin` read-back of `replicator.capture` on broker#116.

## Upgrading Playwright

The pin is exact (`pyproject.toml`), because each release pins one Chromium revision. A bump
is: the lock change merged, then on the host `git pull`, `uv sync --frozen`, **A2's install
line**, and only then the restart. A restart without the install leaves the worker running and
capture unable to launch the new revision. After PR 2 that shows as `capture: unavailable` and a
`CRITICAL` naming this runbook. Remove the old `chromium_headless_shell-<old rev>` directory
afterwards. Node rides inside the package, so it is an outside-apt runtime for
`patching-hosts`, patched by this bump and no other way.
