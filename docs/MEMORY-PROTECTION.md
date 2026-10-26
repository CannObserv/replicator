# Memory protection

How the worker and `tailscaled` outrank the dev sessions that share this VM:
`OOMScoreAdjust=`, the host's swap and `vm.min_free_kbytes`, the `MemoryLow=`
slice grant, and why `earlyoom` is declined. Split out of
[DEPLOYMENT.md](DEPLOYMENT.md) (#140), which keeps the unit's lifecycle and the
`cp` each file below needs; capping a SocratiCode launch is in
[COMMANDS.md](COMMANDS.md).

Both units set `OOMScoreAdjust=-900`. co-replicator is also the dev workspace:
VSCode Server, Claude Code, and any MCP server they start share the VM with the
worker. At the default adj of 0 the worker read `oom_score` 670, second from the
top of the eligible list; at -900 it reads 72, above only `tailscaled` (-950,
#112):

```bash
cat /proc/self/oom_score_adj                                          # 0 from a session shell; -1000 before #125
cat /proc/$(systemctl show -p MainPID --value replicator)/oom_score   # 72 at adj -900; 670 before
```

**Until #125 the sessions were not on that list at all.** `exe-init` 8579326
started every session at `oom_score_adj=-1000` — ineligibility, not a low score;
28 processes held it — a bug exe.dev confirmed on 2026-09-29
(CannObserv/status#5). `exe-init` 14fd603 starts them at 0, so the directive now
wins a real comparison. `/exe.dev/bin/` is exe.dev's to replace, so
`tests/test_deploy.py` pins the 0 live; the swap and its rollback are in #125.

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
as killing this worker. The same 2026-09-24 pass found a session
`dbus-daemon` above the user manager (adj +200, 800), started under
`user@1000` at 04:04. So
[`deploy/tailscaled.service.d/memory.conf`](../deploy/tailscaled.service.d/memory.conf)
gave it the worker's own -900, as CannObserv/broker#21 does — 69 and 68 after
the restart — and since #112 gives it -950, CannObserv/power-map#588's value.
At a tied adj the kernel takes whichever has the larger RSS, a coin toss here:
the worker's python read 72 (88 MiB) and `tailscaled` 71 (79 MiB) on
2026-09-29, and `tailscaled` has peaked at 123 MiB. Below the worker, the
network path outlasts its only consumer: a killed worker restarts into a
working bus path, while a killed `tailscaled` leaves the worker running and cut
off, and the `OnFailure=` alert to `notifier:9000` — sent once the restart
budget is spent, not per kill — travels the tailnet. One cost: a Tailscale SSH
session into this node (`RunSSH: true`) forks from `tailscaled` and inherits
-950, so dev tooling launched from it would outlive the worker — #92's
inversion. Start such a session with `choom -n 0 -- $SHELL`; raising your own
score needs no privilege. CannObserv/watcher#309 and CannObserv/archiver#285 chose -400 because their
services do not use the tailnet; this worker does.

**Since #125 the dev tooling follows the user manager directly.** Measured
2026-09-29 after the restart: a VSCode `MainThread` at 694 (504 MB RSS), then
three `claude` processes at 682–678, all adj 0 and all ahead of every daemon —
the order #92 wanted. It is also why #112 declined earlyoom a second time.

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

**Delegation since #138: the worker leaf re-claims it.** `Delegate=memory pids` and
`DelegateSubgroup=worker` start the worker in `replicator.service/worker`, so that viewport
capture can put each browser in a sibling `replicator.service/browser` with a hard cap. Without
`memory_recursiveprot` the unit's 128M stops at `replicator.service`. So the worker copies the
unit's `memory.low` onto its own leaf at every boot (`src/worker/browser_cgroup.py`), and
`worker ready` reports it as `worker_memory_low`. The slice grant is unchanged, because the
claim moves down a level rather than adding a second one. The browser leaf claims nothing:

- `memory.max` = `REPLICATOR_CAPTURE_MEMORY_MAX_BYTES` (1536 MiB; the spike's longest page took 768 MiB);
- `memory.swap.max=0`;
- `memory.oom.group=1` (a runaway render is group-killed, and the worker sees `TargetClosedError`);
- `pids.max=512`.

`scripts/browser_exec.sh` sets the browser to adj 500, and Chromium ranks its own children from
200 up, all far above the worker's -900. Playwright's Node driver stays in the worker leaf.
`tests/test_deploy.py` reads the leaf's claim back on this host once the installed unit
delegates.

What this is not:

- **Not a substitute for capping the launch.** The cap bounds a SocratiCode
  server before any killer has to act, and its `choom -n 500` keeps the capped
  process killable under an `exe-init` that exempts sessions — at -1000 a cgroup
  cap *stalls* a process rather than killing it. `OOMScoreAdjust=` is the
  unit's half; the launch's, the capped invocation, is in [COMMANDS.md](COMMANDS.md).
- **Not `earlyoom` — declined twice (#112).** At -1000 (2026-09-24, the
  packaged 1.7-2 in `--dryrun`) it could not reach the sessions: it skips -1000
  exactly as the kernel does (`kill.c`), `--prefer` or not, and would have shed
  small daemons — `systemd --user` 733, `tailscaled` 670 until #113, the adj-0
  daemons 666 — while ~1.7 GiB of session tooling stayed out of reach. At 0
  (2026-09-29, after #125) it can, and adds nothing but timing: 17 MiB of RSS
  sits at -1000, ~3.4 GiB is eligible, and the kernel's own order already puts
  the heaviest session process first and the worker and `tailscaled` last.
  The re-run `--dryrun` (1.7-2, unprivileged, thresholds forced) agrees: stock
  arguments take `systemd --user` (11 MiB), as the kernel would first; tuned
  with `--prefer '^(node|npm|claude|MainThread)'` and the worker, `tailscaled`,
  `sshd` and `systemd` avoided, a VSCode `MainThread` (476 MiB, badness 993),
  the kernel's next choice after the 14 MiB user manager. Neither setting adds
  anything. On stock `-s 10` it waits for swap to be ≤10% free as well —
  ~3.7 G paged out here, deep into the thrashing that ends at the kernel's own
  kill. On the `-s 100,100` this
  host would need, it kills at ~10% `MemAvailable`, which still counts page
  cache the kernel reclaims before it kills anything — so a session spike the
  kernel would have absorbed becomes a lost session. Memory PSI read 0 since boot, and none of the
  three saved boots (since 2026-09-23) logged an OOM kill or an allocation
  failure. The cohort read it the same way:
  CannObserv/archiver#285 adopted, measured and purged it, CannObserv/watcher#337
  kept it declined, CannObserv/power-map#588 is removing it. What it would have
  protected is held by scores the kernel honours and by `vm.min_free_kbytes`,
  the one lever for atomic allocations. `tests/test_deploy.py::TestTheEarlyoomDecline`
  fails if its config or its binary returns. Re-deciding it here would start
  from `-s 100,100`: a bare `-s 100` leaves SIGKILL's swap minimum at 50%, 2 G in
  use, which `vm.swappiness = 10` may never reach (gregoryfoster/skills#331).
- **Not `-1000`.** That is exemption, and an exempt worker that leaks
  is unreclaimable — the kernel would work through everything else on the box
  first. `-900` is the cohort's value (CannObserv/broker#25): last of the
  eligible, not exempt; `tailscaled`'s -950 is below it and still eligible.
  `tests/test_deploy.py` pins both bounds, for both units and tailscaled's
  drop-in.

The handler unit carries it for a sharper reason than the worker does: memory
exhaustion is one of the conditions that *fires* it, so the moment it is most
likely to run is the moment an unprotected process is most likely to be killed.

All four files are copies under `/etc/systemd/system/`, so each needs its line
from [DEPLOYMENT.md](DEPLOYMENT.md#server-lifecycle)'s table — `daemon-reload` alone silently keeps the old values, and
tailscaled keeps its old adj until it restarts.
