# Commands

## Setup

```bash
# Mirror the private cannobserv index into ./.wheelhouse. Run BEFORE uv sync on a
# fresh clone and after any co-core version bump — co-core / co-core-aio / co-core-sync resolve
# from that directory via [tool.uv] find-links, not from PyPI.
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py
uv run --no-project python scripts/co_core_closure_diff.py OLD NEW   # every pin move: DEPLOYMENT.md, the co-core pin

uv sync
uv run pre-commit install
```

The sync authenticates as the read-only key named by `REPLICATOR_WHEELHOUSE_CREDENTIALS`
when that is set (`/etc/replicator/co-pypi-reader.json` on the VM), else Application
Default Credentials — in CI the keyless WIF token written by `google-github-actions/auth`.
Either identity needs only `roles/storage.objectViewer`. The VM's
`GOOGLE_APPLICATION_CREDENTIALS` is the replication *writer*, which is why the sync has
a variable of its own.

## Environment

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
```

## Worker

```bash
# Run the bus consumer locally. Set distinct consumer names when the live
# service is also running — the derived defaults are the *same* names the service
# registers under, and a shared name means a shared pending-entries list. One per
# group: the worker runs two loops, and each keeps its own registration.
REPLICATOR_CONSUMER_NAME="replicator-fetch-$(whoami)-dev" \
REPLICATOR_REPLICATE_CONSUMER_NAME="replicator-replicate-$(whoami)-dev" \
  uv run python -m src.worker.main

# Ctrl-C (or SIGTERM) finishes the in-flight message, acks it, and exits 0.
```

### Seeding commands

`scripts/seed_fetch.py` publishes `content.fetch` frames to a **scratch** stream. The live
stream is Watcher's — its issuer since watcher#241, whose traffic is what shows the deployed loop
working (the journal line closing this section). The target is never defaulted: `--redis-url`
and `--topic` are both required.

```bash
# Safe rehearsal: print the frames, contact nothing.
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.seed \
  --dry-run https://example.test/a

# The scratch redis-server from TESTING.md — reaches no worker.
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.seed \
  https://example.test/a https://example.test/b
```

**Never `--redis-url "$REPLICATOR_REDIS_URL"` (#90).** That is the worker's credential, and every
key it can write is production — `replicator.itest.*` is not among its patterns — while the
script's guard knows only `content.fetch`: `--topic content.blobs` would put a command on a fact
stream other services consume, and nothing would refuse it. `content.fetch` itself is `NOPERM`
since CannObserv/broker#14, and the script exits 1 on the first attempt rather than retrying.
`--production` still guards db 0 + `content.fetch`, but using it is an operator act under
Watcher's identity, not an example.

`--watch` reads `content.blobs` for `content.fetch` and `<topic>.blobs` otherwise, so a scratch
seed never watches production's facts; `--blobs-topic` overrides that. **On the live target it
is refused, exit 2** — why is in [STREAMS.md](STREAMS.md) (#129); read the outcome as replicator
with `rcli XRANGE content.blobs <entry_id> +`. One stream, both outcomes: an issuer needs a
single consumer group to see whether its command produced bytes or a reason.
**A fact arrives only from a consumer built on that topic**, though — `test_loop_integration.py`
builds one, and a `uv run` worker never does, because its topics are defaulted arguments rather
than settings — so from the command line a scratch `--watch` waits out `--watch-timeout` and
exits 1.

`--info-source-id` sets the domain key the command carries and both facts echo, required on the
wire since co-core 0.8.0 (#28). It defaults to `seed-harness-not-a-real-info-source`, which no
issuer's InfoSource table contains — so a fact a seed run puts on a scratch stream is recognizably
synthetic. **The live target refuses that default, and a blank value**, exiting 2: a real fetch
broadcasts whatever is passed here to the cluster, so it has to name a real InfoSource.

`--header` and `--timeout` set the command's per-fetch request options (#11). They apply to
every URL in the run, and omitting them is the pre-#11 wire exactly. A dry run prints them inside
the payload, after the script's own stripping — the value that would actually travel:

```bash
# Pin the User-Agent, as Watcher does for fingerprint continuity.
# --header is repeatable; the name is case-insensitive (the worker folds it).
uv run python -m scripts.seed_fetch \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.seed \
  --header 'User-Agent: watcher/0.1.0' --header 'Accept: text/html' \
  --timeout 5 --dry-run https://example.test/a
```

The script rejects a malformed `--header` and a repeated name (exit 2) but deliberately does
**not** pre-empt the worker's refusal list: a `Host` override publishes cleanly here, and the
worker refuses it before any request goes out, closing the command as `fetch_failed` /
`invalid_request_options`. The same is true of a destination the worker will not fetch — a
`http://127.0.0.1:…` URL seeds cleanly here and closes as `destination_refused` (#95), which is
worth knowing before seeding a local test server. The full list is in
[`docs/contracts/content-fetch-issuer-reference.md`](contracts/content-fetch-issuer-reference.md).

Watch the live side with `sudo journalctl -u replicator -f`: each `stored a blob and published
blob_available` is one of Watcher's commands closing.

`scripts/seed_capture.py` does the same for `content.capture` (#138), scratch only: the live stream
(archiver#380's), db 0 and a non-loopback host exit 2.

```bash
uv run python -m scripts.seed_capture \
  --redis-url redis://localhost:6379/15 --topic replicator.itest.capture \
  --full-page --wait selector --selector main https://example.test/
```

### The patching gate's in-flight count

What `patching-hosts` asks before each step (#131):

```bash
.venv/bin/python -m scripts.inflight     # from the repo root; prints this worker's unacked commands
```

The `patching-hosts` knob (`.skills/patching-hosts`) runs it as `inflight` before each apply step
and inside the reboot chain, and as a `health` line for the broker round-trip. It reads
`/etc/replicator/.env` itself (`--env-file` to override), counts only this host's consumer names,
and on any failure prints nothing and exits 1 — the gate never reads a failed read as `0`.

### Inspecting the consume path

**Every `redis-cli` in this file needs the credential** since broker#2's ACL cutover — a bare
`redis-cli` answers `NOAUTH Authentication required`. Load the env first (Common Commands in
AGENTS.md), then source this once per shell from the repo root; the examples below assume it:

```bash
. scripts/rcli.sh   # defines rcli: redis-cli against $REPLICATOR_REDIS_URL
```

**Never hand `$REPLICATOR_REDIS_URL` to `redis-cli -u` (#127).** It puts the password in argv, readable
from `/proc` by every local user while the call runs; this file's old `rcli` alias did. `rcli` strips the URL's userinfo and query, passes `--user`, and hands the
password over in `REDISCLI_AUTH` (broker#47's rule), wherever in the URL redis-py would have
found it — the floor check uses the same helper.
`tests/test_rcli.py` fails on any `redis-cli` in a script or runbook here that is given `-a`
or `--pass`, or a `-u` whose argument is quoted, a variable, or a URL — prose naming the flag
is fine.

**And the credential cannot run everything `redis-cli` can (#85).** The `replicator` user is
scoped to its own topics, permanently and by design, so the operator surface splits in two:

| Runnable here | Denied — ask the broker operator |
|---|---|
| `XPENDING`, `XRANGE`, `XINFO STREAM` on the four command streams; `XLEN`, `XRANGE` on both fact streams, the four `.dlq` streams and `content.fetch-policy`; `XDEL` on the four `.dlq` streams; `INFO` | `SCAN`, `XINFO GROUPS`, `XINFO CONSUMERS`, `CLIENT LIST`, `ACL LOG`, `SELECT`, `XREVRANGE`, `MEMORY USAGE`; any command on a key its row doesn't name, e.g. `XLEN content.fetch` |

**Every read is per key since broker#43 (2026-10-06, #129)**: command and key must both match.
Anything not shown in this file, check against broker's `deploy/redis-acl.conf` first.

**`XPENDING` moved columns on 2026-09-22 (broker#39, #103, #107)**, not as a courtesy: the
delivery ceiling reads it, so before the grant `REPLICATOR_MAX_DELIVERY_ATTEMPTS` could never
fire. Probed afterwards, like every row: both groups answered 0 pending, where 12:55Z
had returned `NOPERM`.

**Draining a dead-letter queue is this service's job, granted since 2026-09-11 (#86,
broker#12).** `XADD <topic>.dlq` was granted first and the deletion not, so the service could not
empty its own queue — found when the #86 rehearsal's `alias_unknown` frame (parked
2026-09-10T21:02:02Z) refused to delete. Broker's Phase 5 had already assigned the queues here, an
unfinished decision rather than a policy, closed with a **Redis 7.0 ACL selector**, not a blanket
command grant:

```
(+xdel ~content.fetch.dlq ~content.replicate.dlq ~content.persist.dlq ~content.capture.dlq)
```

Persist's queue joined 2026-09-26 (broker#64), capture's with broker#114 (`7fe8f44`), each read
from broker's `deploy/redis-acl.conf`. `XDEL` works on those four and is refused on every other key
this credential reaches, command and fact streams included; the root permission set never gains
`+xdel`. Verified on the broker's node against 7.0.15, not from here: a drained queue shows by its
depth (`content.replicate.dlq` is 0), but the selector's shape can't be proven without deleting
something. The grant adds `+xdel` alone, so per-id deletion is the only disposal: precise, never
a queue wipe.

**`XTRIM` is not for these queues, or anything, from here (#106).** Broker withdrew it on
2026-10-06 (CannObserv/broker#41) because nothing here issues it, which
`tests/test_broker_keyspace.py` keeps true. Trimming a fact stream past a group's position
deletes facts that group hasn't been delivered, so a drain faster than one `XDEL` at a time is
a broker issue, not a command to reach for.

**Triage before deleting, and not only for correctness.** The broker's probe copies every DLQ
entry to its `dlq-evidence/` tree on the first tick that sees depth above zero, so a deleted
frame is still recoverable — *unless* it was written and deleted inside one 10-minute tick,
which leaves no evidence file at all. Read the frame, close its command, then delete.

The denied column is marked `# NOPERM` where used below, not removed: each is still the right
command to ask for, and two answer questions nothing else can.
See [Redis](#redis) for what to know before asking.

```bash
# Pending entries: id, holder, idle ms, and delivery count — the last field is
# the times_delivered the DLQ ceiling reads. Add `IDLE <ms>` before the range to
# filter to entries idle at least that long (what claim_stale would reclaim).
for s in fetch replicate persist capture; do rcli XPENDING content.$s replicator.$s - + 10; done

# Dead-lettered frames. Reading, triaging and deleting are all granted here.
# Each entry is the frame plus dlq.reason / dlq.source_id / dlq.group /
# dlq.consumer (#116); older ones carry dlq_reason / dlq_original_id instead.
for s in fetch replicate persist capture; do
  rcli XLEN content.$s.dlq; rcli XRANGE content.$s.dlq - + COUNT 5
done

# Disposal, once a frame is triaged and its command closed. One entry at a
# time by id, which is the whole point of a selector: a drain cannot become a
# wipe. Resting state for every queue is 0 — content.persist.dlq at 0 when its
# stream was created, 2026-09-26; the other two verified 2026-09-11, both at
# XLEN 0 (broker#12). The id comes from the XRANGE above.
ENTRY_ID=1789074122299-0
rcli XDEL content.replicate.dlq "$ENTRY_ID"

# Dedupe keys (one per handled command, TTL REPLICATOR_DEDUPE_TTL_SECONDS).
# What they guard and what a cold start does without them: CONVENTIONS.md,
# "The `replicator:cmd:*` keys". SCAN is an operator command — the worker only
# ever SETs and EXISTSs them.
rcli --scan --pattern 'replicator:cmd:*' | head          # NOPERM as replicator

# Facts published — content.blobs carries both outcomes. On blob_available,
# blob_uri points at REPLICATOR_BLOB_DIR and the fingerprint is the filename, so
# `sha256sum` on the blob must reproduce it.
rcli XLEN content.blobs
rcli XRANGE content.blobs - + COUNT 5

# Just the failures. Matches the payload JSON, which is one line per entry and
# carries the whole fact — do NOT grep the bare token, which also hits the
# hoisted event_type field and interleaves half-records. A dead-lettered command
# should appear here *and* in content.fetch.dlq — the fact is the issuer's
# surface, the DLQ is the operator's.
rcli XRANGE content.blobs - + COUNT 200 | grep '"event_type":"fetch_failed"'
```

## API (dev only)

```bash
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8001 --reload --log-config src/core/log_config.json
curl -s localhost:8001/health | jq
```

## Tests

```bash
uv run pytest                              # full suite, coverage gate active
uv run pytest --no-cov tests/worker/       # subset; skip the gate (it measures all of src/)
uv run pytest --no-cov -m integration      # a scratch redis-server (TESTING.md), plus the OOM
                                           # rows against a broker they spawn themselves (#79)
                                           # and the reconnection rows against one they stop and
                                           # restart (#94); skip the gate

sudo bash scripts/rehearse_reconnect.sh    # the half a pytest cannot reach: systemd's own restart
                                           # semantics across a broker outage (#94). Spawns its
                                           # own broker and a scratch unit under /run/systemd/
                                           # system — never co-broker, never replicator.service.
                                           # --keep leaves both up for inspection.
uv run pytest --no-cov -m gcs              # the T4 rows against the real replicate bucket, and the
                                           # temp store against its own (#7). Skips per destination:
                                           # no REPLICATOR_TEST_GCS_CREDENTIALS skips everything,
                                           # a missing REPLICATOR_TEST_GCS_BUCKET or
                                           # REPLICATOR_TEST_BLOB_BUCKET skips only its half,
                                           # since the two are provisioned separately (#38, #53)
uv run pytest --no-cov -m browser -rs      # the real pinned shell (#138); fails, never skips,
                                           # without it or a delegated scope (TESTING.md)
```

## Lint

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check          # non-gating, advisory only
```

## Redis

The broker is `co-broker`, operated from CannObserv/broker — inspect, don't administer.

`rcli` is the function sourced under [Inspecting the consume path](#inspecting-the-consume-path).

```bash
bash scripts/check_redis_floor.sh                       # assert the >=7.0 server floor
rcli INFO server | grep redis_version

# Bus inspection
rcli XINFO STREAM content.fetch
rcli XINFO GROUPS content.fetch                         # NOPERM as replicator
rcli XINFO CONSUMERS content.fetch replicator.fetch     # NOPERM as replicator
rcli XLEN content.fetch.dlq                             # dead-lettered frames
```

Group and connection state are **not observable from this host at all** — the four commands that
carry it are in the denied column above — so they have to be asked of the broker operator. Two
things to know before asking (#85):

- **`consumers: 0` on `XINFO GROUPS` is not evidence of a dead worker** on a stream that has
  never carried a message. Redis registers a consumer only when a read *returns* an entry, so a
  worker blocked on an empty stream registers nothing — the state of `content.replicate` and
  `content.artifacts` today (broker#7).
- **The liveness signal for a low-traffic group is `flags=b` on the connection**, from
  `CLIENT LIST`: `b` means genuinely blocked in Redis, as against `N` for a connection that
  merely ran a read once. That field is what distinguished a healthy worker from #85's spinning
  one, and `XINFO GROUPS` could not have.

### Politeness — `content.fetch-policy` (#19)

The stream's inspection commands — what the producer published, one host's policy, and the
worker's journal view of the replay and of each apply — are in
[POLITENESS.md](POLITENESS.md#inspecting-the-stream), beside the mechanism they inspect (#146).

### How long a handler holds its group

How long a handler holds its group — the number CannObserv/broker#20 sizes its
undelivered-age warning from, because this consume path reads nothing while a handler runs:

```bash
sudo journalctl -u replicator | grep 'replicated a blob'   # duration_ms: the whole replicate handler
sudo journalctl -u replicator | grep 'transient failure'   # duration_ms: a hold that ends in a retry, on either stream
```

The replicate line's `duration_ms` is the whole handler — guards, source download, provider
write, fact. The **fetch** line's field of that name is not the same window: it is the origin
round trip alone, with the politeness wait beside it as `paced_seconds` and the store write
counted nowhere. Measured 2026-09-18 (#96), the replicate handler runs ~0.24 s on today's
corpus and ~5.3 s for a blob at the `REPLICATOR_MAX_BLOB_BYTES` ceiling; the second grep is
the one that finds a hold longer than that. One repeating on the same `command_id` can go on
indefinitely, but since #98 it no longer stops the group reading.

## Submodules

```bash
git submodule update --init --recursive       # after a fresh clone
bash .skills/doctor.sh                        # repair dangling skill symlinks
git submodule update --remote --merge         # pull upstream skill changes
```

## SocratiCode — the shared index on `co-index`

The store, its endpoints and the one-host-per-`projectId` rule are in
[INFRASTRUCTURE.md](INFRASTRUCTURE.md); the client contract is pinned by
`tests/test_socraticode_config.py`.

**Cap anything that launches a server.** co-replicator's 8 GiB plus 4 G swap (#99)
is headroom, not immunity — broker was 8 GiB too — and the worker runs beside these
sessions. On broker's VM an uncapped launch degraded the tailnet for 57m48s, nothing
OOM-killed (broker#17, our #94). The units' `OOMScoreAdjust=-900`
([MEMORY-PROTECTION.md](MEMORY-PROTECTION.md)) is the other half, not a substitute.
`choom -n 500` is part of the cap: under an `exe-init` starting sessions at
`oom_score_adj=-1000`, as here until #125, a cgroup cap *stalls* a process rather than
killing it.

```bash
# Offline, no server, no network — run these first; they catch a malformed
# config before anything is launched.
node "$SOCRATICODE_DRIVER" validate-store .      # projectId, store mode, the env block
node "$SOCRATICODE_DRIVER" validate-manifest .   # every path in the artifacts manifest resolves

# Host readiness: node, npx, trust, and whether the store answers. Run it BARE —
# inline values would stand in for .claude/settings.json's env block and pass the
# trust line whether or not the session actually carries it.
bash skills-vendor/gregoryfoster-skills/skills/init-socraticode/scripts/preflight.sh

# Anything that starts a server, capped. Measured here: ~40 s, worker untouched,
# available memory never below 2.28 GB.
systemd-run --user --scope -p MemoryHigh=1200M -p MemoryMax=1536M -p MemorySwapMax=0 -p CPUQuota=100% \
  choom -n 500 -- node "$SOCRATICODE_DRIVER" health-check . --probe src/worker/main.py

# The index itself, same cap. Measured 2026-09-18: 21 min wall, full tree (1708
# code / 335 context points, 25/25 artifacts), available memory never below
# 2.33 GB, worker NRestarts=0. Embedding runs on co-index: little local CPU.
systemd-run --user --scope -p MemoryHigh=1200M -p MemoryMax=1536M -p MemorySwapMax=0 -p CPUQuota=100% \
  choom -n 500 -- node "$SOCRATICODE_DRIVER" index .
```

`systemd-run --user` needs linger and the `memory`/`cpu` controllers delegated —
both hold here (`loginctl show-user exedev -p Linger`,
`/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/cgroup.controllers`).

**Re-pinning the server (#152).** Five sites move together, as a decision —
the order, and why each matters, are in
[SOCRATICODE.md](SOCRATICODE.md#two-launch-paths-pinned-separately);
`tests/test_socraticode_pin.py` fails until they agree. Neither command below
starts a server, but both install — the expensive half — so both take the
same ceiling as the hook (`test_the_hook_cap_is_the_manual_ceiling`):

```bash
npm view socraticode version     # pick a literal; never @latest
# Then: SOCRATICODE_SPEC in .claude/settings.json, the version SOCRATICODE.md
# names, and claudeCode.environmentVariables in
# ~/.vscode-server/data/Machine/settings.json.
systemd-run --user --scope -p MemoryHigh=1200M -p MemoryMax=1536M -p MemorySwapMax=0 -p CPUQuota=100% \
  -- npm install --prefix ~/.socraticode/pin socraticode@<version>   # the driver's pre-install
systemd-run --user --scope -p MemoryHigh=1200M -p MemoryMax=1536M -p MemorySwapMax=0 -p CPUQuota=100% \
  -- npx -y -p socraticode@<version> -- node -e 0                    # warm the session's npx entry

# After a full VS Code reconnect, from the session's Bash tool ($PPID is its
# claude) — the only evidence of what the session launched:
ps -o pid,args --ppid "$PPID" | grep '[n]pm exec socraticode'
tr '\0' '\n' < "/proc/$PPID/environ" | grep SOCRATICODE_SPEC
```

**Installing the Qdrant API key.** Not runnable from this VM: exe.dev VMs are
isolated from each other, and notifier#57 D13's `tag:index:22` edge was retired after the
soak. It runs from an operator machine, the only host that reaches both ends.

```bash
scp scripts/install_qdrant_key.sh co-replicator.exe.xyz:~/      # from a notifier checkout
ssh co-index.exe.xyz \
    "sudo sed -n 's/^QDRANT__SERVICE__API_KEY=//p' /etc/socraticode/qdrant.env" \
  | ssh co-replicator.exe.xyz 'bash ~/install_qdrant_key.sh ~/replicator'
```

The key travels on **stdin, never argv** — an argument is in `ps` for the life of
the call and in the caller's shell history. Expect `installed 64 chars`; **any
other length is a truncated transfer**, which 401s exactly like a wrong key, and
the length is the only cheap way to tell them apart. Never run it under `bash -x`
— tracing a script that touches a credential writes the value to stdout, which is
how two were leaked during notifier#57.

## Deploy

```bash
bash scripts/check_main_checkout.sh                     # what the unit asserts before it starts
bash scripts/notify_failure.sh replicator.service       # what OnFailure= runs; records, never dispatches unset

sudo cp deploy/replicator.service /etc/systemd/system/replicator.service
sudo cp 'deploy/replicator-failure-notify@.service' /etc/systemd/system/
sudo install -D -m 644 deploy/tailscaled.service.d/memory.conf /etc/systemd/system/tailscaled.service.d/memory.conf
sudo install -D -m 644 deploy/system.slice.d/replicator-memory.conf /etc/systemd/system/system.slice.d/replicator-memory.conf
sudo systemctl daemon-reload
sudo systemctl restart tailscaled                       # its OOMScoreAdjust= applies at exec only (#113)
sudo systemctl enable --now replicator
# BOTH unit files, and the second one has no restart to pair with it — it is a
# template nothing runs until a unit fails. Which is also why skipping it is
# silent: the miss surfaces at the first incident, the one moment it was meant
# to help (#94). The two drop-ins are as quiet: without them tailscaled sits at
# adj 0 and both MemoryLow= are inert, and only the live checks in
# tests/test_deploy.py notice.

git pull --ff-only && uv sync --frozen && sudo systemctl restart replicator  # merged on GitHub
git push && uv sync --frozen && sudo systemctl restart replicator            # merged locally
# Getting main level with origin is not optional: the guard refuses to start a
# `main` that is ahead of origin/main, because unpushed commits are on no shared
# branch (#48). Which command does it depends on where the merge happened.

# The installed unit is a COPY, not a symlink — a merge that touched
# deploy/replicator.service needs the cp above re-run before the reload, or the
# worker comes up on new code under the old unit with nothing to show for it.
diff /etc/systemd/system/replicator.service deploy/replicator.service
diff '/etc/systemd/system/replicator-failure-notify@.service' 'deploy/replicator-failure-notify@.service'
diff /etc/systemd/system/tailscaled.service.d/memory.conf deploy/tailscaled.service.d/memory.conf
diff /etc/systemd/system/system.slice.d/replicator-memory.conf deploy/system.slice.d/replicator-memory.conf
diff /etc/needrestart/conf.d/replicator.conf deploy/needrestart.conf.d/replicator.conf
diff /etc/apt/preferences.d/tailscale.pref deploy/apt/preferences.d/tailscale.pref

sudo journalctl -u replicator -f
journalctl -t replicator-failure                        # what the OnFailure= handler reported
# NOT `journalctl -u replicator-failure-notify@replicator.service` — `%n` keeps
# the suffix, so the instance is ...@replicator.service.service and the obvious
# name matches nothing. SyslogIdentifier= is what makes the line above work.
```
