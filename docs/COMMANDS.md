# Commands

## Setup

```bash
# Mirror the private cannobserv index into ./.wheelhouse. Run BEFORE uv sync on a
# fresh clone and after any co-core version bump — co-core / co-core-aio resolve
# from that directory via [tool.uv] find-links, not from PyPI.
uv run --no-project --with 'google-cloud-storage>=2,<4' python scripts/sync_wheelhouse.py

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
stream other services consume, and nothing would refuse it. `content.fetch` itself it reaches only
through a gap in the broker's ACL — the key pattern its inbox needs meeting the `+xadd` its fact
streams need — which CannObserv/broker#14 closes with a selector. Until then a frame there is
fetched for real on a command Watcher never issued; afterwards it is `NOPERM`, and the script exits
1 on the first attempt rather than retrying. `--production` still guards db 0 +
`content.fetch`, but using it is an operator act under Watcher's identity, not an example. A
scratch topic on the broker itself takes `citest`, whose only keys are `probe.*` and
`replicator.itest.*`, so it cannot name a production topic — not provisioned on this VM.

`--watch` reads `content.blobs` for `content.fetch` and `<topic>.blobs` otherwise, so a scratch
seed never watches production's facts; `--blobs-topic` overrides that. One stream, both outcomes:
an issuer needs a single consumer group to see whether its command produced bytes or a reason.
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
`invalid_request_options`. The full list is in
[`docs/contracts/content-fetch-issuer-reference.md`](contracts/content-fetch-issuer-reference.md).

Watch the live side with `sudo journalctl -u replicator -f`: each `stored a blob and published
blob_available` is one of Watcher's commands closing.

### Inspecting the consume path

**Every `redis-cli` in this file needs the credential** since broker#2's ACL cutover — a bare
`redis-cli` answers `NOAUTH Authentication required`. Load the env first (Common Commands in
AGENTS.md), then set this once per shell; the examples below assume it:

```bash
alias rcli='redis-cli --no-auth-warning -u "$REPLICATOR_REDIS_URL"'
```

**And the credential cannot run everything `redis-cli` can (#85).** The `replicator` user is
scoped to its own topics, permanently and by design, so the operator surface splits in two:

| Runnable here | Denied — ask the broker operator |
|---|---|
| `XLEN`, `XRANGE`, `XINFO STREAM`, `INFO`, `XDEL` on the two `.dlq` streams | `XPENDING`, `SCAN`, `XINFO GROUPS`, `XINFO CONSUMERS`, `CLIENT LIST`, `ACL LOG`, `SELECT`, `XDEL` anywhere else |

**Draining a dead-letter queue is this service's job, and since 2026-09-11 it has the grant
for it (#86, broker#12).** It briefly did not: `XADD <topic>.dlq` was granted and the deletion
never was, so the service that fills its own queue could not empty it — found when the
`alias_unknown` frame the #86 rehearsal parked at 2026-09-10T21:02:02Z refused to delete. The
broker's Phase 5 had already assigned both queues here, so the gap was an unfinished decision
rather than a policy, and it was closed with a **Redis 7.0 ACL selector** rather than a blanket
command grant:

```
(+xdel ~content.fetch.dlq ~content.replicate.dlq)
```

So `XDEL` works on those two queues and is refused on every other key this credential can
reach — the command streams and the fact streams included. The root permission set never
gains `+xdel`. Verified on the broker's node against 7.0.15, not from here: this host can
confirm a drained queue by its depth (`content.replicate.dlq` is 0) but cannot prove the
selector's shape without deleting something. The grant adds `+xdel` and nothing else, and
`XTRIM` has never been exercised from this credential — so treat per-id deletion as the only
disposal available, which is the point of a selector anyway: precise, never a queue wipe.

**Triage before deleting, and not only for correctness.** The broker's probe copies every DLQ
entry to its `dlq-evidence/` tree on the first tick that sees depth above zero, so a deleted
frame is still recoverable — *unless* it was written and deleted inside one 10-minute tick,
which leaves no evidence file at all. Read the frame, close its command, then delete.

The denied column is marked `# NOPERM` at each use below rather than removed, because the
command is still the right one to ask for — and two of them answer questions nothing else can.
See [Redis](#redis) for what to know before asking.

```bash
# Pending entries: id, holder, idle ms, and delivery count — the last field is
# the times_delivered the DLQ ceiling reads. Add `IDLE <ms>` before the range to
# filter to entries idle at least that long (what claim_stale would reclaim).
rcli XPENDING content.fetch replicator.fetch - + 10      # NOPERM as replicator

# Dead-lettered frames. Reading, triaging and deleting are all granted here.
rcli XLEN content.fetch.dlq
rcli XRANGE content.fetch.dlq - + COUNT 5
rcli XLEN content.replicate.dlq
rcli XRANGE content.replicate.dlq - + COUNT 5

# Disposal, once a frame is triaged and its command closed. One entry at a
# time by id, which is the whole point of a selector: a drain cannot become a
# wipe. Resting state for both queues is 0 — verified 2026-09-11, both at
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
```

## Lint

```bash
uv run ruff check .
uv run ruff format --check .
uv run ty check          # non-gating, advisory only
```

## Redis

The broker is `co-broker`, operated from CannObserv/broker — inspect, don't administer.

`rcli` is the alias defined under [Inspecting the consume path](#inspecting-the-consume-path).

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

Where to start when a host is being fetched more or less often than expected. The stream is
config/state: last-write-wins per host, read **without a consumer group**, replayed from the
beginning at every worker boot.

```bash
# Has the producer published anything at all? An empty stream is not an error —
# it means every host resolves to REPLICATOR_MIN_HOST_INTERVAL_SECONDS — but it
# is the first thing to rule out, and it looks identical to a working consumer.
rcli XLEN content.fetch-policy
rcli XRANGE content.fetch-policy - + COUNT 10

# What one host is actually paced at. `revoked: true` is a tombstone meaning
# "no explicit policy", not "no limit" — it falls back to the env default.
rcli XRANGE content.fetch-policy - + COUNT 500 | grep '"host":"example.test"'

# Expected EMPTY. A group here is a bug: every worker needs every message, so a
# group would compete for them and grow a PEL nothing acks or drains. Denied to
# the `replicator` user (#85) — ask the broker operator, or infer it the way
# tests/worker/test_policy_integration.py does, from the absence of a PEL.
rcli XINFO GROUPS content.fetch-policy   # NOPERM as replicator
```

The worker's own view, from the journal — what it rebuilt at boot and what it has applied since:

```bash
sudo journalctl -u replicator | grep 'replaying the fetch policy stream'  # the boot replay started
sudo journalctl -u replicator | grep 'fetch policy replay complete'   # tracked_hosts, messages, hosts_stricter_than_default, duration_ms
sudo journalctl -u replicator -f | grep 'applied a host fetch policy' # host, min_interval, and the default beside it
sudo journalctl -u replicator | grep 'stricter than the fallback'     # raise REPLICATOR_MIN_HOST_INTERVAL_SECONDS
```

`tracked_hosts: 0` with a non-empty `XLEN` means messages arrived and none applied — check for
`ignoring a ...` warnings on the same boot. The last grep is the one that needs acting on: it
names a host whose real policy is stricter than the fallback that would replace it if the
policy were revoked or missed on a replay.

**The last two greps are silent while nothing changes, by design (#85).** `applied a host fetch
policy` and the `stricter than the fallback` warning beneath it both fire on a *change*, not on
an apply: the producer republishes its whole set on a cron, so ungated they meant unchanging
entries every five minutes forever and one per historical entry during the boot replay — 29,770
of them, at ~31 lines/second, the day #85 was filed.

That gating is right for an event and wrong for a **standing condition**, which is what "this
host's policy is stricter than your fallback" is — it holds until an operator raises
`REPLICATOR_MIN_HOST_INTERVAL_SECONDS` or the producer lowers the policy. So do not read an
empty `stricter than the fallback` grep as "resolved": on a stream nobody has touched for a day
the last warning has rotated out while the condition still holds. Read
**`hosts_stricter_than_default`** on the replay summary instead — every boot re-asserts it, and
non-zero is what says to run the warning grep unwindowed (no `--since`) to find out which hosts.

To confirm the map is populated rather than to watch it move, read `tracked_hosts` on the same
line with `XLEN` beside it; `worker ready` follows the replay, so seeing it means the consume
loops are about to make their first read.

## Submodules

```bash
git submodule update --init --recursive       # after a fresh clone
bash .skills/doctor.sh                        # repair dangling skill symlinks
git submodule update --remote --merge         # pull upstream skill changes
```

## Deploy

```bash
bash scripts/check_main_checkout.sh                     # what the unit asserts before it starts
bash scripts/notify_failure.sh replicator.service       # what OnFailure= runs; records, never dispatches unset

sudo cp deploy/replicator.service /etc/systemd/system/replicator.service
sudo cp 'deploy/replicator-failure-notify@.service' /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now replicator
# BOTH unit files, and the second one has no restart to pair with it — it is a
# template nothing runs until a unit fails. Which is also why skipping it is
# silent: the miss surfaces at the first incident, the one moment it was meant
# to help (#94).

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

sudo journalctl -u replicator -f
journalctl -t replicator-failure                        # what the OnFailure= handler reported
# NOT `journalctl -u replicator-failure-notify@replicator.service` — `%n` keeps
# the suffix, so the instance is ...@replicator.service.service and the obvious
# name matches nothing. SyslogIdentifier= is what makes the line above work.
```
