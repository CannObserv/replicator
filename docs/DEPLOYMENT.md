# Replicator Deployment

Single-VM topology, the systemd unit's lifecycle, and every environment variable
the service reads. `AGENTS.md` keeps the two-env-file boundary and the restart
command; the per-variable reasoning is here.

## Infrastructure

Replicator shares the VM with archiver, watcher, and notifier:

| Service | Framework | Port | Managed by |
|---|---|---|---|
| Worker (live) | asyncio bus consumer | — | `systemctl` (`replicator.service`) |
| API (dev) | FastAPI | 8041 | manual uvicorn |

The worker binds no port. Port 8040 is reserved for Replicator's API should it ever be deployed; 8041 is the dev port. Neighbours: watcher 8000/8001, archiver 8020/8021, notifier 9000/9001. The exe.dev proxy transparently forwards ports 3000–9999; the dev server is reachable at `https://replicator.exe.xyz:8041/`.

### Redis is Archiver-operated — Replicator connects, it does not run its own

The Redis change bus is Archiver-operated cluster infrastructure (the shared VM's `redis-server.service`). Replicator is a **client**: never ship a broker, never claim ownership.

**Redis ≥ 7.0 is Replicator-critical.** Replicator is the cluster's first user of `AsyncBusConsumer.claim_stale`, which reads `XAUTOCLAIM`'s three-element reply — the deleted-ids element added in Redis **server** 7.0. Below that, the crash-recovery path raises. `scripts/check_redis_floor.sh` guards this as an `ExecStartPre`. (The VM runs 7.0.15.)

The **redis-py client** resolves `>=5,<8` transitively via `co-core-aio[bus]`. Don't re-pin it narrower.

## Server Lifecycle

**`replicator.service` runs the worker.** It binds no port, so there is no port to conflict over — but only one process should hold a given consumer name at a time.

| Situation | Action |
|---|---|
| Code merged to main **on GitHub** (the usual path) | `git pull --ff-only && uv sync --frozen && sudo systemctl restart replicator` |
| Code merged to main **locally** | `git push && uv sync --frozen && sudo systemctl restart replicator` |
| Testing a worktree/branch | `uv run python -m src.worker.main` (set a distinct `REPLICATOR_CONSUMER_NAME`) |
| Debugging the live service | `sudo journalctl -u replicator -f` |
| After editing `deploy/replicator.service` | `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart replicator` |
| After a co-core version bump | re-run `sync_wheelhouse.py`, then `uv sync` |

`ExecStart` uses `--frozen --no-sync`, so dependency sync is a deploy step, not a service-start side effect.

**`/etc/systemd/system/replicator.service` is a *copy*, not a symlink to `deploy/`.** So the `cp` above is load-bearing and `daemon-reload` alone silently does nothing — systemd re-reads the installed file, which is still the old one. The failure has no symptom at restart: the worker comes up on the new code under the *old* unit, and the mismatch only surfaces the first time a directive actually matters. Nothing guards it, either — `tests/test_deploy.py` reads the repo file, which is exactly the copy that is still correct. Diff the two when a restart follows a unit edit (#11 deploy).

The copy is deliberate, for the same reason `/etc/replicator/.env` is not read from the repo: the live unit must survive a repo reset, a worktree switch, or a branch checkout that happens to be mid-edit.

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

**Dev server workflow** (the `/health` app, port 8041 so a future live service stays up):

```bash
set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8041 --reload --log-config src/core/log_config.json
```

## Environment Variables

In `.env` (dev/agent only — never read by the service):
- `GH_TOKEN` — GitHub PAT for this repo (used by `gh` CLI)
- `GH_TOKEN_ARCHIVER` / `GH_TOKEN_WATCHER` / `GH_TOKEN_CANNOBSERV` / `GH_TOKEN_SKILLS` — per-repo PATs. Cross-repo work is **filed as an issue**, never edited directly: each repo owns its own review, CI, and deploy cycle, and `main` is the deployed code. Pass the right one as `GH_TOKEN` for a given `gh` call.

Read by neither env file — test-only, defined in `tests/conftest.py`:
- `REPLICATOR_TEST_REDIS_URL` — live broker for `@pytest.mark.integration`; default `redis://localhost:6379/15`. Must not resolve to db 0 (the fixture fails outright if it does) — see **Testing the bus**
  in [TESTING.md](TESTING.md)

In `/etc/replicator/.env` (read by the service):
- `GOOGLE_APPLICATION_CREDENTIALS` — SA key for the wheelhouse mirror (`/etc/replicator/co-pypi-reader.json`)
- `REPLICATOR_REDIS_URL` — change-bus client URL; default `redis://localhost:6379/0`
- `REPLICATOR_BLOB_DIR` — temp-storage root for fetched bytes; default `blobs`. Resolved to an absolute path at store construction — `file://` URIs require it
- `REPLICATOR_BLOB_TTL_SECONDS` — how long a blob survives after it was **last referenced**; default `604800` (7 days), accepted range `0 < n ≤ 315360000` (10 years). Measured from mtime, which the store refreshes on its short-circuit, and published on each fact as `blob_expires_at`. The number is a published commitment to archiver (archiver#118), not a local tuning knob — raise it if a `content.blobs` consumer says it needs longer. **Out of range fails at startup**, not at the first fetch: the horizon is arithmetic, and an absurd value would raise mid-handler *after* the bytes were stored, orphaning them (#28)
- `REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS` — how often the tree is walked; default `900`. Also the staleness bound on the measured byte total the ceiling reads
- `REPLICATOR_BLOB_TEMP_GRACE_SECONDS` — how long a `.tmp` may live before the sweep treats it as debris; default `3600`. Deliberately unrelated to the TTL and far shorter — see **Retention**
  in [STORAGE.md](STORAGE.md)
- `REPLICATOR_BLOB_MAX_TOTAL_BYTES` — ceiling on everything the blob tree holds; default `2147483648` (2 GiB). Crossing it pauses fetching (`TransientFetchError`); it never shortens the TTL
- `REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS` — the most a command's own `timeout_seconds` may ask for; default `120`. Not a default (an omitted field still gets the driver's 30 s) but a ceiling, and a guard rather than a preference: the consume path is serial, so one issuer's timeout is a lien on every other command in the group. Over it ⇒ `PermanentFetchError`. Bounded above by the unit's `TimeoutStopSec` — change one, revisit the other
- `REPLICATOR_MAX_BLOB_BYTES` — ceiling on one fetched body; default `67108864` (64 MiB). A **storage** guard, not a memory one: co-core's fetch driver buffers the whole response before returning it, so the bytes are already resident when this is checked. Over the ceiling ⇒ `PermanentFetchError` ⇒ DLQ
- `REPLICATOR_CONSUMER_GROUP` — consumer group on `content.fetch`; default `replicator.fetch`
- `REPLICATOR_CONSUMER_NAME` — this worker's identity within the group; defaults to `replicator@<hostname>`. Two workers must never share one — Redis tracks pending entries per consumer name, and a shared name makes independent `claim_stale` recovery impossible
- `REPLICATOR_CONSUMER_START_ID` — group start position; default `"$"` (new messages only), `"0"` drains the backlog. Applies **only at group creation** — once `replicator.fetch` exists, changing this also needs a manual `XGROUP SETID`
- `REPLICATOR_READ_BLOCK_MS` — blocking-read window; default `5000`. Bounds worst-case shutdown latency, so the unit's `TimeoutStopSec` must exceed it plus the handler budget **plus an in-flight sweep** — `asyncio.to_thread` puts the tree walk beyond cancellation, so SIGTERM waits it out
- `REPLICATOR_MIN_HOST_INTERVAL_SECONDS` — minimum spacing for a host with **no explicit policy**; default `1.0`. Since #19 this is the *fallback*, not the rule: the per-host numbers arrive on `content.fetch-policy` and an unknown, revoked, or not-yet-replayed host resolves here — never to unlimited, because a boot replay cannot tell a consumer whether the set it received is whole. 1.0 is Watcher's own `DEFAULT_MIN_INTERVAL`, chosen because it invents nothing. A wait ≤ `REPLICATOR_READ_BLOCK_MS` is slept through in the handler; a longer one raises `TransientFetchError` and parks the command, so the effective floor on a *parked* wait is `REPLICATOR_CLAIM_MIN_IDLE_MS`. **`0` no longer disables pacing outright** (#19 narrowed it): it is the fallback for unpublished hosts only, and a host with a policy is still paced by it — letting an env var veto a published value would invert the ownership split the charter settles. Capped at `3600`: past an hour the command parks and re-parks without ever dead-lettering (transient failures are exempt from the delivery ceiling) while the issuer's reaper concludes loss, so a fat-fingered extra zero should fail at startup rather than read as healthy. **It cannot be validated against what a producer might publish** — a published interval has no upper bound — so the strictness contract is enforced the only place it is knowable: a WARNING per host at apply time when a real policy turns out stricter than this number
- `REPLICATOR_CLAIM_MIN_IDLE_MS` — idle time before a pending entry may be reclaimed; default `60000`. Doubles as the retry cadence
- `REPLICATOR_MAX_DELIVERY_ATTEMPTS` — delivery ceiling for *unclassified* failures before DLQ; default `5`. Counted from XPENDING's delivery counter, which only advances on a reclaim ⇒ a bound in time, not retries
- `REPLICATOR_ERROR_BACKOFF_BASE_SECONDS` / `REPLICATOR_ERROR_BACKOFF_MAX_SECONDS` — backoff for a poll *cycle* that raised (broker outage); defaults `1.0` / `30.0`, escalating `base * 2**(n-1)`
- `REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES` — consecutive failed cycles before the worker exits so the unit restarts; default `20` (~8 min at the default backoff). Paired with the unit's `StartLimitIntervalSec=3600` / `StartLimitBurst=3`
- `REPLICATOR_DEDUPE_TTL_SECONDS` — lifetime of the `replicator:cmd:<stream>:<command_id>` dedupe key; default `86400`
- `REPLICATOR_REPLICATE_CONSUMER_GROUP` — the `content.replicate` group; default `replicator.replicate`. Separate from the fetch group because `claim_stale` walks a group's PEL, so a shared name would let recovery on one stream reach into the other's pending entries
- `REPLICATOR_REPLICATE_WRITE_TIMEOUT_SECONDS` — how long one conditional create may run; default `120`. Surfaced rather than inherited from the SDK's own default, which nobody chose (CR #38): a hung write holds its PEL entry for the whole window, and on the write side that window is also how long a large blob has to reach a permanent store over whatever link this host has. **The invariant is `write_timeout < REPLICATOR_CLAIM_MIN_IDLE_MS`**, so a write finishes before its entry becomes claimable — raise the reclaim window first, then this. **The shipped pair does not satisfy it** (120 s against a 60 s window) and that is deliberate rather than overlooked: a write still running when its entry goes claimable can be started a second time by a competing consumer, and under T4 that is a duplicate upload, not a duplicate artifact — one call gets `WROTE`, the other `ALREADY_IDENTICAL`, and both publish the same `public_url`, which MUST-4 already requires issuers to tolerate. The cost is egress, not correctness. The fetch path has carried the same relation since it shipped (`REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS` is also 120). Close the gap on either side if duplicate uploads start showing up in the journal
- `REPLICATOR_WHEELHOUSE_CREDENTIALS` — read-only key for the package index; unset falls back to ADC (what CI uses). Exists so the wheelhouse `ExecStartPre` and the worker do not share one identity: the worker's ADC is the **replication writer**, which needs write on `co-gcs-replication` and has no business reading the wheel index, and the boot step has no business holding write on permanent artifacts (#29)
- `REPLICATOR_REPLICATION_ALIASES_FILE` — path to the JSON alias table; **unset by default, and that is the safe posture**. Nothing provisioned means every `content.replicate` command is refused with `alias_unknown`, so enabling replication to a destination is an explicit operator act on the VM rather than a consequence of a message arriving (contract T5 — it matters most for `ia`, whose items cannot be deleted). A *path* rather than the table itself because the provisioned set is host state but is a table, and one variable per alias per field is a shape env does not hold. Format:

  ```json
  { "primary": { "provider": "gcs", "bucket": "co-gcs-replication" } }
  ```

  **`prefix` is deliberately empty, and the consequence is worth knowing.** The bucket holds `organizations/` and `console_workspace/`; binding the alias to `organizations` would have made the other unreachable, but content may need to live outside that hierarchy, so the alias root is the whole bucket. What that costs is narrower than it sounds: GCS keys are flat, so a prefix was never preventing traversal *out* of anywhere — it would only have stopped a rendered destination landing beside `console_workspace/`. And T4's create-if-absent, backed by `roles/storage.objectCreator` **without** delete, means a stray destination can add an object but can never destroy one. The residual risk is clutter, not loss. Narrow the prefix later if the layout settles; the guard reads it per binding and nothing else changes.

  A binding names **where**, never how to authenticate — the credential is resolved locally from ADC. Fields the binding does not declare are dropped at load, so a key pasted in here never reaches the worker's memory. An unreadable file provisions nothing (logged ERROR); one unusable entry drops only itself (logged WARNING).

  **Each alias gets its own writer, so two aliases may safely name two buckets.** The worker builds one `AsyncGcsDriver` per binding at startup and keys them by alias — a driver *is* a bucket, and keying them by provider once let a command land in a bucket its binding never named (CR #26). A binding whose driver cannot be built — ADC resolves inside that constructor, so an expired key file or a revoked SA raises there — is **skipped and logged at ERROR**, never fatal: commands naming it are refused `provider_disabled`, and the fetch path keeps running. Grep the journal for `could not build a provider writer` after any credential change.
- `REPLICATOR_LOG_LEVEL` — default `INFO`. Governs the **root** logger only, which is the whole tree for the worker. Under the dev server's `--log-config`, uvicorn's own `uvicorn` / `uvicorn.access` / `uvicorn.error` loggers are pinned `INFO` by `src/core/log_config.json` and do not follow it (nor did they under uvicorn's built-in config), so setting `WARNING` will not silence access lines; root itself is `INFO` from boot until the lifespan's `configure_logging()` applies this value
- `REPLICATOR_ALLOW_ANY_CHECKOUT` — bypasses the `main`-checkout guard; **unset by default, which is the enforcing posture**. `scripts/check_main_checkout.sh` runs as the unit's first non-privileged `ExecStartPre` and refuses to start on any branch but `main` — see **The checkout guard** above. Only the literal `1` bypasses it; anything else non-empty logs that it is *not* bypassing rather than silently failing to, because an override that quietly does nothing is worse than none. Exists at all because a guard with no override gets removed rather than overridden — but it is not the way to test a branch on this VM. That is `uv run python -m src.worker.main` under a distinct `REPLICATOR_CONSUMER_NAME`, which never touches the unit and never needs this. Set it only when the thing under test *is* the unit — or when `main` carries a commit that cannot be pushed because the remote is unreachable, which is the one incident-time use the ahead refusal leaves (#37, #48)
- `BUILD_ID` — git SHA stamped by the systemd unit's `ExecStartPre`; defaults to `"dev"` outside systemd
