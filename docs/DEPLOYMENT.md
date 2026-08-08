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
| Code committed to main | `uv sync --frozen && sudo systemctl restart replicator` |
| Testing a worktree/branch | `uv run python -m src.worker.main` (set a distinct `REPLICATOR_CONSUMER_NAME`) |
| Debugging the live service | `sudo journalctl -u replicator -f` |
| After editing `deploy/replicator.service` | `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart replicator` |
| After a co-core version bump | re-run `sync_wheelhouse.py`, then `uv sync` |

`ExecStart` uses `--frozen --no-sync`, so dependency sync is a deploy step, not a service-start side effect.

**`/etc/systemd/system/replicator.service` is a *copy*, not a symlink to `deploy/`.** So the `cp` above is load-bearing and `daemon-reload` alone silently does nothing — systemd re-reads the installed file, which is still the old one. The failure has no symptom at restart: the worker comes up on the new code under the *old* unit, and the mismatch only surfaces the first time a directive actually matters. Nothing guards it, either — `tests/test_deploy.py` reads the repo file, which is exactly the copy that is still correct. Diff the two when a restart follows a unit edit (#11 deploy).

The copy is deliberate, for the same reason `/etc/replicator/.env` is not read from the repo: the live unit must survive a repo reset, a worktree switch, or a branch checkout that happens to be mid-edit.

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
- `REPLICATOR_DEDUPE_TTL_SECONDS` — lifetime of the `replicator:cmd:<command_id>` dedupe key; default `86400`
- `REPLICATOR_LOG_LEVEL` — default `INFO`. Governs the **root** logger only, which is the whole tree for the worker. Under the dev server's `--log-config`, uvicorn's own `uvicorn` / `uvicorn.access` / `uvicorn.error` loggers are pinned `INFO` by `src/core/log_config.json` and do not follow it (nor did they under uvicorn's built-in config), so setting `WARNING` will not silence access lines; root itself is `INFO` from boot until the lifespan's `configure_logging()` applies this value
- `BUILD_ID` — git SHA stamped by the systemd unit's `ExecStartPre`; defaults to `"dev"` outside systemd
