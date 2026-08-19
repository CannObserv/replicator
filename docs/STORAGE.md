# Replicator Temp Storage

The blob tree: how a fingerprint becomes a path, what the three populations under
`REPLICATOR_BLOB_DIR` are, and why the sweep reaps some of them and never others.
Replicator is the producer in archiver's temp-cache protocol, so the cleanup
obligation is Replicator's.

## Two backends, one seam

`REPLICATOR_BLOB_BACKEND` selects which store the worker builds (#7). Everything
downstream takes a `BlobStore` and cannot tell which one it got — that is the
property the seam exists for, and the reason the consume path is untouched by
the second backend.

| | `local` (default) | `gcs` |
|---|---|---|
| `blob_uri` | `file://<blob_dir>/<ab>/<cd>/<sha256>.bin` | `gs://<bucket>/<prefix>/<sha256>.bin` |
| Who can read it | processes on this host, through the filesystem | any identity granted `objectViewer` on the bucket |
| Layout | sharded two levels | **flat** — a bucket namespace has no directories to degrade |
| Write safety | temp file + `os.replace` + `fsync` | `if_generation_match=0` — a create, never a put |
| Retention | the in-worker sweep, on `mtime` | the bucket **lifecycle rule**, on `customTime` |
| Size ceiling | `REPLICATOR_BLOB_MAX_TOTAL_BYTES` | **not enforced** — see below |
| Reachability check | `warn_if_unreachable` walks the ancestors | `preflight_object_store` fails the boot on a one-object listing |
| Per-operation timeout | n/a (local I/O) | `REPLICATOR_BLOB_TIMEOUT_SECONDS`, and it is in the unit's shutdown budget |

**`local` is still the default, and the flip is not this repo's alone to make.**
Watcher parses `blob_uri` into a filesystem path and re-issues the fetch when it
cannot open one — uncapped, on the path that matters (CannObserv/watcher#275) —
so a worker that started announcing `gs://` before Watcher could read it would
produce an unbounded re-fetch loop against live origins rather than a broken
link. Order: Watcher ships, then `/etc/replicator/.env` changes. The boundaries
charter's *Known violation* section is the thing that closes when it does.

**What the object store removes, and what it does not.** It removes the
host-local data plane: a consumer no longer has to live on this VM, and the
ancestor-permission trap below stops existing. It does **not** remove the
coupling — it changes its kind, from a filesystem mode this process can inspect
to an IAM grant it cannot. `preflight_object_store` proves the bucket is there
and readable *by the worker*; whether the consumer may read is verified where
the grant is made ([DEPLOYMENT.md](DEPLOYMENT.md)), not at boot.

**A missing blob is a `FileNotFoundError` on both backends.** The object store
translates the SDK's `NotFound`, so the consume path keeps one catch for "these
bytes are gone" rather than one per backend, and `src/worker/replicate.py` stays
free of `google.api_core`. Every *other* provider failure propagates with its
status intact and is classified by the caller through
`src.core.errors.is_terminal_provider_status` — the same status rule the replicate
*write* path has used since CR #27. Only the transient half is claimed: 5xx,
408/429 and anything with no status become a transient error that parks the
command; a terminal 4xx (a misprovisioned grant) stays unclassified and takes the
existing ceiling-then-DLQ path, which is what gives an operator a reclaim window
to fix the grant before anything is closed against an issuer.

**The preflight is a one-object listing, not an existence check.** `Blob.exists()`
catches `NotFound` and returns `False` — for a missing *bucket* exactly as for a
missing object — so an existence-based probe cannot see the likeliest
misconfiguration there is, a typo in `REPLICATOR_BLOB_BUCKET`. Listing raises, and
needs only `storage.objects.list`, which both grants already carry.

**Every `BlobStore` call from a coroutine goes through `asyncio.to_thread`.** The
seam is synchronous — an async twin would be a wide refactor for no behavioural
gain, and `AsyncGcsDriver` is itself `to_thread` around a blocking SDK — so the
rule lives at the call sites and is held by `tests/worker/test_storage_offloop.py`,
which watches which thread the store actually ran on. It was already needed
before the object store: `_write_atomically` ends in an `fsync`.

The cost of that is a **fourth term in the unit's shutdown budget**: `to_thread`
puts a store beyond cancellation, so SIGTERM waits out an upload in flight
exactly as it waits out a sweep. `tests/test_deploy.py` sums the poll window, the
pacing sleep, the fetch ceiling and `REPLICATOR_BLOB_TIMEOUT_SECONDS` against
`TimeoutStopSec`. The sweep and the storage term are mutually exclusive in
practice — one per backend — so the sum deliberately overcounts.

## The blob tree

- **A blob is `file://<blob_dir>/<ab>/<cd>/<sha256>.bin`.** Sharded two levels to bound directory fan-out; the extension is a constant `.bin`, **never** derived from `media_type` — identical octets can arrive under different Content-Types, and two paths for one fingerprint would defeat `exists()` as a short-circuit. Writes go through a temp file + `os.replace`: presence at a content-addressed path is what readers take as proof the bytes are complete. Design: `docs/plans/2026-07-31-replicator-mvp-open-questions-design.md`.
- **Blob modes are set on creation only.** Files land at `0644`, directories the worker creates at `0755` — both by explicit `chmod`, since `mkstemp` creates at `0600` and `mkdir`'s mode is masked by the umask. A directory that **already exists is never re-chmod'd**: it belongs to whoever provisioned it, and `chmod` on an unowned-but-writable mount raises `EPERM`. The cost is a silent trap — a `0700` level anywhere in the chain stores and publishes normally while no other service can open the `blob_uri` — so `warn_if_unreachable` walks `blob_dir` **and every parent** at startup and names each blocking level. Traversal needs `+x` all the way up, and the likeliest mistake is a restrictive parent over a fine leaf. `src/storage/local.py::ensure_directory`, `src/worker/main.py::warn_if_unreachable`.

## Retention

`docs/plans/2026-07-31-replicator-mvp-open-questions-design.md` §4 scope-cut retention; #5 settles it. Replicator is the producer in archiver's temp-cache protocol, where **the producer cleans up**.

- **The TTL runs from last reference, not first store.** `store` short-circuits on an existing content-addressed path but its caller publishes a fresh `blob_available` either way, so a re-fetch of unchanged bytes would otherwise announce a blob already partway through its TTL. `LocalBlobStore` therefore `os.utime`s on the short-circuit branch, swallowing `ENOENT` — the sweep can unlink between the existence check and the touch, and the fallout of that race is a `blob_uri` that fails to open, not a dead-lettered command.
- **The horizon that clock implies is published, and it errs early.** `blob_available.blob_expires_at` carries `stored_at + REPLICATOR_BLOB_TTL_SECONDS` (cannobserv#301, #28), so a consumer records a cache expiry instead of re-deriving one from a retention policy it does not own and a clock start it cannot see. `stored_at` is read *before* `store`, so it lands at or before the mtime the sweep measures against; the sweep then runs only every `REPLICATOR_BLOB_SWEEP_INTERVAL_SECONDS`. Both terms push the real reap later than the announced horizon, which is the only safe direction — a consumer acting on it re-fetches early rather than opening a dead `blob_uri`.
- **The blob tree holds three populations, and they are not interchangeable.** Finished blobs (`<ab>/<cd>/<sha256>.bin`) reap on the TTL; in-flight temporaries (`.<sha256>.<random>.tmp`) are **not garbage** — reaping one makes the writer's `os.replace` fail with `ENOENT` and dead-letters a good command — so the sweep matches `*.bin`, never `iterdir()`, and ages temps out on their own much longer grace. Empty shard directories go **last**, by `rmdir` only, whose refusal to touch a non-empty directory is the safety property.
- **The ceiling is backpressure, not a faster clock.** Over `REPLICATOR_BLOB_MAX_TOTAL_BYTES` the byte path raises `TransientFetchError` *before* fetching, so the command stays in the PEL and returns via `claim_stale` once a sweep frees space. What it measures is everything the tree holds — surviving blobs **and** the temporaries the sweep is waiting out, since a crash loop fills the disk with debris the ceiling would otherwise not see. The per-population counts stay split in the sweep log so a rising temp count cannot hide inside a healthy blob one. Reaping a blob still inside its TTL to make room would convert a local disk problem into a `blob_uri` another repo cannot open — the one failure mode with no local symptom.
- **One `BlobUsage`, two writers.** The sweep's `observe` is the measured total; the byte path's `add` is the estimate between sweeps, because a burst can cross the ceiling long before the tree is walked again. Wiring the two halves to separate instances leaves both individually correct and the guard permanently unreachable — `tests/worker/test_main.py` pins the identity.
- **Orphans are recorded where they are exact.** A publish that fails after the store leaves bytes with no fact and no `command_id`, invisible to any query starting from `content.blobs`. `src/worker/handler.py::_publish` logs the fingerprint at that moment and re-raises untouched; the sweep then treats orphans as ordinary aged blobs. Reconciling the tree against `content.blobs` instead would make a *delete* decision depend on another service's stream-trimming policy.
- **Under `gcs`, none of the four bullets above apply, and the reasons are worth stating.** The sweep does not run at all — walking a bucket means listing every object every cycle, O(n) Class A operations forever, to re-derive a number the lifecycle rule already acts on, and it would be reaping on a clock this worker no longer owns. The three populations collapse to one: there are no temporaries (a failed conditional create leaves no object, rather than a truncated one at a name that asserts a sha256) and no shard directories to empty. And **`REPLICATOR_BLOB_MAX_TOTAL_BYTES` is not enforced** — it bounds a shared disk and a bucket is not one. Not enforced means the byte path is handed **no ceiling at all** (`ceiling_bytes=None`), not a ceiling it happens not to reach: `BlobUsage` has two writers and only the sweep's `observe` can bring the number *down*, so a ceiling passed to a backend with no sweep would meet the byte path's own ever-rising estimate and park every command there — transiently, forever, waiting on a sweep that does not exist. A worker that has silently stopped fetching while every health signal looks fine. That is a safety property given up rather than replaced: what still holds is the per-blob `REPLICATOR_MAX_BLOB_BYTES` cap, and what covers the rest is a billing budget alert, which no process here can enforce. The worker says so at boot, naming the variable, because the operator who needs that line is the one reading it in `/etc/replicator/.env` and assuming it applies.
- **The lifecycle rule reads `customTime`, not creation age, and the difference is the whole TTL semantics.** "Since last referenced" is expressible on a bucket only because `GcsBlobStore` stamps `customTime` on the re-reference path — the exact port of `os.utime` on the short-circuit branch. A `daysSinceCustomTime` condition then means what the local `mtime` sweep meant. A plain `age` condition would reap a blob re-referenced moments ago, and it would do it invisibly, because re-fetching unchanged bytes never rewrites the object. **The cost is precision**: lifecycle granularity is one day and enforcement is asynchronous, lagging the condition by up to 24 h and sometimes more. So `blob_expires_at` stops being an exact horizon and becomes a **floor** — the blob is guaranteed no *shorter* than announced, and may outlive it. That is the same safe direction the local backend already errs in, one order of magnitude wider.
- **The sweep runs in the worker, not a systemd timer.** A timer survives a crashed worker, but a worker that is not running is not writing blobs either. It rides the same stop event as the consume loop (`src/worker/loop.py::park`) and walks the tree via `asyncio.to_thread`, so retention never becomes a source of consume-path latency. A failed sweep is absorbed and retried next cycle — retention is not load-bearing for correctness, and the ceiling is the guard for a tree that cannot be reaped.
