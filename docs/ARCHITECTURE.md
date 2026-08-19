# Replicator Architecture

Founding design: `docs/plans/2026-06-25-replicator-mvp-design.md`. Parent strategy lives in archiver (`docs/plans/2026-06-25-observer-cluster-integration-strategy-design.md`); Replicator is its Phase 3.

Module layout: what each file under `src/` owns, and where everything else in
the tree sits. `AGENTS.md` keeps the rules needed on nearly every task; what each
bus stream carries is in [STREAMS.md](STREAMS.md), the rules common to all of
them in [CONVENTIONS.md](CONVENTIONS.md), and blob-tree and retention rules in
[STORAGE.md](STORAGE.md).

## Project Layout

```
src/worker/     — Bus consumer; the primary process
src/worker/main.py   — Entry point: client lifetime, consumer group, signals, backend selection
src/worker/loop.py   — The consume path: poll → dispatch → ack, DLQ, dedupe, recovery; one loop per command stream, parameterized by CommandSpec
src/worker/handler.py — The byte path behind the Handler seam: fetch → fingerprint → store → publish
src/worker/reporter.py — The failure fact behind the FailureReporter seam: fetch_failed on content.blobs
src/worker/retention.py — The sweep task: cadence, usage accounting, ceiling reporting
src/worker/pacing.py — Per-host request spacing; the mechanism half of politeness (#12, escalating on 429 since #25)
src/worker/policy.py — The content.fetch-policy consumer: the map, and the groupless tail (#19)
src/storage/    — Temp storage; BlobStore protocol + its two backends
src/storage/base.py  — BlobStore protocol (store / exists / uri_for / open / open_stream)
src/storage/local.py — Content-addressed local backend; file:// URIs, sharded paths
src/storage/gcs.py   — Content-addressed object-store backend; gs:// URIs, flat keys, customTime retention (#7)
src/storage/sweeper.py — Retention: TTL reap, stale temps, empty shards; the measured size
src/api/        — FastAPI app (/health only; not part of the MVP loop)
src/api/main.py — App factory, lifespan, router registration
src/core/       — Shared domain logic, logging, config
src/core/errors.py   — TransientError / PermanentError (what the loop catches) + the *Fetch/*Replicate leaves + FailureReason / ReplicateReason
src/worker/replicate.py — The content.replicate handler: alias resolve, T3/T3a guards, refusals
src/worker/aliases.py — The provisioned destination set, read once from env-referenced host config
src/worker/replicate_reporter.py — replication_failed on content.artifacts
src/worker/checkout.py — Is this checkout main's code? Asked before a write identity is built (#52)
src/core/logging.py  — build_json_formatter() + ColorMessageFilter + configure_logging() + get_logger()
src/core/log_config.json — uvicorn --log-config; routes uvicorn's own loggers through that formatter
src/core/config.py   — Settings / env access (see Environment Variables)
scripts/        — sync_wheelhouse.py, check_redis_floor.sh, check_main_checkout.sh, seed_fetch.py
scripts/seed_fetch.py — the MVP command issuer; publishes content.fetch, --watch tails the facts
tests/          — Mirrors src/ structure; integration tests in `@pytest.mark.integration`
docs/           — Reference docs; the Detail Docs index in AGENTS.md is the roster
docs/contracts/ — Normative contracts, linked to from sibling repos: the issuer-facing half of the wire, and the boundaries charter (what Replicator may become)
docs/plans/     — Implementation plans
deploy/         — Systemd unit + deployment config
.wheelhouse/    — Local mirror of the private cannobserv index (git-ignored except .gitkeep)
```

## Bus Conventions

What each stream carries, and the reasoning behind each rule `AGENTS.md` states
in one line: [STREAMS.md](STREAMS.md).
