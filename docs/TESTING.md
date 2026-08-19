# Testing Replicator

Test layout is mirrored from `src/`, and the suite runs against fakeredis by
default. What follows is the part that is not derivable from reading the tests:
where the fake diverges from the real broker, and which keys a live-broker run is
allowed to touch.

## Test layout

- Test structure mirrors source (`src/foo.py` → `tests/test_foo.py`). A module whose tests outgrow one file splits by concern, not by helper: `tests/worker/test_loop_dlq.py`, `test_loop_recovery.py`, … with the shared wiring in that package's `conftest.py`. Concern is the default axis; **environment** is the one exception — tests needing a live broker split off with an `_integration` suffix (`tests/worker/test_main_integration.py`), and those needing the real GCS bucket with a `_gcs` one (`tests/worker/test_replicate_writer_gcs.py`), so the filename says what the marker enforces. Where that would double — the module under test is already `gcs.py` — the marked file names the resource instead (`tests/storage/test_gcs_bucket.py`), never `_integration`, which is the other marker's suffix

## Testing the bus

**Testing the bus.** `tests/conftest.py` ships a `fake_redis` fixture (fakeredis, Streams-capable) — consumer-group behaviour is testable without a broker, and assertions should read the broker's own view (`xinfo_groups` / `xinfo_consumers`) rather than co-core's private attributes, which are not a stable contract. Anything that genuinely needs the live Archiver-operated Redis goes behind `@pytest.mark.integration` and is excluded by default.

**Where fakeredis diverges.** It is sound for consumer-group *mechanics* — what state a command leaves behind — but diverges on *lifecycle* and *blocking* semantics: it registers a consumer on an empty `XREADGROUP` (real Redis waits for a delivery, GH #3) and it ignores `block` (worked around by `IDLE_SLEEP_SECONDS` in `src/worker/loop.py`). Rule of thumb: an assertion about **what state results** is safe against the fake; an assertion about **when Redis does something** needs a live broker. Both divergences were found by running against the real server, not by the suite.

Live-broker tests use the `real_redis` fixture (`tests/conftest.py`), which connects to `REPLICATOR_TEST_REDIS_URL` (default `redis://localhost:6379/15`), skips when nothing answers (an *auth* failure re-raises — a misconfigured broker must not pass as a skip), expires stray `replicator.itest.*` keys from crashed runs once per session, and refuses db 0 outright — db 0 carries the live `content.fetch` stream that the running `replicator.service` consumes, so a test frame written there would be fetched for real. Confine such tests to scratch stream keys via the `scratch_topic` fixture (`tests/worker/conftest.py`), whose teardown also removes `<topic>.dlq`; the database guard is the backstop, not the plan.

**One namespace the sweeper cannot reach.** `process_message` writes `replicator:cmd:<command_id>`, a constant prefix outside `replicator.itest.*`, so an end-to-end test deletes its own keys via the `dedupe_keys` fixture and shortens their TTL. `test_an_end_to_end_run_only_creates_predictable_keys` asserts the whole promise: every key a run creates is either an itest stream or a dedupe key.

## Testing the write path

**Production `co-gcs-replication` is not a test target, and cannot become one.**
The writer SA holds `storage.objects.{create,get,list}` and no `delete` — which is
what enforces the replicate contract's T4 "never overwrite, never delete" at IAM
rather than only in our code. A conflict fixture cannot reset itself against it,
so the same test cannot run twice, and every run that writes leaves permanent
litter. The property that makes the production grant correct is the property that
makes production untestable (#38).

`tests/test_destinations.py` refuses it in two halves, one static and one runtime:

- **The scan.** No module under `src/` or `tests/` may contain
  `co-gcs-replication` (the bucket) or `co-gcs-replicator` (the writer SA, which
  is also its key file's basename and its email's local part) in a string
  literal. AST-based, docstrings included, and it excludes only itself. Unit
  tests use `example-replication-bucket` and `example-internal-bucket`; the
  wheelhouse reader `co-pypi-reader.json` stays legal, because
  `test_replicate_guards.py` names it on purpose to prove the path guard refuses
  a real secret.
- **The fixtures** (`tests/conftest.py`, autouse). `REPLICATOR_REPLICATION_ALIASES_FILE`
  and `GOOGLE_APPLICATION_CREDENTIALS` are removed from the environment of every
  test — the Common Commands snippet sources `/etc/replicator/.env`, so `uv run
  pytest` inherits both — and `AsyncGcsDriver.__init__` **and
  `GcsBlobStore.__init__`** are patched to refuse any bucket. The refusal
  precedes the call through, because both constructors resolve ADC in their own
  body; checking afterwards would authenticate first and object second.

  **An unmarked constructor handed a `client=` is admitted.** What the guard is
  aimed at is a constructor that resolves credentials itself and reaches a bucket
  by name — a caller supplying the client has already made that impossible, and
  an unmarked test cannot build a real client anyway with the identity scrubbed.
  Without the carve-out, `tests/storage/test_gcs.py` would have to claim the
  `gcs` mark to test decisions that touch no network, which is how a marker stops
  meaning "writes to a bucket" and starts meaning "constructs this class".

  **It is scoped to the unmarked state on purpose.** Placed ahead of the bucket
  comparison it also admitted a *marked* test — the one state with a credential
  actually resolved — so a driver could be pointed at any bucket by handing it a
  client. The moment a destination is expected, the destination is checked.

  **The fakes live in `tests/storage/conftest.py`**, not in whichever test module
  defined them first, and they mirror the SDK rather than the tests: `exists()`
  returns `False` for anything absent because the real one swallows `NotFound`,
  which is the behaviour that made an existence-based preflight untestable and
  wrong at the same time.

The bucket and the SA are provisioned (#50) — `gs://co-gcs-test-replication` and `co-gcs-test-replicator@co-gcs.iam.gserviceaccount.com`, with `roles/storage.objectAdmin` on that bucket and no write on production. The resource, what it deliberately differs from production in, and how each property was verified: **The GCS test bucket** in [DEPLOYMENT.md](DEPLOYMENT.md).

**The bucket is `co-gcs-test-replication`, not `co-gcs-replication-test`.** A
suffixed name contains the production name, which would force the scan to carry a
negative lookahead. Pinned by `test_the_test_bucket_is_not_a_superstring_of_production`.

A test that genuinely writes is marked `@pytest.mark.gcs` and requests the
`gcs_bucket` fixture. The marker is **separate from `integration`**: that one
means the live VM Redis, which is local, free and routinely run, and a marker
that also writes to a bucket changes what `-m integration` costs.

```bash
uv run pytest --no-cov -m gcs
```

**What runs there.** `tests/worker/test_replicate_writer_gcs.py` — the three T4
rows against the real bucket: absent writes and completes, a redelivery onto
identical bytes is a no-op that still emits the *same* `public_url` and leaves the
object's generation untouched, and differing bytes at one destination are a
terminal `destination_conflict` with nothing overwritten. The fourth outcome,
`INDETERMINATE`, is deliberately absent — provoking a delete between the failed
create and the confirming read is either flaky or needs a seam in the write path
that exists only for the test, and it is covered against the fake.

Each test writes under its own `replicator-t4/<random>/` prefix and its teardown
deletes it, then **asserts the prefix is empty** — a cleanup that silently missed
an object would be collected by the lifecycle rule a day later, so nothing would
ever fail and the next reader would inherit the ambiguity #38 spent a comment
thread resolving. The lifecycle rule is litter insurance, not the reset; the reset
is the test SA's `delete`, which is the whole reason the bucket exists.

CI runs these in their own `gcs` job, keyless via the same WIF provider — the only
job in the workflow holding a delete-capable identity, kept separate so an
ordinary unit test's blast radius does not widen to buy three tests' coverage. A
skip there would be a green run with no verification, so the job asserts the
credentials file resolved before it runs pytest.

Three variables, **none with a default** — absent means skip, never "use
whatever the code would have picked":

| Variable | What it names |
|---|---|
| `REPLICATOR_TEST_GCS_BUCKET` | the provisioned replicate test bucket |
| `REPLICATOR_TEST_BLOB_BUCKET` | the temp-blob test bucket (#7); unprovisioned today, so `tests/storage/test_gcs_bucket.py` skips |
| `REPLICATOR_TEST_GCS_CREDENTIALS` | the test SA key; the fixture maps it onto `GOOGLE_APPLICATION_CREDENTIALS` for marked tests only |

**Two buckets, because the grants are opposites.** The replicate destination's SA
holds no `delete` — the property that puts T4 at IAM rather than only in our code
— while the temp-store tests create objects and clean up after themselves. One
bucket serving both would mean either those tests cannot tidy or the permanent
store can be erased, and only one of those failures is recoverable. The missing
*bucket* therefore skips in the fixture that hands it over rather than in the
autouse one: the two destinations are provisioned independently, and a host with
one should still run the tests it can. A missing *identity* still skips
everything, since nothing marked can run without it.

All three are dev-only and belong in the repo `.env` or the invoking shell —
never in `/etc/replicator/.env`, which is the file the service reads. Contrast
`REPLICATOR_TEST_REDIS_URL`, which *does* default: db 15 on localhost cannot be
the live database, and `real_redis` refuses db 0 outright. No bucket name has
that property.
