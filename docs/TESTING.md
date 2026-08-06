# Testing Replicator

Test layout is mirrored from `src/`, and the suite runs against fakeredis by
default. What follows is the part that is not derivable from reading the tests:
where the fake diverges from the real broker, and which keys a live-broker run is
allowed to touch.

## Testing the bus

**Testing the bus.** `tests/conftest.py` ships a `fake_redis` fixture (fakeredis, Streams-capable) — consumer-group behaviour is testable without a broker, and assertions should read the broker's own view (`xinfo_groups` / `xinfo_consumers`) rather than co-core's private attributes, which are not a stable contract. Anything that genuinely needs the live Archiver-operated Redis goes behind `@pytest.mark.integration` and is excluded by default.

**Where fakeredis diverges.** It is sound for consumer-group *mechanics* — what state a command leaves behind — but diverges on *lifecycle* and *blocking* semantics: it registers a consumer on an empty `XREADGROUP` (real Redis waits for a delivery, GH #3) and it ignores `block` (worked around by `IDLE_SLEEP_SECONDS` in `src/worker/loop.py`). Rule of thumb: an assertion about **what state results** is safe against the fake; an assertion about **when Redis does something** needs a live broker. Both divergences were found by running against the real server, not by the suite.

Live-broker tests use the `real_redis` fixture (`tests/conftest.py`), which connects to `REPLICATOR_TEST_REDIS_URL` (default `redis://localhost:6379/15`), skips when nothing answers (an *auth* failure re-raises — a misconfigured broker must not pass as a skip), expires stray `replicator.itest.*` keys from crashed runs once per session, and refuses db 0 outright — db 0 carries the live `content.fetch` stream that the running `replicator.service` consumes, so a test frame written there would be fetched for real. Confine such tests to scratch stream keys via the `scratch_topic` fixture (`tests/worker/conftest.py`), whose teardown also removes `<topic>.dlq`; the database guard is the backstop, not the plan.

**One namespace the sweeper cannot reach.** `process_message` writes `replicator:cmd:<command_id>`, a constant prefix outside `replicator.itest.*`, so an end-to-end test deletes its own keys via the `dedupe_keys` fixture and shortens their TTL. `test_an_end_to_end_run_only_creates_predictable_keys` asserts the whole promise: every key a run creates is either an itest stream or a dedupe key.
