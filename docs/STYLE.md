# Replicator Style & Logging

Code style beyond the conventions `AGENTS.md` keeps inline. Today that is the
logging stack: one formatter, two installers, and the places its output is
deliberately not JSON.

## Logging

**One formatter, two installers.** `build_json_formatter()` is the single definition of the JSON schema (`timestamp`, `level`, `logger`, `message`). `configure_logging()` installs it on the root logger; `src/core/log_config.json` names the *same factory* through dictConfig's `"()"` key, so there is no second fmt string to drift. The dev server must be launched with `--log-config src/core/log_config.json` — uvicorn's `uvicorn` / `uvicorn.access` / `uvicorn.error` loggers ship with `propagate=False` and their own plain-text handlers, so a root-only config never reaches them and the output is half JSON, half plain text. **The worker runs no uvicorn**, so `replicator.service` needs no `--log-config`; its `ExecStart` is `python -m src.worker.main` and `configure_logging()` is the whole story there (#14).

**Not everything in the journal is JSON.** The claim above is about the *app's own records*. **Every `ExecStartPre` writes plain text**, and that is structural rather than an oversight: each runs before — or entirely outside — the environment `build_json_formatter()` lives in, so making them emit JSON would mean a hand-maintained second copy of the schema in exactly the files that cannot single-source it. Three of them speak today:

- **wheelhouse sync** — `wheelhouse in sync: N downloaded, M already present -> …` on the happy path, `error: could not sync gs://…` on the non-fatal failure path. It runs `uv run --no-project`, before the deploy's `uv sync`, in an environment holding `google-cloud-storage` and nothing else (#15, skills#83).
- **`check_redis_floor.sh`** — `check_redis_floor: Redis X meets the >=7.0 floor`, or the refusal below it. A bash preflight; there is no interpreter to import from.
- **`check_main_checkout.sh`** — `check_main_checkout: on main at <sha>`, or a refusal, or a warn-tier line about a stale/unpushed/dirty tree (#37). Same reason.

A shipper reading journald natively is unaffected (the message is a field alongside `_SYSTEMD_UNIT` / `_PID`); a pipeline that `json.loads` every `MESSAGE` must tolerate all of them — **the refusal lines especially, since they appear exactly when something is already wrong**, which is when a dropped log line costs the most. Two of the three can *only* speak on a bad start.

**The colour strip is a filter on the loggers, deliberately.** uvicorn attaches an ANSI-coloured duplicate of each lifecycle message as `extra={"color_message": ...}`, and every extra reaches the JSON payload. `ColorMessageFilter` deletes it from the record at its source — not via the formatter's `reserved_attrs`, and not on the stdout handler. Both alternatives scope the fix to *this* sink: a handler that builds its payload from the record's `__dict__` rather than a `logging.Formatter` resurrects the field, and OpenTelemetry's `LoggingHandler` is exactly that (its own reserved list does not cover `color_message`). Mutating the record once means the strip survives a sink swap with no failing test to warn you it had stopped working. `tests/core/test_logging.py` pins the filter's placement, not just its effect.
