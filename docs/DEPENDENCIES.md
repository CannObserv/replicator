# Dependencies

Where co-core comes from, how to reach it, and its pin. `AGENTS.md` keeps the
rule; the reasoning is here. The unit and the deploy recipe are in
[DEPLOYMENT.md](DEPLOYMENT.md); every variable is in [ENVIRONMENT.md](ENVIRONMENT.md).

## The co-pypi index

`co-core`, `co-core-aio` and `co-core-sync` resolve from cannobserv's Artifact Registry
index, `https://us-west1-python.pkg.dev/co-gcs/co-pypi/simple/` ("Path C" in
cannobserv's `docs/RELEASING.md`), never from PyPI. It replaced the GCS wheelhouse
in #183. `pyproject.toml` declares it `explicit`, with a `[tool.uv.sources]` entry per
package: uv asks it for those three only, so neither side can shadow the other.
`uv.lock` records each file's URL and sha256, which `uv sync` checks;
`tests/test_dependency_index.py` pins the rules.

**The index takes a token; a warm cache doesn't** [measured 2026-11-10]:

| Command | Token |
|---|---|
| `uv run …`, `uv lock --check`, the unit's `ExecStart` | none, even with the network cut |
| `uv sync` whose co-core files are installed or cached | none |
| any `uv lock`, even a PyPI-only bump; `uv sync` on a cold cache | **required**: a 401 on co-core without it |

So the deploy step and every relock go through the wrapper:

```bash
. scripts/load_env.sh                      # REPLICATOR_CO_PYPI_CREDENTIALS
bash scripts/co-pypi.sh uv sync --frozen   # the deploy step
bash scripts/co-pypi.sh uv lock
```

**The wrapper.** `scripts/co-pypi.sh` runs `scripts/co_pypi_token.py`, which mints an
access token from the read-only key `REPLICATOR_CO_PYPI_CREDENTIALS` names
(`co-pypi-reader@`, `/etc/replicator/co-pypi-reader.json`) and `exec`s uv with
`UV_INDEX_CO_PYPI_USERNAME` / `_PASSWORD`. Each rule is tested
(`tests/test_co_pypi_token.py`):

- **One key, no fallback.** Never `GOOGLE_APPLICATION_CREDENTIALS`: on this host
  that is the worker's *writer* (#114). Unset, the wrapper exits 2 and names the
  variable.
- **An allowlisted environment.** The deploy shell carries the writer's key path
  and the repo `.env`'s org-wide PATs, and uv (with the `uv_build` backend it runs)
  starts with neither. The shell script and the Python hold the same list; a
  variable uv turns out to need goes on both.
- **The token is never printed, logged, put in argv or written.** Anything but
  `uv` or `wheels` is refused before a mint.

**CI never runs it.** Each job's read-only WIF step mints the token
(`token_format: access_token`, id `co_pypi`) and writes no credentials file; the
`uv sync` step alone receives it, masked (`tests/ci/test_workflow_pins.py`).

**Limits.** The token reaches what the wrapped uv spawns (the build backend),
bounded by the reader's role and the token's hour. The `wheels` verb sends it to
`urllib`, for the index host alone and across same-host redirects only: the index
answers a file with a 307.

## Moving the pin

1. Edit the three specifiers in `pyproject.toml`.
2. `bash scripts/co-pypi.sh uv lock`, and sync through it into a worktree's own
   venv, never the shared one the service imports (#114).
3. Diff the import closure ([below](#the-co-core-pin-and-why-the-patch-floor-is-load-bearing)):

   ```bash
   bash scripts/co-pypi.sh wheels OLD NEW /tmp/co-core-wheels
   uv run --no-project python scripts/co_core_closure_diff.py OLD NEW --wheels /tmp/co-core-wheels
   ```

4. Paste the report into the pin comment and raise the floor by the rules below.

`wheels` checks each file against the index's sha256. The closure diff's
real-wheel test reads `REPLICATOR_TEST_CLOSURE_WHEELS`, a directory `wheels 0.36.0
0.37.0` filled, and skips without it, in CI too.

**Ask the index, not a cache.** The #77 review reported 0.13.1 "not mirrored" from a
local `ls` while the index already carried it. Anything local is a cache; `wheels`
or `uv lock` asks the index.

## The co-core pin, and why the patch floor is load-bearing

Pin the current minor — `>=0.45.0,<0.46`, on all three. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead — a ValidationError at construction (0.8.0 requires `info_source_id` on all three fetch payloads, #19/#28) or an ImportError at load, never reaching a running worker (0.9.4 cuts the replicate contracts, #29; 0.19.3 ships the shared `BlobStore`, #114; 0.19.6 cuts the persist contract, #114 step 6).

**The 0.10 → 0.13 jump (#77) was the first departure from "pin the current minor", and what justified it is evidence rather than judgement.** The hazard that rule guards is payload fields moving under `extra="ignore"`; every module this service imports proved byte-identical between the two wheels — `pure/models/changes.py`, `pure/adapters/bus/{envelope,exceptions}.py`, `effects/{bus,fetch,gcs}.py`, `pure/util/{hashing,gcs}.py`, and all of `co_core_aio` bar its `__version__`. Only `streams.py` gained anything (`group_name`, `stream_kind` — cannobserv#384), and Replicator imports no `co_v1` adapter, which is where 0.13.2's other changes landed. That `diff -rq` of both wheels from the then-wheelhouse took a minute and turned a deferred change into a safe one.

**Diff the import closure before every pin move**, a single minor included: #136 needed it for one. The version distance is not the risk, the moved surface is, and the import lines are not the surface. At 0.19 → 0.36 (#140), `changes.py`'s import of `extract.canonical` brought in the `pure/extract` package, which had changed (cannobserv#523), and no import line in Replicator names it. `scripts/co_core_closure_diff.py OLD NEW` (#145) walks the closure from `src/` and `scripts/` through all three wheels at each version, fetched first ([Moving the pin](#moving-the-pin)). It prints the counts, a verdict per file that isn't identical, the Requires-Dist delta and the closure's third-party imports, and `--diff` adds each changed file's code diff. Paste the report into the pin comment and write the reasons around it by hand. `version-only` and `docstrings-only` need no reading. `changed` does, a model under `extra="ignore"` above all. `added` and `removed` are modules joining or leaving the closure. An **Unresolved imports** line names a module that is imported but isn't in the wheel, which fails at import. A **Not parsed** line names a file whose imports the walk couldn't follow. Each prints only when there's something to list. The walk follows `TYPE_CHECKING` imports and lists dynamic ones without following them. At 0.45.0 that's co-core-sync's two lazy `__getattr__` sites in `drivers/` and `drivers/blobstore/`. Both are expected, because each target is named again under `TYPE_CHECKING`. A new site whose targets aren't named there is a gap to read by hand. Flags: `--root`, `--package`, `--wheels` (required); the defaults are this repo's.
