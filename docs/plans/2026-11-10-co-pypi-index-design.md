---
title: co-core from the co-pypi index — retire the GCS wheelhouse
date: 2026-11-10
status: approved 2026-11-10 (items 1–5 as written; operator authorized the host `.env` rename)
issue: "#183 (epic #154; part 1 of #169)"
---

# co-core from the co-pypi index

## Intent

`co-core`, `co-core-aio` and `co-core-sync` resolve today from `./.wheelhouse`. That directory is
mirrored from `gs://co-gcs-pypi` by `scripts/sync_wheelhouse.py` and read through `[tool.uv]
find-links` ("Path B" in cannobserv's `docs/RELEASING.md`). This change moves them to cannobserv's
Artifact Registry index, `https://us-west1-python.pkg.dev/co-gcs/co-pypi/simple/` ("Path C"), and
retires the wheelhouse.

The pin stays at `>=0.41.0,<0.42`: one change per deploy, and #184 moves the pin to 0.42 after
this. The precedent is CannObserv/watcher#358 part 1, live since 2026-11-06. Where this design
departs from it, that's said below, along with why.

### Pre-flight [measured 2026-11-10 on `c74be97`]

- **Same bytes.** Read as `co-pypi-reader@` from `/etc/replicator/co-pypi-reader.json`, all
  three `/simple/<pkg>/` pages return 200. For 0.40.1 and 0.41.0, every file's index `#sha256=`
  equals `sha256sum` of the host's `.wheelhouse/` copy: 12 of 12 (three packages, each a wheel
  and an sdist, at two versions).
- **The index lists 0.42.0.** The `<0.42` bound keeps it out of the lock.
  `tests/test_dependency_index.py` asserts that.
- **Baseline.** 1914 passed, 1 skipped. AGENTS.md is at 5969 of 6000 tokens; COMMANDS 9840,
  DEPLOYMENT 9636 and ENVIRONMENT 9209 of 10000.
- **Corrections to the brief** were posted before this doc: issuecomment-6774312177.

## The decisions (brief weigh items 1–5)

### 1. The writer's credentials never reach uv

Here, `GOOGLE_APPLICATION_CREDENTIALS` is the worker's **writer** (`co-gcs-replicator-writer@`,
#114). The deploy shell is `. scripts/load_env.sh`, which loads `/etc/replicator/.env` *and* the
repo `.env`. So the shell an operator runs `uv` from holds the writer's key path and every
org-wide `GH_TOKEN_*` PAT. Watcher's wrapper passes its environment through, so each of those
would reach the wrapped `uv` and everything it spawns, including the `uv_build` backend it fetches
from PyPI and runs.

**Decision: a wrapper that starts uv from an allowlisted environment and reads one key variable.**

- `bash scripts/co-pypi.sh uv <args…>` runs `scripts/co_pypi_token.py` under `uv run --no-project
  --with 'google-auth[requests]>=2,<3'`. That mints an access token from the key named by
  `REPLICATOR_CO_PYPI_CREDENTIALS` (item 2), then `exec`s the command with
  `UV_INDEX_CO_PYPI_USERNAME=oauth2accesstoken` and `UV_INDEX_CO_PYPI_PASSWORD=<token>` set.
- **The allowlist is applied twice.** The shell script starts the outer `uv run` under `env -i`,
  carrying only an allowlist: `PATH`, `HOME`, `USER`, `LOGNAME`, `LANG`, `LC_*`, `TERM`,
  `TMPDIR`, `XDG_*`, `SSL_CERT_*`, the proxy variables, `UV_*`, and the key variable. The Python
  side filters the same way before its `exec`, and drops `VIRTUAL_ENV`, which the outer `uv run`
  sets.
- **Why both layers.** The writer key path and the PATs can't reach either `uv`, and a direct
  `python scripts/co_pypi_token.py` run gets the same guarantee.
- **No fallback.** If `REPLICATOR_CO_PYPI_CREDENTIALS` is unset, it exits 2 and names the
  variable. It never consults `GOOGLE_APPLICATION_CREDENTIALS` or ADC.
  `service_account.Credentials.from_service_account_file` takes the path explicitly, so
  google-auth's ADC search never runs.
- **CI never runs the wrapper** (item 6 below), so there's no ADC branch anywhere. This is the
  brief's "ADC only in CI" made unnecessary, not dropped.
- **Kept from Watcher:** it refuses anything but `uv` (and the `wheels` verb, item 4), and the
  token is never printed, logged, put in argv or written to disk, on any failure path.
- **Tests (red first):**
  - with `GOOGLE_APPLICATION_CREDENTIALS` set and the key variable unset → exit 2, nothing minted;
  - with both set → the minted key is the reader's, never the writer's;
  - the environment passed to `exec` holds neither `GOOGLE_APPLICATION_CREDENTIALS` nor any
    `GH_TOKEN*`;
  - `scripts/co-pypi.sh`, run with a fake `uv` first on `PATH` that dumps its environment,
    shows the same for the outer process;
  - the token is absent from stdout and stderr when the mint fails and when the `exec` fails.

### 2. Rename the variable: `REPLICATOR_CO_PYPI_CREDENTIALS`

**Decision: rename it.** Under item 3 nothing in the unit reads the variable any more. Only the
operator's deploy shell does. So the name describes a different mechanism and a different reader,
and "wheelhouse" names something that's being deleted. Kept, it would send the next reader looking
for a mirror that doesn't exist.

- **Cost:** one operator line in `/etc/replicator/.env`, `REPLICATOR_WHEELHOUSE_CREDENTIALS=` →
  `REPLICATOR_CO_PYPI_CREDENTIALS=`, same value. It's host step 1, and it must precede step 3
  (the wrapped sync), not the restart.
- **If it's missed,** the wrapper exits 2 and names the variable. The worker never reads it, so
  a miss can't affect a start.
- **No compatibility read of the old name.** That would be a second source to test and later
  retire, for a single edit.

### 3. The start step: option A, drop the refresh

**Decision: delete the wheelhouse `ExecStartPre` and add nothing in its place.**

- `ExecStart` is already `uv run --frozen --no-sync`. The unit's stated rule is that dependency
  sync is a deploy step, never a start side effect (`deploy/replicator.service:194`,
  DEPLOYMENT.md).
- Under A, a start needs no network and no token, which is what a cold boot behind a late tailnet
  wants (#88). The start's shared `TimeoutStartSec` budget also loses its only network step
  besides the floor check.
- **Why not option B** (Watcher's fail-closed `uv sync --locked --offline`):
  - it **changes the venv at start**: it rebuilds the project and removes extra packages. In the
    venv the service imports from, that's the side effect the unit rules out;
  - it adds lockfile and venv I/O to every restart, including `Restart=on-failure` loops during a
    broker outage, inside the budget #134's survival floor counts;
  - its benefit, failing closed on a venv that has drifted from the lock, is a deploy-discipline
    gap that exists today and that the refresh never covered either.
- A non-mutating check (`--check` without a project rebuild) is a separate improvement if
  wanted. It isn't needed for this change, and Watcher's finding shows `--check` is subtle.
- **The deploy line becomes `bash scripts/co-pypi.sh uv sync --frozen`.** That's the form every
  time, not only "when the cache lacks a file", because a rule that depends on uv's cache isn't
  one an operator can apply.
- **Measured before the report, under `unshare -rn`:** the unit's exact `ExecStart` argv, run
  against the relocked project, imports all three packages with the network cut. The new
  `pyproject.toml` must not make `--frozen --no-sync` reach for the index.
- **`tests/test_deploy.py` gains a test:** no `ExecStartPre` runs `uv` or names
  `sync_wheelhouse`/`co-pypi`, so a start stays network-free and token-free.

### 4. The closure diff's wheels: fetched from the index into a scratch directory

**Decision: a `wheels` verb on the same wrapper, plus a `--wheels DIR` flag that the closure diff
now requires.**

- `bash scripts/co-pypi.sh wheels OLD NEW DIR` mints the token as in item 1. It then reads each
  package's `/simple/<pkg>/` page and downloads the six `py3-none-any` wheels to `DIR`, three
  packages at each of the two versions.
- **Each file is verified against the index's `#sha256=` fragment** before it's renamed into
  place. A mismatch or a missing file exits 1 and leaves no partial file.
- **The token goes to `urllib`**, as an `Authorization` header on requests to the index host and
  nowhere else. A file URL on another host is refused. This is the one place the token reaches
  something other than `uv`. The bound is the same: a read-only role, one hour, one host.
- **Why not a uv-only route:** `uv pip install --target` gives an installed tree, not a wheel,
  and uv has no command that downloads wheel files. Making the closure diff read installed trees
  would be a larger change than a 60-line fetch.
- **`scripts/co_core_closure_diff.py` itself stays standard-library and token-free:**
  - `--wheelhouse` (default `.wheelhouse`) becomes `--wheels` with no default, since no
    directory exists to default to;
  - the missing-wheel hint names the `wheels` verb instead of `sync_wheelhouse.py`.
- **Shown here as a dry run, 0.40.1 → 0.41.0.** Its output is compared with the report #179
  pasted into `pyproject.toml`, which was made from the wheelhouse. Proving it on a real pin move
  is #184's first step.

### 5. The proof: lock = index = wheelhouse, per file

**Decision: a one-shot test in history, plus the table in the merge commit and the ship comment.
Watcher's shape.**

- **The relock** runs through the wrapper from an empty cache: `UV_CACHE_DIR=<scratch>`. It must
  move only the three co-core entries, each gaining `sdist` and `wheels` with `url` and `hash`.
  If any other package moves, I stop and report.
- **The relock commit adds `test_the_relock_matches_the_retired_wheelhouse`**, with the six
  0.41.0 digests as literals taken from `sha256sum` of the host's `.wheelhouse/` files. It
  asserts the lock's `hash` for each of the six. The next commit deletes the test, which leaves
  history a CI-green commit where both sources agreed.
- **The table** (file, lock sha256, `sha256sum`) goes in the merge commit message and the ship
  comment, generated by a scratch script that reads `uv.lock` and the host's files.
- **The host's `.wheelhouse/` files stay until one restart has passed** (Watcher's operator's
  choice). That's host step 6.
- **`.gitkeep` is deleted, but the ignore stays.** `.gitignore` keeps a plain `.wheelhouse/`
  line, and `.socraticodeignore` keeps its line, until the files go. Without them the deploy
  checkout's `git status` would list 748 untracked files, and SocratiCode would index them. The
  checkout guard ignores untracked files (`check_main_checkout.sh:174`), so this is noise, not
  an outage. Both lines go in a one-line follow-up commit after step 6.

## The rest of the surface

### `pyproject.toml` and `uv.lock`

`find-links` is replaced by:

```toml
[[tool.uv.index]]
name = "co-pypi"
url = "https://us-west1-python.pkg.dev/co-gcs/co-pypi/simple/"
explicit = true

[tool.uv.sources]
co-core = { index = "co-pypi" }
co-core-aio = { index = "co-pypi" }
co-core-sync = { index = "co-pypi" }
```

`tests/test_dependency_index.py` pins it:
- the URL, byte-exact;
- `explicit = true`;
- all three declared directly in `[project.dependencies]` with a source each;
- every co-core lock entry from that URL with a sha256 on every file;
- nothing else from the index;
- no locked version outside the pin, so 0.42.0 is excluded.

### CI (`.github/workflows/ci.yml`)

- **The four "Sync cannobserv wheelhouse" steps go:** `lint`, `test`, `browser`, `gcs`.
- **Each read-only WIF step gains `id: co_pypi`, `token_format: access_token` and
  `create_credentials_file: false`.** The reader's ADC then stops reaching later steps such as
  pytest and ruff. The `uv sync` step alone takes `UV_INDEX_CO_PYPI_USERNAME` and
  `UV_INDEX_CO_PYPI_PASSWORD` from `steps.co_pypi.outputs.access_token`, which the action masks.
- **`gcs`'s writer identity** still authenticates after the sync, unchanged.
- **"Assert wheelhouse auth is configured"** becomes "Assert co-pypi auth is configured". The
  `browser` job gains it, which settles #76's accepted limit instead of carrying it.
- **`tests/ci/test_workflow_pins.py`:**
  - the prune-premise check filters on the index URL, so it still bites ("every co-core package
    is locked to a wheel");
  - a new check: the token pair appears in exactly the `uv sync` steps' `env`, and in no
    `run:` text.
- **Bounds (`test_workflow_timeouts.py` and the `jobs:` comment).** They're re-derived from the
  branch's green runs and the merge run by the existing rule. The comment records the new
  medians and maxima and how many runs they come from.
  - On so few runs, a bound only moves down if the rule says so with the slow-apt allowance
    intact.
  - Otherwise the old bound stays, and the comment says why.

### The unit

`deploy/replicator.service` loses the wheelhouse `ExecStartPre` and its comment. That's a
`deploy/` copy, so a `cp`, a `daemon-reload` and a restart are owed. The host steps are below.

### Retired

- `scripts/sync_wheelhouse.py`
- `.wheelhouse/.gitkeep`
- the `wheelhouse-sync` manifest artifact (with `codebase_update`, same change)

Everything else that names the wheelhouse, rewritten:

| Area | Files |
|---|---|
| Docs | README, ARCHITECTURE, COMMANDS, ENVIRONMENT, DEPLOYMENT, TESTING, STYLE, `reference/tailscale.md` |
| Docstrings | `scripts/__init__.py`, `scripts/seed_fetch.py`, `src/worker/retention.py`, `tests/test_deploy.py`, `tests/test_destinations.py` |
| Comments | `.github/dependabot.yml` and `tests/ci/test_dependabot.py`: the reason only, since the `uv` updater stays parked under #169 |
| Curation config | `.skills/doc-sections`, `.skills/doc-sensitive-paths` |

Historical design docs under `docs/plans/` stay as written.

### Docs and budget

- **A new `docs/DEPENDENCIES.md`**, Watcher's name, takes the index (the wrapper, the key
  variable, the CI token, locking, the `wheels` verb). It also takes DEPLOYMENT.md's section
  "The co-core pin, and why the patch floor is load-bearing", moved, not copied.
  - DEPLOYMENT.md, at 9636, gets room back.
  - `tests/test_co_core_pin.py::test_deployment_md_names_the_pin` follows the section.
- **AGENTS.md's "co-core comes from the wheelhouse" paragraph and code block** (about 200
  tokens) become two lines pointing at DEPENDENCIES.md. A Detail Docs line is added. The net must
  be negative.
- **COMMANDS.md's setup block** becomes the wrapper's commands.
- **ENVIRONMENT.md's variable entry** is renamed and shortened. It's read by the deploy shell,
  never the service.
- **`measure-context.sh --exact --no-write`** runs before and after, and every doc must stay
  ≤ its budget.

## Host steps after the merge (restart owed)

1. **Operator:** in `/etc/replicator/.env`, rename `REPLICATOR_WHEELHOUSE_CREDENTIALS` to
   `REPLICATOR_CO_PYPI_CREDENTIALS`, keeping the value.
2. `sudo cp deploy/replicator.service /etc/systemd/system/ && sudo systemctl daemon-reload`.
3. `. scripts/load_env.sh && bash scripts/co-pypi.sh uv sync --frozen`. Expect only the three
   co-core packages to be reinstalled (the source moved, the bytes didn't).
4. `sudo systemctl restart replicator`.
5. **Read-back, from the body's "Done when":**
   - `worker ready` with all three features `enabled`;
   - `uv pip show co-core co-core-aio co-core-sync` → 0.41.0;
   - the replay is clean;
   - `diff deploy/replicator.service /etc/systemd/system/replicator.service` is empty.
6. After that restart, retire the host's `.wheelhouse/` files. A one-line commit then drops the
   `.gitignore` and `.socraticodeignore` lines.

## Verification (gates)

- **Default suite**, in a clean `env -i` shell and in a loaded one (`. scripts/load_env.sh`).
- **`tests/ci`**, **`-m integration`** and **`-m browser`** (the venv is rebuilt), and **ruff**.
- **CI green by job**, with no wheelhouse step and the token masked in the logs.
- **The docs** within budget.
- **Measured once:**
  - `uv run` with no token after the relock works with a warm cache, so the dev loop doesn't
    need the wrapper unless a co-core file is missing;
  - the `ExecStart` argv under `unshare -rn`;
  - the closure-diff dry run.
- Full gate output is saved to files.

## Accepted limits (proposed)

- **Every `uv lock` needs the wrapper,** even for a PyPI-only bump: without the token, an
  explicit index still returns 401 on resolve (Watcher measured this).
- **The token reaches whatever the wrapped `uv` spawns** (the `uv_build` backend). The allowlist
  bounds what else does; the token itself is bounded by a read-only role and an hour.
- **The `wheels` verb sends the token to `urllib`,** for one host.
- **A forgotten deploy sync runs the old venv.** That's today's behavior: option A doesn't close
  it, and option B would only close it by changing the venv at start.

## For #184

- A pin move is now:
  - the pin edit;
  - `bash scripts/co-pypi.sh uv lock`;
  - `bash scripts/co-pypi.sh wheels OLD NEW <scratch>`;
  - `uv run --no-project python scripts/co_core_closure_diff.py OLD NEW --wheels <scratch>`.
- No mirror sync, no `.whl`-only links, and no sdist churn: the index lock records sdists by
  design.
- The closure-diff path is first proven for real there.
