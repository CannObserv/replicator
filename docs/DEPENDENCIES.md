# Dependencies

Where co-core comes from, how to reach it, and its pin; then the `uv_build` bound,
CI's uv, and what Dependabot proposes. `AGENTS.md` keeps the co-core rule and
`[build-system]`'s comment keeps the bound's; the reasoning is here. The unit and the
deploy recipe are in [DEPLOYMENT.md](DEPLOYMENT.md); every variable is in
[ENVIRONMENT.md](ENVIRONMENT.md).

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

So every release build, a dev venv's sync and every relock go through the wrapper.
`scripts/deploy.sh` runs it as `UV_WRAPPER` (#151); neither it nor a shell needs an
env file for the key:

```bash
bash scripts/co-pypi.sh uv sync --frozen   # a dev venv
bash scripts/co-pypi.sh uv lock
```

**The wrapper is the cohort's, never edited here** (#201). `scripts/co-pypi.sh` and
`scripts/co_pypi_token.py` are deploying-releases' marked copies, installed with
`install.sh --co-pypi`; a change goes upstream, then a reinstall, and
`tests/test_co_pypi.py` fails on a hand edit. The minter mints an access token from
the read-only `co-pypi-reader@` key and `exec`s uv with `UV_INDEX_CO_PYPI_USERNAME` /
`_PASSWORD`. Its rules are tested in the skill's own suite:

- **One key, never the writer's.** `CO_PYPI_KEY` when set, else
  `/etc/<[project].name>/co-pypi-reader.json` — here `/etc/replicator/co-pypi-reader.json`,
  so nothing sets it. Never `GOOGLE_APPLICATION_CREDENTIALS`: on this host that is the
  worker's *writer* (#114).
- **An allowlisted environment.** The deploy shell carries the writer's key path
  and the repo `.env`'s org-wide PATs, and uv (with the `uv_build` backend it runs)
  starts with neither.
- **The token is never printed, logged, put in argv or written.** Anything but
  `uv` is refused before a mint.
- **It runs from the lock's `co-pypi` group** (`google-auth[requests]`), `--frozen`,
  so nothing in the process holding the key resolves at run time. The group is not a
  default one: neither a release nor CI syncs it. ruff excludes the minter, whose
  marker a `--fix` would break.

**CI never runs it.** Each job's read-only WIF step mints the token
(`token_format: access_token`, id `co_pypi`) and writes no credentials file; the
`uv sync` step alone receives it, masked (`tests/ci/test_workflow_pins.py`).

**Limits.** The token reaches what the wrapped uv spawns (the build backend, and
the wheel fetcher below), bounded by the reader's role and the token's hour. The
fetcher sends it to `urllib`, for the index host alone and across same-host
redirects only: the index answers a file with a 307.

## Moving the pin

1. Edit the three specifiers in `pyproject.toml`.
2. `bash scripts/co-pypi.sh uv lock`, and sync through it into a worktree's own
   venv, never the shared one the service imports (#114).
3. Diff the import closure ([below](#the-co-core-pin-and-why-the-patch-floor-is-load-bearing)):

   ```bash
   bash scripts/co-pypi.sh uv run --no-project python scripts/co_pypi_wheels.py \
     --dest /tmp/co-core-wheels --version OLD --version NEW co-core co-core-aio co-core-sync
   uv run --no-project python scripts/co_core_closure_diff.py OLD NEW --wheels /tmp/co-core-wheels
   ```

4. Paste the report into the pin comment and raise the floor by the rules below.

**The fetcher** (#201) exists because uv has no download command and leaves no `.whl`
behind [measured, uv 0.13.5]. It runs under the wrapper and reads the pair the wrapper
hands uv, so it holds no key of its own; packages and versions are arguments, the
index is `pyproject.toml`'s `co-pypi`. Each file is checked against the index's sha256
and renamed into place; a missing version or a mismatch exits 1 naming the file. It is
standard library only and names nothing of this repo's, built to be proposed to
deploying-releases beside the wrapper. The closure diff's real-wheel test reads
`REPLICATOR_TEST_CLOSURE_WHEELS`, a directory it filled at 0.36.0 and 0.37.0, and
skips without it, in CI too.

**Ask the index, not a cache.** The #77 review reported 0.13.1 "not mirrored" from a
local `ls` while the index already carried it. Anything local is a cache; the
fetcher or `uv lock` asks the index.

## The co-core pin, and why the patch floor is load-bearing

Pin the current minor — `>=0.53.1,<0.54`, on all three. The **patch** floor is load-bearing, not tidiness: the change-bus payloads are `extra="ignore"`, so on an older wheel a model constructed with fields it does not have yet succeeds and silently discards them. Raise the floor with every co-core feature the code starts depending on, or a version skew publishes facts that look right and carry nothing (#10). Both floors since fail *loudly* instead — a ValidationError at construction (0.8.0 requires `info_source_id` on all three fetch payloads, #19/#28) or an ImportError at load, never reaching a running worker (0.9.4 cuts the replicate contracts, #29; 0.19.3 ships the shared `BlobStore`, #114; 0.19.6 cuts the persist contract, #114 step 6).

**The 0.10 → 0.13 jump (#77) was the first departure from "pin the current minor", and what justified it is evidence rather than judgement.** The hazard that rule guards is payload fields moving under `extra="ignore"`; every module this service imports proved byte-identical between the two wheels — `pure/models/changes.py`, `pure/adapters/bus/{envelope,exceptions}.py`, `effects/{bus,fetch,gcs}.py`, `pure/util/{hashing,gcs}.py`, and all of `co_core_aio` bar its `__version__`. Only `streams.py` gained anything (`group_name`, `stream_kind` — cannobserv#384), and Replicator imports no `co_v1` adapter, which is where 0.13.2's other changes landed. That `diff -rq` of both wheels from the then-wheelhouse took a minute and turned a deferred change into a safe one.

**Diff the import closure before every pin move**, a single minor included: #136 needed it for one. The version distance is not the risk, the moved surface is, and the import lines are not the surface. At 0.19 → 0.36 (#140), `changes.py`'s import of `extract.canonical` brought in the `pure/extract` package, which had changed (cannobserv#523), and no import line in Replicator names it. `scripts/co_core_closure_diff.py OLD NEW` (#145) walks the closure from `src/` and `scripts/` through all three wheels at each version, fetched first ([Moving the pin](#moving-the-pin)). It prints the counts, a verdict per file that isn't identical, the Requires-Dist delta and the closure's third-party imports, and `--diff` adds each changed file's code diff. Paste the report into the pin comment and write the reasons around it by hand. `version-only` and `docstrings-only` need no reading. `changed` does, a model under `extra="ignore"` above all. `added` and `removed` are modules joining or leaving the closure. An **Unresolved imports** line names a module that is imported but isn't in the wheel, which fails at import. A **Not parsed** line names a file whose imports the walk couldn't follow. Each prints only when there's something to list. The walk follows `TYPE_CHECKING` imports and lists dynamic ones without following them. At 0.53.1 that's co-core-sync's two lazy `__getattr__` sites in `drivers/` and `drivers/blobstore/`. Both are expected, because each target is named again under `TYPE_CHECKING`. A new site whose targets aren't named there is a gap to read by hand. Flags: `--root`, `--package`, `--wheels` (required); the defaults are this repo's.

## The build backend's bound, and CI's uv

`[build-system]` requires `uv_build>=0.13.0,<0.14`: **one minor, the host's uv's.**
ci.yml's four `setup-uv` steps take the same range as `version:`, verbatim.
setup-uv installs the newest uv inside it (`resolution-strategy: highest`), so
patches arrive on their own and a minor needs a deliberate bump.
`tests/ci/test_workflow_pins.py` holds the five strings equal and the range to
one minor. Dependabot ignores `uv-build` (#169): a bot bump would
raise the floor in the first string alone.

**When to bump:** the host's `/usr/local/bin/uv`, which the operator installs, is
past the bound when the deploy sync prints
`build_system.requires = [...] does not contain the current uv version`. Read
uv's changelog for the new minor's `[tool.uv.build-backend]` notes (this repo
sets `module-name` and `module-root`), then move all five strings. `uv.lock`
doesn't record build requirements, so nothing relocks.

**What the warning costs: noise, not a fetch** [measured, #195]. Out of bound,
uv 0.13.5 still builds the editable install with its built-in backend: `-v`
logs `Performing direct build` before the warning, and makes no network request.
The bound sat unchanged from #1 until #195. It printed on every sync from uv 0.13's
release (CI, 2026-10-09) or the host's upgrade (2026-10-22), in the output a deploy
reads to see which packages moved.

**Rejected:**

- **`[tool.uv] required-version`.** setup-uv would read it, which leaves one
  string. But uv enforces it on every command, the unit's
  `uv run --frozen --no-sync` included (measured: exit 2), so a host uv upgrade
  would have refused service start. Since #151 no unit runs `uv`; the deploy's
  build still would refuse.
- **An exact CI pin.** Nothing moves `with.version`. Dependabot's
  `github-actions` updater rewrites the SHA only, and its `uv` updater (#169)
  reads `pyproject.toml` and `uv.lock`, not `ci.yml`.
- **Unpinned.** CI took each new minor the day it shipped, which is how the
  warning got into CI logs.

## Dependabot proposes the action and Python bumps

`.github/dependabot.yml` has two blocks, both weekly:

- **`github-actions`** (#76). Every `uses:` in `ci.yml` pins a commit SHA with a `# vX.Y.Z` comment, and Dependabot moves both. `context-cadence.yml` is excluded, because the installer re-renders it (gregoryfoster/skills#373).
- **`uv`** (#169). It proposes one PR per runtime package and one `dev-tools` group PR. `google-auth[requests]` gets its own PR, because an extras requirement inside a group drops the whole group without an error (#169). Under `increase-if-necessary` each PR raises the requirement's floor in `pyproject.toml` along with the lock. It resolves co-core from the co-pypi index over OIDC, through the org registry that admits this repository. It ignores `co-core*`, which moves by epic #154's pin-move children, and `uv-build`, which moves with the host's uv ([above](#the-build-backends-bound-and-cis-uv)). It proposes `playwright` too, although it is pinned `==`: the deploy installs each release's headless shell (#228, [DEPLOYMENT.md](DEPLOYMENT.md#the-headless-shell-228)), and CI's `browser` job proves the new revision. **Majors arrive as PRs.** To keep a cap, close the PR with `@dependabot ignore this major version`.

Guarded by `tests/ci/`. Merge a bot PR [like any PR](DEPLOYMENT.md#main-takes-pull-requests-208), then `git pull --ff-only` before the next local commit. If a bot PR is red at the WIF auth step, suspect the Dependabot run's OIDC token before the bump (the verdict table on #76). Read the release notes, not the diff: setup-uv v9 flipped `prune-cache` silently. A bump that changes behaviour takes fix commits pushed onto its branch.

**Whether a bot PR owes a deploy:**

- **An action PR** touches only `.github/`, so it needs no deploy and no restart.
- **A `uv` PR owes one only if it changes what a release installs.** `deploy.sh` builds with `uv sync --locked --no-dev`. Some dev packages are also runtime transitives: `anyio`, `httpx` and `pyyaml` from `dev`, and `google-auth` from `co-pypi`. So the group a PR comes from can't tell you, and neither can whether a `[project].dependencies` line moved. A lock-only security PR can move a runtime transitive too.

Diff the two install sets. It needs no index token and no checkout of the PR:

```bash
release_set() {  # what a release of <rev> installs
  local d; d=$(mktemp -d)
  git show "$1:pyproject.toml" >"$d/pyproject.toml" && git show "$1:uv.lock" >"$d/uv.lock" &&
    (cd "$d" && uv export --frozen --no-dev --no-emit-project --no-hashes --no-header -q)
  rm -rf "$d"
}
git fetch -q origin pull/<n>/head
diff <(release_set origin/main) <(release_set FETCH_HEAD) && echo "no deploy owed"
```

If any line differs, merge the PR, then deploy the merge commit with `scripts/deploy.sh`, which restarts the worker.
