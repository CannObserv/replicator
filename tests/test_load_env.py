"""`scripts/load_env.sh` parses the env files, and nothing tracked teaches sourcing them (#170).

Sourcing an env file runs it as shell code, and the repo `.env` is the
developer's file, holding org-wide PATs and `ANTHROPIC_API_KEY`
(gregoryfoster/skills#334 printed one into a transcript). The four documented
load lines sourced both files. They now source a tracked *script*, which is code,
and the script parses the two *files*, which are data.

The loader itself is not copied here: the script lifts `load_env` in place from
the vendored recipe in `shipping-work-python-fastapi`'s `pre-ship.sh` header, as
`init-project-fastapi`'s template does. Running the real script against that real
recipe is what keeps the two from drifting apart unseen.

Ported in shape from gregoryfoster/skills'
`tests/structural/test_init_fastapi_env_handling.py`, not in helpers.
"""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = "scripts/load_env.sh"
RECIPE = "skills/shipping-work-python-fastapi/scripts/pre-ship.sh"
THIS = "tests/test_load_env.py"

# Running an env file as shell code: `.` or `source` after a line start or a
# command separator. `>> ./.env`, `grep … .env` and `load_env .env` do not match.
_SOURCES_AN_ENV_FILE = re.compile(r"(?:^|[;&|(`\s])(?:\.|source)\s+\S*\.env\b")

_SCANNED_SUFFIXES = (".md", ".sh", ".py")
# Vendored text is upstream's to fix; a dated plan is the record of what was run.
_EXCLUDED_PREFIXES = ("skills-vendor/", "docs/plans/")
# Lines that name the patterns in order to refuse them, file by file.
_CARVE_OUTS = {
    # The OnFailure= handler's own guard: it loads no env file itself.
    "tests/test_notify_failure.py": {'    for loader in (". ", "source ", "set -a"):'},
}

# The documented load lines (#170), each now naming the script.
LOAD_SITES = ("AGENTS.md", "README.md", "docs/COMMANDS.md", "docs/DEPLOYMENT.md")


def _tracked_lines():
    """Every line of every tracked file in scope. `git ls-files` rather than a
    walk: `skills/*` and `.claude/hooks/*` are tracked symlinks into
    `skills-vendor/`, which a walk would follow past the prefix check."""
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO, capture_output=True, check=True
    ).stdout.decode()
    for name in sorted(filter(None, out.split("\0"))):
        path = REPO / name
        if (
            not name.endswith(_SCANNED_SUFFIXES)
            or name.startswith(_EXCLUDED_PREFIXES)
            or name == THIS
            or path.is_symlink()
            or not path.is_file()
        ):
            continue
        allowed = _CARVE_OUTS.get(name, set())
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if line not in allowed:
                yield name, n, line


class TestNothingTeachesSourcing:
    def test_no_set_a(self):
        hits = [f"{p}:{n}" for p, n, ln in _tracked_lines() if "set -a" in ln]
        assert not hits, (
            "`set -a` exports every variable a sourced file sets. Load env files "
            f"with `. {SCRIPT}`, which parses them. Sites: {hits}"
        )

    def test_no_env_file_is_sourced(self):
        hits = [f"{p}:{n}" for p, n, ln in _tracked_lines() if _SOURCES_AN_ENV_FILE.search(ln)]
        assert not hits, (
            f"an env file is data: load it with `. {SCRIPT}`, never run it with "
            f"`.` or `source`. Sites: {hits}"
        )

    @pytest.mark.parametrize(
        "line",
        [
            "set -a; . /etc/replicator/.env 2>/dev/null; . .env 2>/dev/null; set +a",
            ". .env",
            "source ~/app/.env",
            "(. /etc/replicator/.env)",
        ],
    )
    def test_the_sourcing_detector_fires(self, line):
        assert _SOURCES_AN_ENV_FILE.search(line), line

    @pytest.mark.parametrize(
        "line",
        [
            "cat >> ./.env <<ENV",
            "grep -q '^GH_TOKEN=' .env",
            "load_env /etc/replicator/.env; load_env .env",
            "1. **`/etc/replicator/.env`** — production config.",
            f". {SCRIPT}; . scripts/rcli.sh",
        ],
    )
    def test_the_sourcing_detector_spares_reads_and_writes(self, line):
        assert not _SOURCES_AN_ENV_FILE.search(line), line

    def test_each_carve_out_still_exists(self):
        """A carve-out whose line moved would exempt nothing and hide nothing,
        but it would also stop saying why that file once matched."""
        for name, lines in _CARVE_OUTS.items():
            text = (REPO / name).read_text().splitlines()
            assert lines <= set(text), f"{name}: carved-out line gone: {lines - set(text)}"

    @pytest.mark.parametrize("site", LOAD_SITES)
    def test_each_load_site_names_the_script(self, site):
        assert f". {SCRIPT}" in (REPO / site).read_text(), site


SENTINEL_KEY = "LOAD_ENV_SENTINEL_170"
ETC_ENV = Path("/etc/replicator/.env")


# Pinned locally, so a global commit.gpgsign or defaultBranch cannot move a case.
_GIT_CONFIG = ("user.name=t", "user.email=t@t", "commit.gpgsign=false", "init.defaultBranch=main")


def _git(cwd: Path, *args: str) -> None:
    pinned = [arg for kv in _GIT_CONFIG for arg in ("-c", kv)]
    subprocess.run(["git", *pinned, *args], cwd=cwd, check=True, capture_output=True)


def _checkout(tmp_path: Path, *, recipe: bool = True) -> Path:
    """A primary checkout carrying the script and, by default, the recipe at the
    path the script reads it from. `.env` stays untracked, as it is here."""
    main = tmp_path / "main"
    (main / "scripts").mkdir(parents=True)
    shutil.copy(REPO / SCRIPT, main / SCRIPT)
    if recipe:
        (main / RECIPE).parent.mkdir(parents=True)
        shutil.copy(REPO / RECIPE, main / RECIPE)
    (main / ".gitignore").write_text(".env\n")
    _git(main, "init", "-q")
    _git(main, "add", "-A")
    _git(main, "commit", "-q", "-m", "init")
    return main


def _worktree(main: Path) -> Path:
    wt = main.parent / "wt"
    _git(main, "worktree", "add", "-q", str(wt))
    return wt


def _source(checkout: Path, after: str = f'printf "%s" "${{{SENTINEL_KEY}-unset}}"'):
    """Source the script in a fresh bash, as an agent's tool call is: the
    caller's environment carries nothing of ours in."""
    return subprocess.run(
        ["bash", "-c", f". {SCRIPT}\nrc=$?\n{after}\nexit $rc"],
        cwd=checkout,
        env={"PATH": "/usr/bin:/bin", "HOME": str(checkout)},
        capture_output=True,
        text=True,
        timeout=60,
    )


def _names(env_file: Path) -> set[str]:
    """Key names only, never values: what /etc/replicator/.env may add here."""
    if not env_file.is_file():
        return set()
    pattern = re.compile(r"^\s*(?:export\s+)?([A-Za-z0-9_]+)=")
    return {m[1] for ln in env_file.read_text().splitlines() if (m := pattern.match(ln))}


class TestTheScriptParses:
    def test_it_loads_the_checkouts_env(self, tmp_path: Path):
        main = _checkout(tmp_path)
        (main / ".env").write_text(f"# dev\nexport {SENTINEL_KEY}='from main'\n")
        r = _source(main)
        assert r.returncode == 0, r.stderr
        assert r.stdout == "from main"

    def test_it_exports_what_it_loads(self, tmp_path: Path):
        """rcli and every `uv run` read the URL from the environment (rcli.sh)."""
        main = _checkout(tmp_path)
        (main / ".env").write_text(f"{SENTINEL_KEY}=exported\n")
        r = _source(main, f"bash -c 'printf %s \"${SENTINEL_KEY}\"'")
        assert r.stdout == "exported", r.stderr

    @pytest.mark.parametrize("value", ["$(touch PWNED)", "`touch PWNED`"])
    def test_a_value_is_data_not_code(self, tmp_path: Path, value: str):
        main = _checkout(tmp_path)
        (main / ".env").write_text(f"{SENTINEL_KEY}={value}\n")
        r = _source(main)
        assert r.returncode == 0, r.stderr
        assert not (main / "PWNED").exists(), "the env file was executed"
        assert r.stdout == value

    def test_a_worktree_falls_back_to_the_primarys_env(self, tmp_path: Path):
        """`.env` is git-ignored, so a linked worktree has none of its own."""
        main = _checkout(tmp_path)
        wt = _worktree(main)
        (main / ".env").write_text(f"{SENTINEL_KEY}=primary\n")
        assert not (wt / ".env").exists()
        r = _source(wt)
        assert r.returncode == 0, r.stderr
        assert r.stdout == "primary", "a linked worktree loaded nothing"

    def test_a_worktrees_own_env_wins(self, tmp_path: Path):
        main = _checkout(tmp_path)
        wt = _worktree(main)
        (main / ".env").write_text(f"{SENTINEL_KEY}=primary\n")
        (wt / ".env").write_text(f"{SENTINEL_KEY}=its-own\n")
        assert _source(wt).stdout == "its-own"

    def test_it_leaves_only_the_loaded_names_behind(self, tmp_path: Path):
        """Sourced into the caller's shell, so its own names must not stay
        there: not its locals, and not the recipe's `PRE_SHIP_ENV_NAMES`."""
        main = _checkout(tmp_path)
        (main / ".env").write_text(f"{SENTINEL_KEY}=x\n")
        script = (
            "true | true\n"  # bash sets PIPESTATUS at the first pipeline: not ours
            "compgen -v | sort > before\n"
            f". {SCRIPT}\n"
            "compgen -v | sort > after\n"
            "comm -13 before after\n"
        )
        r = subprocess.run(
            ["bash", "-c", script],
            cwd=main,
            env={"PATH": "/usr/bin:/bin"},
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert r.returncode == 0, r.stderr
        etc_names = _names(ETC_ENV)
        new = set(r.stdout.split()) - etc_names - {SENTINEL_KEY}
        assert not new, f"left behind in the caller's shell: {sorted(new)}"

    @pytest.mark.skipif(not _names(ETC_ENV), reason=f"{ETC_ENV} unreadable or empty here")
    def test_the_repo_env_overrides_etc(self, tmp_path: Path):
        """/etc first, then the checkout's `.env`, so the developer's file wins
        (CR 3). The path is fixed, so this runs where the production file is
        readable; it takes one key *name* from it, never a value."""
        key = sorted(_names(ETC_ENV))[0]
        main = _checkout(tmp_path)
        (main / ".env").write_text(f"{key}=the-developers\n")
        r = _source(main, f'[ "${key}" = the-developers ] && echo wins')
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "wins", f"/etc/replicator/.env's {key} won"

    def test_outside_git_it_never_reads_the_callers_cwd(self, tmp_path: Path):
        """No `.env` and no repository: the fallback's `rev-parse` fails, and
        `dirname ""` is `.`, which loaded the caller's cwd's `.env` (CR 2)."""
        tree = tmp_path / "tree"
        (tree / "scripts").mkdir(parents=True)
        shutil.copy(REPO / SCRIPT, tree / SCRIPT)
        (tree / RECIPE).parent.mkdir(parents=True)
        shutil.copy(REPO / RECIPE, tree / RECIPE)
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / ".env").write_text(f"{SENTINEL_KEY}=unrelated\n")
        r = subprocess.run(
            ["bash", "-c", f'. "{tree / SCRIPT}"\nprintf "%s" "${{{SENTINEL_KEY}-unset}}"'],
            cwd=elsewhere,
            env={"PATH": "/usr/bin:/bin", "GIT_CEILING_DIRECTORIES": str(tmp_path)},
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert r.returncode == 0, r.stderr
        assert r.stdout == "unset", "loaded an .env from the caller's cwd"
        assert r.stderr == "", r.stderr

    def test_cdpath_cannot_move_the_root(self, tmp_path: Path):
        """`dirname` gives a relative `scripts`, which `cd` resolves through
        CDPATH first: an interactive shell's CDPATH naming a directory with a
        `scripts/` in it moved the root there, and `cd` printed it (CR 1)."""
        main = _checkout(tmp_path)
        (main / ".env").write_text(f"{SENTINEL_KEY}=right-root\n")
        decoy = tmp_path / "decoy"
        (decoy / "scripts").mkdir(parents=True)
        r = subprocess.run(
            ["bash", "-c", f'. {SCRIPT}\nprintf "%s" "${{{SENTINEL_KEY}-unset}}"'],
            cwd=main,
            env={"PATH": "/usr/bin:/bin", "CDPATH": str(decoy)},
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert r.returncode == 0, r.stderr
        assert r.stdout == "right-root"

    def test_a_missing_recipe_fails_loudly_without_killing_the_shell(self, tmp_path: Path):
        """An uninitialised submodule leaves no recipe to lift. Loading nothing
        quietly would surface later as `rcli: REPLICATOR_REDIS_URL is unset`."""
        main = _checkout(tmp_path, recipe=False)
        (main / ".env").write_text(f"{SENTINEL_KEY}=x\n")
        r = _source(main, "echo still-here")
        assert r.returncode != 0
        assert "still-here" in r.stdout, "a sourced script must return, not exit"
        assert "submodule" in r.stderr, r.stderr
