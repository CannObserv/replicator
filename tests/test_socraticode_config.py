"""The SocratiCode client contract: which store this repo addresses, and where the key isn't (#92).

Adopting the cohort's shared Qdrant on `co-index` moved this repo from a store
it hosted to one it is a *client* of. Six variables in `.claude/settings.json`
and a `projectId` in `.socraticode.json` are the whole of that client
configuration, and every way it goes wrong is silent:

- A wrong `QDRANT_URL` reads as a network fault, because the fallback built
  from `QDRANT_HOST` uses `QDRANT_PORT`, whose default is 16333, not 6333.
- `QDRANT_COLLECTION_PREFIX` is prepended to the store-wide
  `socraticode_metadata` collection too, so one client setting it splits the
  cohort's namespace for all of them.
- A `projectId` that drifts renames this repo's collections and orphans the
  old set under a name nothing maps back to a repo.
- An absolute `linkedProjects` entry encodes one host's layout into a file
  every host reads.

None of these fails loudly. `codebase_health` calls all of them green, and a
`codebase_search` that answers from the wrong collection returns well-formed
results with real paths. So the configuration is pinned here rather than left
to a tool that cannot tell it is wrong.

**The key is the other half, and it is the half with teeth.** Qdrant holds a
single global `service.api_key`: no key list, no per-client identity, no
per-collection scope. Every cohort VM holds the same secret and a leak anywhere
is a rotation everywhere, with no overlap window. `CannObserv/broker` — a
*public* repo — carried no ignore rule for `.claude/settings.local.json` while
four of the five cohort repos did, which is exactly what made the assertion
"it's ignored" read as true (notifier#68, broker#18). This file asserts the
rule instead of reading it, by asking git.

Precedent for pinning a non-Python artifact from pytest: `test_deploy.py` (the
systemd unit), `test_ci.py` (workflow concurrency), `test_skills_hook.py` (the
refresh hook), `test_doc_sensitive_paths.py` (a gate's configuration).
"""

import json
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import pytest

from tests.test_doc_sensitive_paths import read_tracked_files

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG = REPO_ROOT / ".socraticode.json"
SETTINGS = REPO_ROOT / ".claude" / "settings.json"
LOCAL_SETTINGS = REPO_ROOT / ".claude" / "settings.local.json"
LOCAL_SETTINGS_REL = ".claude/settings.local.json"

# What every other client of the shared store uses. A collection holds vectors
# from one model at one dimension, so these are not this repo's choice to make
# — they are the store's, and disagreeing with them produces a dimension
# mismatch at index time rather than a wrong answer at query time.
EXPECTED_ENV = {
    "QDRANT_MODE": "external",
    "QDRANT_URL": "https://index.taild0fb76.ts.net:6333",
    "OLLAMA_MODE": "external",
    "OLLAMA_URL": "http://index:11434",
    "EMBEDDING_MODEL": "nomic-embed-text",
    "EMBEDDING_DIMENSIONS": "768",
}

# Upstream throws on a project id outside this class rather than sanitizing it.
PROJECT_ID = re.compile(r"^[A-Za-z0-9_-]+$")

# `QDRANT_API_KEY` bound to something that could be a key. Both carriers the
# cohort actually uses are covered: a JSON member (`"QDRANT_API_KEY": "…"`) and
# a shell assignment (`QDRANT_API_KEY=…`).
KEY_ASSIGNMENT = re.compile(r"""QDRANT_API_KEY["']?\s*[:=]\s*["']?[A-Za-z0-9_\-+/=]{20,}""")

# Settings keys that must never appear in the *tracked* file. `QDRANT_API_KEY`
# is the secret; the other three are the namespace-splitters of trap 5 and the
# host-form URL of trap 3, none of which has a correct value here.
FORBIDDEN_TRACKED_ENV = (
    "QDRANT_API_KEY",
    "QDRANT_COLLECTION_PREFIX",
    "QDRANT_HOST",
    "QDRANT_PORT",
)

# The same splitters, asked of the git-ignored file — which the server reads
# *before* the tracked one. `QDRANT_API_KEY` is deliberately absent: that file
# is where the key belongs.
FORBIDDEN_LOCAL_ENV = (
    "QDRANT_COLLECTION_PREFIX",
    "QDRANT_HOST",
    "QDRANT_PORT",
)


@pytest.fixture(scope="module")
def config() -> dict:
    assert CONFIG.exists(), f"{CONFIG.name} is missing — this repo addresses the store by path hash"
    return json.loads(CONFIG.read_text())


@pytest.fixture(scope="module")
def settings_env() -> dict:
    return json.loads(SETTINGS.read_text()).get("env", {})


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False
    )


def _read_text_lines(path: Path) -> list[str]:
    """Lines of a tracked file, with binary blobs skipped rather than decoded."""
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except (UnicodeDecodeError, OSError):
        return []


class TestProjectId:
    """`projectId` names this repo's collections in a store five repos share."""

    def test_project_id_is_the_repo_name(self, config: dict) -> None:
        """`codebase_replicator`, not `codebase_<12 hex>`.

        With no `projectId` the id is `sha256(<absolute path>)[:12]`, which is
        not even per-host: two VMs checking the repo out at the same path
        resolve to the same hash. The committed name is what makes every
        checkout — worktrees included — address one collection set.
        """
        assert config["projectId"] == "replicator"

    def test_project_id_is_a_legal_collection_name(self, config: dict) -> None:
        assert PROJECT_ID.match(config["projectId"]), (
            "upstream throws on characters outside [A-Za-z0-9_-] rather than sanitizing them"
        )


class TestLinkedProjects:
    """Cross-repo search, and the two ways its configuration encodes one host."""

    def test_links_the_four_cohort_siblings(self, config: dict) -> None:
        assert sorted(config["linkedProjects"]) == [
            "../archiver",
            "../broker",
            "../notifier",
            "../watcher",
        ]

    def test_every_link_is_relative_to_the_repo_root(self, config: dict) -> None:
        """An absolute entry names one VM's layout in a file every VM reads.

        The entry resolves against wherever the repo is checked out, so
        `../archiver` is correct on the deployment checkout, in a worktree, and
        on a laptop. `/home/exedev/archiver` is correct on exactly one of them
        and silently resolves to nothing on the others — and a link that does
        not resolve is dropped by the server without a word (skills#287).
        """
        for entry in config["linkedProjects"]:
            assert entry.startswith("../"), f"{entry!r} is not relative to the repo root"

    def test_does_not_link_itself(self, config: dict) -> None:
        """`../replicator` would query this repo's own collection twice."""
        assert "../replicator" not in config["linkedProjects"]


class TestStoreConfig:
    """The six variables that point the server at `co-index` instead of Docker."""

    @pytest.mark.parametrize(("name", "value"), sorted(EXPECTED_ENV.items()))
    def test_client_variable(self, settings_env: dict, name: str, value: str) -> None:
        assert settings_env.get(name) == value

    def test_qdrant_url_is_the_full_magicdns_name(self, settings_env: dict) -> None:
        """The short name is not in the certificate's SAN.

        Qdrant serves TLS *because* upstream refuses to send `QDRANT_API_KEY`
        over a non-TLS, non-loopback connection — which is the whole reason the
        hostname has to satisfy a certificate at all. `https://index:6333`
        reaches the same machine and fails verification 20 s in (notifier#57
        D14).
        """
        url = urlparse(settings_env["QDRANT_URL"])
        assert url.scheme == "https"
        assert url.hostname == "index.taild0fb76.ts.net"
        assert url.port == 6333, (
            "16333 is QDRANT_PORT's default, and the mistake reads as a network fault"
        )
        assert url.path in ("", "/"), (
            "the server's Qdrant client drops a path rather than honouring it"
        )

    def test_embedding_dimensions_match_the_model(self, settings_env: dict) -> None:
        """`nomic-embed-text` is 768-dimensional; the store's collections are too."""
        assert settings_env["EMBEDDING_MODEL"] == "nomic-embed-text"
        assert settings_env["EMBEDDING_DIMENSIONS"] == "768"

    @pytest.mark.parametrize("name", FORBIDDEN_TRACKED_ENV)
    def test_tracked_settings_carry_no_secret_and_no_namespace_splitter(
        self, settings_env: dict, name: str
    ) -> None:
        assert name not in settings_env

    def test_branch_awareness_is_not_enabled(self, settings_env: dict) -> None:
        """A fresh collection set per branch, which every health check calls green.

        `SOCRATICODE_BRANCH_AWARE` suffixes the branch name onto a path-hash
        id. A `projectId` ignores it today — so this pins the *pairing*, not a
        live failure: the variable becomes load-bearing again the moment
        `.socraticode.json` is removed, and the two changes would arrive
        separately.
        """
        assert settings_env.get("SOCRATICODE_BRANCH_AWARE") in (None, "false")


class TestKeyIsNotCommittable:
    """The single global key, and the rule that keeps it out of a public repo."""

    def test_local_settings_are_ignored_by_a_rule_this_repo_tracks(self) -> None:
        """Two questions, and only `-q` answers the first one.

        **`-v` is not a test of whether the file is ignored.** It reports the
        *last matching* rule and exits 0 even when that rule is a **negation**,
        so `.claude/*` followed by `!.claude/*.json` prints
        `.gitignore:2:!.claude/*.json` and exits 0 while `git add -A` stages the
        file. Measured against a scratch repository, not assumed. `-q` is the
        verdict — exit 0 ignored, 1 not — and it is checked first.

        `-v` then answers the *second* question, which is where the rule lives.
        A global `core.excludesfile` or `.git/info/exclude` protects one
        machine, not the repo, and a reader who sees only "ignored" cannot tell
        the two apart. The pattern is checked for a leading `!` as well: a
        belt-and-braces guard, since `-q` already refuses that state, and it
        makes this test fail with the reason rather than with a bare exit code.
        """
        verdict = _git("check-ignore", "-q", "--", LOCAL_SETTINGS_REL)
        assert verdict.returncode == 0, (
            f"{LOCAL_SETTINGS_REL} is not ignored — the cohort's Qdrant key is one "
            "`git add -A` from GitHub (notifier#68)"
        )

        described = _git("check-ignore", "-v", "--", LOCAL_SETTINGS_REL)
        source, _, pattern = described.stdout.split("\t", 1)[0].split(":", 2)
        assert source == ".gitignore", (
            f"the rule lives in {source!r}, which protects this machine rather than every clone"
        )
        assert not pattern.startswith("!"), (
            f"the matching rule {pattern!r} is a negation — it re-includes the file"
        )

    def test_local_settings_are_not_tracked(self) -> None:
        """`git check-ignore` reports a tracked path as not ignored, and no rule untracks it."""
        result = _git("ls-files", "--error-unmatch", "--", LOCAL_SETTINGS_REL)
        assert result.returncode != 0, (
            f"{LOCAL_SETTINGS_REL} is tracked — `git rm --cached` it and audit its history"
        )

    def test_no_tracked_file_assigns_the_key(self) -> None:
        """The name may be documented; a key-shaped value beside it may not.

        Only tracked content is searched, which is the population that matters:
        an untracked file holding the key is the design, not the leak.

        The value arm is a length class rather than "anything", because the
        docs have to be able to show the shape (`"QDRANT_API_KEY": "<the
        key>"`) without the guard reading a placeholder as a secret. A real key
        is 64 characters; twenty is far below that and far above every
        placeholder here.

        Matched in Python, not handed to `git grep -E`: git's POSIX ERE reads
        `\\"` inside a bracket expression as a literal *backslash*, so the
        obvious pattern gained a third member and matched the `\\n` in this
        test's own failure message.
        """
        offenders = [
            f"{path}:{number}"
            for path in read_tracked_files()
            if path and (REPO_ROOT / path).is_file()
            for number, line in enumerate(_read_text_lines(REPO_ROOT / path), start=1)
            if KEY_ASSIGNMENT.search(line)
        ]
        assert not offenders, f"a tracked file assigns a key-shaped QDRANT_API_KEY: {offenders}"


class TestLocalSettingsCarryOnlyTheKey:
    """The git-ignored file outranks the tracked one, and nothing else checks it."""

    @pytest.mark.parametrize("name", FORBIDDEN_LOCAL_ENV)
    def test_no_namespace_splitter_in_local_settings(self, name: str) -> None:
        """A prefix here splits the namespace for all five repos, silently.

        Resolution order is the process environment, then
        `.claude/settings.local.json`, then `.claude/settings.json`, then user
        settings — so this file **outranks** the one `TestStoreConfig` pins, and
        a value set here wins. `QDRANT_COLLECTION_PREFIX` is the dangerous one:
        it is prepended to the store-wide `socraticode_metadata` collection as
        well as to this project's, so one machine setting it splits the cohort's
        namespace for every other client, and every health check still reports
        green.

        Skipped where the file does not exist, which is every CI checkout: this
        guards the developer VM, which is the only place the file lives.
        """
        if not LOCAL_SETTINGS.exists():
            pytest.skip("no .claude/settings.local.json on this host")
        env = json.loads(LOCAL_SETTINGS.read_text()).get("env", {})
        assert name not in env, (
            f"{name} in {LOCAL_SETTINGS_REL} overrides the tracked settings and is "
            "invisible to every other check"
        )

    def test_branch_awareness_is_not_enabled_locally(self) -> None:
        if not LOCAL_SETTINGS.exists():
            pytest.skip("no .claude/settings.local.json on this host")
        env = json.loads(LOCAL_SETTINGS.read_text()).get("env", {})
        assert env.get("SOCRATICODE_BRANCH_AWARE") in (None, "false")


class TestLinkedProjectsHaveOneSource:
    """The pre-#287 variable is retired, not left as a second copy."""

    def test_local_settings_do_not_also_list_linked_projects(self) -> None:
        """Upstream reads both sources and de-duplicates by *resolved* path.

        So the committed file is correct the moment it is written, and the
        variable is not wrong — it is a second copy, in a git-ignored file, of
        a list every other cohort repo now keeps in a tracked one. It drifts
        the way any second copy does, and nothing compares them.

        Skipped where the file does not exist, which is every CI checkout: this
        guards the developer VM the variable was written on.
        """
        if not LOCAL_SETTINGS.exists():
            pytest.skip("no .claude/settings.local.json on this host")
        env = json.loads(LOCAL_SETTINGS.read_text()).get("env", {})
        assert "SOCRATICODE_LINKED_PROJECTS" not in env, (
            "linkedProjects in .socraticode.json is the one source (skills#287) — "
            "delete this key and keep the rest of the env block"
        )


# A package spec this repo is willing to launch. `@latest` and any range let
# npx resolve a version nobody chose, at session start, on this VM. A
# prerelease is admitted — `1.15.0-rc.1` is still exact, so it resolves from
# cache without an install, which is the property being pinned. A range is not.
PINNED_SPEC = re.compile(r"^socraticode@\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")

# The SessionStart entry that runs the daily health check, found by its marker.
HEALTH_HOOK = "socraticode-health"
HEALTH_SCRIPT = REPO_ROOT / ".claude" / "hooks" / f"{HEALTH_HOOK}.sh"

# What `install-hook.sh` writes for this hook, argument for argument from the
# vendored `socraticode-health.install`. The cap is deliberately not one of its
# arguments (skills#330): the installer rebuilds the command on every run and
# keeps only the timeout (skills#259), so a cap spelled here would not survive
# the next `init-socraticode` Step C.
HEALTH_COMMAND = f'bash "${{CLAUDE_PROJECT_DIR:-.}}/.claude/hooks/{HEALTH_HOOK}.sh" # {HEALTH_HOOK}'
HEALTH_TIMEOUT_S = 120

# The hook's default cap as its one assignment spells it, and the capped call
# itself — the only place `choom` is handed `--` and a command. The --help text
# and a log line name the same properties, so a bare substring check stays
# green after the cap is gone.
CAP_DEFAULT = re.compile(r'^CAP_DEFAULT="([^"]*)"', re.MULTILINE)
CHOOM_CALL = "choom -n 500 -- "

# The manual invocations' ceiling, one `-p` property per flag.
COMMANDS = REPO_ROOT / "docs" / "COMMANDS.md"
MANUAL_CAP = re.compile(r"systemd-run --user --scope((?: -p \S+)+)")


def _session_start_hooks() -> list[dict]:
    """Every SessionStart hook entry in the tracked settings file."""
    hooks = json.loads(SETTINGS.read_text()).get("hooks", {}).get("SessionStart", [])
    return [entry for group in hooks for entry in group.get("hooks", [])]


class TestServerLaunchCost:
    """What this repo is willing to spend to start a SocratiCode server (#99).

    Two launch paths exist here and they resolve the server differently: the
    plugin's MCP manifest spawns `npx -y --prefer-online <spec>`, and the
    SessionStart health hook runs `mcp-driver.mjs`. Both start a server on the
    VM that also runs `replicator.service`, and the expensive case is not the
    server — it is the *install* it does first. Measured on `CannObserv/broker`:
    75 MB pinned, 129 MB from a warm npx cache, 1.2 G for a cold install, where
    all 126 `MemoryHigh` throttle events landed.

    Neither cost is visible from a green health check, and the failure they
    produce is not an OOM kill. On 2026-09-16 broker's kernel failed *atomic*
    allocations in `tailscaled` and `ksoftirqd`, nothing was killed, and the bus
    was down 57m48s. Agent sessions inherit `oom_score_adj` -1000 from
    `exe-init`, so the killer cannot choose one and takes the service instead.

    So both paths are pinned here rather than left to resolve at launch.
    """

    def test_the_plugin_spec_is_a_literal_version(self, settings_env: dict) -> None:
        """`SOCRATICODE_SPEC` is the only lever over the plugin's own launch.

        The plugin's manifest spawns `${SOCRATICODE_SPEC:-socraticode@latest}`,
        and Claude Code cannot override a plugin's MCP *command* — so this
        variable is the whole of the control this repo has over it. Unset, the
        plugin resolves `@latest` against the registry at every session start,
        and `--prefer-online` means a warm cache is not a warm path on any day
        the package moved.
        """
        spec = settings_env.get("SOCRATICODE_SPEC")
        assert spec is not None, (
            "SOCRATICODE_SPEC is unset — the plugin will resolve socraticode@latest "
            "at every session start, installing on any day the package moved"
        )
        assert PINNED_SPEC.match(spec), (
            f"SOCRATICODE_SPEC is {spec!r} — pin a literal version, since only an "
            "exact version is already in the npx cache and resolves without an install"
        )

    def test_the_health_hook_caps_its_own_launch(self) -> None:
        """The one launch that happens unattended is the one that must be bounded.

        `docs/COMMANDS.md` caps the *manual* driver invocations, but this hook
        runs from SessionStart once per UTC day with nobody watching, which is
        exactly the shape of #94 — an uncapped launch cost this cluster 58
        minutes of bus. From #99 a wrapper in `settings.json` bounded it; since
        skills#330 the vendored hook opens that scope itself, so the cap is
        read here from the script `.claude/hooks/` resolves to.

        `choom` is part of the contract, not decoration: a session process sits
        at `oom_score_adj` -1000, where a cgroup cap *stalls* the process rather
        than killing it. How the hook probes and falls back is upstream's to
        pin (`tests/structural/test_health_hook_cap.py`); this asserts only that
        the build vendored here has a cap at all.
        """
        assert HEALTH_SCRIPT.exists(), (
            "skills-vendor/ is not checked out: run .skills/doctor.sh locally, "
            "or add `submodules: true` to the CI job's actions/checkout (#27)"
        )
        script = HEALTH_SCRIPT.read_text()
        default = CAP_DEFAULT.search(script)
        assert default and "MemoryMax=" in default.group(1), (
            f"the vendored {HEALTH_HOOK}.sh launches a SocratiCode server uncapped — "
            "bump skills-vendor/gregoryfoster-skills to gregoryfoster/skills@32128c9 "
            "or later (skills#330)"
        )
        assert CHOOM_CALL in script, (
            f"the vendored {HEALTH_HOOK}.sh is capped but not re-scored — at "
            "oom_score_adj -1000 a cgroup cap stalls the process instead of killing it"
        )

    def test_the_hook_cap_is_the_manual_ceiling(self) -> None:
        """One ceiling for every SocratiCode launch on this host.

        `docs/SKILLS.md` says the hook runs at `docs/COMMANDS.md`'s ceiling, and
        the manual invocations were sized on this VM (#99). An upstream change to
        the hook's default would otherwise leave the two quietly different.
        """
        default = CAP_DEFAULT.search(HEALTH_SCRIPT.read_text())
        assert default, f"the vendored {HEALTH_HOOK}.sh no longer assigns CAP_DEFAULT"
        hook = frozenset(default.group(1).split())
        calls = MANUAL_CAP.finditer(COMMANDS.read_text())
        manual = {frozenset(m.group(1).split()[1::2]) for m in calls}
        assert manual == {hook}, (
            f"docs/COMMANDS.md caps at {sorted(map(sorted, manual))}, the hook at "
            f"{sorted(hook)} — re-size one, or set SOCRATICODE_HEALTH_CAP in "
            ".claude/settings.local.json"
        )

    def test_the_health_hook_is_registered_as_the_installer_writes_it(self) -> None:
        """No wrapper: the hook's own scope is the cap, and a wrapper is not one.

        `systemd-run --scope` starts its own unit rather than nesting in the
        caller's, so a wrapper's scope ends up holding only the hook's shell
        while the driver runs under the hook's cap. Kept, it reads as a second
        ceiling that bounds nothing — and the installer erases it on its next
        run regardless (skills#259). A ceiling this host needs goes in
        `SOCRATICODE_HEALTH_CAP`; see the test below for where.
        """
        health = [h for h in _session_start_hooks() if HEALTH_HOOK in h.get("command", "")]
        assert len(health) == 1, f"expected one SessionStart entry for {HEALTH_HOOK}: {health}"
        assert health[0].get("command") == HEALTH_COMMAND, (
            f"{HEALTH_HOOK} is not registered as install-hook.sh writes it: "
            f"{health[0].get('command')!r} — the vendored hook caps itself (skills#330)"
        )
        assert health[0].get("timeout") == HEALTH_TIMEOUT_S

    def test_the_cap_ceiling_is_left_to_the_host(self, settings_env: dict) -> None:
        """`SOCRATICODE_HEALTH_CAP` belongs in `settings.local.json`, not here.

        A ceiling is a property of one machine's memory, and `off` in the
        tracked file would uncap every checkout — this VM included — while the
        hook logs the reason only to a file nobody reads when the day is clean.
        """
        assert "SOCRATICODE_HEALTH_CAP" not in settings_env, (
            "SOCRATICODE_HEALTH_CAP is set in the tracked .claude/settings.json — "
            "move it to .claude/settings.local.json's env block (skills#330)"
        )
