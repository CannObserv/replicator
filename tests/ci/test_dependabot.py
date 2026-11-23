"""Pin ``.github/dependabot.yml``, the bot that proposes action bumps (#76)
and Python dependency bumps (#169).

Dependabot's config fails silently: a mistyped ecosystem, a bad directory or a
missing schedule stops producing PRs without erroring anywhere a person looks,
and a quiet bot reads like "no updates available". Ported from
CannObserv/archiver#192 (``c5a7a22``) and CannObserv/watcher's ``uv`` guards,
assertions only:

* **Two ecosystems, ``github-actions`` and ``uv``.** The ``uv`` updater resolves
  co-core from the co-pypi index (#183) over OIDC, through the org registry
  that admits this repository; ``pyproject.toml``'s index URL matches the
  registry's byte for byte, so there is no ``registries:`` key.
  ``skills-vendor/``'s deliberate submodule bumps stay out.
* **``context-cadence.yml`` is excluded.** ``install-cadence.sh`` generates it,
  and a re-render reverts any bump made in it (gregoryfoster/skills#373). Its
  pin moves with the installer.
* **Every index package and every exact pin is ignored by name, and nothing
  else is.** co-core moves by the pin-move children of epic #154, and a
  name-only ignore keeps Dependabot from asking PyPI, which it does before the
  index, about a name a squatter could take (the PyPI-first limit, accepted
  2026-11-22). Both sets are derived from ``pyproject.toml``, so a new one
  fails here until it's ignored. **One exception:** ``playwright``'s ``==`` pin
  moves with its headless shell, and the deploy's ``migrate`` hook installs that
  shell (#228), so the bot proposes it and CI's ``browser`` job proves it.
* **``increase-if-necessary``.** The default ``auto`` resolves to ``widen``,
  which uv can't do (dependabot-core#15290).
* **Dev tools are one grouped PR; runtime packages are one PR each.**
* **The open-PR limits outlast a full batch.** Past the limit (default 5)
  Dependabot opens nothing and says so nowhere (CannObserv/notifier#103). The
  bounds are derived (one PR per action from the workflows, one per runtime
  package not ignored plus one per group), never transcribed.
* **Commits match AGENTS.md's no-issue form**, ``<type>: <description>``.

The pins it moves are ``test_workflow_pins.py``'s. Pure file reads.
"""

import re
import tomllib
from fnmatch import fnmatchcase

import pytest
import yaml
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from tests.ci.test_workflow_pins import GENERATED, REPO_ROOT, _scanned, _uses

CONFIG = REPO_ROOT / ".github" / "dependabot.yml"
PYPROJECT = REPO_ROOT / "pyproject.toml"

ECOSYSTEMS = ["github-actions", "uv"]

# AGENTS.md → Conventions → Commit Messages.
COMMIT_TYPES = {"feat", "fix", "refactor", "docs", "test", "chore"}

# `owner/repo@ref` or `owner/repo/path@ref`: what the github-actions updater bumps.
_ACTION = re.compile(r"^([\w.-]+/[\w.-]+)(?:/[^@]*)?@.+$")


def _load() -> dict:
    """The parsed config, or an empty document while the file is missing.

    Tolerating absence keeps collection alive, so the failure surfaces as
    ``test_config_exists``'s message rather than a collection error.
    """
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8")) if CONFIG.is_file() else {}


def _updates() -> list[dict]:
    return _load().get("updates", [])


def _directories(block: dict) -> set[str]:
    """A block's directories, whichever of the two spellings it uses."""
    return set(block.get("directories", [])) | (
        {block["directory"]} if "directory" in block else set()
    )


def _actions() -> set[str]:
    """The distinct ``owner/repo`` actions the scanned workflows pin: one PR each."""
    refs = [u for path in _scanned() for u in _uses(yaml.safe_load(path.read_text("utf-8")))]
    return {m.group(1) for ref in refs if (m := _ACTION.match(ref))}


def test_config_exists():
    assert CONFIG.is_file(), (
        "no .github/dependabot.yml: nothing proposes the action bumps the "
        "workflows' SHA pins make invisible (#76)"
    )


def test_config_is_version_2():
    """Version 1 configs are dead; Dependabot ignores them without erroring."""
    assert _load().get("version") == 2


def test_exactly_the_two_ecosystems_are_configured():
    ecosystems = sorted(u.get("package-ecosystem") for u in _updates())
    assert ecosystems == ECOSYSTEMS, (
        f"ecosystems {ecosystems}: expected one block each for {ECOSYSTEMS} "
        "(#76's action bumps, #169's Python bumps); gitsubmodule is its own decision"
    )


def test_no_registries_key():
    """The org registry serves an admitted repository's jobs only when the index
    URL in ``pyproject.toml`` matches its own; a ``registries:`` entry here would
    be a second, repo-level credential path (cannobserv ``docs/DEPENDABOT.md``)."""
    assert "registries" not in _load()
    assert all("registries" not in u for u in _updates())


def _block(ecosystem: str) -> dict:
    blocks = [u for u in _updates() if u.get("package-ecosystem") == ecosystem]
    assert len(blocks) == 1, f"expected one {ecosystem} block, found {len(blocks)}"
    return blocks[0]


@pytest.fixture
def actions_block() -> dict:
    return _block("github-actions")


@pytest.fixture
def uv_block() -> dict:
    return _block("uv")


def test_actions_block_covers_the_workflows(actions_block):
    """``/`` is the spelling that scans ``.github/workflows/``; any other is silence."""
    assert _directories(actions_block) == {"/"}


def test_actions_block_excludes_only_the_generated_workflow(actions_block):
    """A re-render of the cadence workflow reverts a bump (skills#373); nothing
    else is excluded, or its pins would go stale with nobody told."""
    generated = GENERATED.relative_to(REPO_ROOT).as_posix()
    assert actions_block.get("exclude-paths") == [generated]
    assert GENERATED.is_file(), f"{generated} moved: re-point the exclusion"


def test_actions_block_runs_weekly(actions_block):
    """No interval is a config Dependabot rejects, visibly only in the repo's
    Dependabot tab, which nobody watches."""
    assert actions_block.get("schedule", {}).get("interval") == "weekly"


def test_open_pr_limit_outlasts_a_full_batch(actions_block):
    actions = _actions()
    assert actions, "found no `uses:` in .github/workflows/: the derivation broke"
    limit = actions_block.get("open-pull-requests-limit", 5)
    assert limit > len(actions), (
        f"limit {limit} can be filled by {len(actions)} action PRs ({sorted(actions)}); "
        "past it Dependabot opens nothing and says so nowhere. Raise "
        "open-pull-requests-limit"
    )


@pytest.mark.parametrize("ecosystem", ECOSYSTEMS)
def test_commits_follow_the_repo_convention(ecosystem):
    """The PR's commit reaches ``main`` as is, so it must read like ours."""
    prefix = _block(ecosystem).get("commit-message", {}).get("prefix")
    assert prefix in COMMIT_TYPES, (
        f"commit-message.prefix {prefix!r}: Dependabot's default 'Bump …' subject "
        f"matches none of AGENTS.md's types {sorted(COMMIT_TYPES)}"
    )


def _pyproject() -> dict:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def _runtime() -> list[Requirement]:
    """``[project].dependencies``: the runtime packages, one PR each."""
    return [Requirement(r) for r in _pyproject()["project"]["dependencies"]]


def _index_packages() -> set[str]:
    """The names ``[tool.uv.sources]`` binds to an index: co-core, from co-pypi."""
    sources = _pyproject().get("tool", {}).get("uv", {}).get("sources", {})
    return {canonicalize_name(n) for n, s in sources.items() if "index" in s}


def _exact_pins() -> set[str]:
    """Runtime names pinned ``==``: each moves by a deliberate change, never the bot."""
    return {
        canonicalize_name(r.name)
        for r in _runtime()
        if any(s.operator in ("==", "===") for s in r.specifier)
    }


def _ignored_names(block: dict) -> list[str]:
    """Every ignore entry's name pattern, narrowed entries included."""
    return [e.get("dependency-name", "") for e in block.get("ignore", [])]


def _is_ignored(name: str, block: dict) -> bool:
    """Whether a name-only entry matches: only that drops every version of it."""
    entries = block.get("ignore", [])
    patterns = [e.get("dependency-name", "") for e in entries if set(e) == {"dependency-name"}]
    return any(fnmatchcase(name, pattern) for pattern in patterns)


def test_uv_block_covers_the_project(uv_block):
    """``/`` holds ``pyproject.toml`` and ``uv.lock``; any other directory is silence."""
    assert _directories(uv_block) == {"/"}


def test_uv_block_runs_weekly(uv_block):
    assert uv_block.get("schedule", {}).get("interval") == "weekly"


def test_uv_strategy_is_not_the_unsupported_widen(uv_block):
    """The default ``auto`` resolves to ``widen``, which uv can't do
    (dependabot-core#15290): the run fails in a log nobody reads."""
    assert uv_block.get("versioning-strategy") == "increase-if-necessary"


def test_uv_dev_tools_are_one_grouped_pr(uv_block):
    """One group, for development dependencies only. Not a no-deploy marker:
    some dev packages are runtime transitives too (docs/DEPENDENCIES.md)."""
    groups = uv_block.get("groups", {})
    assert list(groups) == ["dev-tools"]
    assert groups["dev-tools"].get("dependency-type") == "development"
    assert set(groups["dev-tools"]) <= {"dependency-type", "exclude-patterns"}


def _group_requirements() -> list[Requirement]:
    """Every ``[dependency-groups]`` requirement: Dependabot's "development" type."""
    groups = _pyproject().get("dependency-groups", {}).values()
    return [Requirement(r) for group in groups for r in group if isinstance(r, str)]


def _dependabot_name(req: Requirement) -> str:
    """The name Dependabot's uv updater gives a requirement: extras included."""
    name = canonicalize_name(req.name)
    return f"{name}[{','.join(sorted(req.extras))}]" if req.extras else name


def _build_requirements() -> set[str]:
    """``[build-system].requires``: development to Dependabot, and in no lock."""
    requires = _pyproject().get("build-system", {}).get("requires", [])
    return {canonicalize_name(Requirement(r).name) for r in requires}


def test_dev_tools_group_holds_no_extras_requirement(uv_block):
    """Dependabot gives a requirement with extras no previous version, and a
    group change with one such member and one unchanged requirement is dropped
    whole, logged as "Nothing to update" (dependabot-core
    ``DependencyChange#all_have_previous_version?``; the first run, #169). On its
    own PR an extras requirement works, so exclude it from the group."""
    excluded = uv_block.get("groups", {}).get("dev-tools", {}).get("exclude-patterns", [])
    caught = [
        name
        for name in map(_dependabot_name, _group_requirements())
        if "[" in name
        and not _is_ignored(name, uv_block)
        and not any(fnmatchcase(name, pattern) for pattern in excluded)
    ]
    assert not caught, (
        f"{caught}: a dependency-group requirement with extras inside the dev-tools "
        "group drops the whole group PR; add `<name>*` to its exclude-patterns"
    )


def test_dev_tools_exclusions_are_not_stale(uv_block):
    excluded = uv_block.get("groups", {}).get("dev-tools", {}).get("exclude-patterns", [])
    names = [_dependabot_name(r) for r in _group_requirements()]
    stale = [p for p in excluded if not any(fnmatchcase(n, p) for n in names)]
    assert not stale, f"exclude-patterns {stale} match no dependency-group requirement"


def test_uv_ignores_every_build_requirement(uv_block):
    """A build requirement has no lock entry, so it would drop the dev-tools
    group (as above). ``uv_build``'s range is the host uv's minor, repeated in
    ci.yml's setup-uv inputs (test_workflow_pins.py): it moves by hand."""
    requires = _build_requirements()
    assert requires, "found no [build-system] requires: the derivation broke"
    missed = sorted(r for r in requires if not _is_ignored(r, uv_block))
    assert not missed, f"{missed}: build requirements not ignored by name"


def test_uv_ignores_every_index_package(uv_block):
    """Dependabot asks PyPI before the index; a name-only ignore stops the lookup."""
    packages = _index_packages()
    assert packages, "found no index-bound name in [tool.uv.sources]: the derivation broke"
    missed = sorted(p for p in packages if not _is_ignored(p, uv_block))
    assert not missed, (
        f"{missed}: bound to the co-pypi index but not ignored, so Dependabot would "
        "look it up on PyPI first and could propose a squatter's version (#169)"
    )


# Exact pins the deploy installs for, so a bot bump needs no hand step: each one's
# migrate hook line is held below.
DEPLOY_INSTALLED = {"playwright": '/.venv/bin/playwright" install chromium-headless-shell'}


def test_uv_ignores_every_exact_pin_the_deploy_does_not_install(uv_block):
    """An ``==`` pin is a decision a bot bump would undo, unless the deploy
    carries what the pin moves (playwright's headless shell, #228)."""
    missed = sorted(
        p for p in _exact_pins() - DEPLOY_INSTALLED.keys() if not _is_ignored(p, uv_block)
    )
    assert not missed, f"{missed}: pinned == in [project].dependencies but not ignored"


@pytest.mark.parametrize(("name", "install"), DEPLOY_INSTALLED.items())
def test_the_deploy_installs_what_each_unignored_pin_moves(uv_block, name, install):
    assert name in _exact_pins(), f"{name} is no longer pinned ==: drop it here"
    assert not _is_ignored(name, uv_block), f"{name} is ignored: the bot never proposes it"
    hooks = (REPO_ROOT / "scripts" / "deploy_hooks.sh").read_text()
    assert install in hooks.split("\nmigrate() {", 1)[-1], f"migrate runs no `{install}`"


def test_uv_ignores_are_name_only_and_none_is_stale(uv_block):
    """A narrowed entry (``versions``, ``update-types``) still lets the bot ask
    PyPI about the name; an entry matching nothing hides a package we never meant
    to (a typo, or a dependency since dropped). Majors flow as PRs (#169): close
    one with ``@dependabot ignore this major version`` to keep a cap."""
    entries = uv_block.get("ignore", [])
    narrowed = [e for e in entries if set(e) != {"dependency-name"}]
    assert not narrowed, f"narrowed ignore entries {narrowed}: make each name-only"
    wanted = _index_packages() | (_exact_pins() - DEPLOY_INSTALLED.keys()) | _build_requirements()
    stale = [n for n in _ignored_names(uv_block) if not any(fnmatchcase(w, n) for w in wanted)]
    assert not stale, (
        f"ignore entries {stale} match no index package, exact pin or build requirement"
    )


def test_uv_open_pr_limit_outlasts_a_full_batch(uv_block):
    """One PR per runtime package the bot may move, one per requirement the
    group excludes, plus one per group."""
    runtime = {canonicalize_name(r.name) for r in _runtime()}
    excluded = uv_block.get("groups", {}).get("dev-tools", {}).get("exclude-patterns", [])
    loose = {
        name
        for name in map(_dependabot_name, _group_requirements())
        if any(fnmatchcase(name, pattern) for pattern in excluded)
    }
    movable = sorted(n for n in runtime | loose if not _is_ignored(n, uv_block))
    assert movable, "found no runtime dependency to bump: the derivation broke"
    groups = len(uv_block.get("groups", {}))
    limit = uv_block.get("open-pull-requests-limit", 5)
    assert limit > len(movable) + groups, (
        f"limit {limit} can be filled by {len(movable)} single PRs ({movable}) and "
        f"{groups} group PRs; past it Dependabot opens nothing and says so nowhere. "
        "Raise open-pull-requests-limit"
    )
