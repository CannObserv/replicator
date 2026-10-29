"""Pin ``.github/dependabot.yml``, the bot that proposes action bumps (#76).

Dependabot's config fails silently: a mistyped ecosystem, a bad directory or a
missing schedule stops producing PRs without erroring anywhere a person looks,
and a quiet bot reads like "no updates available". Ported from
CannObserv/archiver#192 (``c5a7a22``), assertions only:

* **``github-actions`` only.** ``uv`` waits on #76's blocker 2: co-core
  resolves only from the private wheelhouse, which Dependabot's updater cannot
  sync (CannObserv/cannobserv#554). ``skills-vendor/``'s deliberate submodule
  bumps stay out too. Any second block fails here.
* **``context-cadence.yml`` is excluded.** ``install-cadence.sh`` generates it,
  and a re-render reverts any bump made in it (gregoryfoster/skills#373). Its
  pin moves with the installer.
* **The open-PR limit outlasts a full batch.** Past the limit (default 5)
  Dependabot opens nothing and says so nowhere (CannObserv/notifier#103). One
  PR per action, so the bound is derived from the workflows, not transcribed.
* **Commits match AGENTS.md's no-issue form**, ``<type>: <description>``.

The pins it moves are ``test_workflow_pins.py``'s. Pure file reads.
"""

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / ".github" / "dependabot.yml"
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
GENERATED = ".github/workflows/context-cadence.yml"

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


def _uses(node: object) -> list[str]:
    """Every ``uses:`` value in a parsed workflow, at any depth."""
    if isinstance(node, dict):
        found = [node["uses"]] if isinstance(node.get("uses"), str) else []
        return found + [u for value in node.values() for u in _uses(value)]
    if isinstance(node, list):
        return [u for item in node for u in _uses(item)]
    return []


def _actions() -> set[str]:
    """The distinct ``owner/repo`` actions the scanned workflows pin: one PR each."""
    scanned = [
        path
        for path in sorted(WORKFLOWS.glob("*.y*ml"))
        if path.relative_to(REPO_ROOT).as_posix() != GENERATED
    ]
    refs = [u for path in scanned for u in _uses(yaml.safe_load(path.read_text("utf-8")))]
    return {m.group(1) for ref in refs if (m := _ACTION.match(ref))}


def test_config_exists():
    assert CONFIG.is_file(), (
        "no .github/dependabot.yml: nothing proposes the action bumps the "
        "workflows' SHA pins make invisible (#76)"
    )


def test_config_is_version_2():
    """Version 1 configs are dead; Dependabot ignores them without erroring."""
    assert _load().get("version") == 2


def test_only_github_actions_is_configured():
    ecosystems = [u.get("package-ecosystem") for u in _updates()]
    assert ecosystems == ["github-actions"], (
        f"ecosystems {ecosystems}: #76 ships github-actions alone. A `uv` block "
        "needs the private wheelhouse reachable from Dependabot's updater "
        "(CannObserv/cannobserv#554); gitsubmodule is its own decision"
    )


@pytest.fixture
def actions_block() -> dict:
    blocks = [u for u in _updates() if u.get("package-ecosystem") == "github-actions"]
    assert len(blocks) == 1, f"expected one github-actions block, found {len(blocks)}"
    return blocks[0]


def test_actions_block_covers_the_workflows(actions_block):
    """``/`` is the spelling that scans ``.github/workflows/``; any other is silence."""
    assert _directories(actions_block) == {"/"}


def test_actions_block_excludes_only_the_generated_workflow(actions_block):
    """A re-render of the cadence workflow reverts a bump (skills#373); nothing
    else is excluded, or its pins would go stale with nobody told."""
    assert actions_block.get("exclude-paths") == [GENERATED]
    assert (REPO_ROOT / GENERATED).is_file(), f"{GENERATED} moved: re-point the exclusion"


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


def test_commits_follow_the_repo_convention(actions_block):
    """The PR's commit reaches ``main`` as is, so it must read like ours."""
    prefix = actions_block.get("commit-message", {}).get("prefix")
    assert prefix in COMMIT_TYPES, (
        f"commit-message.prefix {prefix!r}: Dependabot's default 'Bump …' subject "
        f"matches none of AGENTS.md's types {sorted(COMMIT_TYPES)}"
    )
