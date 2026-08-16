"""Every vendored `.claude/hooks/` entry must stay a symlink into its skill, and
the refresh hook must stay registered in `.claude/settings.json`.

`managing-skills` Step 1 prescribes a symlink, not a copy, for one reason: a
copy freezes at whatever the script was the day it was installed and drifts
silently thereafter — no dangling link, no error, just a hook that stops
gaining upstream fixes. This repo's copy had missed the whole `.skills/doctor.sh`
commit path by the time it was noticed (#16). Nothing else catches a re-copy,
so it is pinned here rather than left to the next cohort sweep.

The registration is the *other* half of the same contract, and it is the half
that actually failed: the symlink was present and tracked for nine days while
`settings.json` never named it, so the hook never ran and the vendored skills
froze at one commit (#39). Listing `.claude/hooks/` showed a hook that was right
there and did nothing. A symlink-only test is green in exactly that state, which
is why the registration is pinned here too.
"""

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
HOOKS_DIR = ROOT / ".claude" / "hooks"
SETTINGS = ROOT / ".claude" / "settings.json"

# Every hook whose source of truth is a vendored skill, and must therefore be a
# symlink rather than a copy. `socraticode-reminder.sh` is deliberately absent:
# it is repo-authored — no vendored original exists — so a real file is correct
# there and asserting a link would be wrong.
#
# `socraticode-health.sh` is the one under active pressure. `init-socraticode`
# Phase 3 step C instructs a *copy*, contradicting `managing-skills`' symlink
# rule, so the next re-run of that skill will silently overwrite the link and
# restore the #16 drift with nothing failing (skills#179). That is precisely
# what this test is for.
VENDORED_HOOKS = ("skills-submodule-update.sh", "socraticode-health.sh")
INSTALLER = (
    ROOT
    / "skills-vendor"
    / "gregoryfoster-skills"
    / "skills"
    / "managing-skills"
    / "scripts"
    / "install-refresh.sh"
)


@pytest.mark.parametrize("name", VENDORED_HOOKS)
def test_the_vendored_hook_is_a_symlink_into_the_submodule(name):
    hook = HOOKS_DIR / name
    assert hook.is_symlink(), f"{name} is a copy; re-link it into skills-vendor/"

    target = Path(os.readlink(hook))
    assert not target.is_absolute(), "a relative target survives a clone to any path"
    assert target.parts[:2] == ("..", ".."), "the link is resolved from .claude/hooks/"
    assert "skills-vendor" in target.parts


@pytest.mark.parametrize("name", VENDORED_HOOKS)
def test_the_hook_the_symlink_points_at_actually_exists(name):
    """A populated submodule is the other half — a dangling link is a silent no-op."""
    hook = HOOKS_DIR / name
    assert hook.exists(), (
        "skills-vendor/ is not checked out: run .skills/doctor.sh locally, "
        "or add `submodules: true` to the CI job's actions/checkout (#27)"
    )
    assert os.access(hook, os.X_OK)


def test_the_hook_is_registered_in_settings_json():
    """Claude Code runs what settings.json names, not what .claude/hooks/ holds.

    Read directly rather than through the installer so the assertion still means
    something on a checkout where skills-vendor/ is unpopulated.
    """
    entries = json.loads(SETTINGS.read_text())["hooks"]["SessionStart"]
    commands = [h.get("command", "") for e in entries for h in e.get("hooks", [])]

    assert any("skills-submodule-update.sh" in c for c in commands), (
        "the auto-refresh hook is not registered in .claude/settings.json — the "
        "symlink alone never runs, which froze this repo's skills for nine days (#39). "
        f"Repair with: bash {INSTALLER.relative_to(ROOT)}"
    )


def test_the_installer_agrees_that_both_halves_are_present():
    """`--check` is the contract's own arbiter: exit 0 both present, 3 either missing.

    Belt-and-braces over the two tests above — it is the vendored definition of
    installed, so an upstream change to what that means reaches CI rather than
    waiting for the next cohort audit.
    """
    assert INSTALLER.exists(), (
        "skills-vendor/ is not checked out: run .skills/doctor.sh locally, "
        "or add `submodules: true` to the CI job's actions/checkout (#27)"
    )
    result = subprocess.run(
        ["bash", str(INSTALLER), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "install-refresh.sh --check reported a half-installed hook:\n"
        f"{result.stdout}{result.stderr}"
    )
