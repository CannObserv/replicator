"""The SessionStart skills hook must stay a symlink into the vendored skill.

`managing-skills` Step 1 prescribes a symlink, not a copy, for one reason: a
copy freezes at whatever the script was the day it was installed and drifts
silently thereafter — no dangling link, no error, just a hook that stops
gaining upstream fixes. This repo's copy had missed the whole `.skills/doctor.sh`
commit path by the time it was noticed (#16). Nothing else catches a re-copy,
so it is pinned here rather than left to the next cohort sweep.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HOOK = ROOT / ".claude" / "hooks" / "skills-submodule-update.sh"


def test_the_session_start_hook_is_a_symlink_into_the_submodule():
    assert HOOK.is_symlink(), f"{HOOK.name} is a copy; re-link it into skills-vendor/"

    target = Path(os.readlink(HOOK))
    assert not target.is_absolute(), "a relative target survives a clone to any path"
    assert target.parts[:2] == ("..", ".."), "the link is resolved from .claude/hooks/"
    assert "skills-vendor" in target.parts


def test_the_hook_the_symlink_points_at_actually_exists():
    """A populated submodule is the other half — a dangling link is a silent no-op."""
    assert HOOK.exists(), (
        "skills-vendor/ is not checked out: run .skills/doctor.sh locally, "
        "or add `submodules: true` to the CI job's actions/checkout (#27)"
    )
    assert os.access(HOOK, os.X_OK)
