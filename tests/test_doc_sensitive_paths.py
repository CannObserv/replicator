"""`.skills/doc-sensitive-paths` still matches this tree (#75).

The list tailors the Step 1.5 doc-drift gate in
`shipping-work-python-fastapi/scripts/doc-check.sh`. Upstream
gregoryfoster/skills#252 made a list where *no* entry matches anything an
exit-2 failure, and made an exit-0 name the entries that matched nothing — but
the individually-dead entry is only a *note* on an otherwise-green run, which
is exactly the kind of signal a passing gate trains its reader to skim past.

That is the failure this file exists to catch, and it is reached by an ordinary
rename: move `deploy/` to `systemd/`, split `scripts/`, retire a `.claude/`
tree, and the corresponding entry stops matching. Nothing breaks, the gate
stays green, and the doc-drift it was tailored to catch is silently no longer
watched. A pytest guard turns that into a red test at the moment of the rename.

Precedent for testing a non-Python gate artifact from pytest: `test_ci.py`
(workflow concurrency), `test_deploy.py` (the systemd unit), `test_skills_hook.py`
(the refresh hook), `test_check_main_checkout.py` (a shell guard). The list is
one more artifact a gate reads and no interpreter validates.

`path_matches` below is a port of the script's matcher, not an approximation.
A looser port would vouch for entries the real gate never hits, which is the
same silent-pass this file is here to prevent — so it is written case-for-case
against the bash and asserted against known inputs before it is trusted.
"""

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
LIST_PATH = REPO_ROOT / ".skills" / "doc-sensitive-paths"


def path_matches(file: str, entry: str) -> bool:
    """Port of `path_matches` in doc-check.sh — segment matching, both arms.

    Trailing-slash entries match a directory at any depth. Slash-less entries
    name a file *or* a directory, and every continuation requires a literal
    `/` after the entry, which is what keeps `pyproject.toml` from also
    claiming `pyproject.toml.bak`.
    """
    if entry.endswith("/"):
        return file.startswith(entry) or f"/{entry}" in file
    return (
        file == entry
        or file.endswith(f"/{entry}")
        or file.startswith(f"{entry}/")
        or f"/{entry}/" in file
    )


def parse_list(text: str) -> list[str]:
    """Port of doc-check.sh's reader: strip, drop blanks and `#`-comments."""
    entries = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        entries.append(stripped)
    return entries


@pytest.fixture(scope="module")
def entries() -> list[str]:
    return parse_list(LIST_PATH.read_text())


@pytest.fixture(scope="module")
def tracked_files() -> list[str]:
    """Every tracked path, read the way the gate reads it.

    `core.quotePath=false` for the same reason the script sets it: git
    otherwise C-quotes any non-ASCII path, and the leading quote defeats the
    anchored arm of the matcher.
    """
    out = subprocess.run(
        ["git", "-c", "core.quotePath=false", "ls-files"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.split("\n")


@pytest.mark.parametrize(
    ("file", "entry", "expected"),
    [
        # Trailing-slash entries reach any depth — the #252 fix.
        ("src/worker/main.py", "src/", True),
        ("packages/co-core/src/x.py", "src/", True),
        ("tests/worker/test_loop.py", "src/", False),
        # Slash-less entries name a file at any depth, and only whole segments.
        ("pyproject.toml", "pyproject.toml", True),
        ("packages/co/pyproject.toml", "pyproject.toml", True),
        ("pyproject.toml.bak", "pyproject.toml", False),
        # A slash-less entry still covers a directory of that name.
        ("docs/a.md", "docs", True),
        # Multi-segment entries anchor on the whole run of segments.
        (".claude/settings.json", ".claude/settings.json", True),
        ("other/.claude/settings.json", ".claude/settings.json", True),
        (".claude/settings.json.bak", ".claude/settings.json", False),
    ],
)
def test_matcher_port_agrees_with_the_script(file: str, entry: str, expected: bool) -> None:
    """The port is the thing vouching for the list; pin it before trusting it."""
    assert path_matches(file, entry) is expected


def test_comments_and_blanks_are_ignored() -> None:
    assert parse_list("# a\n\n  b  \n\t# c\nd\n") == ["b", "d"]


def test_list_is_not_empty(entries: list[str]) -> None:
    """An empty list is an exit-2 in the gate — a check that did not run."""
    assert entries, f"{LIST_PATH} lists no paths; remove it to fall back to the defaults"


def test_every_entry_matches_a_tracked_file(entries: list[str], tracked_files: list[str]) -> None:
    """No dead entries.

    A dead entry cannot contribute to any verdict, so the part of the doc
    surface it was added to watch is unwatched. The gate reports this as a note
    under a green; here it is a failure, which is what a rename needs to hit.
    """
    dead = [e for e in entries if not any(path_matches(f, e) for f in tracked_files)]
    assert not dead, (
        f"{LIST_PATH} entries match no tracked file, so they cannot contribute to "
        f"any doc-check verdict: {dead}. Retarget or remove them."
    )


def test_entries_the_list_must_keep_covering(entries: list[str], tracked_files: list[str]) -> None:
    """The four low-churn files the list is tailored to watch (#75 CR).

    Named individually rather than left to the dead-entry sweep: each was added
    because a *specific* doc section drifts with it, and a broad entry that
    happens to cover one today would hide its removal tomorrow.
    """
    must_cover = {
        # docs/SKILLS.md documents hook registration and suspension as edits
        # here, with the hook script left in place.
        ".claude/settings.json": "docs/SKILLS.md hook registration",
        # The "Vendor submodules" table transcribes this file. The low-churn
        # stand-in for skills-vendor/, which the daily refresh bumps.
        ".gitmodules": "docs/SKILLS.md vendor submodule table",
        # The list must cover itself: editing it obsoletes its own doc section.
        ".skills/doc-sensitive-paths": "docs/SKILLS.md sensitive-path section",
        # AGENTS.md Project Layout names every module under src/.
        "src/worker/main.py": "AGENTS.md Project Layout",
    }
    uncovered = {
        path: why
        for path, why in must_cover.items()
        if not any(path_matches(path, e) for e in entries)
    }
    assert not uncovered, f"no entry covers these doc-sensitive paths: {uncovered}"


def test_named_paths_are_actually_tracked(tracked_files: list[str]) -> None:
    """Guard the guard: the paths above must exist, or the test above is vacuous."""
    for path in (".claude/settings.json", ".gitmodules", ".skills/doc-sensitive-paths"):
        assert path in tracked_files, f"{path} is no longer tracked"
