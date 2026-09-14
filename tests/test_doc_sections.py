"""`.skills/doc-sections` routes every doc-check hit at a doc this repo keeps (#93).

`.skills/doc-sensitive-paths` (#75) says what the Step 1.5 gate in
`shipping-work-python-fastapi/scripts/doc-check.sh` watches; this file says
what to do about a hit. Absent, the gate prints the skill's two built-in lines
— a "route table" this worker-first service has no equivalent of, and no doc
under `docs/` — so a `scripts/` hit never names `docs/COMMANDS.md`, the doc
that drifts with it. From gregoryfoster/skills#284 the gate also ends every
such hit with a note naming the half that is still the default.

Upstream runs no dead-entry check on advice, since it is prose: a doc renamed
out from under a line keeps being named and nothing fails. The one decidable
part is the doc each line leads with, and that is checked here the way
`test_doc_sensitive_paths.py` checks the list — red at the moment of the rename.

Whether a line routes a given sensitive path is deliberately untested. A
checker for that is satisfied by pasting paths into the advice, which makes the
advice worse (gregoryfoster/skills#284).
"""

import re
from pathlib import Path

import pytest

from tests.test_doc_sensitive_paths import parse_list, read_tracked_files

REPO_ROOT = Path(__file__).resolve().parents[1]
ADVICE_PATH = REPO_ROOT / ".skills" / "doc-sections"

# `<doc>: ` at the start of a line. The doc is one whitespace-free token, and
# the colon must be followed by a space — which is what keeps prose such as
# `see #12: …` from reading as a lead.
_LEAD = re.compile(r"^([^\s:]+): ")


def lead_doc(line: str) -> str | None:
    """The doc an advice line routes to, or None when it names none."""
    match = _LEAD.match(line)
    return match.group(1) if match else None


@pytest.fixture(scope="module")
def sections() -> list[str]:
    return parse_list(ADVICE_PATH.read_text())


@pytest.fixture(scope="module")
def tracked_files() -> list[str]:
    return read_tracked_files()


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("AGENTS.md: Project Layout (src/)", "AGENTS.md"),
        ("docs/COMMANDS.md: every script with its flags", "docs/COMMANDS.md"),
        # A directory lead keeps its trailing slash, and so its meaning.
        ("docs/contracts/: the normative contracts", "docs/contracts/"),
        # No space after the colon, or no colon at all, is no lead.
        ("AGENTS.md:Project Layout", None),
        ("spot-check the docs", None),
        # An issue reference later in the line is content, not a lead.
        ("see #12: charter and tests together", None),
    ],
)
def test_lead_parser(line: str, expected: str | None) -> None:
    """Pin the parser before trusting it to vouch for the file."""
    assert lead_doc(line) == expected


def test_advice_is_tailored() -> None:
    """Absent, the gate falls back to advice naming a route table (#93)."""
    assert ADVICE_PATH.is_file(), (
        f"{ADVICE_PATH} is missing, so every doc-check hit prints the skill's "
        "built-in advice instead of this repo's docs"
    )


def test_advice_is_not_empty(sections: list[str]) -> None:
    """An empty advice file is an exit-2 in the gate — a check that did not run."""
    assert sections, f"{ADVICE_PATH} lists no sections; remove it to fall back to the defaults"


def test_every_line_leads_with_a_tracked_doc(sections: list[str], tracked_files: list[str]) -> None:
    """No advice pointing nowhere.

    A trailing-slash lead names a directory and must hold a tracked file; any
    other lead must be a tracked file itself.
    """

    def tracked(doc: str) -> bool:
        if doc.endswith("/"):
            return any(f.startswith(doc) for f in tracked_files)
        return doc in tracked_files

    leads = [(s, lead_doc(s)) for s in sections]
    unled = [s for s, doc in leads if doc is None]
    assert not unled, f"{ADVICE_PATH} lines must lead with `<doc>: `: {unled}"
    dead = [s for s, doc in leads if doc is not None and not tracked(doc)]
    assert not dead, (
        f"{ADVICE_PATH} lines lead with a doc this tree does not track, so the "
        f"advice points nowhere: {dead}. Retarget or remove them."
    )
