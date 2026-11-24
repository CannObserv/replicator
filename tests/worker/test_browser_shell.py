"""The ``-m browser`` run's missing-shell message (#236), pinned in the default suite.

The state directory is the deploy's to install into (#228): a message that hands
``exedev`` a ``playwright install`` there fails on the directory's mode, and run as
``replicator`` it is the bare install whose GC can delete the live worker's shell.
Keyed on the resolved path, not on the variable: docs/TESTING.md's own command sets
the variable *to* the state directory.
"""

import re
from pathlib import Path

import pytest

from tests.worker._browser_shell import (
    DEPLOYMENT_SECTION,
    STATE_DIRECTORY,
    VARIABLE,
    browsers_path,
    missing_shell_message,
)

REPO = Path(__file__).resolve().parents[2]


def test_unset_resolves_to_the_settings_default():
    assert browsers_path({}) == STATE_DIRECTORY == Path("/var/lib/replicator/ms-playwright")


def test_set_resolves_to_the_variable(tmp_path):
    assert browsers_path({VARIABLE: str(tmp_path)}) == tmp_path


@pytest.mark.parametrize(
    "environ",
    [
        pytest.param({}, id="unset"),
        pytest.param({VARIABLE: "/var/lib/replicator/ms-playwright"}, id="set-to-state-dir"),
        pytest.param({VARIABLE: "/var/lib/replicator/ms-playwright/"}, id="trailing-slash"),
        pytest.param({VARIABLE: "/var/lib/replicator/./ms-playwright"}, id="dot-segment"),
    ],
)
def test_the_state_directory_names_the_deploy_and_gives_no_install(environ):
    message = missing_shell_message(browsers_path(environ))

    assert "playwright install" not in message
    assert "migrate" in message
    assert DEPLOYMENT_SECTION in message
    # The scratch-path way out stays, naming the variable.
    assert VARIABLE in message


def test_a_scratch_path_gets_the_install_for_that_path(tmp_path):
    message = missing_shell_message(browsers_path({VARIABLE: str(tmp_path)}))

    assert (
        f"PLAYWRIGHT_BROWSERS_PATH={tmp_path} uv run playwright install chromium-headless-shell"
    ) in message
    assert DEPLOYMENT_SECTION not in message


def _anchor(heading: str) -> str:
    """GitHub's anchor for a heading: lowercased, punctuation dropped, spaces to hyphens."""
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


def test_the_linked_section_exists():
    doc, _, anchor = DEPLOYMENT_SECTION.partition("#")
    headings = re.findall(r"^#+ (.+)$", (REPO / doc).read_text(), re.MULTILINE)

    assert anchor in {_anchor(h) for h in headings}
