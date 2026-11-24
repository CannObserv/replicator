"""Where ``-m browser`` finds the pinned shell, and what it says when the shell is missing.

Its own module because ``test_browser_real.py`` is all ``-m browser``, deselected by
default; ``test_browser_shell.py`` pins the message in every suite (#236).
"""

import os
from collections.abc import Mapping
from pathlib import Path

from src.core.config import Settings
from src.worker.browser import pinned_shell

VARIABLE = "REPLICATOR_PLAYWRIGHT_BROWSERS_PATH"
# The worker's own default, so the check below can't drift from src/core/config.py.
STATE_DIRECTORY: Path = Settings.model_fields["playwright_browsers_path"].default
DEPLOYMENT_SECTION = "docs/DEPLOYMENT.md#the-headless-shell-228"


def browsers_path(environ: Mapping[str, str] = os.environ) -> Path:
    """The shell's root a run launches from: the variable, else the state directory."""
    return Path(environ.get(VARIABLE, STATE_DIRECTORY))


def missing_shell_message(path: Path) -> str:
    """What a run with no pinned shell under ``path`` tells the reader to do.

    The state directory gets no install command: it's ``replicator``'s, so ``exedev``'s
    fails on its mode, and run as ``replicator`` it is the bare install whose GC can
    delete the shell the live worker launches (#228). Any other path is the reader's
    own, and Playwright's GC touches only the path it was given.
    """
    shell = pinned_shell(path)
    if path.resolve() == STATE_DIRECTORY.resolve():
        return (
            f"no pinned shell at {shell}: the deploy installs it there, as replicator, "
            f"in migrate (#228). Never a bare install into the state directory; the hand "
            f"order is {DEPLOYMENT_SECTION}. Or point {VARIABLE} at a scratch path you can "
            "write, and the run names its install"
        )
    return (
        f"no pinned shell at {shell}: install it with "
        f"PLAYWRIGHT_BROWSERS_PATH={path} uv run playwright install chromium-headless-shell"
    )
