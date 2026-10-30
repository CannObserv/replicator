"""Tracked text, for a guard that reads the whole repo (#170, #171)."""

import subprocess
from collections.abc import Iterator
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def tracked_texts(exclude: tuple[str, ...] = ()) -> Iterator[tuple[str, str]]:
    """`(path, text)` for every tracked text file whose path starts with no `exclude`.

    `git ls-files` rather than a walk: `skills/*` and `.claude/hooks/*` are
    tracked symlinks into `skills-vendor/`, which a walk would follow past the
    prefix check. Symlinks and binaries are skipped.
    """
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=REPO, capture_output=True, check=True
    ).stdout.decode()
    for name in sorted(filter(None, out.split("\0"))):
        path = REPO / name
        if name.startswith(exclude) or path.is_symlink() or not path.is_file():
            continue
        try:
            yield name, path.read_text()
        except UnicodeDecodeError:  # binary: nothing to read
            continue
