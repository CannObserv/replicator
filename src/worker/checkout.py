"""Is this checkout main's code? Asked at the moment a write identity is built.

``replicator.service`` already asks, as an ``ExecStartPre`` running
``scripts/check_main_checkout.sh`` (#37, #48). The dev worker never does: the
documented way to test a branch on this VM is ``uv run python -m
src.worker.main`` under a distinct ``REPLICATOR_CONSUMER_NAME``, which involves
no systemd and so runs no ``ExecStartPre``.

That was harmless while replication was unprovisioned and every command refused.
It stops being harmless the moment an alias table exists: ``AGENTS.md`` instructs
loading ``/etc/replicator/.env`` for shell work, so a worker started that way
inherits the production ADC *and* the production alias table, and a feature
branch acquires a write identity against a bucket whose objects cannot be deleted
by anyone who holds it (#38, #52).

**The verdict is the script's, not a reimplementation.** The script distinguishes
seven conditions and argues each: ahead of ``origin/main`` refuses because those
commits are on no shared branch, behind only warns because a never-fetched ref
cannot prove staleness, a dubiously-owned repository gets its own message so a
failed start does not send an operator hunting the wrong hypothesis. Deciding any
of that again here would be two sources of truth for one question, and the second
would be the one nobody updates. The ``REPLICATOR_ALLOW_ANY_CHECKOUT=1`` override
is the script's too, for the same reason — it is read there and nowhere in this
module.

**Refusal is what an unrunnable guard returns**, matching the script's own stance
that absence is hard rather than soft: fail-open would make removing ``bash``, or
the script itself, a silent bypass.

The caller is ``build_writers``, and only when there is a provider binding to
build — a worker that replicates nothing shells out to nothing.
"""

import os
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "check_main_checkout.sh"

# The script makes no network call by design — that is the whole argument for
# reading `origin/main` from the cached ref rather than fetching — so this bounds
# a hang, not a slow answer.
GUARD_TIMEOUT_SECONDS = 10.0

# Everything the guard needs, and nothing else (CR #2). ``PATH`` to find ``git``,
# ``HOME`` because git reads the global config there — that is where a
# ``safe.directory`` entry lives, and the dubious-ownership branch exists to
# report its absence — and the override the script itself reads.
#
# Passing an explicit environment rather than inheriting is the point.
# ``replicator.service`` reads only ``/etc/replicator/.env``, but a *dev* worker
# is started from a shell AGENTS.md tells you to load the repo ``.env`` into as
# well, which carries org-wide PATs. That file's own rule is that handing them to
# a subprocess "widens the blast radius of any crash dump or subprocess for no
# benefit", and this is the only subprocess the worker spawns.
GUARD_ENV_KEYS = ("PATH", "HOME", "REPLICATOR_ALLOW_ANY_CHECKOUT")


def _guard_env() -> dict[str, str]:
    """``GUARD_ENV_KEYS`` that are actually set, and no others.

    Absent keys are **omitted rather than empty**: the script branches on
    ``REPLICATOR_ALLOW_ANY_CHECKOUT`` being unset versus ``1`` versus anything
    else, and its third branch logs "is '', not '1' — not bypassing" on every
    start. An empty value would make that line permanent noise (#48).
    """
    return {key: os.environ[key] for key in GUARD_ENV_KEYS if key in os.environ}


def checkout_refusal() -> str | None:
    """``None`` when this checkout is main's code, else why it is not.

    The returned string is the script's own stderr wherever there is one, so the
    journal reads the same whether the refusal came from systemd or from here.
    """
    if not GUARD.is_file():
        return f"{GUARD} is missing — cannot verify this checkout is main's code"

    try:
        completed = subprocess.run(
            ["bash", str(GUARD)],
            cwd=REPO_ROOT,
            env=_guard_env(),
            capture_output=True,
            text=True,
            timeout=GUARD_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return f"could not run {GUARD.name}: {type(exc).__name__}: {exc}"

    if completed.returncode == 0:
        return None
    return completed.stderr.strip() or f"{GUARD.name} exited {completed.returncode}"
