"""Is this code a release? Asked at the moment a write identity is built (#52).

``replicator.service`` runs a release (#151): a root-owned, read-only tree that
``scripts/deploy.sh`` built from a CI-green ``main`` commit and finished by
writing ``REVISION`` last, as root. **That file, a regular file owned by root, is
the proof.** Only the deploy writes one, through ``sudo``, after its gate; an
``echo > REVISION`` in a checkout is the deploy user's, and a link could name any
root-owned file.

**A checkout builds no provider writer.** The documented way to test a branch on
this VM is ``uv run python -m src.worker.main`` under distinct consumer names,
and ``AGENTS.md`` tells shell work to load ``/etc/replicator/.env``, so a worker
started that way inherits the production ADC *and* the production alias table:
a branch would hold a write identity against a bucket whose objects cannot be
deleted by anyone who holds it (#38, #52). Since #151 that includes a checkout
on ``main``: the checkout is no longer what production runs.
``REPLICATOR_ALLOW_ANY_CHECKOUT=1`` builds the writers anyway, and says so.

Until #151 the verdict was ``scripts/check_main_checkout.sh``'s, a git question.
A release has no ``.git``, and asking it there refused every writer the service
built; the deploy's ``post_build_check`` now runs this function from each release
before switching to it, so that trap is refused at build time.

The caller is ``build_writers``, and only when there is a provider binding to
build.
"""

import os
import stat
from pathlib import Path

from src.core.logging import get_logger

logger = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
OVERRIDE = "REPLICATOR_ALLOW_ANY_CHECKOUT"
# Root writes REVISION (scripts/deploy.sh, `sudo -n tee`). A module constant so
# the tests, which cannot chown, can name their own user instead.
RELEASE_OWNER_UID = 0


def checkout_refusal(root: Path = REPO_ROOT) -> str | None:
    """``None`` when ``root`` is a release, or the override is set; else why not."""
    revision = root / "REVISION"
    try:
        found = revision.lstat()
    except FileNotFoundError:
        found = None

    if found is not None and stat.S_ISREG(found.st_mode) and found.st_uid == RELEASE_OWNER_UID:
        return None

    if found is None:
        why = f"{root} is not a release (no REVISION)"
    elif not stat.S_ISREG(found.st_mode):
        why = f"{revision} is not a regular file, so {root} is not a release"
    else:
        why = f"{revision} is not owned by root, so {root} is not a release"

    # Exactly "1", as the script that preceded this read it: "true" or "yes" is
    # someone guessing, and a guess should not grant a write identity.
    if os.environ.get(OVERRIDE) == "1":
        logger.warning(
            "building provider writers from a checkout",
            extra={"reason": why, "override": f"{OVERRIDE}=1"},
        )
        return None

    return f"{why}: a checkout builds no provider writer; set {OVERRIDE}=1 to build one anyway"
