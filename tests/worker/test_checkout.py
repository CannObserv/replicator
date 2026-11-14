"""Whether this code may hold a production write identity (#52, #151).

The service runs a release: a root-owned tree the deploy built from a CI-green
main commit, finished by root writing ``REVISION`` last. That file, owned by
root, is the proof — only ``scripts/deploy.sh`` writes one, through ``sudo``,
after its gate. A checkout has none, and a dev worker started in one (``uv run
python -m src.worker.main``, with the production env loaded for shell work)
builds no provider writer unless ``REPLICATOR_ALLOW_ANY_CHECKOUT=1`` says to.

Until #151 the answer was ``scripts/check_main_checkout.sh``'s git verdict. A
release has no ``.git`` and the script is gone, so a port of it would refuse
every writer the service builds: the trap the deploy's ``post_build_check``
now asks this function about before every switch.
"""

import os
import stat
from pathlib import Path

import pytest

from src.worker import checkout
from src.worker.checkout import OVERRIDE, REPO_ROOT, checkout_refusal


@pytest.fixture
def owned_by_this_user(monkeypatch):
    """Tests can't chown to root, so the release owner becomes the test's user."""
    monkeypatch.setattr(checkout, "RELEASE_OWNER_UID", os.getuid())


@pytest.fixture(autouse=True)
def _no_override(monkeypatch):
    monkeypatch.delenv(OVERRIDE, raising=False)


def _release(root: Path) -> Path:
    revision = root / "REVISION"
    revision.write_text("0123456789ab\n")
    revision.chmod(0o444)
    return root


def test_the_root_is_this_repository_s():
    """The one assertion a stubbed root can't make: a wrong ``parents[n]`` would
    look for ``REVISION`` in the wrong directory and refuse every writer."""
    assert (REPO_ROOT / "src" / "worker" / "checkout.py").is_file()
    assert (REPO_ROOT / "pyproject.toml").is_file()


def test_a_release_is_not_refused(tmp_path, owned_by_this_user):
    assert checkout_refusal(_release(tmp_path)) is None


def test_the_release_owner_is_root():
    assert checkout.RELEASE_OWNER_UID == 0


def test_a_checkout_is_refused_and_told_how_to_override(tmp_path):
    refusal = checkout_refusal(tmp_path)

    assert refusal is not None
    assert "not a release" in refusal
    assert f"{OVERRIDE}=1" in refusal


def test_a_revision_anyone_could_write_is_not_a_release(tmp_path):
    """``echo > REVISION`` in a checkout is the deploy user's file, not root's."""
    refusal = checkout_refusal(_release(tmp_path))

    assert refusal is not None
    assert "not owned by root" in refusal


def test_a_linked_revision_is_not_a_release(tmp_path, owned_by_this_user):
    """The deploy writes a regular file; a link could name any root-owned file."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (tmp_path / "REVISION").symlink_to(_release(elsewhere) / "REVISION")

    assert stat.S_ISLNK((tmp_path / "REVISION").lstat().st_mode)
    assert checkout_refusal(tmp_path) is not None


def test_the_override_builds_writers_in_a_checkout_and_says_so(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(OVERRIDE, "1")

    assert checkout_refusal(tmp_path) is None
    assert [r.override for r in caplog.records if r.name == checkout.__name__] == [f"{OVERRIDE}=1"]


@pytest.mark.parametrize("value", ["", "0", "yes", "true"])
def test_only_1_overrides(tmp_path, monkeypatch, value):
    monkeypatch.setenv(OVERRIDE, value)

    assert checkout_refusal(tmp_path) is not None


def test_this_checkout_is_refused():
    """The suite runs from a checkout, never a release, so the default root
    answers a refusal: the autouse fixture in ``conftest.py`` exists for that."""
    assert not (REPO_ROOT / "REVISION").exists()
    assert checkout_refusal() is not None
