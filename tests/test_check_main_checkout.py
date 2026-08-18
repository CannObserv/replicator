"""`scripts/check_main_checkout.sh` decides correctly, driven as a process (#37).

Split from `tests/test_deploy.py`, which owns the *unit file's* shape — that it
invokes this guard, unprefixed, ahead of the `BUILD_ID` stamp. This file owns the
guard's own behaviour, which is a different concern reached by a different
mechanism (`subprocess` against throwaway repositories rather than a regex over
an ini file), per `docs/TESTING.md`'s split-by-concern rule and the precedent
`tests/test_seed_fetch.py` sets for a `scripts/` module having its own tests.

Every case runs the real script against a real repository. A guard whose only
test is a unit-file parser is half-tested — the unborn-HEAD message bug that
prompted one of these cases was invisible to the parser and surfaced only by
executing it.

**Two refusal paths are deliberately untested**, and they are named here rather
than left to look like oversights:

* *dubiously-owned repository* — git compares the repository's owner against the
  calling euid, so reproducing it needs a second uid and therefore root. Asserted
  by reading, not by running.
* *`--abbrev-ref` returning an empty name* — unreachable once `rev-parse --verify
  HEAD` has succeeded, which the script does first. It is refused on its own line
  anyway; the branch exists to keep a git failure from being reported as
  "detached".
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
GUARD = REPO_ROOT / "scripts" / "check_main_checkout.sh"

# The bypass the guard honours. Scrubbed from every invocation below: an operator
# who exported it in their shell (or sourced /etc/replicator/.env via the Common
# Commands snippet) would otherwise turn every refusal assertion in this file
# green, and a suite that passes because the thing under test was disabled is
# worse than no suite.
OVERRIDE = "REPLICATOR_ALLOW_ANY_CHECKOUT"


def _git(repo: Path, *args: str) -> None:
    """Run a git command in ``repo``, with identity and signing pinned locally.

    ``-c`` rather than the ambient config: the suite must behave the same on a
    machine whose global config sets ``commit.gpgsign`` or a different
    ``init.defaultBranch``.
    """
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A throwaway repository on ``main`` with one commit and a clean tree.

    Deliberately not the real checkout: the guard's whole job is to refuse a
    checkout, and a test that had to put the live deployment tree into the state
    under test would be the very mistake #37 guards against.
    """
    work = tmp_path / "checkout"
    work.mkdir()
    _git(work, "init", "--initial-branch=main", "--quiet")
    (work / "file.txt").write_text("one\n")
    _git(work, "add", "file.txt")
    _git(work, "commit", "--quiet", "--no-gpg-sign", "-m", "initial")
    return work


def _run_guard(cwd: Path, **env_extra: str) -> subprocess.CompletedProcess[str]:
    """Invoke the guard with ``cwd`` standing in for the unit's ``WorkingDirectory``.

    The environment is scrubbed of the override *and of every* ``GIT_*`` *var*.
    The second is not hypothetical tidiness: with ``GIT_DIR`` or ``GIT_WORK_TREE``
    exported — as they are inside any git hook, and as a wrapper script may leave
    them — a run from an empty directory would find a work tree, and
    :func:`test_the_guard_refuses_a_tree_it_cannot_verify` would fail in a way
    that reads like a bug in the guard rather than in the environment.
    """
    env = {
        key: value
        for key, value in os.environ.items()
        if key != OVERRIDE and not key.startswith("GIT_")
    }
    env.update(env_extra)
    return subprocess.run(
        ["bash", str(GUARD)],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )


def test_the_guard_accepts_a_clean_main_checkout(repo: Path):
    result = _run_guard(repo)

    assert result.returncode == 0, result.stderr


def test_the_guard_refuses_a_feature_branch(repo: Path):
    """The case that motivated #37: a restart while the checkout sat on a branch."""
    _git(repo, "checkout", "--quiet", "-b", "37-some-feature")

    result = _run_guard(repo)

    assert result.returncode != 0
    assert "37-some-feature" in result.stderr


def test_the_guard_refuses_a_detached_head(repo: Path):
    """Refused for free, but it gets its own message rather than a confusing one."""
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    _git(repo, "checkout", "--quiet", head)

    result = _run_guard(repo)

    assert result.returncode != 0
    assert "detached" in result.stderr


def test_the_guard_refuses_a_tree_it_cannot_verify(tmp_path: Path):
    """No git work tree ⇒ no evidence the code is main's code ⇒ refuse.

    The fail-loud direction on purpose: soft-passing here would make "delete the
    .git directory" a silent bypass of the guard.
    """
    result = _run_guard(tmp_path)

    assert result.returncode != 0
    assert "not a git work tree" in result.stderr
    # The generic line, not the ownership one — they are separate messages
    # precisely so an operator is not sent after the wrong hypothesis.
    assert "dubious" not in result.stderr


def test_the_guard_refuses_an_unborn_head_by_name(tmp_path: Path):
    """A fresh `git init` refuses, and says why.

    ``rev-parse --abbrev-ref HEAD`` answers the literal string ``HEAD`` here, the
    same as for a detached checkout, so without its own branch the guard reported
    "detached at" followed by an empty SHA. The verdict was right and the message
    was misleading, which is the failure mode #37 is about one level down.
    """
    work = tmp_path / "empty"
    work.mkdir()
    _git(work, "init", "--initial-branch=main", "--quiet")

    result = _run_guard(work)

    assert result.returncode != 0
    assert "unborn" in result.stderr
    assert "detached" not in result.stderr


def test_the_override_lets_a_branch_through(repo: Path):
    """A guard with no override gets removed rather than overridden."""
    _git(repo, "checkout", "--quiet", "-b", "37-some-feature")

    result = _run_guard(repo, **{OVERRIDE: "1"})

    assert result.returncode == 0, result.stderr


def test_the_override_only_answers_to_1(repo: Path):
    """`=true` is a plausible typo, and a silently-ineffective override is worse than none."""
    _git(repo, "checkout", "--quiet", "-b", "37-some-feature")

    result = _run_guard(repo, **{OVERRIDE: "true"})

    assert result.returncode != 0
    assert OVERRIDE in result.stderr


def test_a_dirty_tree_on_main_only_warns(repo: Path):
    """Refusing would block an operator mid-incident."""
    (repo / "file.txt").write_text("edited\n")

    result = _run_guard(repo)

    assert result.returncode == 0, result.stderr
    assert "uncommitted" in result.stderr


def test_a_dirty_submodule_is_not_a_dirty_tree(tmp_path: Path):
    """Vendored skills moving does not make the worker's code un-deployed.

    This repo carries two `skills-vendor/` submodules and a once-a-day refresh
    hook that moves them, so counting submodule state would fire the dirty-tree
    warning on the live VM routinely, for a reason that has nothing to do with the
    code the worker runs. Permanent noise in a warning tier is the same failure as
    no warning at all.
    """
    inner = tmp_path / "inner"
    inner.mkdir()
    _git(inner, "init", "--initial-branch=main", "--quiet")
    (inner / "vendored.txt").write_text("v1\n")
    _git(inner, "add", "vendored.txt")
    _git(inner, "commit", "--quiet", "--no-gpg-sign", "-m", "vendored")

    work = tmp_path / "outer"
    work.mkdir()
    _git(work, "init", "--initial-branch=main", "--quiet")
    (work / "file.txt").write_text("one\n")
    _git(work, "add", "file.txt")
    # A file:// submodule needs the escape hatch added for CVE-2022-39253.
    _git(
        work,
        "-c",
        "protocol.file.allow=always",
        "submodule",
        "--quiet",
        "add",
        str(inner),
        "sub",
    )
    _git(work, "commit", "--quiet", "--no-gpg-sign", "-m", "add submodule")
    assert _run_guard(work).returncode == 0

    (work / "sub" / "vendored.txt").write_text("v2\n")

    result = _run_guard(work)

    assert result.returncode == 0, result.stderr
    assert "uncommitted" not in result.stderr


def test_being_behind_origin_main_only_warns(repo: Path):
    """A stale-but-shared commit is a different problem from an unshared one.

    And refusing would make the service unstartable during a network outage, since
    ``origin/main`` is only ever as fresh as the last fetch.
    """
    (repo / "file.txt").write_text("two\n")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-am", "second")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    _git(repo, "reset", "--hard", "--quiet", "HEAD~1")

    result = _run_guard(repo)

    assert result.returncode == 0, result.stderr
    assert "behind" in result.stderr


def test_being_ahead_of_origin_main_refuses(repo: Path):
    """Unpushed commits on ``main`` are the "on no shared branch" case #37 objects to (#48).

    The asymmetry with the *behind* case above is the whole argument: ``origin/main``
    is updated by fetch **and** by a successful push from this repository, so a
    never-fetched ref can hide behind-ness — remote commits this checkout cannot see
    — but it cannot manufacture ahead-ness for commits this checkout pushed. Ahead is
    therefore local evidence, and no network call is needed for it to be sound.
    """
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    (repo / "file.txt").write_text("two\n")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-am", "second")

    result = _run_guard(repo)

    assert result.returncode != 0
    assert "ahead" in result.stderr
    # The two remedies and the staleness hypothesis, so an operator who believes the
    # commits are already pushed is not left to guess which of the two is wrong.
    assert "git push" in result.stderr
    assert "git fetch" in result.stderr


def test_the_override_lets_unpushed_commits_through(repo: Path):
    """The network-partition answer: committed a hotfix, cannot reach the remote (#48)."""
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    (repo / "file.txt").write_text("two\n")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-am", "second")

    result = _run_guard(repo, **{OVERRIDE: "1"})

    assert result.returncode == 0, result.stderr


def test_a_diverged_main_refuses_and_names_both_sides(repo: Path):
    """Ahead *and* behind: the refusal wins, but the behind warning still prints.

    Divergence is what a PR merged by squash or rebase looks like locally — the
    upstream commits carry different SHAs, so the tree is not merely ahead. An
    operator seeing only "ahead" would push and get rejected; the behind line names
    the other half of what has to be reconciled.
    """
    (repo / "file.txt").write_text("upstream\n")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-am", "upstream")
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    _git(repo, "reset", "--hard", "--quiet", "HEAD~1")
    (repo / "file.txt").write_text("local\n")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-am", "local")

    result = _run_guard(repo)

    assert result.returncode != 0
    assert "ahead" in result.stderr
    assert "behind" in result.stderr


def test_a_refused_start_still_names_the_dirty_tree(repo: Path):
    """Every warn prints before any refusal, so one start reports every condition (#48 CR #22).

    The behind/ahead pair is ordered for this reason already; a tree that is both
    ahead and dirty has the same claim on being told once rather than across two
    failed starts.
    """
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    (repo / "file.txt").write_text("two\n")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-am", "second")
    (repo / "file.txt").write_text("and uncommitted\n")

    result = _run_guard(repo)

    assert result.returncode != 0
    assert "ahead" in result.stderr
    assert "uncommitted" in result.stderr


def test_a_missing_origin_main_ref_warns_but_starts(repo: Path):
    """No ref to compare against ⇒ no evidence either way ⇒ say so, and start (#48).

    Deliberately *not* the fail-loud treatment the unverifiable-tree cases get: there
    the missing thing is the whole checkout, here HEAD has already been proven to be
    ``main``. But once ahead-of-origin refuses, silence here would make
    ``git remote remove origin`` a quiet bypass of that refusal, so the absence is
    named rather than passed over.
    """
    result = _run_guard(repo)

    assert result.returncode == 0, result.stderr
    assert "no origin/main" in result.stderr
