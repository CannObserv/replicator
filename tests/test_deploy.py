"""The systemd unit's restart limiter must fit the worker's own failure timescale.

`replicator.service` and `Settings` encode two halves of one decision: how long
the worker absorbs a broker outage before exiting, and how many such exits the
unit tolerates before staying `failed`. Documented in both places, enforced
here — raising the ceiling without widening the window silently restores the
failure mode the pairing exists to prevent (a permanently unreachable Redis that
reads as `active (running)` forever).

The second half of the file covers the preflight that keeps the unit honest about
*which* code it starts: `scripts/check_main_checkout.sh` (#37). Its shape is
asserted from the unit file (invoked, fatal, ahead of the `BUILD_ID` stamp) and
its behaviour from throwaway git repositories, because a guard whose only test is
a unit-file parser is half-tested.
"""

import os
import re
import subprocess
from pathlib import Path

import pytest

from src.core.config import Settings

REPO_ROOT = Path(__file__).resolve().parents[1]
UNIT = REPO_ROOT / "deploy" / "replicator.service"
GUARD = REPO_ROOT / "scripts" / "check_main_checkout.sh"

# The bypass the guard honours, and the env var name the tests must scrub so an
# operator's shell (or /etc/replicator/.env, loaded by the Common Commands
# snippet) cannot quietly turn every refusal assertion green.
OVERRIDE = "REPLICATOR_ALLOW_ANY_CHECKOUT"


def _directive(name: str) -> str:
    """The last value assigned to ``name`` in the unit (systemd's own semantics)."""
    matches = re.findall(rf"^{name}=(.*)$", UNIT.read_text(), flags=re.MULTILINE)
    assert matches, f"{name} is not set in {UNIT.name}"
    return matches[-1].strip()


def _exec_start_pre() -> list[str]:
    """Every ``ExecStartPre`` value, in the order systemd will run them.

    Unlike :func:`_directive` this keeps all of them: ``ExecStartPre`` is a list
    directive, and both the fatality of one entry and the relative order of two
    are the properties under test.
    """
    values = [
        value.strip()
        for value in re.findall(r"^ExecStartPre=(.*)$", UNIT.read_text(), flags=re.MULTILINE)
    ]
    assert values, f"ExecStartPre is not set in {UNIT.name}"
    return values


def _guard_step() -> str:
    """The single ``ExecStartPre`` that runs the main-checkout guard."""
    matches = [value for value in _exec_start_pre() if GUARD.name in value]
    assert len(matches) == 1, f"expected exactly one {GUARD.name} ExecStartPre, got {matches}"
    return matches[0]


def test_the_start_limit_window_fits_a_burst_of_slow_exits():
    settings = Settings()
    window = float(_directive("StartLimitIntervalSec"))
    burst = int(_directive("StartLimitBurst"))

    # Each failed cycle costs at most worst_case_outage_seconds before the exit,
    # so `burst` of them must land inside one window for the limiter to trip.
    assert window >= burst * settings.worst_case_outage_seconds


def test_the_stop_timeout_outlasts_a_blocking_read():
    """SIGTERM is only checked between polls, so the grace period must exceed one."""
    settings = Settings()
    timeout_stop = float(_directive("TimeoutStopSec"))

    assert timeout_stop > settings.read_block_ms / 1000


def test_the_stop_timeout_outlasts_the_slowest_fetch_a_command_may_ask_for():
    """The second half of the #11 pairing.

    A command carries its own ``timeout_seconds`` now, so the handler's budget is
    no longer the driver's fixed 30s — it is whatever
    ``REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS`` permits. A poll that starts just
    before SIGTERM can therefore cost a full read window *plus* a full fetch, and
    a grace period shorter than the sum SIGKILLs the worker mid-message on every
    deploy that lands during a slow fetch — turning a routine restart into a
    stale-claim round-trip.

    Strictly greater, not equal: the sweep is a third term this cannot quantify
    (it rides an uncancellable ``asyncio.to_thread``), so the margin is where it
    lives.
    """
    settings = Settings()
    timeout_stop = float(_directive("TimeoutStopSec"))

    assert timeout_stop > settings.read_block_ms / 1000 + settings.max_fetch_timeout_seconds


def test_the_stop_timeout_absorbs_a_pacing_wait_as_well():
    """The #12 term.

    A handler may now sleep out a per-host politeness window before it fetches,
    bounded by the poll window (``build_handler``'s ``park_above_seconds``
    default — anything longer parks instead). The stop event cuts that sleep
    short, so this is belt-and-braces rather than the primary guard: the sum is
    asserted because the alternative is discovering at the next deploy that
    three separately-reasonable numbers no longer fit inside one.
    """
    settings = Settings()
    timeout_stop = float(_directive("TimeoutStopSec"))
    worst_case = (
        settings.read_block_ms / 1000  # a poll already in flight
        + settings.read_block_ms / 1000  # the pacing sleep bound, derived from it
        + settings.max_fetch_timeout_seconds  # the slowest fetch a command may ask for
    )

    assert timeout_stop > worst_case


# --- The main-checkout guard (#37) -------------------------------------------
#
# "Code committed to main is the deployed code" is the invariant AGENTS.md states
# and nothing enforced until #37. These assert the unit actually consults the
# guard, then that the guard actually decides correctly.


def test_the_unit_runs_the_main_checkout_guard():
    """The guard has to be wired in, not merely present in ``scripts/``."""
    assert GUARD.exists(), f"{GUARD.name} is missing"
    assert GUARD.name in "\n".join(_exec_start_pre())


def test_the_main_checkout_guard_is_fatal():
    """A ``-``-prefixed guard is not a guard — its refusal would be logged and ignored."""
    step = _guard_step()

    assert not step.startswith("-"), (
        f"{GUARD.name} is '-' prefixed, so its refusal would not stop the start"
    )
    # `-` is the prefix that matters, but asserting on an absolute path rules out
    # every systemd prefix character at once (`-`, `@`, `:`, `+`, `!`, `!!`), so a
    # future edit cannot weaken this by reaching for a different one.
    assert step.startswith("/"), f"expected an unprefixed absolute path, got {step!r}"


def test_the_main_checkout_guard_runs_before_the_build_id_stamp():
    """A refused start must not leave a misleading build id behind in /run.

    ``/run/replicator/build-id`` outlives the failed start (nothing removes it),
    so stamping the branch SHA first would leave the journal describing code that
    never ran — the same "looks correct, is not" failure #37 exists to close.
    """
    steps = _exec_start_pre()
    guard_at = next(i for i, step in enumerate(steps) if GUARD.name in step)
    stamp_at = next(i for i, step in enumerate(steps) if "build-id" in step)

    assert guard_at < stamp_at, (
        f"{GUARD.name} runs at ExecStartPre #{guard_at}, after the BUILD_ID stamp at #{stamp_at}"
    )


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
    """Invoke the guard with ``cwd`` standing in for the unit's ``WorkingDirectory``."""
    env = {key: value for key, value in os.environ.items() if key != OVERRIDE}
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


def test_being_ahead_of_origin_main_only_warns(repo: Path):
    """Not in #37's table, and arguably the stronger case — see the script's comment.

    Local commits on ``main`` that were never pushed are exactly the "code that is
    on no shared branch" the issue objects to, but ``origin/main`` is a cached ref,
    so refusing on it would fail an operator whose only sin is not having fetched.
    Warn, and leave the refuse/warn call to a follow-up.
    """
    _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    (repo / "file.txt").write_text("two\n")
    _git(repo, "commit", "--quiet", "--no-gpg-sign", "-am", "second")

    result = _run_guard(repo)

    assert result.returncode == 0, result.stderr
    assert "ahead" in result.stderr
