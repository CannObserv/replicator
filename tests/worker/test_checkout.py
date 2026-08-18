"""Whether this checkout may hold a production write identity (#52).

`scripts/check_main_checkout.sh` already answers "is this main's code?" for the
service, as an `ExecStartPre`. The dev worker never runs it: `uv run python -m
src.worker.main` is the documented way to test a branch on this VM, and systemd
is not involved. That is the gap — `AGENTS.md` also tells us to load
`/etc/replicator/.env` for shell work, so a worker started that way inherits the
production ADC and, once #50 provisions it, the production alias table too. A
feature branch then holds a write identity against a bucket whose objects cannot
be deleted (#38).

**The verdict is the script's, taken wholesale.** Not reimplemented here: the
script distinguishes seven conditions and argues each one — ahead of origin
refuses because those commits are on no shared branch, behind warns because a
never-fetched ref cannot prove staleness, dubious ownership gets its own message
so an operator does not hunt the wrong hypothesis. Re-deciding any of that in
Python would be two sources of truth for one question, and the second one would
be the one nobody updates.

So these tests are about the *seam*, not the tiers. What the script decides is
`tests/test_check_main_checkout.py`'s subject, including the
`REPLICATOR_ALLOW_ANY_CHECKOUT=1` override, which this module never reads.
"""

import subprocess

import pytest

from src.worker import checkout
from src.worker.checkout import GUARD, checkout_refusal


class FakeCompleted:
    def __init__(self, returncode, stderr=""):
        self.returncode = returncode
        self.stderr = stderr


def test_the_guard_script_is_where_this_module_thinks_it_is():
    """The one assertion a mocked seam cannot make.

    Every other test here stubs `subprocess.run`, so a wrong path would pass all
    of them and refuse every writer on a live host — a guard that always fires is
    indistinguishable from a guard that works until the day replication is
    provisioned.
    """
    assert GUARD.is_file()
    assert GUARD.name == "check_main_checkout.sh"


def test_a_checkout_the_script_accepts_is_not_refused(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeCompleted(0))

    assert checkout_refusal() is None


def test_a_refusal_carries_the_script_s_own_words(monkeypatch):
    """The message is the script's, so the journal reads the same either way.

    An operator who has seen this refusal from systemd should recognise it from
    the worker, and paraphrasing it here would produce two vocabularies for one
    condition.
    """
    stderr = (
        "check_main_checkout: HEAD is on '52-non-main-write-guard', not 'main'\n"
        "check_main_checkout: refusing to start — the service would run unmerged code\n"
    )
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeCompleted(1, stderr))

    refusal = checkout_refusal()

    assert refusal is not None
    assert "not 'main'" in refusal


def test_a_silent_failure_still_refuses(monkeypatch):
    """Non-zero with nothing on stderr is still a refusal, with a reason of our own."""
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: FakeCompleted(1, "   \n"))

    refusal = checkout_refusal()

    assert refusal is not None
    assert "1" in refusal


@pytest.mark.parametrize(
    "exc",
    [
        pytest.param(FileNotFoundError("bash"), id="no-bash"),
        pytest.param(PermissionError("denied"), id="not-executable"),
        pytest.param(subprocess.TimeoutExpired("bash", 10), id="hung"),
    ],
)
def test_an_unrunnable_guard_refuses_rather_than_passes(monkeypatch, exc):
    """Absence is hard, not soft — the script says so about itself, for this reason.

    Fail-open here would make "remove bash from the box" a silent bypass, which
    is the same shape as the `git`-not-found case the script refuses on.
    """

    def raises(*args, **kwargs):
        raise exc

    monkeypatch.setattr(subprocess, "run", raises)

    assert checkout_refusal() is not None


def test_a_missing_script_refuses(monkeypatch, tmp_path):
    monkeypatch.setattr(checkout, "GUARD", tmp_path / "gone.sh")

    refusal = checkout_refusal()

    assert refusal is not None
    assert "gone.sh" in refusal


@pytest.mark.parametrize(
    "carried",
    [
        pytest.param("GH_TOKEN", id="the-repo-pat"),
        pytest.param("GH_TOKEN_ARCHIVER", id="a-sibling-repo-pat"),
        pytest.param("GOOGLE_APPLICATION_CREDENTIALS", id="the-write-identity"),
    ],
)
def test_the_child_does_not_inherit_the_shell_s_secrets(monkeypatch, carried):
    """CR #2. The guard is the only subprocess the worker spawns — keep it starved.

    AGENTS.md justifies the two-env-file boundary in exactly these terms: the
    repo `.env` holds org-wide PATs, and handing them "to a process whose job is
    fetching public URLs widens the blast radius of any crash dump **or
    subprocess** for no benefit". A dev worker is started from a shell that
    AGENTS.md itself tells you to load both files into, so the full environment
    is real, not hypothetical — and this script needs none of it.
    """
    monkeypatch.setenv(carried, "a-secret-the-guard-has-no-use-for")
    seen = {}

    def record(argv, **kwargs):
        seen["env"] = kwargs.get("env")
        return FakeCompleted(0)

    monkeypatch.setattr(subprocess, "run", record)

    checkout_refusal()

    assert seen["env"] is not None, "no env= means the child inherits everything"
    assert carried not in seen["env"]


def test_the_child_gets_what_the_script_actually_needs(monkeypatch):
    """PATH to find git, HOME because git reads the global config for
    `safe.directory`, and the override the script itself reads."""
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("HOME", "/home/someone")
    monkeypatch.setenv("REPLICATOR_ALLOW_ANY_CHECKOUT", "1")
    seen = {}

    def record(argv, **kwargs):
        seen["env"] = kwargs.get("env")
        return FakeCompleted(0)

    monkeypatch.setattr(subprocess, "run", record)

    checkout_refusal()

    assert seen["env"] == {
        "PATH": "/usr/bin",
        "HOME": "/home/someone",
        "REPLICATOR_ALLOW_ANY_CHECKOUT": "1",
    }


def test_a_variable_the_shell_never_set_is_simply_absent(monkeypatch):
    """An unset override must not arrive as an empty string.

    The script branches on `${REPLICATOR_ALLOW_ANY_CHECKOUT:-}` being empty
    versus `1` versus anything else, and its third branch logs "is '', not '1'
    — not bypassing" on every start. Passing the key with an empty value would
    make that line permanent noise (#48).
    """
    monkeypatch.delenv("REPLICATOR_ALLOW_ANY_CHECKOUT", raising=False)
    seen = {}

    def record(argv, **kwargs):
        seen["env"] = kwargs.get("env")
        return FakeCompleted(0)

    monkeypatch.setattr(subprocess, "run", record)

    checkout_refusal()

    assert "REPLICATOR_ALLOW_ANY_CHECKOUT" not in seen["env"]


def test_the_script_runs_against_this_checkout_not_the_working_directory(monkeypatch):
    """`cwd` is derived from this module's own path.

    The script inspects the current directory, which under systemd is the unit's
    `WorkingDirectory` and is therefore right. A dev worker is started from
    wherever the operator happens to be standing, and the checkout worth
    verifying is the one this code was imported from — not that.
    """
    seen = {}

    def record(argv, **kwargs):
        seen["argv"] = argv
        seen["cwd"] = kwargs.get("cwd")
        return FakeCompleted(0)

    monkeypatch.setattr(subprocess, "run", record)

    checkout_refusal()

    assert seen["cwd"] == GUARD.parent.parent
    assert str(GUARD) in seen["argv"]
