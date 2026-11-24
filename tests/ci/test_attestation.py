"""The local-gate attestation's CI side (#215, gregoryfoster/skills#688).

With ``.skills/attest-local-gate`` committed as ``on``, shipping-work's
``push.sh`` runs the gate on the exact tree, pushes, and posts the commit
status ``gate/local`` = ``success`` on the pushed SHA. ``test``'s ``attest``
step reads it, and an attested human PR skips **one** step: the default pytest
run, which is the gate's own run plus coverage. Everything else keeps running.

The shape is CannObserv/status's, our closest match (``pr`` merge mode, GitHub
merge commits, a ``deploy.sh`` that reads ``main``'s push run):

- **The read runs on ``pull_request`` only.** A merge made on GitHub carries no
  status, so on ``main`` the read could only add a failure mode, and a
  transient API error there would fail ``test`` and make ``deploy.sh`` refuse
  the commit. With the step PR-only, its output is unset on every push run,
  and ``!= 'yes'`` runs the suite: ``main`` runs in full by construction.
- **The grant is on the job.** Every job here declares its own
  ``permissions``, and a job's block replaces the workflow's, so a
  workflow-level ``statuses: read`` would be inert (watcher, broker on #688).
- **The step's comments live in YAML,** not in ``run:``: ``context's`` is an
  unbalanced quote to ``shlex`` (watcher, status on #688).
- **The verdict is logged** with ``tee`` (#697 item 7).

Accepted limitation: an attested PR skips coverage's ``fail_under``, since the
gate runs ``--no-cov``. The backstop is ``main``'s push run, which every
deploy needs.
"""

import os
import re
import shlex
import subprocess
from pathlib import Path

import pytest

from tests.ci.test_workflow_pins import _jobs

SKIPPABLE = "pytest (default, excludes integration)"
REPO = "CannObserv/replicator"


def _step(job: str, *, id_: str | None = None, name: str | None = None) -> dict:
    for step in _jobs()[job]["steps"]:
        if (id_ and step.get("id") == id_) or (name and step.get("name") == name):
            return step
    raise AssertionError(f"no step {id_ or name!r} in job {job!r}")


# --- Where the read happens ---------------------------------------------------


def test_only_the_test_job_reads_the_status():
    """``lint``, ``browser`` and ``gcs`` never skip: ``browser`` and ``gcs`` aren't
    part of the local gate, and ``lint`` is cheap."""
    holders = [
        job
        for job, spec in _jobs().items()
        for step in spec.get("steps", [])
        if step.get("id") == "attest"
    ]
    assert holders == ["test"], f"the attest step belongs to test alone, found in {holders}"


def test_the_test_job_grants_the_status_read_beside_what_it_had():
    """Job-level ``permissions`` replace the workflow's, so the grant goes here."""
    perms = _jobs()["test"]["permissions"]
    assert perms == {"contents": "read", "id-token": "write", "statuses": "read"}


def test_no_other_job_is_granted_the_status_read():
    for job, spec in _jobs().items():
        if job != "test":
            assert "statuses" not in spec.get("permissions", {}), f"{job} has no use for statuses"


def test_the_attest_step_runs_on_pull_requests_only():
    assert _step("test", id_="attest")["if"].strip() == "github.event_name == 'pull_request'"


def test_the_attest_step_reads_the_prs_head_not_the_merge_commit():
    """On ``pull_request``, ``github.sha`` is the merge commit, which nobody gated."""
    env = _step("test", id_="attest")["env"]
    assert env["HEAD"] == "${{ github.event.pull_request.head.sha }}"
    assert env["GH_TOKEN"] == "${{ github.token }}"
    assert env["REPO"] == "${{ github.repository }}"


def test_the_attest_step_comes_after_the_sync_and_before_the_suite():
    """Late in the job: ``push.sh`` posts seconds after the push, so a read made
    after auth and ``uv sync`` usually finds it (broker's race on #688)."""
    names = [s.get("id") or s.get("name") for s in _jobs()["test"]["steps"]]
    assert names.index("uv sync") < names.index("attest") < names.index(SKIPPABLE)


def test_only_the_default_pytest_step_reads_the_verdict():
    """The OOM integration step never skips: the gate deselects ``integration``."""
    readers = [
        (job, step.get("name"))
        for job, spec in _jobs().items()
        for step in spec.get("steps", [])
        if "steps.attest" in str(step.get("if", ""))
    ]
    assert readers == [("test", SKIPPABLE)]


def test_the_attest_steps_script_has_no_comments_and_splits_under_shlex():
    """The shlex trap (watcher, status on #688): keep prose out of ``run:``."""
    run = _step("test", id_="attest")["run"]
    assert not [line for line in run.splitlines() if line.lstrip().startswith("#")]
    shlex.split(run)
    assert "${{" not in run, "expressions reach the script through env, not interpolation"


# --- The two `if:`s, evaluated ------------------------------------------------
#
# A small evaluator for the subset these two expressions use: string literals,
# dotted context paths (an unset one is null), ==, !=, &&, ||, parentheses and
# startsWith(). GitHub coerces null against a string to number vs NaN, so null
# equals no string, which is what Python's None does here. A bare `!` is
# refused rather than given Python's precedence for `not`.

_TOKEN = re.compile(
    r"\s*(?:(?P<str>'(?:[^']|'')*')|(?P<op>\|\||&&|==|!=|\(|\)|,)|(?P<bang>!)"
    r"|(?P<name>[A-Za-z_][\w.\-]*))"
)


def _evaluate(expression: str, context: dict) -> bool:
    def lookup(path: str):
        node = context
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        return node

    python, pos = [], 0
    expression = expression.strip()
    while pos < len(expression):
        match = _TOKEN.match(expression, pos)
        assert match, f"cannot read {expression[pos:]!r}"
        pos = match.end()
        if match["str"]:
            python.append(repr(match["str"][1:-1].replace("''", "'")))
        elif match["bang"]:
            raise AssertionError("a bare ! is outside this evaluator's subset")
        elif match["op"]:
            python.append({"||": " or ", "&&": " and "}.get(match["op"], match["op"]))
        elif match["name"] == "startsWith":
            python.append("starts_with")
        else:
            python.append(f"lookup({match['name']!r})")

    def starts_with(text, prefix):
        return isinstance(text, str) and text.lower().startswith(str(prefix).lower())

    return bool(eval("".join(python), {"lookup": lookup, "starts_with": starts_with}))  # noqa: S307


def _pr(user_type: str = "User", head_repo: str = REPO) -> dict:
    return {
        "pull_request": {"user": {"type": user_type}, "head": {"repo": {"full_name": head_repo}}}
    }


# (event, ref, event payload, a `gate/local` success on the commit it reads)
CASES = {
    "attested human PR": ("pull_request", "refs/pull/1/merge", _pr(), True),
    "unattested human PR": ("pull_request", "refs/pull/1/merge", _pr(), False),
    "Dependabot PR": ("pull_request", "refs/pull/2/merge", _pr("Bot"), True),
    "fork PR": ("pull_request", "refs/pull/3/merge", _pr(head_repo="someone/replicator"), True),
    "push to main": ("push", "refs/heads/main", {}, True),
    "manual run": ("workflow_dispatch", "refs/heads/main", {}, True),
}


def _runs_the_suite(event: str, ref: str, payload: dict, status: bool) -> bool:
    github = {"event_name": event, "ref": ref, "repository": REPO, "event": payload}
    attest_runs = _evaluate(_step("test", id_="attest")["if"], {"github": github})
    outputs = {"attested": "yes" if status else "no"} if attest_runs else {}
    context = {"github": github, "steps": {"attest": {"outputs": outputs}}}
    return _evaluate(_step("test", name=SKIPPABLE)["if"], context)


@pytest.mark.parametrize("case", list(CASES))
def test_the_suite_skips_only_for_an_attested_human_pr(case):
    assert _runs_the_suite(*CASES[case]) is (case != "attested human PR")


def test_the_evaluator_reads_null_as_unequal_to_every_string():
    assert _evaluate("steps.attest.outputs.attested != 'yes'", {})
    assert not _evaluate("github.event.pull_request.user.type == 'Bot'", {})


# --- The step's script, run -----------------------------------------------------


def _run_attest(tmp_path: Path, gh_body: str) -> tuple[subprocess.CompletedProcess, str, str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls"
    gh = bin_dir / "gh"
    gh.write_text(
        f'#!/usr/bin/env bash\nprintf "%s\\n" "$*" >>"{calls}"\n{gh_body}\n', encoding="utf-8"
    )
    gh.chmod(0o755)
    output = tmp_path / "output"
    output.touch()
    script = tmp_path / "attest.sh"
    script.write_text(_step("test", id_="attest")["run"], encoding="utf-8")
    env = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "GITHUB_OUTPUT": str(output),
        "HEAD": "0" * 40,
        "REPO": REPO,
        "GH_TOKEN": "unused",
    }
    # `bash -e {0}`: the shell Actions gives a step that names none.
    result = subprocess.run(
        ["bash", "-e", str(script)], env=env, capture_output=True, text=True, check=False
    )
    return (
        result,
        output.read_text(encoding="utf-8"),
        calls.read_text(encoding="utf-8") if calls.exists() else "",
    )


@pytest.mark.parametrize(("count", "verdict"), [("1", "yes"), ("2", "yes"), ("0", "no")])
def test_the_step_writes_and_logs_its_verdict(tmp_path, count, verdict):
    result, output, calls = _run_attest(tmp_path, f"echo {count}")
    assert result.returncode == 0, result.stderr
    assert output == f"attested={verdict}\n"
    assert f"attested={verdict}" in result.stdout, "the verdict must be in the log (#697 item 7)"
    assert f"repos/{REPO}/commits/{'0' * 40}/status?per_page=100" in calls
    assert 'select(.context == "gate/local" and .state == "success")' in calls


def test_a_refused_read_fails_the_step_and_writes_nothing(tmp_path):
    result, output, _ = _run_attest(tmp_path, "echo 'HTTP 403' >&2; exit 1")
    assert result.returncode != 0
    assert output == ""
