"""Every job, and every step that waits on apt or a browser download, has a
``timeout-minutes`` (#180). Without one a job falls back to GitHub's 360-minute
default: on ``74c69a7`` an ``apt-get update`` hung the ``test`` job for three
hours, which held the run's result, and with it every "CI green by job" check,
until someone cancelled it.

**The job rule** covers every workflow, the generated ``context-cadence.yml``
included. A hung runner doesn't care who rendered the file, which is the same
reasoning as the prune rule in ``test_workflow_pins.py``.

**The step rule** is derived, not named: a step whose ``run:`` calls
``apt-get`` or ``playwright install`` carries its own bound, so the next such
step is caught when it is added. Its bound must sit below its job's, or it
never fires. The wheelhouse sync and ``uv sync`` are left under the job bound
on purpose. The sizes, and why those two are excluded, are in the comment
above ``jobs:`` in ``ci.yml``.

**``apt-get update`` stays non-fatal.** A bound answers a hang, not an exit,
so it is no reason to make an unrelated vendor's index a gate.
"""

import re
from pathlib import Path

import yaml

from tests.ci.test_workflow_pins import CI, WORKFLOWS

# What a job or step without `timeout-minutes` gets.
GITHUB_DEFAULT_MINUTES = 360

# A command that waits on apt or on Playwright's CDN. `apt-get` must start a
# command (a line, or after `&&`, `||` or `;`), so a message that only names it
# doesn't count.
_NETWORK_INSTALL = re.compile(
    r"(?:^|&&|\|\||;)\s*(?:sudo\s+)?apt-get\b|\bplaywright install\b(?!-deps)",
    re.MULTILINE,
)


def _workflow_jobs(path: Path) -> dict[str, dict]:
    return yaml.safe_load(path.read_text(encoding="utf-8")).get("jobs", {})


def _unbounded_jobs(workflows: list[Path]) -> list[str]:
    return [
        f"{path.name}:{name}"
        for path in workflows
        for name, job in _workflow_jobs(path).items()
        if "timeout-minutes" not in job
    ]


def _network_install_steps(path: Path) -> list[tuple[str, dict, dict]]:
    """(job name, job, step) for each step whose ``run:`` waits on apt or a download."""
    return [
        (name, job, step)
        for name, job in _workflow_jobs(path).items()
        for step in job.get("steps", [])
        if _NETWORK_INSTALL.search(step.get("run", ""))
    ]


def _misbounded_steps(path: Path) -> list[str]:
    """Network install steps with no bound, or one its job's bound would beat."""
    return [
        f"{path.name}:{name}: {step.get('name', step['run'].splitlines()[0])!r}"
        for name, job, step in _network_install_steps(path)
        if "timeout-minutes" not in step
        or step["timeout-minutes"] >= job.get("timeout-minutes", GITHUB_DEFAULT_MINUTES)
    ]


def test_every_job_has_a_timeout():
    workflows = sorted(WORKFLOWS.glob("*.y*ml"))
    assert CI in workflows, "ci.yml not found: the derivation broke"
    bad = _unbounded_jobs(workflows)
    assert not bad, (
        "jobs without `timeout-minutes` fall back to GitHub's 360 minutes, and a "
        f"hung step then holds the run that long (#180): {bad}"
    )


def test_the_step_rule_finds_the_network_installs():
    """A renamed command would leave the bound check below reading nothing, green."""
    found = {(name, step["name"]) for name, _, step in _network_install_steps(CI)}
    assert found == {
        ("test", "Install redis-server (the OOM suite spawns its own broker)"),
        ("browser", "Install the pinned headless shell and its libraries"),
    }


def test_every_network_install_step_has_a_bound_below_its_jobs():
    bad = [step for path in sorted(WORKFLOWS.glob("*.y*ml")) for step in _misbounded_steps(path)]
    assert not bad, (
        "a step that waits on apt or a download needs its own `timeout-minutes`, "
        f"below its job's, so a hang fails there and names the step (#180): {bad}"
    )


def test_the_step_rule_rejects_a_missing_and_a_dead_bound(tmp_path):
    workflow = tmp_path / "other.yml"
    workflow.write_text(
        "jobs:\n"
        "  a:\n"
        "    timeout-minutes: 10\n"
        "    steps:\n"
        "      - name: unbounded\n"
        "        run: sudo apt-get update\n"
        "      - name: dead\n"
        "        timeout-minutes: 10\n"
        "        run: uv run playwright install chromium\n"
        "      - name: bounded\n"
        "        timeout-minutes: 5\n"
        "        run: sudo apt-get install -y redis-server\n"
        "      - name: not a download\n"
        "        run: uv run playwright install-deps --dry-run chromium\n"
        "      - name: chained\n"
        "        run: cd /tmp && sudo apt-get install -y jq\n"
        "      - name: only names it\n"
        "        run: echo '::error::missing after apt-get install'\n"
    )
    assert _misbounded_steps(workflow) == [
        "other.yml:a: 'unbounded'",
        "other.yml:a: 'dead'",
        "other.yml:a: 'chained'",
    ]
    assert _unbounded_jobs([workflow]) == []


# An `apt-get update` command, with its line continuations joined.
_APT_UPDATE = re.compile(r"(?:^|&&|;)\s*(?:sudo\s+)?apt-get update\b(.*)$", re.MULTILINE)


def _apt_updates(path: Path) -> list[tuple[str, bool]]:
    """(step name, tolerated) for each ``apt-get update``: tolerated means an ``||`` follows."""
    return [
        (step.get("name", ""), m.group(1).lstrip().startswith("||"))
        for job in _workflow_jobs(path).values()
        for step in job.get("steps", [])
        for m in _APT_UPDATE.finditer(re.sub(r"\\\n\s*", " ", step.get("run", "")))
    ]


def test_apt_get_update_stays_non_fatal():
    """The bound is for a hang; it doesn't change how an exit is read. A third-party
    index's Hash Sum mismatch exits 100 with every Ubuntu index fresh, so the update
    is tolerated and the install decides (the redis step's comment, #138 CR 24)."""
    updates = _apt_updates(CI)
    assert len(updates) == 2, f"expected the redis and headless-shell updates: {updates}"
    fatal = [name for name, tolerated in updates if not tolerated]
    assert not fatal, f"`apt-get update` made fatal; keep its `|| echo ::warning::`: {fatal}"


def test_the_update_check_tells_a_tolerated_update_from_a_fatal_one(tmp_path):
    workflow = tmp_path / "other.yml"
    workflow.write_text(
        "jobs:\n"
        "  a:\n"
        "    steps:\n"
        "      - name: tolerated\n"
        "        run: |\n"
        "          sudo apt-get update \\\n"
        "            || echo warn\n"
        "      - name: fatal\n"
        "        run: sudo apt-get update && sudo apt-get install -y jq\n"
    )
    assert _apt_updates(workflow) == [("tolerated", True), ("fatal", False)]
