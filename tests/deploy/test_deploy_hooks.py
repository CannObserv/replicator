"""This repo's deploy hooks, held to the files they restate (#151).

The core's consumer tests (beside this one) hold the contract; these hold
replicator's own values: the CI jobs to the workflow, the host configs to the
paths the rest of the suite reads, and verify's start-record check to the line the
worker actually logs. Design: docs/plans/2026-11-14-releases-design.md.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest
import yaml

from tests.test_deploy import NEEDRESTART_INSTALLED, TAILSCALE_PINS_INSTALLED

REPO = Path(__file__).resolve().parents[2]
HOOKS = REPO / "scripts" / "deploy_hooks.sh"


def sourced(expression: str) -> list[str]:
    result = subprocess.run(
        ["bash", "-c", f'source "$1"; {expression}', "_", str(HOOKS)],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.splitlines()


def test_ci_jobs_are_every_job_the_workflow_runs_on_a_push():
    workflow = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text())
    names = {job.get("name", key) for key, job in workflow["jobs"].items()}
    assert set(sourced('printf "%s\\n" "${CI_JOBS[@]}"')) == names


def _host_configs() -> dict[str, str]:
    return dict(line.split("=", 1) for line in sourced('printf "%s\\n" "${HOST_CONFIGS[@]}"'))


def test_every_host_config_is_a_tracked_file():
    for name in _host_configs():
        assert (REPO / "deploy" / name).is_file(), name


@pytest.mark.parametrize(
    ("name", "installed"),
    [
        ("needrestart.conf.d/replicator.conf", NEEDRESTART_INSTALLED),
        ("apt/preferences.d/tailscale.pref", TAILSCALE_PINS_INSTALLED),
    ],
)
def test_host_configs_name_the_paths_the_suite_reads(name, installed):
    assert _host_configs()[name] == str(installed)


def test_keep_is_zero_spares():
    """A rollback needs no spare; chosen at 20 GiB for disk, kept at 30 (#204)."""
    assert sourced('printf "%s\\n" "$KEEP"') == ["0"]


def test_the_wrapper_finds_its_key_without_an_env_file():
    """A deploy loads no env file, so the shared wrapper's own default must name
    the host's key (#201): the hooks export no key variable at all."""
    wrapper = sourced('printf "%s\\n" "$UV_WRAPPER"')[0]
    assert wrapper == "scripts/co-pypi.sh" and (REPO / wrapper).is_file()
    # From an empty environment, so a loaded shell's own variables don't count.
    exported = sourced('env -i PATH="$PATH" bash -c \'source "$0"; compgen -e\' "$1"')
    assert not [v for v in exported if "CO_PYPI" in v], "the wrapper's default is the key"
    spec = importlib.util.spec_from_file_location("minter", REPO / "scripts" / "co_pypi_token.py")
    minter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(minter)
    assert minter.key_path({}) == "/etc/replicator/co-pypi-reader.json"


WANT = "0123456789ab"


def _verify(tmp_path: Path, unit: str, journal: list[str]) -> subprocess.CompletedProcess:
    """verify <unit> WANT, with the core's helpers and the host's tools stubbed."""
    log = tmp_path / "journal"
    log.write_text("".join(line + "\n" for line in journal))
    stubs = (
        'verify_started_on() { return 0; }; say() { :; }; note() { echo "$*" >&2; }; '
        "systemctl() { echo 4242; }; "
        'journalctl() { [[ "$1" == _PID=4242 ]] && cat "$JOURNAL"; }; '
        "sleep() { SECONDS=100000; }; "
    )
    return subprocess.run(
        ["bash", "-c", f'source "$1"; {stubs} verify "$2" "$3"', "_", str(HOOKS), unit, WANT],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "JOURNAL": str(log)},
    )


def _ready(build: str) -> str:
    return json.dumps({"level": "INFO", "message": "worker ready", "build": build})


def test_verify_passes_on_a_start_record_naming_the_build(tmp_path):
    journal = ["not json", json.dumps({"message": "starting"}), _ready(WANT)]
    assert _verify(tmp_path, "replicator.service", journal).returncode == 0


@pytest.mark.parametrize("journal", [[], [_ready("3abaa81")], [json.dumps({"build": WANT})]])
def test_verify_fails_without_one(tmp_path, journal):
    result = _verify(tmp_path, "replicator.service", journal)
    assert result.returncode == 1
    assert "worker ready" in result.stderr


def test_verify_has_no_check_for_the_failure_handler(tmp_path):
    result = _verify(tmp_path, "replicator-failure-notify@.service", [])
    assert result.returncode == 2


# --- migrate: the release's pinned headless shell (#228) ---------------------
#
# Installed before the switch, as the worker's user, into its state directory.
# Playwright's install garbage-collects first: it drops every .links entry it can't
# read and every browser no remaining link pins. The host's only link pointed into
# /home, which `replicator` can't reach (#202), so an install alone would delete the
# shell the live worker launches per render. migrate re-registers the live release
# first, GC skipped, then installs the new one with GC on.

OLD, NEW = "aaaaaaaaaaaa", "bbbbbbbbbbbb"


def _migrate(tmp_path: Path, live: str | None, fail_on: int = 0):
    """migrate live (the target) deploying NEW, with sudo stubbed to record each call.

    In a subshell with errexit on, under the core's nounset and pipefail."""
    root = tmp_path / "srv"
    for build in (OLD, NEW):
        (root / "releases" / build).mkdir(parents=True)
    if live:
        (root / "live").symlink_to(f"releases/{live}")
    calls = tmp_path / "calls"
    stubs = (
        'sudo() { n=1; [[ ! -f "$CALLS" ]] || n=$(( $(wc -l <"$CALLS") + 1 )); '
        'echo "$PWD|$*" >>"$CALLS"; (( n != FAIL_ON )); }; '
        "say() { :; }; note() { :; }; journal() { :; }; "
    )
    result = subprocess.run(
        ["bash", "-c", f'source "$1"; {stubs} (set -euo pipefail; migrate live)', "_", str(HOOKS)],
        capture_output=True,
        text=True,
        cwd=REPO,
        env={
            "PATH": "/usr/bin:/bin",
            "CALLS": str(calls),
            "FAIL_ON": str(fail_on),
            "ROOT": str(root),
            "release": str(root / "releases" / NEW),
        },
    )
    lines = calls.read_text().splitlines() if calls.exists() else []
    return result, [line.split("|", 1) for line in lines], root


def _install(root: Path, build: str, *, gc: bool) -> str:
    from src.core.config import Settings

    browsers = Settings.model_fields["playwright_browsers_path"].default
    skip = [] if gc else ["PLAYWRIGHT_SKIP_BROWSER_GC=1"]
    return " ".join(
        ["-n", "-u", "replicator", "env", f"PLAYWRIGHT_BROWSERS_PATH={browsers}", *skip]
        + [f"{root}/releases/{build}/.venv/bin/playwright", "install", "chromium-headless-shell"]
    )


def test_migrate_registers_the_live_release_then_installs_the_new_one(tmp_path):
    result, calls, root = _migrate(tmp_path, live=OLD)
    assert result.returncode == 0, result.stderr
    assert calls == [
        ["/", _install(root, OLD, gc=False)],
        ["/", _install(root, NEW, gc=True)],
    ]


@pytest.mark.parametrize("live", [None, NEW], ids=["first deploy", "redeploy"])
def test_migrate_installs_once_when_no_other_release_is_live(tmp_path, live):
    result, calls, root = _migrate(tmp_path, live=live)
    assert result.returncode == 0, result.stderr
    assert calls == [["/", _install(root, NEW, gc=True)]]


@pytest.mark.parametrize("fail_on", [1, 2])
def test_a_failed_install_refuses_the_deploy(tmp_path, fail_on):
    result, calls, _ = _migrate(tmp_path, live=OLD, fail_on=fail_on)
    assert result.returncode != 0
    assert len(calls) == fail_on, "nothing runs after a failed install"
