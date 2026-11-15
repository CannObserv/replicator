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
