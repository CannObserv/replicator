"""Behaviour of scripts/check_redis_floor.sh - the broker precondition guard.

Run as an `ExecStartPre` on replicator.service. One blocking assertion: the
server must be >= 7.0, because Replicator is the cluster's first user of
`AsyncBusConsumer.claim_stale`, which reads `XAUTOCLAIM`'s three-element reply.

Otherwise soft by design - exit 0 (letting the worker start) when the broker is
unreachable. These tests drive it with a stub `redis-cli` on PATH so no live
Redis is required and each branch is exercised deterministically.

Added with CannObserv/archiver#195, which is also why the auth cases exist: the
script sent stderr to /dev/null and judged only stdout, so a broker that
*refused the credential* and a broker that *could not be reached* printed the
same line. During CannObserv/broker#1's cutover that line was on every start of
this service for days while describing the wrong system.
"""

import shutil
import subprocess
import time
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_redis_floor.sh"

_WRONGPASS = "AUTH failed: WRONGPASS invalid username-password pair or user is disabled."
_NOAUTH = "NOAUTH Authentication required."
_REFUSED = "Could not connect to Redis at broker:6379: Connection refused"
_CLI_PASSWORD_WARNING = (
    "Warning: Using a password with '-a' or '-u' option on the command line "
    "interface may not be safe."
)


def _stub_redis_cli(
    tmp_path: Path,
    *,
    version: str | None,
    tls: bool = True,
    stderr: str = "",
    exit_code: int = 0,
) -> Path:
    """Write a fake `redis-cli` to a bin dir; return the dir for PATH.

    `--help` output includes `--tls` iff `tls`. `INFO server` prints a
    `redis_version:` line iff `version` is given. `stderr` and `exit_code` model
    the failures that produce no stdout at all - before archiver#195 they were
    indistinguishable from each other.
    """
    binder = tmp_path / "bin"
    binder.mkdir()
    help_tls = "  --tls    Use TLS.\n" if tls else ""
    version_line = f'  echo "redis_version:{version}"' if version is not None else "  true"
    # %b, not %s: a multi-line `stderr` arrives here as a repr with an escaped
    # newline, and only %b expands it back into two lines.
    fail_lines = (
        f'  printf "%b\\n" {stderr!r} >&2\n  exit {exit_code}\n' if stderr or exit_code else ""
    )
    (binder / "redis-cli").write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "--help" ]]; then\n'
        f'  printf "usage: redis-cli\\n{help_tls}"\n'
        "  exit 0\n"
        "fi\n"
        f"{fail_lines}"
        'case "$*" in\n'
        "  *'INFO server'*)\n"
        f"{version_line}\n"
        "    ;;\n"
        "esac\n"
    )
    (binder / "redis-cli").chmod(0o755)
    return binder


def _run(bindir: Path | None, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run the script with no boot wait unless a test opts in.

    ``REPLICATOR_REDIS_FLOOR_WAIT=0`` by default keeps every single-probe case
    at a single probe; the retry cases below set their own window.
    """
    path = f"{bindir}:/usr/bin:/bin" if bindir else "/usr/bin:/bin"
    return subprocess.run(
        ["bash", str(SCRIPT)],
        env={"PATH": path, "REPLICATOR_REDIS_FLOOR_WAIT": "0", **env},
        text=True,
        capture_output=True,
    )


def _flaky_redis_cli(
    tmp_path: Path, *, fail_first: int, stderr: str, version: str = "7.0.15"
) -> tuple[Path, Path]:
    """A `redis-cli` whose first `fail_first` calls fail with `stderr`, then answer.

    Returns the bin dir for PATH and the file counting the calls, so a test can
    assert how many probes the script made - the retry's whole contract.
    """
    binder = tmp_path / "bin"
    binder.mkdir()
    calls = tmp_path / "calls"
    (binder / "redis-cli").write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "--help" ]]; then\n'
        '  printf "usage: redis-cli\\n  --tls    Use TLS.\\n"; exit 0\n'
        "fi\n"
        f'n=$(( $(cat "{calls}" 2>/dev/null || echo 0) + 1 )); echo "$n" > "{calls}"\n'
        f'if [ "$n" -le {fail_first} ]; then printf "%b\\n" {stderr!r} >&2; exit 1; fi\n'
        f'case "$*" in *"INFO server"*) echo "redis_version:{version}" ;; esac\n'
    )
    (binder / "redis-cli").chmod(0o755)
    return binder, calls


def test_version_at_floor_passes(tmp_path: Path) -> None:
    bindir = _stub_redis_cli(tmp_path, version="7.0.15")
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})
    assert result.returncode == 0
    assert "meets the >=7.0 floor" in result.stdout


def test_version_below_floor_blocks(tmp_path: Path) -> None:
    """The one case where blocking is right: a version was actually read."""
    bindir = _stub_redis_cli(tmp_path, version="6.2.14")
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})
    assert result.returncode == 1
    assert "below the >=7.0 change-bus floor" in result.stderr


@pytest.mark.parametrize("message", [_WRONGPASS, _NOAUTH], ids=["wrongpass", "noauth"])
def test_auth_failure_is_reported_as_authentication(tmp_path: Path, message: str) -> None:
    """The operator must be sent to the credential, not to the network."""
    bindir = _stub_redis_cli(tmp_path, version=None, stderr=message, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert result.returncode == 0
    assert "authenticate" in result.stderr.lower()
    assert "unreachable" not in result.stderr.lower()


def test_auth_failure_names_the_empty_username_trap(tmp_path: Path) -> None:
    """`redis://:pw@host` authenticates for redis-py and fails for redis-cli:
    the latter sends a two-argument ``AUTH "" pw`` against a user that does not
    exist. So the worker starts green while this probe cannot connect at all."""
    bindir = _stub_redis_cli(tmp_path, version=None, stderr=_WRONGPASS, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert "default:" in result.stderr


def test_auth_failure_says_the_floor_is_unverified(tmp_path: Path) -> None:
    """A probe that cannot read the version has not *passed* the floor check,
    it has skipped it. Saying so is the difference between a guard known to be
    off and one assumed to be on."""
    bindir = _stub_redis_cli(tmp_path, version=None, stderr=_WRONGPASS, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert "unverified" in result.stderr.lower()


def test_auth_failure_does_not_block_the_start(tmp_path: Path) -> None:
    """Deliberately not blocking, against archiver#195's own suggestion.

    That suggestion rests on "a probe that cannot authenticate is evidence the
    service cannot either" - which is exactly what this bug disproves.
    `redis://:pw@host` fails for redis-cli and **succeeds for redis-py**, so the
    probe's verdict is not the worker's. Blocking on it would have converted a
    latent trap into a total outage at the cutover, for a URL that worked.
    """
    bindir = _stub_redis_cli(tmp_path, version=None, stderr=_WRONGPASS, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert result.returncode == 0


def test_unreachable_broker_is_still_reported_as_unreachable(tmp_path: Path) -> None:
    """The other half: a real connection failure must not blame the credential."""
    bindir = _stub_redis_cli(tmp_path, version=None, stderr=_REFUSED, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})

    assert result.returncode == 0
    assert "unreachable" in result.stderr.lower()
    assert "authenticate" not in result.stderr.lower()


# --- The boot wait (#88) ------------------------------------------------------
#
# Measured on co-replicator's cold boots: this check ran 0.2 s after tailscaled
# reached Running, and MagicDNS answered `broker` with no address (EAI_NODATA)
# for a moment longer. A wait for the tailnet *address* was tried and disproved
# - the address was local while the name still did not resolve - so the check
# retries the dependency it actually has: resolve and connect.

_NODATA = "Could not connect to Redis at broker:6379: No address associated with hostname"
_URL = {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"}


def _calls(counter: Path) -> int:
    return int(counter.read_text())


def test_a_broker_that_resolves_during_the_wait_is_verified(tmp_path: Path) -> None:
    bindir, calls = _flaky_redis_cli(tmp_path, fail_first=2, stderr=_NODATA)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "10"})

    assert result.returncode == 0
    assert "meets the >=7.0 floor" in result.stdout
    assert "reachable after" in result.stdout
    assert _calls(calls) == 3


def test_an_unreachable_broker_is_retried_until_the_wait_runs_out(tmp_path: Path) -> None:
    """Bounded, and still soft: the floor is reported UNVERIFIED, the start goes ahead."""
    bindir, calls = _flaky_redis_cli(tmp_path, fail_first=99, stderr=_NODATA)
    started = time.monotonic()
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "2"})

    assert result.returncode == 0
    assert "UNVERIFIED" in result.stderr
    assert _calls(calls) >= 2
    assert time.monotonic() - started < 10


def test_an_authentication_refusal_is_not_retried(tmp_path: Path) -> None:
    """The broker answered; waiting cannot change a wrong credential."""
    bindir, calls = _flaky_redis_cli(tmp_path, fail_first=99, stderr=_WRONGPASS)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "10"})

    assert result.returncode == 0
    assert _calls(calls) == 1


def test_a_silent_failure_is_not_retried(tmp_path: Path) -> None:
    """No stderr is what a timeout kill leaves - the probe already spent its timeout."""
    bindir, calls = _flaky_redis_cli(tmp_path, fail_first=99, stderr="")
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "10"})

    assert result.returncode == 0
    assert _calls(calls) == 1


def test_a_zero_wait_probes_once(tmp_path: Path) -> None:
    bindir, calls = _flaky_redis_cli(tmp_path, fail_first=99, stderr=_NODATA)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "0"})

    assert result.returncode == 0
    assert _calls(calls) == 1


def test_a_malformed_wait_is_named_and_the_default_used(tmp_path: Path) -> None:
    """An override that quietly did nothing would be worse than none."""
    bindir, calls = _flaky_redis_cli(tmp_path, fail_first=1, stderr=_NODATA)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "soon"})

    assert "REPLICATOR_REDIS_FLOOR_WAIT" in result.stderr
    assert "meets the >=7.0 floor" in result.stdout
    assert _calls(calls) == 2


def test_silent_failure_claims_neither_cause(tmp_path: Path) -> None:
    """A timeout kill leaves no stderr. With nothing to classify, the message
    must not guess - guessing is how the original line misled for days."""
    bindir = _stub_redis_cli(tmp_path, version=None, stderr="", exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})

    assert result.returncode == 0
    assert "could not reach or authenticate" in result.stderr.lower()


def test_broker_quote_excludes_the_cli_s_own_warning(tmp_path: Path) -> None:
    """redis-cli prints that advisory on every `-u` call, so quoting it back
    under "broker said:" buries the real error and blames the server for the
    client's warning. Observed live against the authenticated broker."""
    bindir = _stub_redis_cli(
        tmp_path,
        version=None,
        stderr=f"{_CLI_PASSWORD_WARNING}\n{_WRONGPASS}",
        exit_code=1,
    )
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert "may not be safe" not in result.stderr
    assert "WRONGPASS" in result.stderr


def test_redis_cli_absent_is_soft(tmp_path: Path) -> None:
    """No redis-cli on PATH at all -> cannot verify, do not block.

    An empty dir is not enough: `_run` appends /usr/bin:/bin, which is where
    redis-cli lives. Build a dir holding only the tools the script needs, so
    absence is simulated rather than assumed.
    """
    bindir = tmp_path / "nocli"
    bindir.mkdir()
    for tool in ("bash", "sed", "tr", "grep", "env", "mktemp", "timeout"):
        src = shutil.which(tool)
        if src:
            (bindir / tool).symlink_to(src)
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        env={"PATH": str(bindir), "REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"},
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0
    assert "redis-cli not found" in result.stderr


@pytest.mark.parametrize("tool", ["bash", "sed", "tr", "mktemp"])
def test_required_tools_exist(tool: str) -> None:
    """Guard against the stub-PATH tests silently passing because a tool the
    script relies on is missing from the environment."""
    assert shutil.which(tool) is not None
