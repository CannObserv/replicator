"""Behaviour of scripts/check_redis_floor.sh - the broker precondition guard.

Run as an `ExecStartPre` on replicator.service. One blocking assertion: the
server must be >= 7.2. 7.0 is where `XAUTOCLAIM`'s three-element reply arrived, which
`AsyncBusConsumer.claim_stale_page` reads; 7.2 is the broker's engine, and since #256
the only one anything here runs against.

Otherwise soft by design - exit 0 (letting the worker start) when the broker is
unreachable. These tests drive it with a stub `valkey-cli` on a PATH that holds
no real client (`tests.conftest.client_free_bin`, #256), so no live broker is
required, none is ever dialled, and each branch is exercised deterministically.

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

from tests.conftest import client_free_bin

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_redis_floor.sh"

_WRONGPASS = "AUTH failed: WRONGPASS invalid username-password pair or user is disabled."
_NOAUTH = "NOAUTH Authentication required."
_REFUSED = "Could not connect to Valkey at broker:6379: Connection refused"


def _stub_client(
    tmp_path: Path,
    *,
    version: str | None,
    name: str = "valkey-cli",
    tls: bool = True,
    stderr: str = "",
    exit_code: int = 0,
    info: tuple[str, ...] = (),
) -> Path:
    """Write a fake client called `name` to a client-free bin dir; return it for PATH.

    `--help` output includes `--tls` iff `tls`. `INFO server` prints a
    `redis_version:` line iff `version` is given, then the `info` lines. `stderr`
    and `exit_code` model the failures that produce no stdout at all - before
    archiver#195 they were indistinguishable from each other.
    """
    binder = client_free_bin(tmp_path / "bin")
    help_tls = "  --tls    Use TLS.\n" if tls else ""
    version_line = f'  echo "redis_version:{version}"' if version is not None else "  true"
    version_line += "".join(f'\n  echo "{line}"' for line in info)
    # %b, not %s: a multi-line `stderr` arrives here as a repr with an escaped
    # newline, and only %b expands it back into two lines.
    fail_lines = (
        f'  printf "%b\\n" {stderr!r} >&2\n  exit {exit_code}\n' if stderr or exit_code else ""
    )
    (binder / name).write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "--help" ]]; then\n'
        f'  printf "usage: {name}\\n{help_tls}"\n'
        "  exit 0\n"
        "fi\n"
        f"{fail_lines}"
        'case "$*" in\n'
        "  *'INFO server'*)\n"
        f"{version_line}\n"
        "    ;;\n"
        "esac\n"
    )
    (binder / name).chmod(0o755)
    return binder


def _run(bindir: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run the script with no boot wait unless a test opts in.

    ``REPLICATOR_REDIS_FLOOR_WAIT=0`` by default keeps every single-probe case
    at a single probe; the retry cases below set their own window.
    """
    return subprocess.run(
        [shutil.which("bash"), str(SCRIPT)],
        env={"PATH": str(bindir), "REPLICATOR_REDIS_FLOOR_WAIT": "0", **env},
        text=True,
        capture_output=True,
    )


def _flaky_client(
    tmp_path: Path, *, fail_first: int, stderr: str, version: str = "7.2.4"
) -> tuple[Path, Path]:
    """A `valkey-cli` whose first `fail_first` calls fail with `stderr`, then answer.

    Returns the bin dir for PATH and the file counting the calls, so a test can
    assert how many probes the script made - the retry's whole contract.
    """
    binder = client_free_bin(tmp_path / "bin")
    calls = tmp_path / "calls"
    (binder / "valkey-cli").write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "--help" ]]; then\n'
        '  printf "usage: valkey-cli\\n  --tls    Use TLS.\\n"; exit 0\n'
        "fi\n"
        f'n=$(( $(cat "{calls}" 2>/dev/null || echo 0) + 1 )); echo "$n" > "{calls}"\n'
        f'if [ "$n" -le {fail_first} ]; then printf "%b\\n" {stderr!r} >&2; exit 1; fi\n'
        f'case "$*" in *"INFO server"*) echo "redis_version:{version}" ;; esac\n'
    )
    (binder / "valkey-cli").chmod(0o755)
    return binder, calls


@pytest.mark.parametrize("version", ["7.2.0", "7.2.4", "7.10.1", "8.0.1"])
def test_version_at_or_above_the_floor_passes(tmp_path: Path, version: str) -> None:
    bindir = _stub_client(tmp_path, version=version)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})
    assert result.returncode == 0
    assert f"Redis {version} meets the >=7.2 floor" in result.stdout


def test_valkey_is_judged_by_its_redis_version(tmp_path: Path) -> None:
    """#251: since broker#85 the broker is Ubuntu's Valkey 7.2.14, whose `INFO server`
    carries two versions. The floor reads `redis_version`, the Redis compatibility
    level Valkey reports (7.2.4, measured), never `valkey_version` (7.2.14), and the
    line says `Redis 7.2.4`: the level the floor is defined against, not the engine.
    The #243 deploy's `ExecStartPre` printed this line; #256 appended the client.
    """
    bindir = _stub_client(
        tmp_path, version="7.2.4", info=("server_name:valkey", "valkey_version:7.2.14")
    )
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})

    assert result.returncode == 0
    assert result.stdout == (
        "check_redis_floor: Redis 7.2.4 meets the >=7.2 floor (via valkey-cli)\n"
    )


@pytest.mark.parametrize("version", ["6.2.14", "7.0.15", "7.1.0"])
def test_version_below_floor_blocks(tmp_path: Path, version: str) -> None:
    """The one case where blocking is right: a version was actually read.

    7.0 and 7.1 now block too (#256): they serve `XAUTOCLAIM`, but no engine below
    7.2 is tested here any more, so a broker downgraded to one is running a worker
    nobody has measured against it.
    """
    bindir = _stub_client(tmp_path, version=version)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})
    assert result.returncode == 1
    assert f"Redis {version} is below the >=7.2 floor" in result.stderr


@pytest.mark.parametrize("version", ["7", "seven", "7.x"])
def test_a_version_it_cannot_compare_blocks(tmp_path: Path, version: str) -> None:
    """A version was read, so this is not the soft "unverified" case; and a floor
    that waved through what it could not parse would be no floor."""
    bindir = _stub_client(tmp_path, version=version)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})
    assert result.returncode == 1
    assert "below the >=7.2 floor" in result.stderr


@pytest.mark.parametrize("message", [_WRONGPASS, _NOAUTH], ids=["wrongpass", "noauth"])
def test_auth_failure_is_reported_as_authentication(tmp_path: Path, message: str) -> None:
    """The operator must be sent to the credential, not to the network."""
    bindir = _stub_client(tmp_path, version=None, stderr=message, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert result.returncode == 0
    assert "authenticate" in result.stderr.lower()
    assert "unreachable" not in result.stderr.lower()


def test_auth_failure_sends_the_operator_to_the_credential(tmp_path: Path) -> None:
    """Since #127 this probe authenticates exactly as the worker does, so a
    refusal here is the worker's refusal too - not the old empty-username trap,
    which the helper's `AUTH` construction removed."""
    bindir = _stub_client(tmp_path, version=None, stderr=_WRONGPASS, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://replicator:pw@broker:6379/0"})

    assert "REPLICATOR_REDIS_URL" in result.stderr
    assert "default:" not in result.stderr


def test_auth_failure_says_the_floor_is_unverified(tmp_path: Path) -> None:
    """A probe that cannot read the version has not *passed* the floor check,
    it has skipped it. Saying so is the difference between a guard known to be
    off and one assumed to be on."""
    bindir = _stub_client(tmp_path, version=None, stderr=_WRONGPASS, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert "unverified" in result.stderr.lower()


def test_auth_failure_does_not_block_the_start(tmp_path: Path) -> None:
    """Deliberately not blocking, against archiver#195's own suggestion.

    The worker meets the same refusal and reports it in its own terms; this
    guard asserts the version floor only. Blocking would spend the unit's start
    budget on a credential problem - the shape of 2026-09-16, when three
    ExecStartPre refusals burned StartLimitBurst in sixteen seconds.
    """
    bindir = _stub_client(tmp_path, version=None, stderr=_WRONGPASS, exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://:pw@broker:6379/0"})

    assert result.returncode == 0


def test_unreachable_broker_is_still_reported_as_unreachable(tmp_path: Path) -> None:
    """The other half: a real connection failure must not blame the credential."""
    bindir = _stub_client(tmp_path, version=None, stderr=_REFUSED, exit_code=1)
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

_NODATA = "Could not connect to Valkey at broker:6379: No address associated with hostname"
_URL = {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"}


def _calls(counter: Path) -> int:
    return int(counter.read_text())


def test_a_broker_that_resolves_during_the_wait_is_verified(tmp_path: Path) -> None:
    bindir, calls = _flaky_client(tmp_path, fail_first=2, stderr=_NODATA)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "10"})

    assert result.returncode == 0
    assert "meets the >=7.2 floor" in result.stdout
    assert "reachable after" in result.stdout
    assert _calls(calls) == 3


def test_an_unreachable_broker_is_retried_until_the_wait_runs_out(tmp_path: Path) -> None:
    """Bounded, and still soft: the floor is reported UNVERIFIED, the start goes ahead."""
    bindir, calls = _flaky_client(tmp_path, fail_first=99, stderr=_NODATA)
    started = time.monotonic()
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "2"})

    assert result.returncode == 0
    assert "UNVERIFIED" in result.stderr
    assert _calls(calls) >= 2
    assert time.monotonic() - started < 10


def test_an_authentication_refusal_is_not_retried(tmp_path: Path) -> None:
    """The broker answered; waiting cannot change a wrong credential."""
    bindir, calls = _flaky_client(tmp_path, fail_first=99, stderr=_WRONGPASS)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "10"})

    assert result.returncode == 0
    assert _calls(calls) == 1


def test_a_silent_failure_is_not_retried(tmp_path: Path) -> None:
    """No stderr is what a timeout kill leaves - the probe already spent its timeout."""
    bindir, calls = _flaky_client(tmp_path, fail_first=99, stderr="")
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "10"})

    assert result.returncode == 0
    assert _calls(calls) == 1


def test_a_zero_wait_probes_once(tmp_path: Path) -> None:
    bindir, calls = _flaky_client(tmp_path, fail_first=99, stderr=_NODATA)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "0"})

    assert result.returncode == 0
    assert _calls(calls) == 1


def test_a_malformed_wait_is_named_and_the_default_used(tmp_path: Path) -> None:
    """An override that quietly did nothing would be worse than none."""
    bindir, calls = _flaky_client(tmp_path, fail_first=1, stderr=_NODATA)
    result = _run(bindir, {**_URL, "REPLICATOR_REDIS_FLOOR_WAIT": "soon"})

    assert "REPLICATOR_REDIS_FLOOR_WAIT" in result.stderr
    assert "meets the >=7.2 floor" in result.stdout
    assert _calls(calls) == 2


def test_silent_failure_claims_neither_cause(tmp_path: Path) -> None:
    """A timeout kill leaves no stderr. With nothing to classify, the message
    must not guess - guessing is how the original line misled for days."""
    bindir = _stub_client(tmp_path, version=None, stderr="", exit_code=1)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})

    assert result.returncode == 0
    assert "could not reach or authenticate" in result.stderr.lower()


# --- The credential stays off argv (#127) -------------------------------------
#
# This probe runs on every service start, and `/proc/<pid>/cmdline` is readable
# by every local user while it does. `timeout` is in the argv chain too, which
# is why both paths are covered: the stub sees what `timeout` was handed.

_SECRET = "s3cr3t"


def _recording_bindir(tmp_path: Path, *, with_timeout: bool) -> Path:
    """Only the tools the script needs, plus a `valkey-cli` that records argv and auth."""
    binder = client_free_bin(tmp_path / "bin")
    if not with_timeout:
        (binder / "timeout").unlink()
    (binder / "valkey-cli").write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "--help" ]]; then printf "usage: valkey-cli\\n  --tls\\n"; exit 0; fi\n'
        f'printf "%s\\n" "$@" >> "{tmp_path}/argv"\n'
        f'printf "%s" "${{REDISCLI_AUTH-unset}}" > "{tmp_path}/auth"\n'
        'echo "redis_version:7.2.4"\n'
    )
    (binder / "valkey-cli").chmod(0o755)
    return binder


@pytest.mark.parametrize("with_timeout", [True, False], ids=["timeout", "no-timeout"])
def test_the_probe_keeps_the_password_off_argv(tmp_path: Path, with_timeout: bool) -> None:
    bindir = _recording_bindir(tmp_path, with_timeout=with_timeout)
    result = subprocess.run(
        [shutil.which("bash"), str(SCRIPT)],
        env={
            "PATH": str(bindir),
            "REPLICATOR_REDIS_FLOOR_WAIT": "0",
            "REPLICATOR_REDIS_URL": f"redis://replicator:{_SECRET}@broker:6379/0",
        },
        text=True,
        capture_output=True,
    )

    assert "meets the >=7.2 floor" in result.stdout, result.stderr
    argv = (tmp_path / "argv").read_text().splitlines()
    assert argv == ["--user", "replicator", "-u", "redis://broker:6379/0", "INFO", "server"]
    assert (tmp_path / "auth").read_text() == _SECRET


def test_a_url_with_no_scheme_is_named_and_never_probed(tmp_path: Path) -> None:
    bindir = _recording_bindir(tmp_path, with_timeout=True)
    result = subprocess.run(
        [shutil.which("bash"), str(SCRIPT)],
        env={
            "PATH": str(bindir),
            "REPLICATOR_REDIS_FLOOR_WAIT": "0",
            "REPLICATOR_REDIS_URL": f"replicator:{_SECRET}@broker:6379/0",
        },
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert "scheme" in result.stderr
    assert "UNVERIFIED" in result.stderr
    assert _SECRET not in result.stderr
    assert not (tmp_path / "argv").exists()


def test_a_missing_helper_is_named_not_guessed_at(tmp_path: Path) -> None:
    """The probe cannot run without scripts/rcli.sh, and saying "timed out?"
    instead would be the wrong-cause guess archiver#195 removed. Still soft."""
    lone = tmp_path / "lone"
    lone.mkdir()
    shutil.copy(SCRIPT, lone / SCRIPT.name)
    bindir = _stub_client(tmp_path, version="7.2.4")
    result = subprocess.run(
        [shutil.which("bash"), str(lone / SCRIPT.name)],
        env={"PATH": str(bindir), "REPLICATOR_REDIS_FLOOR_WAIT": "0", **_URL},
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert "rcli.sh" in result.stderr
    assert "UNVERIFIED" in result.stderr
    assert "timed out" not in result.stderr


def test_a_helper_that_will_not_load_is_not_called_missing(tmp_path: Path) -> None:
    """A syntax error is not an absence; bash's own message says which it was."""
    lone = tmp_path / "lone"
    lone.mkdir()
    shutil.copy(SCRIPT, lone / SCRIPT.name)
    (lone / "rcli.sh").write_text("rcli_command() {\n")
    bindir = _stub_client(tmp_path, version="7.2.4")
    result = subprocess.run(
        [shutil.which("bash"), str(lone / SCRIPT.name)],
        env={"PATH": str(bindir), "REPLICATOR_REDIS_FLOOR_WAIT": "0", **_URL},
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert "could not be loaded" in result.stderr
    assert "syntax error" in result.stderr
    assert "UNVERIFIED" in result.stderr


def test_no_client_at_all_is_soft_and_unverified(tmp_path: Path) -> None:
    """Neither valkey-cli nor redis-cli -> cannot verify, do not block (#256 kept it soft).

    The worker needs no CLI, so a client lost to an autoremove would otherwise stop a
    healthy worker at its next start. `apt-mark manual valkey-tools` is the protection;
    this line is how the gap shows.
    """
    bindir = client_free_bin(tmp_path / "bin")
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})

    assert result.returncode == 0
    assert "neither valkey-cli nor redis-cli" in result.stderr
    assert "UNVERIFIED" in result.stderr


def test_the_success_line_names_the_client_it_read_with(tmp_path: Path) -> None:
    """The journal is where a deploy proves which client the release used (#256)."""
    bindir = _stub_client(tmp_path, version="7.2.4", name="redis-cli")
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "redis://default:pw@broker:6379/0"})

    assert result.returncode == 0
    assert result.stdout == (
        "check_redis_floor: Redis 7.2.4 meets the >=7.2 floor (via redis-cli)\n"
    )


def test_a_tls_url_probes_the_resolved_client_for_tls(tmp_path: Path) -> None:
    """The --tls probe asks the client that will connect, not a fixed name."""
    bindir = _stub_client(tmp_path, version="7.2.4", tls=False)
    result = _run(bindir, {"REPLICATOR_REDIS_URL": "rediss://default:pw@broker:6379/0"})

    assert "valkey-cli lacks TLS support" in result.stderr


@pytest.mark.parametrize("tool", ["bash", "sed", "tr", "mktemp"])
def test_required_tools_exist(tool: str) -> None:
    """Guard against the stub-PATH tests silently passing because a tool the
    script relies on is missing from the environment."""
    assert shutil.which(tool) is not None
