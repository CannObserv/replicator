"""Behaviour of scripts/wait_for_tailnet_addr.sh - the boot wait for this host's tailnet address.

Run as an `ExecStartPre` on replicator.service, ahead of `check_redis_floor.sh`
(#88). `After=tailscaled.service` orders against tailscaled *starting*, not
against it *running*: the with-service reboot test on 2026-09-11 put the floor
check ~1 s before MagicDNS could resolve `broker`, and it reported the floor
UNVERIFIED on a cold boot. The wait closes that gap.

Carried from CannObserv/broker's `deploy/wait-for-tailnet-addr.sh`: probe
`/proc/net/fib_trie`, never `ip addr` (CannObserv/observo#479), and match the
address as `/32 host LOCAL`, never as bare digits a peer's route could share.
One deliberate difference, pinned below: the address is matched whole, so
waiting for `100.114.136.2` is not satisfied by a local `100.114.136.20`.

Driven against fixture fib_trie files via the script's third argument, so no
tailnet is needed and each branch runs deterministically.
"""

import subprocess
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "wait_for_tailnet_addr.sh"
ADDR = "100.114.136.20"

# The shape of this host's /proc/net/fib_trie, trimmed to what the probe reads.
_LOCAL = f"""Main:
  +-- 0.0.0.0/0 3 0 5
     |-- 0.0.0.0
        /0 universe UNICAST
Local:
  +-- 0.0.0.0/0 3 0 5
     +-- 10.42.0.0/16 2 0 2
        |-- 10.42.0.42
           /32 host LOCAL
     |-- {ADDR}
        /32 host LOCAL
     +-- 127.0.0.0/8 2 0 2
        |-- 127.0.0.1
           /32 host LOCAL
"""

# Before tailscaled assigns the address: the rest of the table is already there.
_ABSENT = """Main:
  +-- 0.0.0.0/0 3 0 5
     |-- 0.0.0.0
        /0 universe UNICAST
Local:
  +-- 0.0.0.0/0 3 0 5
     +-- 10.42.0.0/16 2 0 2
        |-- 10.42.0.42
           /32 host LOCAL
"""


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["bash", str(SCRIPT), *args], text=True, capture_output=True)


def _fib(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "fib_trie"
    path.write_text(text)
    return path


def test_an_assigned_address_returns_at_once(tmp_path: Path) -> None:
    result = _run(ADDR, "1", str(_fib(tmp_path, _LOCAL)))

    assert result.returncode == 0
    assert result.stdout == ""  # nothing to report on the ordinary boot


def test_a_route_to_the_address_is_not_the_address(tmp_path: Path) -> None:
    """The digits can appear without being ours; only `host LOCAL` counts."""
    as_a_route = _ABSENT.replace("Local:", f"     |-- {ADDR}\n        /32 link UNICAST\nLocal:")
    result = _run(ADDR, "1", str(_fib(tmp_path, as_a_route)))

    assert result.returncode == 1


def test_a_longer_local_address_does_not_satisfy_a_shorter_one(tmp_path: Path) -> None:
    """broker's copy matches ``|-- <addr>`` as a prefix; this one matches it whole."""
    result = _run("100.114.136.2", "1", str(_fib(tmp_path, _LOCAL)))

    assert result.returncode == 1


def test_an_address_assigned_during_the_wait_is_waited_for(tmp_path: Path) -> None:
    fib = _fib(tmp_path, _ABSENT)
    wait = subprocess.Popen(
        ["bash", str(SCRIPT), ADDR, "10", str(fib)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(1.2)
    fib.write_text(_LOCAL)
    out, _ = wait.communicate(timeout=15)

    assert wait.returncode == 0
    assert f"tailnet address {ADDR} present after" in out


def test_a_timeout_names_the_address_and_exits_nonzero(tmp_path: Path) -> None:
    """Nonzero so the journal shows a failed step; the unit's `-` keeps it from blocking."""
    result = _run(ADDR, "1", str(_fib(tmp_path, _ABSENT)))

    assert result.returncode == 1
    assert f"tailnet address {ADDR} not assigned after 1s" in result.stderr


def test_an_unreadable_table_times_out_rather_than_crashing(tmp_path: Path) -> None:
    result = _run(ADDR, "1", str(tmp_path / "missing"))

    assert result.returncode == 1
    assert "not assigned" in result.stderr


def test_an_address_is_required() -> None:
    result = _run()

    assert result.returncode != 0
    assert "usage" in result.stderr
