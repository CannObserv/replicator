"""The rehearsal's broker reaping, which must end even when Valkey ignores SIGTERM.

`scripts/rehearse_reconnect.sh` runs as root and is never run by the suite, so its
one piece of process control is lifted out and run here alone. Valkey aborts a
shutdown whose final AOF write fails (#256): an unbounded ``wait`` after the TERM
then hangs the script in its EXIT trap, and the Ctrl-C that ends the hang orphans
the broker the trap was there to stop.
"""

import re
import shutil
import subprocess
import time
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "rehearse_reconnect.sh"


def _function(name: str) -> str:
    match = re.search(rf"^{name}\(\) \{{\n.*?^\}}\n", SCRIPT.read_text(), re.M | re.S)
    assert match, f"{name} is not defined in {SCRIPT.name}"
    return match.group(0)


def test_a_broker_that_ignores_term_is_killed_after_the_grace() -> None:
    driver = (
        _function("_reap_broker")
        + "bash -c 'trap \"\" TERM; while :; do sleep 0.1; done' &\n"
        + "child=$!\nsleep 0.3\n"
        + '_reap_broker "${child}" 5\n'
        + 'kill -0 "${child}" 2>/dev/null && echo alive || echo gone\n'
    )
    started = time.monotonic()
    result = subprocess.run(
        [shutil.which("bash"), "-c", driver], capture_output=True, text=True, timeout=20
    )

    assert result.stdout.strip() == "gone"
    assert time.monotonic() - started < 5


def test_every_stop_goes_through_the_bounded_reap() -> None:
    for name in ("cleanup", "stop_broker"):
        body = _function(name)
        assert "_reap_broker" in body, f"{name} stops the broker without the bounded reap"
        assert 'wait "${BROKER_PID}"' not in body, f"{name} still waits unbounded"


def test_a_broker_that_honours_term_is_not_held_for_the_grace() -> None:
    """Each outage step stops the broker; a full grace per stop would skew its timings."""
    driver = (
        _function("_reap_broker")
        + "sleep 60 &\nchild=$!\n"
        + '_reap_broker "${child}"\n'
        + 'kill -0 "${child}" 2>/dev/null && echo alive || echo gone\n'
    )
    started = time.monotonic()
    result = subprocess.run(
        [shutil.which("bash"), "-c", driver], capture_output=True, text=True, timeout=20
    )

    assert result.stdout.strip() == "gone"
    assert time.monotonic() - started < 2
