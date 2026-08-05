"""The systemd unit's restart limiter must fit the worker's own failure timescale.

`replicator.service` and `Settings` encode two halves of one decision: how long
the worker absorbs a broker outage before exiting, and how many such exits the
unit tolerates before staying `failed`. Documented in both places, enforced
here — raising the ceiling without widening the window silently restores the
failure mode the pairing exists to prevent (a permanently unreachable Redis that
reads as `active (running)` forever).
"""

import re
from pathlib import Path

from src.core.config import Settings

UNIT = Path(__file__).resolve().parents[1] / "deploy" / "replicator.service"


def _directive(name: str) -> str:
    """The last value assigned to ``name`` in the unit (systemd's own semantics)."""
    matches = re.findall(rf"^{name}=(.*)$", UNIT.read_text(), flags=re.MULTILINE)
    assert matches, f"{name} is not set in {UNIT.name}"
    return matches[-1].strip()


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
