"""The systemd unit's restart limiter must fit the worker's own failure timescale.

`replicator.service` and `Settings` encode two halves of one decision: how long
the worker absorbs a broker outage before exiting, and how many such exits the
unit tolerates before staying `failed`. Documented in both places, enforced
here — raising the ceiling without widening the window silently restores the
failure mode the pairing exists to prevent (a permanently unreachable Redis that
reads as `active (running)` forever).

The second half covers the *wiring* of the preflight that keeps the unit honest
about which code it starts: `scripts/check_main_checkout.sh` (#37) is invoked, is
unprefixed, and runs ahead of the `BUILD_ID` stamp. All three are properties of
this ini file, reached by parsing it.

The unit is also ordered behind `tailscaled` — ordering, never dependency —
because the broker it consumes is reached by tailnet name (#88).

Whether the guard then *decides* correctly is a different concern reached by a
different mechanism — a real process against throwaway repositories — and lives
in `tests/test_check_main_checkout.py`, per `docs/TESTING.md`'s split-by-concern
rule.
"""

import re
from pathlib import Path

from src.core.config import Settings

REPO_ROOT = Path(__file__).resolve().parents[1]
UNIT = REPO_ROOT / "deploy" / "replicator.service"
GUARD = REPO_ROOT / "scripts" / "check_main_checkout.sh"
FLOOR = REPO_ROOT / "scripts" / "check_redis_floor.sh"

# systemd's DefaultTimeoutStartSec, which governs the unit because it sets no
# TimeoutStartSec of its own: every ExecStartPre shares this one budget.
DEFAULT_TIMEOUT_START_SEC = 90


def _directive(name: str) -> str:
    """The last value assigned to ``name`` in the unit (systemd's own semantics)."""
    matches = re.findall(rf"^{name}=(.*)$", UNIT.read_text(), flags=re.MULTILINE)
    assert matches, f"{name} is not set in {UNIT.name}"
    return matches[-1].strip()


def _exec_start_pre() -> list[str]:
    """Every ``ExecStartPre`` value, in the order systemd will run them.

    Unlike :func:`_directive` this keeps all of them: ``ExecStartPre`` is a list
    directive, and both the fatality of one entry and the relative order of two
    are the properties under test.
    """
    values = [
        value.strip()
        for value in re.findall(r"^ExecStartPre=(.*)$", UNIT.read_text(), flags=re.MULTILINE)
    ]
    assert values, f"ExecStartPre is not set in {UNIT.name}"
    return values


def _guard_step() -> str:
    """The single ``ExecStartPre`` that runs the main-checkout guard."""
    matches = [value for value in _exec_start_pre() if GUARD.name in value]
    assert len(matches) == 1, f"expected exactly one {GUARD.name} ExecStartPre, got {matches}"
    return matches[0]


def _unit_list(name: str) -> list[str]:
    """Every unit named by a list directive like ``After=``, in systemd's semantics.

    Each assignment appends its whitespace-separated names, and an empty
    assignment resets the list — so a later ``After=`` line can silently drop
    what an earlier one declared.
    """
    units: list[str] = []
    for value in re.findall(rf"^{name}=(.*)$", UNIT.read_text(), flags=re.MULTILINE):
        units = units + value.split() if value.strip() else []
    return units


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
    """The #12 and #7 terms.

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
        # The #7 term. Storage runs inside ``asyncio.to_thread``, which puts it
        # beyond cancellation exactly as the sweep is, so SIGTERM waits out an
        # upload in flight. Added when the object-store backend made this a
        # network round trip rather than a write to local disk (CR #5) — the
        # docstring's "three separately-reasonable numbers" became four, which
        # is the failure it predicted.
        + settings.blob_timeout_seconds
    )

    assert timeout_stop > worst_case


# --- Ordering behind the tailnet (#88) ---------------------------------------
#
# The broker is reached as `broker` over the tailnet, so at boot the worker and
# its floor check both need tailscaled. Ordering, never dependency: the
# worker's backoff already absorbs a slow tailnet, and what this buys is the
# floor check seeing the broker instead of reporting it UNVERIFIED.


def test_the_unit_starts_after_tailscaled():
    """Without it the floor check races the tailnet at every boot, and loses quietly."""
    assert "tailscaled.service" in _unit_list("After")


def _floor_default(variable: str) -> int:
    """The default ``check_redis_floor.sh`` gives ``variable`` (``${VAR:-N}``)."""
    match = re.search(rf"\${{{variable}:-(\d+)}}", FLOOR.read_text())
    assert match, f"{FLOOR.name} has no numeric default for {variable}"
    return int(match.group(1))


def test_the_floor_checks_boot_wait_leaves_most_of_the_start_budget():
    """``After=`` alone was measured insufficient (#88), so the floor check now
    waits out an unreachable broker itself — inside the start.

    Every ExecStartPre shares one TimeoutStartSec, and the wheelhouse sync that
    follows goes to the network. A wait that could eat the budget would turn a
    slow tailnet into a start timeout: a failure, counted against the three
    starts an hour. The last probe can begin just before the wait expires, so
    the bound is the wait plus one probe's timeout.
    """
    worst = _floor_default("REPLICATOR_REDIS_FLOOR_WAIT") + _floor_default(
        "REPLICATOR_REDIS_FLOOR_TIMEOUT"
    )

    assert worst <= DEFAULT_TIMEOUT_START_SEC / 2
    assert "TimeoutStartSec" not in UNIT.read_text(), (
        "the unit now sets TimeoutStartSec; compare against it instead of the default"
    )


def test_the_unit_does_not_depend_on_tailscaled():
    """``After=`` orders; these directives also *propagate*, and that is the hazard.

    ``Requires=``/``BindsTo=``/``PartOf=`` carry tailscaled's stops and restarts
    through to the worker — an apt upgrade of tailscale would restart it and
    spend one of the unit's three starts an hour. ``Wants=``/``Requisite=``
    are the dependency trap the unit's own comment block records for the
    retired redis-server ordering.
    """
    for name in ("Wants", "Requires", "Requisite", "BindsTo", "PartOf"):
        assert "tailscaled.service" not in _unit_list(name), (
            f"{name}=tailscaled.service makes the worker's lifecycle follow tailscaled's"
        )


# --- The main-checkout guard (#37) -------------------------------------------
#
# "Code committed to main is the deployed code" is the invariant AGENTS.md states
# and nothing enforced until #37. These assert the unit actually consults the
# guard, then that the guard actually decides correctly.


def test_the_unit_runs_the_main_checkout_guard():
    """The guard has to be wired in, not merely present in ``scripts/``."""
    assert GUARD.exists(), f"{GUARD.name} is missing"
    assert GUARD.name in "\n".join(_exec_start_pre())


def test_the_main_checkout_guard_is_fatal():
    """A ``-``-prefixed guard is not a guard — its refusal would be logged and ignored."""
    step = _guard_step()

    assert not step.startswith("-"), (
        f"{GUARD.name} is '-' prefixed, so its refusal would not stop the start"
    )
    # `-` is the prefix that matters, but asserting on an absolute path rules out
    # every systemd prefix character at once (`-`, `@`, `:`, `+`, `!`, `!!`), so a
    # future edit cannot weaken this by reaching for a different one.
    assert step.startswith("/"), f"expected an unprefixed absolute path, got {step!r}"


def test_the_main_checkout_guard_runs_before_the_build_id_stamp():
    """A refused start must not leave a misleading build id behind in /run.

    ``/run/replicator/build-id`` outlives the failed start (nothing removes it),
    so stamping the branch SHA first would leave the journal describing code that
    never ran — the same "looks correct, is not" failure #37 exists to close.
    """
    steps = _exec_start_pre()
    guard_at = next(i for i, step in enumerate(steps) if GUARD.name in step)
    stamp_at = next(i for i, step in enumerate(steps) if "build-id" in step)

    assert guard_at < stamp_at, (
        f"{GUARD.name} runs at ExecStartPre #{guard_at}, after the BUILD_ID stamp at #{stamp_at}"
    )
