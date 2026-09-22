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

import pytest

from src.core.config import Settings
from src.worker.egress import RESOLVE_TIMEOUT_SECONDS

REPO_ROOT = Path(__file__).resolve().parents[1]
UNIT = REPO_ROOT / "deploy" / "replicator.service"
GUARD = REPO_ROOT / "scripts" / "check_main_checkout.sh"
FLOOR = REPO_ROOT / "scripts" / "check_redis_floor.sh"
NOTIFY_UNIT = REPO_ROOT / "deploy" / "replicator-failure-notify@.service"
NOTIFY = REPO_ROOT / "scripts" / "notify_failure.sh"

# systemd's DefaultTimeoutStartSec, which governs the unit because it sets no
# TimeoutStartSec of its own: every ExecStartPre shares this one budget.
DEFAULT_TIMEOUT_START_SEC = 90


def _directive(name: str, unit: Path = UNIT) -> str:
    """The last value assigned to ``name`` in ``unit`` (systemd's own semantics).

    ``unit`` defaults to the worker's, which is what every caller but the
    memory-protection test wants. That test asks the same question of both
    units, and a second copy of this parsing would be two spellings of one
    systemd rule, free to drift.
    """
    matches = re.findall(rf"^{name}=(.*)$", unit.read_text(), flags=re.MULTILINE)
    assert matches, f"{name} is not set in {unit.name}"
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


# The longest broker outage this cluster has actually had: 2026-09-16, degraded
# from ~14:28 UTC until redis-server answered again at 15:26:34 (CannObserv/broker#17,
# reported to us in #94). A number from an incident, not a guess — raise it when a
# worse one happens, and let that raise fail this test rather than pass silently.
WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS = 58 * 60


def test_the_unit_absorbs_the_worst_outage_this_cluster_has_had():
    """Surviving a *real* outage unaided, not merely tripping the limiter coherently.

    The test above is internal consistency — the window fits the exits. This one
    is the external fact #94 was filed about: `burst` exits have to add up to more
    wall-clock than the broker has ever actually been away, or the unit stays
    `failed` while the broker is still coming back and an operator is required.

    At the values #94 found (3 x ~10 min = 30 min) the 2026-09-16 outage was
    nearly twice the budget, and survived only by the accident of when continuous
    failure began — the worker did not start failing until ~15:00.
    """
    settings = Settings()
    burst = int(_directive("StartLimitBurst"))
    absorbed = burst * settings.worst_case_outage_seconds

    assert absorbed >= WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS, (
        f"the unit absorbs {absorbed / 60:.0f} min across {burst} starts, but this "
        f"cluster has had a {WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS / 60:.0f} min "
        "outage — it would have needed an operator"
    )


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
    """The #12, #7 and #100 terms.

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
        # The #100 term. The destination guard resolves ahead of httpx, so the
        # resolve sits outside the fetch's own timeout rather than inside its
        # connect phase. Five numbers now. One hop's resolve, as the fetch term
        # is one operation's timeout: httpx bounds operations, not a fetch (#104).
        + RESOLVE_TIMEOUT_SECONDS
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


# The unit bounds its restart loops and then stays `failed` on purpose, so the
# terminal state is by design — but a terminal state nobody is told about is the
# outage (#94). The unit's own comment claimed failure was visible "in systemctl
# status + OnFailure=" while carrying no OnFailure= directive at all: the comment
# was the only thing asserting it, and on 2026-09-16 the unit sat failed for 56
# minutes until a *sibling repo* noticed from the broker side. These assert the
# handler is wired, so the claim and the ini file cannot drift apart again.


def test_the_unit_names_an_onfailure_handler():
    """The terminal `failed` state has to page someone, not just sit in the journal."""
    assert _directive("OnFailure"), "OnFailure= is not set — a failed unit notifies nobody"


def test_the_onfailure_handler_is_the_notifier_template():
    """Wired to the template in ``deploy/``, instantiated with the failed unit's name.

    ``%n`` is what lets one handler serve any unit that points at it, and it is
    the only way the notification can name which unit failed — a handler that
    cannot say what broke is barely better than the journal line it replaces.
    """
    handler = _directive("OnFailure")

    assert NOTIFY_UNIT.exists(), f"{NOTIFY_UNIT.name} is missing from deploy/"
    assert handler.startswith("replicator-failure-notify@"), (
        f"expected the replicator-failure-notify@ template, got {handler!r}"
    )
    assert "%n" in handler, f"handler {handler!r} is not instantiated with %n"


def test_the_onfailure_handler_cannot_retrigger_itself():
    """A handler that can fail into itself turns one outage into an unbounded loop.

    systemd honours ``OnFailure=`` on the handler too, so the template must not
    carry one, and it must not restart: a notification is a one-shot attempt
    whose failure is logged and dropped, never retried into a second unit start.
    """
    text = NOTIFY_UNIT.read_text()

    assert not re.search(r"^OnFailure=", text, flags=re.MULTILINE), (
        f"{NOTIFY_UNIT.name} sets OnFailure=, so a failing notification would recurse"
    )
    assert re.search(r"^Type=oneshot$", text, flags=re.MULTILINE), (
        f"{NOTIFY_UNIT.name} must be Type=oneshot"
    )


def test_the_notify_script_is_wired_to_the_handler():
    """The template has to run the real script, not merely sit beside it."""
    assert NOTIFY.exists(), f"{NOTIFY.name} is missing from scripts/"
    assert NOTIFY.name in NOTIFY_UNIT.read_text(), (
        f"{NOTIFY_UNIT.name} does not invoke {NOTIFY.name}"
    )


def test_the_notify_handler_is_greppable_by_a_stable_identifier():
    """Retrieval must not depend on the instance name, which is not what anyone would guess.

    ``%n`` expands to the *full* unit name, suffix included, so
    ``OnFailure=replicator-failure-notify@%n.service`` instantiates as
    ``replicator-failure-notify@replicator.service.service`` — a doubled suffix.
    That is the canonical systemd idiom and it is kept, because ``%i`` is then the
    precise name of the unit that failed and that is what the record and the
    notification carry. The cost is that the obvious
    ``journalctl -u replicator-failure-notify@replicator.service`` finds nothing,
    which is a bad thing to discover mid-incident (observed in the #94 rehearsal).

    A ``SyslogIdentifier=`` pays that cost off: ``journalctl -t`` reaches the
    record whatever the instance is called.
    """
    text = NOTIFY_UNIT.read_text()

    assert re.search(r"^SyslogIdentifier=\S+$", text, flags=re.MULTILINE), (
        f"{NOTIFY_UNIT.name} sets no SyslogIdentifier, so the record is only reachable "
        "under a doubled-suffix unit name nobody would guess"
    )


def test_the_notify_handler_reads_the_production_env_file():
    """The notifier endpoint is configuration, so it lives where the unit's config lives.

    ``/etc/replicator/.env`` is the only file the service reads (AGENTS.md's env
    boundary), and it must be optional (``-``) so an absent file leaves the
    handler writing its journal record rather than failing to start.
    """
    text = NOTIFY_UNIT.read_text()

    assert re.search(r"^EnvironmentFile=-/etc/replicator/\.env$", text, flags=re.MULTILINE), (
        f"{NOTIFY_UNIT.name} must read /etc/replicator/.env, optionally"
    )


# The kernel's own ceiling: -1000 makes a process unkillable by the OOM killer
# altogether, which is a worse failure than the one being fixed — a worker
# leaking memory would then be unreclaimable and the kernel would work its way
# through everything else on the box first. -900 is the cohort's value
# (CannObserv/broker#25): last to be chosen, not exempt.
OOM_FLOOR = -1000
COHORT_OOM_SCORE_ADJUST = -900


@pytest.mark.parametrize("unit", [UNIT, NOTIFY_UNIT], ids=lambda p: p.name)
def test_the_production_units_outrank_dev_tooling_for_the_oom_killer(unit: Path):
    """This VM's OOM killer must reach the worker last, not first.

    Everything descended from an exe.dev session inherits ``oom_score_adj=-1000``
    from ``exe-init`` and ``sshd``: VSCode Server, Claude Code, and any MCP
    server they start. **-1000 is ineligibility, not a low score** — the kernel
    skips such a process entirely, and 28 of them were counted here. So this
    directive was never going to win a comparison against the dev tooling; what
    it changes is the worker's rank among the processes that *can* be chosen.

    Measured on this VM while adopting the shared SocratiCode index (#92): the
    worker read ``oom_score`` 670 at the default adj of 0 — second from the top
    of the eligible list — and 72 at -900, which is the bottom of it.

    Two things that rank does not buy, both recorded in docs/DEPLOYMENT.md so
    the doc and this test say the same thing. It is no substitute for capping
    whatever launches a SocratiCode server, since a cgroup cap on a process at
    -1000 stalls it rather than killing it. And the process now at the top of
    the eligible list is ``tailscaled`` at 675 — which is what degraded in
    CannObserv/broker#17, a 57-minute bus outage with nothing OOM-killed at all:
    the kernel failed *atomic* allocations while every process stayed alive, and
    this worker did not reconnect on its own (#94).

    ``earlyoom`` does not close it either: it floors a ``--prefer`` match at 300,
    and a service at adj 0 reads ~670 here, so it too would choose the worker.
    """
    assert re.search(r"^OOMScoreAdjust=", unit.read_text(), flags=re.MULTILINE), (
        f"{unit.name} sets no OOMScoreAdjust, so it sits at the default 0 and reads "
        "~670 — second from the top of this VM's eligible list"
    )
    adjust = int(_directive("OOMScoreAdjust", unit))
    assert adjust <= COHORT_OOM_SCORE_ADJUST, (
        f"{unit.name} sets OOMScoreAdjust={adjust}, which does not outrank dev tooling"
    )
    assert adjust > OOM_FLOOR, (
        f"{unit.name} sets OOMScoreAdjust={adjust} — exempt from the OOM killer entirely, "
        "so a leak here would be unreclaimable"
    )
