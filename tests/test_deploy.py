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

import os
import re
import socket
import subprocess
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
    memory-protection tests wants. Those ask the same question of the handler
    unit and of the drop-ins (#113), and a second copy of this parsing would be
    two spellings of one systemd rule, free to drift.
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


def test_the_stop_timeout_outlasts_the_slowest_fetch_there_can_be():
    """The second half of the #11 pairing, and since #104 a real bound.

    A command carries its own ``timeout_seconds``, so the handler's budget stopped
    being the driver's fixed 30s at #11. Until #104 this summed
    ``REPLICATOR_MAX_FETCH_TIMEOUT_SECONDS``, which bounds one httpx *operation*,
    not a fetch — a trickling body or a chain of redirects ran past it, and a
    deploy SIGKILLed that fetch partway. ``REPLICATOR_MAX_FETCH_SECONDS`` bounds the
    whole of it (``test_handler_deadline.py``), so it is the number summed here.
    A poll that starts just before SIGTERM can cost a full read window *plus* a
    full fetch, and a grace period shorter than the sum SIGKILLs the worker
    mid-message on every deploy that lands during a slow fetch — turning a
    routine restart into a stale-claim round-trip.

    Strictly greater, not equal: the sweep is a third term this cannot quantify
    (it rides an uncancellable ``asyncio.to_thread``), so the margin is where it
    lives.
    """
    settings = Settings()
    timeout_stop = float(_directive("TimeoutStopSec"))

    assert timeout_stop > settings.read_block_ms / 1000 + settings.max_fetch_seconds


def test_the_stop_timeout_absorbs_a_pacing_wait_as_well():
    """The #12 and #7 terms, and the #100 term #104 folded into the fetch.

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
        # The whole fetch (#104): the guard's resolve on every hop, every connect,
        # every read. It replaced two terms, the per-operation timeout and the #100
        # resolve, each of which bounded one step and neither a fetch — which is
        # why the sum could leave a trickling origin out.
        + settings.max_fetch_seconds
        # The #7 term. Storage runs inside ``asyncio.to_thread``, which puts it
        # beyond cancellation exactly as the sweep is, so SIGTERM waits out an
        # upload in flight. Added when the object-store backend made this a
        # network round trip rather than a write to local disk (CR #5) — the
        # docstring's "three separately-reasonable numbers" became four, which
        # is the failure it predicted.
        + settings.blob_timeout_seconds
    )

    assert timeout_stop > worst_case


def test_the_guards_resolve_cap_fits_inside_the_fetch_deadline():
    """Why the sum above no longer adds ``RESOLVE_TIMEOUT_SECONDS`` (#100, #104).

    The guard resolves inside ``_fetch``'s deadline — ``test_handler_deadline.py``
    shows the deadline reaching a stalled resolve first — so the stop budget counts
    it once, as part of the fetch. What still depends on the cap is the guard's own
    answer: under the default deadline a resolve that stalls is refused as
    unresolvable, naming the name, rather than as a fetch that ran long.
    """
    assert RESOLVE_TIMEOUT_SECONDS < Settings().max_fetch_seconds


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
    -1000 stalls it rather than killing it. And the worker's rank protects
    nothing if the kernel reaches ``tailscaled`` first — it read 670 in #112,
    just below the user manager, until #113 gave it the same -900
    (``test_tailscaled_ranks_with_the_worker_it_carries``). It is what degraded
    in CannObserv/broker#17, a 57-minute bus outage with nothing OOM-killed at
    all: the kernel failed *atomic* allocations while every process stayed
    alive, and this worker did not reconnect on its own (#94).

    ``earlyoom`` does not close it either: it skips -1000 as the kernel does, so
    it takes the same list in the same order, only sooner
    (``TestTheEarlyoomDecline``).
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


# --- The worker's network path (#113) ----------------------------------------
#
# The broker is `broker` on the tailnet (#88), so losing tailscaled takes the
# bus from this worker as completely as killing the worker. At the packaged
# unit's adj of 0 it read 670 in #112 — second on this VM's eligible list.
# These are drop-ins on a unit this repo does not ship, installed under
# /etc/systemd/system/ like everything else in deploy/: copies.

TAILSCALED_DROPIN = REPO_ROOT / "deploy" / "tailscaled.service.d" / "memory.conf"
SLICE_DROPIN = REPO_ROOT / "deploy" / "system.slice.d" / "replicator-memory.conf"


def test_tailscaled_ranks_with_the_worker_it_carries():
    """The network path and its only consumer, together at the bottom of the list.

    The same bounds as the units above, for a different reason: nothing here
    competes with the dev tooling, but a tailscaled the kernel reaches first
    ends the worker's bus exactly as killing the worker would, and a failing
    tailscaled was a named symptom of CannObserv/broker#17. broker gives it
    -900 (CannObserv/broker#21); watcher's -400 (CannObserv/watcher#309) is for
    a dashboard that does not use the tailnet, which this worker does.
    """
    assert TAILSCALED_DROPIN.exists(), (
        f"{TAILSCALED_DROPIN.name} is missing from deploy/tailscaled.service.d/ — "
        "tailscaled sits at adj 0, second on this VM's OOM list"
    )
    adjust = int(_directive("OOMScoreAdjust", TAILSCALED_DROPIN))
    assert adjust <= COHORT_OOM_SCORE_ADJUST, (
        f"tailscaled's drop-in sets OOMScoreAdjust={adjust}, above the worker it carries"
    )
    assert adjust > OOM_FLOOR, (
        f"tailscaled's drop-in sets OOMScoreAdjust={adjust} — exempt, so a leak in it "
        "would be unreclaimable"
    )


# Every file that sets MemoryLow=, with the section its unit type reads it from.
RESERVATIONS = {UNIT: "Service", TAILSCALED_DROPIN: "Service", SLICE_DROPIN: "Slice"}

# What a reservation must not become. A cap stalls the worker or tailscaled
# while it still reports `active` — on system.slice, every service at once —
# and MemoryMin= is a floor the kernel holds even when the alternative is an
# OOM kill elsewhere.
NOT_RESERVATIONS = ("MemoryMin", "MemoryHigh", "MemoryMax", "MemorySwapMax")

_SIZE_SUFFIXES = ("", "K", "M", "G", "T")


def _size(value: str) -> int:
    """A systemd byte size in bytes: digits and an optional base-1024 suffix.

    Strict on purpose: systemd does not strip a trailing ``# comment`` from a
    value, so ``MemoryLow=128M  # margin`` fails to parse and the directive is
    dropped with a log line nobody reads. This refuses the same input.
    """
    match = re.fullmatch(r"(\d+)([KMGT]?)", value)
    assert match, f"{value!r} is not a byte size systemd would parse"
    return int(match.group(1)) * 1024 ** _SIZE_SUFFIXES.index(match.group(2))


def _memory_low(path: Path) -> int:
    return _size(_directive("MemoryLow", path))


def _section_of(path: Path, name: str) -> str | None:
    """The ``[Section]`` holding the last assignment of ``name`` in ``path``."""
    section = found = None
    for line in path.read_text().splitlines():
        if header := re.fullmatch(r"\[(\w+)\]", line.strip()):
            section = header.group(1)
        elif line.startswith(f"{name}="):
            found = section
    return found


@pytest.mark.parametrize("path", RESERVATIONS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_each_reservation_sits_where_its_unit_type_reads_it(path: Path):
    """``MemoryLow=`` under the wrong header is ignored, not rejected.

    A slice reads ``[Slice]`` and a service ``[Service]``; anywhere else the
    file installs, reloads, and reserves nothing.
    """
    assert path.exists(), f"{path.relative_to(REPO_ROOT)} is missing"
    assert _memory_low(path) > 0, f"{path.name} reserves nothing"
    assert _section_of(path, "MemoryLow") == RESERVATIONS[path], (
        f"{path.name} sets MemoryLow= outside [{RESERVATIONS[path]}], where systemd ignores it"
    )


@pytest.mark.parametrize("path", RESERVATIONS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_each_reservation_is_only_a_reservation(path: Path):
    text = path.read_text()
    for directive in NOT_RESERVATIONS:
        assert not re.search(rf"^{directive}=", text, flags=re.MULTILINE), (
            f"{path.name} sets {directive}= — MemoryLow= reserves, this caps or pins"
        )


def test_system_slice_grants_what_its_children_claim():
    """Without the grant, every ``MemoryLow=`` below it protects nothing.

    cgroup2 here is mounted without ``memory_recursiveprot``, so a unit keeps no
    more ``memory.low`` than its slice grants, and ``system.slice`` defaults to
    0. The competitor is ``init.scope`` — the agent sessions — a root-level
    sibling, so the slice's grant is what moves reclaim onto them.
    """
    claimed = _memory_low(UNIT) + _memory_low(TAILSCALED_DROPIN)
    granted = _memory_low(SLICE_DROPIN)
    assert granted >= claimed, (
        f"system.slice grants {granted} bytes but the worker and tailscaled claim {claimed}"
    )


# --- The same, read from the kernel -------------------------------------------
#
# Every file above is a copy once installed, and neither of these settings shows
# its effect in `systemctl show`: OOMScoreAdjust= applies at exec, so after a
# daemon-reload the unit reports the new value while the running process keeps
# the old one (CannObserv/watcher#309), and a MemoryLow= under an ungranted
# slice is reported faithfully and protects nothing. So these read /proc and
# /sys/fs/cgroup, on the host that runs the units.

# The host this repo deploys to. Replicator is developed only there, so a
# session anywhere else is not the host #112 measured, nor one running the units.
HOST = "co-replicator"

CGROUP_ROOT = Path("/sys/fs/cgroup")
LIVE = {
    "replicator.service": UNIT,
    "tailscaled.service": TAILSCALED_DROPIN,
    "system.slice": SLICE_DROPIN,
}
on_the_host = pytest.mark.skipif(socket.gethostname() != HOST, reason=f"not {HOST}")


def _cgroup(unit: str) -> Path:
    return CGROUP_ROOT / unit if unit.endswith(".slice") else CGROUP_ROOT / "system.slice" / unit


def _live_memory_low(cgroup: Path) -> int:
    raw = (cgroup / "memory.low").read_text().strip()
    return 2**63 if raw == "max" else int(raw)


@on_the_host
@pytest.mark.parametrize("unit", ["replicator.service", "tailscaled.service"])
def test_the_running_process_carries_its_units_adjust(unit: str):
    """Verify ``/proc``, not ``systemctl show``: a missed restart reads correct there."""
    pid = subprocess.run(
        ["systemctl", "show", "-p", "MainPID", "--value", unit],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert pid not in ("", "0"), f"{unit} is not running on {HOST}"
    live = int(Path(f"/proc/{pid}/oom_score_adj").read_text())
    expected = int(_directive("OOMScoreAdjust", LIVE[unit]))
    assert live == expected, (
        f"{unit} (pid {pid}) reads oom_score_adj {live}, the repo says {expected} — "
        f"install {LIVE[unit].relative_to(REPO_ROOT)}, daemon-reload, and restart {unit}"
    )


@on_the_host
@pytest.mark.parametrize("unit", LIVE)
def test_the_kernel_holds_each_reservation(unit: str):
    # systemd removes a stopped service's cgroup: name that, not a FileNotFoundError.
    assert _cgroup(unit).is_dir(), f"{_cgroup(unit)} is missing — is {unit} running?"
    live = _live_memory_low(_cgroup(unit))
    expected = _memory_low(LIVE[unit])
    assert live == expected, (
        f"{unit} holds memory.low {live}, the repo says {expected} — "
        f"install {LIVE[unit].relative_to(REPO_ROOT)} and daemon-reload"
    )


@on_the_host
def test_the_live_slice_covers_every_child_on_the_host():
    """Any unit on the host can claim a share, not only the two this repo reserves.

    An oversubscribed slice divides its grant among the claimants in proportion
    to what each uses of its claim — with or without ``memory_recursiveprot`` —
    so a claim added outside this repo would quietly shrink the worker's and
    tailscaled's. This sums the claims rather than their use, which is stricter
    than the kernel on purpose: use moves, a claim does not.
    """
    slice_dir = _cgroup("system.slice")
    children = [c for c in slice_dir.iterdir() if (c / "memory.low").exists()]
    claimed = sum(_live_memory_low(c) for c in children)
    granted = _live_memory_low(slice_dir)
    assert granted >= claimed, (
        f"system.slice grants {granted} bytes but its children claim {claimed}: "
        + ", ".join(f"{c.name}={_live_memory_low(c)}" for c in children if _live_memory_low(c))
    )


SYSCTL = REPO_ROOT / "deploy" / "99-co-replicator-memory.conf"

# Low enough that swap stays a safety net rather than a routine paging path for
# the worker's hot pages, but never 0: at 0 the kernel declines to swap
# anonymous pages under pressure, which reinstates the failure this file exists
# to prevent.
SWAPPINESS_BOUNDS = (1, 30)

# The kernel's reserve for *atomic* (non-sleeping) allocations. The default
# rescaled to only ~11 MB after the 4 -> 8 GiB resize, and an exhausted reserve
# is precisely how broker's outage presented: failed atomic allocations in
# tailscaled and ksoftirqd, with nothing OOM-killed.
MIN_FREE_KBYTES_FLOOR = 32768


def _sysctl(name: str) -> str:
    """The last value assigned to ``name``, matching sysctl's own last-wins rule."""
    values = re.findall(
        rf"^\s*{re.escape(name)}\s*=\s*(\S+)", SYSCTL.read_text(), flags=re.MULTILINE
    )
    assert values, f"{SYSCTL.name} does not set {name}"
    return values[-1]


class TestHostMemoryTunables:
    """The host's memory posture is config, so it belongs in the repo.

    `deploy/` is where this repo keeps host configuration of record — both
    units, and since #108 `notifier-template.json` — because a VM rebuild
    restores from here. `vm.swappiness` and `vm.min_free_kbytes` were set on
    co-replicator on 2026-09-23 (#99) and existed only on the host until this
    file, which a rebuild would have lost silently: the host would come back
    with a ~11 MB atomic reserve and nothing naming that as wrong.

    They are the host-wide half. The per-unit half is `MemoryLow=` on the
    worker and `tailscaled`, which works here only because
    `deploy/system.slice.d/` grants it (#113): cgroup2 is mounted without
    `memory_recursiveprot`, so without the grant both reservations would be
    inert. It shields working sets from reclaim; the atomic reserve below is
    the one lever for allocations that cannot wait for reclaim at all.
    """

    def test_the_drop_in_is_tracked(self) -> None:
        assert SYSCTL.exists(), (
            f"{SYSCTL.name} is missing — the host's swappiness and atomic reserve "
            "would exist only on the VM, and a rebuild would lose them silently"
        )

    def test_swappiness_keeps_swap_a_net_not_a_path(self) -> None:
        low, high = SWAPPINESS_BOUNDS
        value = int(_sysctl("vm.swappiness"))
        assert low <= value <= high, (
            f"vm.swappiness={value} is outside {low}-{high}: 0 declines to swap "
            "anonymous pages under pressure, and a high value pages the worker's "
            "hot pages routinely"
        )

    def test_the_atomic_reserve_is_raised_above_the_rescaled_default(self) -> None:
        value = int(_sysctl("vm.min_free_kbytes"))
        assert value >= MIN_FREE_KBYTES_FLOOR, (
            f"vm.min_free_kbytes={value} is below {MIN_FREE_KBYTES_FLOOR} — the "
            "default rescaled to only ~11 MB after the resize, and an exhausted "
            "atomic reserve is how broker's 57m48s outage presented"
        )


# What exe.dev starts a session from; the -1000 is theirs, inherited or not.
SESSION_PARENTS = frozenset({"exe-init", "sshd"})


def _session_root_adj(proc: Path, pid: int) -> int | None:
    """``oom_score_adj`` of the process exe.dev started ``pid``'s session from.

    Walks the ancestry to the first process whose parent is in
    ``SESSION_PARENTS`` and reads that one, not ``pid`` itself: a leaf can be
    ``choom``'d (COMMANDS.md's capped launch is), the session root cannot.
    ``None`` when no ancestor is a session — CI, a systemd unit, cron.
    """
    while pid > 1:
        status = (proc / str(pid) / "status").read_text()
        ppid = int(re.search(r"^PPid:\s*(\d+)", status, flags=re.MULTILINE).group(1))
        if ppid < 1:
            return None
        if (proc / str(ppid) / "comm").read_text().strip() in SESSION_PARENTS:
            return int((proc / str(pid) / "oom_score_adj").read_text())
        pid = ppid
    return None


def _fake_process(proc: Path, pid: int, ppid: int, comm: str, adj: int) -> None:
    (proc / str(pid)).mkdir(parents=True)
    (proc / str(pid) / "status").write_text(f"Name:\t{comm}\nPPid:\t{ppid}\n")
    (proc / str(pid) / "comm").write_text(f"{comm}\n")
    (proc / str(pid) / "oom_score_adj").write_text(f"{adj}\n")


class TestTheEarlyoomDecline:
    """#112 declined earlyoom on a premise that belongs to exe.dev, not to us.

    Sessions here inherit ``oom_score_adj`` -1000, and earlyoom 1.7 skips a
    -1000 process exactly as the kernel does (``kill.c:250``), ``--prefer`` or
    not — so it cannot reach the dev tooling that caused CannObserv/broker#17,
    and would shed small daemons instead (and, before #113, ``tailscaled``).
    The premise is not universal: notifier's sessions sit at 0
    (CannObserv/notifier#74, gregoryfoster/skills#303), and what decides it was
    never determined. So it is pinned live rather than assumed; if it flips,
    the decline no longer holds and #112 reopens.
    """

    def test_a_session_under_exe_init_reports_its_root(self, tmp_path: Path) -> None:
        _fake_process(tmp_path, 217, 1, "exe-init", -1000)
        _fake_process(tmp_path, 581, 217, "bash", -1000)
        _fake_process(tmp_path, 900, 581, "python3", 500)  # a choom'd leaf
        assert _session_root_adj(tmp_path, 900) == -1000

    def test_a_session_under_sshd_reports_its_root(self, tmp_path: Path) -> None:
        _fake_process(tmp_path, 216, 1, "sshd", -1000)
        _fake_process(tmp_path, 700, 216, "bash", 0)  # notifier's shape
        _fake_process(tmp_path, 701, 700, "python3", 0)
        assert _session_root_adj(tmp_path, 701) == 0

    def test_no_session_ancestor_is_none(self, tmp_path: Path) -> None:
        _fake_process(tmp_path, 1, 0, "systemd", 0)
        _fake_process(tmp_path, 300, 1, "systemd", 100)
        _fake_process(tmp_path, 301, 300, "python3", 0)
        assert _session_root_adj(tmp_path, 301) is None

    @on_the_host
    def test_sessions_here_are_still_exempt(self) -> None:
        adj = _session_root_adj(Path("/proc"), os.getpid())
        if adj is None:
            pytest.skip("not run from an exe.dev session")
        assert adj == OOM_FLOOR, (
            f"this session's root reads oom_score_adj={adj}, not {OOM_FLOOR}: "
            "exe.dev no longer exempts sessions here, so earlyoom's --prefer now "
            "reaches the dev tooling and #112's decline no longer holds — "
            "reopen it (notifier's deploy/earlyoom.default is the working shape, "
            "plus -s 100: with swap, earlyoom otherwise waits for swap to drain)"
        )
