"""The systemd unit's restart limiter must fit the worker's own failure timescale.

`replicator.service` and `Settings` encode two halves of one decision: how long
the worker absorbs a broker outage before exiting, and how many such exits the
unit tolerates before staying `failed`. Documented in both places, enforced
here — raising the ceiling without widening the window silently restores the
failure mode the pairing exists to prevent (a permanently unreachable Redis that
reads as `active (running)` forever).

The second half covers what the unit starts (#151): the live release under
`/srv/replicator`, never a checkout. The build id comes from that release's
`REVISION`, and the interpreter is the release's own, resolved before `exec` so
a worker draining through a deploy never loads the next release's packages.
Both are reached by running the unit's own command lines against a fake release
tree. The deploy itself (`scripts/deploy.sh`, the deploying-releases core) is
held by `tests/deploy/`.

The unit is also ordered behind `tailscaled` — ordering, never dependency —
because the broker it consumes is reached by tailnet name (#88).
"""

import os
import re
import shutil
import socket
import subprocess
from pathlib import Path

import pytest

from src.core.config import Settings
from src.worker.egress import RESOLVE_TIMEOUT_SECONDS
from src.worker.loop import error_backoff_seconds

REPO_ROOT = Path(__file__).resolve().parents[1]
UNIT = REPO_ROOT / "deploy" / "replicator.service"
RELEASE_LINK = "/srv/replicator/live"
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


def _floor_of_one_start(settings: Settings) -> float:
    """The least wall-clock one start spends waiting out a broker that stays down.

    The waits before the ceiling, never ``worst_case_outage_seconds``: the
    attempt that reaches ``max_consecutive_cycle_failures`` raises without
    waiting, and the first few back off less than the cap. Each attempt itself
    is counted as free — a broker that refuses instantly (a restarting
    redis-server) costs nothing per attempt, where an unreachable one costs a
    connect timeout (#134).
    """
    return sum(
        error_backoff_seconds(
            failures,
            base=settings.error_backoff_base_seconds,
            maximum=settings.error_backoff_max_seconds,
        )
        for failures in range(1, settings.max_consecutive_cycle_failures)
    )


def test_the_unit_absorbs_the_worst_outage_this_cluster_has_had():
    """Surviving a *real* outage unaided, not merely tripping the limiter coherently.

    The test above is internal consistency — the window fits the exits. This one
    is the external fact #94 was filed about: the starts have to add up to more
    wall-clock than the broker has ever actually been away, or the unit stays
    `failed` while the broker is still coming back and an operator is required.

    At the values #94 found (3 x ~10 min = 30 min) the 2026-09-16 outage was
    nearly twice the budget, and survived only by the accident of when continuous
    failure began — the worker did not start failing until ~15:00.

    A *lower* bound, because survival is a claim about the worst case (#134).
    Until #134 this multiplied ``StartLimitBurst`` by ``worst_case_outage_seconds``,
    an upper bound, and passed at six starts while the floor was ~48 min against
    the 58 pinned. The floor is ``StartLimitBurst`` starts *including the one
    already running*: systemd counts every start against the burst, a deploy's
    or a manual restart's as well, so a unit started within the window before
    the outage has one restart fewer to spend (#134 CR 1). Each restart is
    preceded by ``RestartSec`` and the floor check's wait — which a refusing
    broker spends in full, being an 'unreachable' probe. The guard and
    interpreter start are left out: real, unmeasured, and only ever additions
    to a floor.
    """
    settings = Settings()
    burst = int(_directive("StartLimitBurst"))
    per_start = _floor_of_one_start(settings)
    # Less one second: the floor check times its wait on bash's whole-second
    # SECONDS, so it can stop as much as a second short of the full value.
    floor_wait = _floor_default("REPLICATOR_REDIS_FLOOR_WAIT") - 1
    between_starts = float(_directive("RestartSec")) + floor_wait
    absorbed = burst * per_start + (burst - 1) * between_starts

    assert absorbed >= WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS, (
        f"the unit absorbs as little as {absorbed / 60:.1f} min across {burst} starts, "
        f"but this cluster has had a {WORST_OBSERVED_CLUSTER_OUTAGE_SECONDS / 60:.0f} min "
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

    Every ExecStartPre shares one TimeoutStartSec. A wait that could eat the
    budget would turn a slow tailnet into a start timeout: a failure, counted against the three
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


def test_a_start_needs_no_package_index():
    """Dependency sync is a deploy step, never a start side effect (#183, design
    option A). No ``ExecStartPre`` runs ``uv`` or reaches the co-pypi index, so a
    cold boot behind a late tailnet needs no network or token for packages, and
    nothing at start changes the venv the worker imports from. The retired
    wheelhouse refresh was the last step that did."""
    starting = [*_exec_start_pre(), _directive("ExecStart")]
    offenders = [
        v
        for v in starting
        if re.search(r"sync_wheelhouse|co-pypi|co_pypi|\buv (sync|lock|pip)\b", v)
    ]
    assert offenders == []
    assert not re.search(r"(^|[/\s])uv(\s|$)", _directive("ExecStart"))


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


# --- The live release (#151) ---------------------------------------------------
#
# The unit runs whatever /srv/replicator/live names, a root-owned release the
# deploy built from a CI-green main commit. The main-checkout guard (#37, #48)
# that stood here is deleted, not ported: no checkout is started any more.


STAMP = "/run/replicator/build-id.env"
# The name every release before #202 stamps, as exedev, and its rollback still does.
PRE_202_STAMP = "/run/replicator/build-id"


def _stamp_step() -> str:
    """The single ``ExecStartPre`` that writes the build id."""
    matches = [value for value in _exec_start_pre() if "build-id" in value]
    assert len(matches) == 1, f"expected exactly one build-id ExecStartPre, got {matches}"
    return matches[0]


def _run_stamp(root: Path, **kwargs) -> subprocess.CompletedProcess:
    """The stamp step's script, run from ``root/live`` with ``/run/replicator`` at ``root``."""
    step = _stamp_step().removeprefix("+")
    assert step.startswith("/bin/bash -ec "), step
    script = step.removeprefix("/bin/bash -ec ").strip("'").replace("/run/replicator", str(root))
    return subprocess.run(["/bin/bash", "-ec", script], cwd=root / "live", **kwargs)


def _fake_release(root: Path, build: str = "0123456789ab") -> Path:
    """``root/releases/<build>`` with a REVISION and a stand-in interpreter, and
    ``root/live`` linked to it, as the deploy leaves them."""
    release = root / "releases" / build
    python = release / ".venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text('#!/bin/sh\necho "$0 $*"\n')
    python.chmod(0o755)
    (release / "REVISION").write_text(build + "\n")
    (root / "live").symlink_to(release)
    return release


def test_the_unit_runs_the_live_release():
    assert _directive("WorkingDirectory") == RELEASE_LINK


def test_nothing_the_unit_starts_is_in_a_checkout():
    starting = [*_exec_start_pre(), _directive("ExecStart")]
    assert [v for v in starting if "/home/" in v or "check_main_checkout" in v] == []


def test_the_start_runs_the_release_it_resolved_not_the_link(tmp_path):
    """``sys.prefix`` follows the path the interpreter was started by, so a worker
    started as ``live/.venv/bin/python`` reads site-packages through the link and,
    once a deploy moves it, imports the next release's packages while it drains
    (measured 2026-11-14). The start resolves the link once, then ``exec``s, so the
    interpreter is the main PID that ``KillMode=mixed`` signals."""
    release = _fake_release(tmp_path)
    command = _directive("ExecStart")
    assert command.startswith("/bin/sh -c "), command
    script = command.removeprefix("/bin/sh -c ").strip("'")
    assert script.startswith("exec "), "without exec, sh would be the main PID"

    ran = subprocess.run(
        ["/bin/sh", "-c", script], cwd=tmp_path / "live", capture_output=True, text=True, check=True
    )

    assert ran.stdout.split() == [
        str(release / ".venv" / "bin" / "python"),
        "-m",
        "src.worker.main",
    ]


def test_the_build_id_is_the_releases_revision(tmp_path):
    release = _fake_release(tmp_path)

    _run_stamp(tmp_path, check=True)

    assert (tmp_path / "build-id.env").read_text() == f"BUILD_ID={release.name}\n"


def test_a_release_without_a_revision_fails_the_start(tmp_path):
    """The core never links a release without ``REVISION``; one made by hand must
    not start under an empty ``BUILD_ID``. Not ``-``-prefixed, so the failure stops
    it; ``+`` only says who runs it."""
    _fake_release(tmp_path)
    (tmp_path / "live" / "REVISION").unlink()
    step = _stamp_step()
    assert step.removeprefix("+").startswith("/"), (
        f"a '-'-prefixed stamp would not stop the start: {step!r}"
    )

    ran = _run_stamp(tmp_path, capture_output=True)

    assert ran.returncode != 0
    assert not (tmp_path / "build-id.env").exists()


def test_the_stamp_is_roots_and_never_the_name_a_rollback_writes(tmp_path):
    """A rollback past #202 runs a unit that stamps ``build-id`` as ``exedev``,
    truncating it in place, and chowns only the directory. Truncation needs write
    on the *file*: one ``replicator`` or root owned refuses it and the rollback's
    start fails (measured 2026-11-15). So this release never writes that name. It
    removes it, which a rollback recreates as its own, and root writes its own
    stamp, which a reboot can't leave owned by the wrong user."""
    release = _fake_release(tmp_path)
    (tmp_path / "build-id").write_text("BUILD_ID=a8900f5d15a2\n")

    _run_stamp(tmp_path, check=True)

    assert _stamp_step().startswith("+"), "the stamp is root's, whoever User= is"
    assert not (tmp_path / "build-id").exists(), "a stale build-id names the wrong build"
    assert (tmp_path / "build-id.env").read_text() == f"BUILD_ID={release.name}\n"


@pytest.mark.parametrize("unit", [UNIT, NOTIFY_UNIT], ids=lambda p: p.name)
def test_both_units_read_the_stamp_this_release_writes(unit: Path):
    files = re.findall(r"^EnvironmentFile=(.*)$", unit.read_text(), flags=re.MULTILINE)
    assert f"-{STAMP}" in [f.strip() for f in files], f"{unit.name} must read {STAMP}, optionally"
    assert f"-{PRE_202_STAMP}" not in [f.strip() for f in files]


def test_the_worker_never_writes_the_runtime_directory():
    """Only root's ``+`` steps write ``/run/replicator``, and the directory is root's.
    Never chowned to ``User=``: that is a second place the user is named, and under
    ``ProtectSystem=strict`` the worker could not write there anyway. Taken back to
    root (#202 CR 4): a pre-#202 start leaves it exedev's, and root then writes the
    stamp into a directory the dev user could plant a symlink in."""
    steps = [s for s in _exec_start_pre() if "/run/replicator" in s]
    for step in steps:
        assert step.startswith("+"), f"a /run/replicator step that is not root's: {step!r}"
    made = next(s for s in steps if "mkdir -p /run/replicator" in s)
    assert "chown root:root /run/replicator" in made, made
    for step in _exec_start_pre():
        for target in re.findall(r"chown\s+(\S+)", step):
            assert target == "root:root", f"chowned to someone other than root: {step!r}"


def test_the_runtime_directory_exists_before_the_stamp():
    steps = _exec_start_pre()
    made_at = next(i for i, step in enumerate(steps) if "mkdir -p /run/replicator" in step)
    assert made_at < steps.index(_stamp_step())


def test_the_handler_runs_the_live_release():
    assert _directive("WorkingDirectory", NOTIFY_UNIT) == RELEASE_LINK
    notify = f"/bin/bash {RELEASE_LINK}/scripts/notify_failure.sh %i"
    assert _directive("ExecStart", NOTIFY_UNIT) == notify


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


# --- The service user (#202) ----------------------------------------------------
#
# Until #202 both units ran as exedev, which has passwordless sudo, docker, and
# /home/exedev, where the org-wide PATs live, so a compromised fetch or render was
# root on the host. The worker now runs as the system user `replicator`, which
# reads /etc/replicator through its group (root:replicator 750/640; exedev is a
# member, so the agent's shell work is unchanged).

SERVICE_USER = "replicator"
DEV_USER = "exedev"

# Each directive was proven against the pinned headless shell before it landed:
# `-m browser` in a transient system service carrying these, as a uid with no home
# and no groups, and a live public render (#202). The namespace-and-seccomp sandbox
# needs none of setuid, a home, or a writable system tree.
CONFINEMENT = {
    "NoNewPrivileges": "yes",
    "ProtectSystem": "strict",
    "ProtectHome": "yes",
    "PrivateTmp": "yes",
}


@pytest.mark.parametrize("unit", [UNIT, NOTIFY_UNIT], ids=lambda p: p.name)
def test_both_units_run_as_the_service_user(unit: Path):
    assert _directive("User", unit) == SERVICE_USER
    assert _section_of(unit, "User") == "Service"


@pytest.mark.parametrize("unit", [UNIT, NOTIFY_UNIT], ids=lambda p: p.name)
def test_no_production_unit_names_the_dev_user(unit: Path):
    """Not as ``User=``, not in a chown: one stray mention puts the escalation back."""
    live = [
        line
        for line in unit.read_text().splitlines()
        if DEV_USER in line and not line.lstrip().startswith("#")
    ]
    assert live == [], f"{unit.name} still names {DEV_USER}: {live}"


@pytest.mark.parametrize("option", sorted(CONFINEMENT))
@pytest.mark.parametrize("unit", [UNIT, NOTIFY_UNIT], ids=lambda p: p.name)
def test_both_units_are_confined(unit: Path, option: str):
    assert _directive(option, unit) == CONFINEMENT[option]
    assert _section_of(unit, option) == "Service"


def test_the_worker_leaves_chromium_its_user_namespaces():
    """The control for the proof above: the same run with ``RestrictNamespaces=yes``
    failed every launch, ``No usable sandbox!``, and Chromium refused to fall back
    (#202). A namespace restriction here takes capture and browser fetch down, so
    it is ruled out until the sandbox is something other than user namespaces."""
    assert not re.search(r"^RestrictNamespaces=", UNIT.read_text(), flags=re.MULTILINE)


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

    co-replicator is also the dev workspace, and a session starts VSCode Server,
    Claude Code, and any MCP server they start — the tooling that took
    CannObserv/broker#17 down. At the default adj of 0 the worker read
    ``oom_score`` 670, second from the top of the eligible list (#92); at -900
    it reads 72, above only tailscaled (-950, #112).

    Until #125 that rank was all it bought: ``exe-init`` 8579326 started every
    session at ``oom_score_adj=-1000``, which is ineligibility, not a low score
    — 28 processes held it, and the kernel skipped them entirely. Sessions now
    start at 0 (``TestTheSessionPremise``), so the comparison this test is named
    for is a real one.

    Two things that rank does not buy, both recorded in docs/MEMORY-PROTECTION.md so
    the doc and this test say the same thing. It is no substitute for capping
    whatever launches a SocratiCode server — the cap bounds the launch before
    any killer has to act. And the worker's rank protects nothing if the kernel
    reaches ``tailscaled`` first — it read 670 in #112, just below the user
    manager, until #113 gave it -900 and #112 -950
    (``test_tailscaled_outlasts_the_worker_it_carries``). It is what degraded
    in CannObserv/broker#17, a 57-minute bus outage with nothing OOM-killed at
    all: the kernel failed *atomic* allocations while every process stayed
    alive, and this worker did not reconnect on its own (#94).
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


def test_tailscaled_outlasts_the_worker_it_carries():
    """The network path goes after its only consumer, never with or before it.

    Nothing here competes with the dev tooling, but a tailscaled the kernel
    reaches first ends the worker's bus exactly as killing the worker would,
    and a failing tailscaled was a named symptom of CannObserv/broker#17. At a
    tied adj the larger RSS goes first — a coin toss here, 72 against 71 on
    2026-09-29, which tailscaled's 123 MiB peak reverses — so it sits strictly
    below the worker: -950, CannObserv/power-map#588's value (#112). A killed
    worker restarts into a working bus path; a killed tailscaled leaves it
    running and cut off, and the ``OnFailure=`` alert to ``notifier:9000``,
    sent once the restart budget is spent, needs the tailnet. watcher's and
    archiver's -400 (CannObserv/watcher#309, CannObserv/archiver#285) is for
    services that do not use the tailnet, which this worker does.
    """
    assert TAILSCALED_DROPIN.exists(), (
        f"{TAILSCALED_DROPIN.name} is missing from deploy/tailscaled.service.d/ — "
        "tailscaled sits at adj 0, second on this VM's OOM list"
    )
    adjust = int(_directive("OOMScoreAdjust", TAILSCALED_DROPIN))
    worker = int(_directive("OOMScoreAdjust", UNIT))
    assert adjust < worker, (
        f"tailscaled's drop-in sets OOMScoreAdjust={adjust}, not below the worker's "
        f"{worker} — the kernel can take the bus's only path before its consumer"
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


# --- The browser cgroup (#138) ------------------------------------------------
#
# Delegation moves the worker into <unit>/worker, where the unit's MemoryLow=
# no longer reaches it on this host (no memory_recursiveprot): the worker
# re-claims it at boot (src/worker/browser_cgroup.py). These pin the unit half
# and, on the host, that the claim landed.

BROWSER_EXEC = REPO_ROOT / "scripts" / "browser_exec.sh"
INSTALLED_UNIT = Path("/etc/systemd/system/replicator.service")


def test_a_stop_signals_the_worker_alone_and_kills_the_browser_after():
    """CR 7: the default control-group mode SIGTERMs ``browser/`` too, failing the
    render in flight as a ``handler_error``. ``mixed`` signals the main process (uv
    forwards it to the worker, measured on uv 0.13.5) and SIGKILLs the rest after."""
    assert _directive("KillMode") == "mixed"
    assert _section_of(UNIT, "KillMode") == "Service"


def test_the_unit_delegates_memory_and_pids_to_a_worker_leaf():
    assert set(_directive("Delegate").split()) == {"memory", "pids"}
    assert _directive("DelegateSubgroup") == "worker"
    assert _section_of(UNIT, "Delegate") == "Service"
    assert _section_of(UNIT, "DelegateSubgroup") == "Service"


def test_the_browser_launcher_is_executable_and_joins_the_cgroup_before_exec():
    assert os.access(BROWSER_EXEC, os.X_OK), f"{BROWSER_EXEC.name} is not executable"
    text = BROWSER_EXEC.read_text()
    assert "set -euo pipefail" in text, "a launch that cannot join the cgroup must fail"
    join = 'echo "$$" > "$REPLICATOR_BROWSER_CGROUP_PROCS"'
    assert text.index(join) < text.index('exec "$exe"'), (
        "the launcher must join the browser cgroup before it becomes the browser"
    )
    assert "echo 500 > /proc/self/oom_score_adj" in text


@on_the_host
def test_the_worker_leaf_holds_the_units_reservation():
    """After a #138 deploy: <unit>/worker carries what systemd wrote on <unit>."""
    if "DelegateSubgroup=worker" not in INSTALLED_UNIT.read_text():
        pytest.skip(f"{INSTALLED_UNIT} predates #138 — the post-merge steps install it")
    service = _cgroup("replicator.service")
    leaf = service / "worker"
    assert leaf.is_dir(), f"{leaf} is missing — restart replicator after daemon-reload"
    assert _live_memory_low(leaf) == _live_memory_low(service), (
        f"{leaf} holds memory.low {_live_memory_low(leaf)}, the unit {_live_memory_low(service)} "
        "— the worker's boot claim did not land; read `worker ready` in the journal"
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
    units, and the host configs the deploy compares but never installs
    (`HOST_CONFIGS`, #151) — because a VM rebuild restores from here.
    `vm.swappiness` and `vm.min_free_kbytes` were set on
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


# What exe.dev starts a session from; the session's adj is theirs, inherited or not.
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


class TestTheSessionPremise:
    """Sessions here must be killable, or two things this repo relies on stop holding.

    The -900 on both units wins a real comparison against the dev tooling only
    while the tooling is eligible, and #112's earlyoom decline is decided on
    the same premise. The other memory settings (``MemoryLow=``, the sysctls)
    hold at either value.

    Until #125, ``exe-init`` 8579326 started every session at ``oom_score_adj``
    -1000 — a bug exe.dev confirmed on 2026-09-29 (CannObserv/status#5) — so the
    kernel and earlyoom 1.7 (``kill.c:250``) both passed over the dev tooling
    that caused CannObserv/broker#17 and would have shed the host's daemons
    instead. That is why #112 first declined earlyoom. ``exe-init`` 14fd603
    starts sessions at the default 0, notifier's shape (CannObserv/notifier#74),
    where the kernel's own order takes them first — why #112 declined it again
    (``TestTheEarlyoomDecline``). Pinned live because ``/exe.dev/bin/`` is exe.dev's
    to replace: if -1000 comes back, so does every consequence above.
    """

    def test_a_session_under_exe_init_reports_its_root(self, tmp_path: Path) -> None:
        _fake_process(tmp_path, 217, 1, "exe-init", -1000)
        _fake_process(tmp_path, 581, 217, "bash", -1000)  # 8579326's shape
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
    def test_sessions_here_are_killable(self) -> None:
        adj = _session_root_adj(Path("/proc"), os.getpid())
        if adj is None:
            pytest.skip("not run from an exe.dev session")
        assert adj == 0, (
            f"this session's root reads oom_score_adj={adj}, not 0: "
            "/exe.dev/bin/exe-init starts sessions exempt again (#125) — check "
            "`/exe.dev/bin/exe-init --version` against 14fd603. Until it is "
            "replaced the kernel skips the dev tooling and takes the host's daemons "
            "instead, and #112's reason for declining earlyoom at 0 no longer holds"
        )


class TestTheEarlyoomDecline:
    """earlyoom stays off this host: the kernel's own order already does its job (#112).

    Declined twice. At -1000 (2026-09-24) it could not reach the sessions at
    all. At 0 (2026-09-29, after #125) it can, but the kernel already ranks the
    heaviest session process first and the worker and tailscaled last, so
    earlyoom's only addition is timing, and neither setting helps: stock
    ``-s 10`` also waits for swap to be ≤10% free, deep into the thrashing that
    ends at the kernel's own kill, and the ``-s 100,100`` this host would need
    kills at ~10% ``MemAvailable``, which still counts page cache the kernel
    reclaims before it kills anything. The
    cohort's reading too — CannObserv/archiver#285 purged it, CannObserv/watcher#337
    kept it declined, CannObserv/power-map#588 is removing it. What it would
    have protected is held by scores the kernel honours (the units' -900,
    tailscaled's -950) and by ``vm.min_free_kbytes`` for the atomic allocations
    no killer helps.
    """

    def test_the_repo_ships_no_earlyoom_config(self) -> None:
        found = [p.relative_to(REPO_ROOT) for p in (REPO_ROOT / "deploy").rglob("*earlyoom*")]
        assert not found, (
            f"{found} is earlyoom config, which #112 declined — re-decide it there first"
        )

    @on_the_host
    def test_earlyoom_is_not_installed_here(self) -> None:
        assert shutil.which("earlyoom") is None, (
            "earlyoom is installed on this host, which #112 declined — "
            "`sudo apt purge earlyoom`, or re-decide it on #112"
        )


NEEDRESTART_DROPIN = REPO_ROOT / "deploy" / "needrestart.conf.d" / "replicator.conf"
NEEDRESTART_INSTALLED = Path("/etc/needrestart/conf.d/replicator.conf")
NEEDRESTART_MAIN = Path("/etc/needrestart/needrestart.conf")

# needrestart's config is Perl, eval'd into `%nrconf`; evaluating it the same way
# is the only honest parse. A syntax error makes needrestart die, and apt's hook
# swallows that with `|| true` — restarting nothing, so it fails safe but silent.
_NRCONF_EVAL = (
    "our %nrconf; our $LOGPREF = q(); "
    "eval do { local(@ARGV, $/) = $ARGV[0]; <> }; die $@ if $@; "
    "print defined $nrconf{restart} ? $nrconf{restart} : q(undef);"
)


def _needrestart_mode(conf: Path) -> str:
    """Evaluate ``conf`` as needrestart does and return ``$nrconf{restart}``."""
    perl = shutil.which("perl")
    if perl is None:
        pytest.skip("perl not available")
    return subprocess.run(
        [perl, "-e", _NRCONF_EVAL, str(conf)], capture_output=True, text=True, check=True
    ).stdout


class TestNeedrestartListsNeverRestarts:
    """apt's needrestart hook lists restarts and never performs them (#122).

    ``/etc/apt/apt.conf.d/99needrestart`` runs ``needrestart -m u`` after every
    dpkg run, and Ubuntu's patch makes that *automatic* restarts while
    ``$nrconf{restart}`` is unset — the stock state. The worker runs on
    ``/usr/bin/python3.12`` and maps ``libc6`` and ``libexpat1``, so a security
    apply would restart it mid-apply, outside the owner's window, spending a
    start from the 6-per-2h budget. The drop-in makes that independent of
    whether ``NEEDRESTART_MODE=l`` survives every process between the operator
    and the hook. Shape from CannObserv/broker#65 and CannObserv/notifier#91.
    """

    def test_the_drop_in_sets_list_only(self) -> None:
        assert _needrestart_mode(NEEDRESTART_DROPIN) == "l"

    def test_the_drop_in_sets_nothing_else(self) -> None:
        """One key: ``$nrconf{ui}`` stays unset, since setting it without the
        restart key forces interactive mode instead."""
        lines = [
            line.strip()
            for line in NEEDRESTART_DROPIN.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        assert lines == ["$nrconf{restart} = 'l';"]

    @on_the_host
    def test_the_installed_copy_matches_the_tracked_one(self) -> None:
        if not NEEDRESTART_INSTALLED.exists():
            pytest.skip(f"{NEEDRESTART_INSTALLED} not installed yet")
        assert NEEDRESTART_INSTALLED.read_text() == NEEDRESTART_DROPIN.read_text()

    @on_the_host
    def test_the_live_chain_resolves_to_list_only(self) -> None:
        """The main config globs ``conf.d/*.conf`` in sort order, so a later
        file could override this one: evaluate the chain needrestart reads."""
        if not NEEDRESTART_INSTALLED.exists():
            pytest.skip(f"{NEEDRESTART_INSTALLED} not installed yet")
        assert _needrestart_mode(NEEDRESTART_MAIN) == "l"


TAILSCALE_PINS = REPO_ROOT / "deploy" / "apt" / "preferences.d" / "tailscale.pref"
TAILSCALE_PINS_INSTALLED = Path("/etc/apt/preferences.d/tailscale.pref")
TAILSCALE_SITE = "pkgs.tailscale.com"
# What the origin was added for: the client and the key that signs its archive.
TAILSCALE_PACKAGES = {"tailscale", "tailscale-archive-keyring"}


def _pin_stanzas(path: Path) -> list[dict[str, str]]:
    """apt_preferences(5): blank-line-separated stanzas of ``Field: value``."""
    stanzas = []
    for block in path.read_text().split("\n\n"):
        fields = {}
        for line in block.splitlines():
            if line.strip() and not line.lstrip().startswith("#"):
                key, _, value = line.partition(":")
                fields[key.strip()] = value.strip()
        if fields:
            stanzas.append(fields)
    return stanzas


class TestTailscaleOriginIsScoped:
    """The Tailscale origin is *follow*, scoped by apt pins (#131).

    The ``patching-hosts`` maintenance lane takes a followed origin by its site,
    and at apt's default 500 the site ties Ubuntu: any package it served under an
    Ubuntu name at a higher version would replace Ubuntu's. So everything from the
    site sits below 500, and only what the origin was added for sits above it.
    Without both, the probe reports ``unscoped:pkgs.tailscale.com`` and
    ``unpinned:<package>`` (the skill's policy.md, "Two lanes").
    """

    def test_the_file_name_is_one_apt_reads(self) -> None:
        """apt ignores a ``preferences.d`` file with any extension but ``.pref``."""
        assert TAILSCALE_PINS.suffix == ".pref"

    def test_the_site_sits_below_ubuntu(self) -> None:
        (catch_all,) = [s for s in _pin_stanzas(TAILSCALE_PINS) if s["Package"] == "*"]
        assert catch_all["Pin"] == f"origin {TAILSCALE_SITE}"
        assert int(catch_all["Pin-Priority"]) < 500

    def test_each_package_it_was_added_for_sits_above(self) -> None:
        named = {s["Package"]: s for s in _pin_stanzas(TAILSCALE_PINS) if s["Package"] != "*"}
        assert set(named) == TAILSCALE_PACKAGES
        for stanza in named.values():
            assert stanza["Pin"] == f"origin {TAILSCALE_SITE}"
            assert int(stanza["Pin-Priority"]) > 500

    @on_the_host
    def test_the_installed_copy_matches_the_tracked_one(self) -> None:
        if not TAILSCALE_PINS_INSTALLED.exists():
            pytest.skip(f"{TAILSCALE_PINS_INSTALLED} not installed yet")
        assert TAILSCALE_PINS_INSTALLED.read_text() == TAILSCALE_PINS.read_text()
