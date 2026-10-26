"""The browser's cgroup: a runaway render costs the browser, not the worker (#138).

``deploy/replicator.service`` carries ``Delegate=memory pids`` and
``DelegateSubgroup=worker``: systemd starts the worker in ``<unit>/worker`` and hands the
unit's cgroup to ``exedev``. This module makes the sibling ``<unit>/browser`` and writes
its limits; ``scripts/browser_exec.sh`` moves each launched browser into it before
``exec``, so every Chromium child is born there.

- ``memory.max`` — ``REPLICATOR_CAPTURE_MEMORY_MAX_BYTES``. The spike's longest page peaked at
  768 MiB of browser; the default is twice that.
- ``memory.swap.max=0`` — a browser over its cap is killed, never paged out to make room.
- ``memory.oom.group=1`` — the kernel kills the whole browser tree at once. Measured: six
  processes killed, the worker untouched, Playwright raising ``TargetClosedError``.
- ``pids.max`` — a fork bomb in a page is the browser's problem.

**The worker leaf re-claims the unit's ``memory.low``** (:func:`claim_worker_memory_low`).
This host has no ``memory_recursiveprot``, so the ``MemoryLow=`` systemd writes on the unit
is not handed down: a child at ``memory.low=0`` gets no protection from its parent's. The
value is copied from the unit's cgroup rather than restated, so ``deploy/`` stays the one
place it is set (docs/MEMORY-PROTECTION.md).

Everything here takes ``root`` and ``proc_self_cgroup`` so the tests can point it at a
fake cgroupfs; production uses the defaults.
"""

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from src.core.logging import get_logger

logger = get_logger(__name__)

CGROUP_ROOT = Path("/sys/fs/cgroup")
PROC_SELF_CGROUP = Path("/proc/self/cgroup")
WORKER_LEAF = "worker"
BROWSER_LEAF = "browser"
DEFAULT_PIDS_MAX = 512


class CgroupUnavailableError(RuntimeError):
    """There is no delegated cgroup to put a browser in, so no browser may launch."""


def _own_cgroup(root: Path, proc_self_cgroup: Path) -> Path:
    # cgroup v2 only: one line, "0::<path>".
    for line in proc_self_cgroup.read_text().splitlines():
        hierarchy, _, path = line.partition("::")
        if hierarchy == "0":
            return root / path.lstrip("/")
    raise CgroupUnavailableError(f"{proc_self_cgroup} names no cgroup v2 path")


def delegated_service_cgroup(
    *, root: Path = CGROUP_ROOT, proc_self_cgroup: Path = PROC_SELF_CGROUP
) -> Path | None:
    """The unit's cgroup when this process runs in its ``worker`` leaf, else ``None``.

    ``None`` is a dev run, a test, or a unit installed before #138: no delegation.
    """
    own = _own_cgroup(root, proc_self_cgroup)
    if own.name != WORKER_LEAF:
        return None
    return own.parent


def claim_worker_memory_low(
    *, root: Path = CGROUP_ROOT, proc_self_cgroup: Path = PROC_SELF_CGROUP
) -> int | None:
    """Copy the unit's ``memory.low`` onto the worker leaf; the bytes claimed, or ``None``.

    Runs at every boot, capture on or off: delegation moves the worker into the leaf
    whether or not a browser ever launches. systemd 255 leaves a delegated unit's
    ``subtree_control`` empty, so the leaf has no ``memory.low`` until the controller is
    enabled here (CR 1); ``BrowserCgroup.prepare`` writes the same line, harmlessly.
    """
    service = delegated_service_cgroup(root=root, proc_self_cgroup=proc_self_cgroup)
    if service is None:
        return None
    _enable_controllers(service)
    value = (service / "memory.low").read_text().strip()
    (service / WORKER_LEAF / "memory.low").write_text(value)
    return 2**63 if value == "max" else int(value)


def _enable_controllers(service: Path) -> None:
    (service / "cgroup.subtree_control").write_text("+memory +pids")


@dataclass(frozen=True)
class BrowserCgroup:
    """The prepared ``<unit>/browser`` cgroup."""

    path: Path

    @classmethod
    def prepare(
        cls,
        *,
        memory_max: int,
        pids_max: int = DEFAULT_PIDS_MAX,
        root: Path = CGROUP_ROOT,
        proc_self_cgroup: Path = PROC_SELF_CGROUP,
        mkdir: Callable[[Path], None] = lambda path: path.mkdir(exist_ok=True),
    ) -> "BrowserCgroup":
        """Make (or reuse) the sibling and write its limits.

        A browser left in it by a worker that died mid-render is killed first: its
        render has no owner any more, and its memory counts against the next one's cap.
        """
        service = delegated_service_cgroup(root=root, proc_self_cgroup=proc_self_cgroup)
        if service is None:
            raise CgroupUnavailableError(
                "the worker is not in a delegated unit's worker leaf: install "
                "deploy/replicator.service (Delegate=memory pids, DelegateSubgroup=worker), "
                "or set REPLICATOR_CAPTURE_CGROUP=off on a dev run"
            )
        if not os.access(service / "cgroup.subtree_control", os.W_OK):
            raise CgroupUnavailableError(f"{service} is not delegated to this user")
        _enable_controllers(service)
        browser = service / BROWSER_LEAF
        mkdir(browser)
        cgroup = cls(browser)
        if cgroup.populated():
            logger.warning("killing a browser left by an earlier render")
            cgroup.kill()
        (browser / "memory.max").write_text(str(memory_max))
        (browser / "memory.swap.max").write_text("0")
        (browser / "memory.oom.group").write_text("1")
        (browser / "pids.max").write_text(str(pids_max))
        return cgroup

    @property
    def procs_file(self) -> Path:
        """What ``scripts/browser_exec.sh`` writes its pid into."""
        return self.path / "cgroup.procs"

    def populated(self) -> bool:
        try:
            return bool(self.procs_file.read_text().split())
        except FileNotFoundError:
            return False

    def kill(self) -> None:
        """SIGKILL everything in the cgroup, without waiting (kernel ≥ 5.14)."""
        (self.path / "cgroup.kill").write_text("1")

    def oom_group_kills(self) -> int:
        """How many times the kernel has group-killed this cgroup since it was made."""
        for line in (self.path / "memory.events").read_text().splitlines():
            key, _, value = line.partition(" ")
            if key == "oom_group_kill":
                return int(value)
        return 0
