"""The browser's cgroup: where a runaway render is killed instead of the worker (#138).

Against a fake cgroupfs in ``tmp_path``: every control file is a plain file, so these pin
what is *written* where. That the kernel then kills the right thing is the ``-m browser``
suite's to show, against a real delegated cgroup.

The tests that matter most:

- **the worker leaf re-claims the unit's ``memory.low``**, because without
  ``memory_recursiveprot`` the unit's ``MemoryLow=`` protects nothing once the worker sits
  in ``worker/`` (the #113 knock-on);
- **no delegation, no browser cgroup**, said as an error naming the fix, because the
  alternative is a browser with no cap in the worker's own cgroup at -900;
- **the cap, swap, group-kill and pid limit are all written**, because each one missing is
  a different way the cap stops holding.
"""

from pathlib import Path

import pytest

from src.worker.browser_cgroup import (
    BrowserCgroup,
    CgroupUnavailableError,
    claim_worker_memory_low,
    delegated_service_cgroup,
)

SERVICE = "system.slice/replicator.service"


def _fs(tmp_path: Path, leaf: str = f"{SERVICE}/worker", *, memory_low: str = "134217728"):
    root = tmp_path / "cgroup"
    own = root / leaf
    own.mkdir(parents=True)
    (own / "memory.low").write_text("0\n")
    (own.parent / "memory.low").write_text(f"{memory_low}\n")
    (own.parent / "cgroup.subtree_control").write_text("\n")
    proc = tmp_path / "proc-self-cgroup"
    proc.write_text(f"0::/{leaf}\n")
    return root, proc


def _prepare(root: Path, proc: Path, **kwargs) -> BrowserCgroup:
    def make_browser(path: Path) -> None:
        # A real mkdir under cgroupfs creates the control files; fake that here.
        path.mkdir(exist_ok=True)
        for name in ("memory.max", "memory.swap.max", "memory.oom.group", "pids.max"):
            (path / name).touch()
        for name in ("cgroup.procs", "cgroup.kill"):
            (path / name).touch()

    return BrowserCgroup.prepare(
        memory_max=kwargs.pop("memory_max", 1536 * 2**20),
        root=root,
        proc_self_cgroup=proc,
        mkdir=make_browser,
        **kwargs,
    )


def test_the_delegated_service_cgroup_is_the_worker_leafs_parent(tmp_path):
    root, proc = _fs(tmp_path)

    assert delegated_service_cgroup(root=root, proc_self_cgroup=proc) == root / SERVICE


def test_a_worker_outside_a_worker_leaf_is_not_delegated(tmp_path):
    root, proc = _fs(tmp_path, leaf="user.slice/user-1000.slice/session-3.scope")

    assert delegated_service_cgroup(root=root, proc_self_cgroup=proc) is None


def test_the_worker_leaf_reclaims_the_units_reservation(tmp_path):
    root, proc = _fs(tmp_path)

    claimed = claim_worker_memory_low(root=root, proc_self_cgroup=proc)

    assert claimed == 134217728
    assert (root / SERVICE / "worker" / "memory.low").read_text() == "134217728"


def test_an_undelegated_worker_claims_nothing(tmp_path):
    root, proc = _fs(tmp_path, leaf="user.slice/session-3.scope")

    assert claim_worker_memory_low(root=root, proc_self_cgroup=proc) is None


def test_the_browser_sibling_carries_every_limit(tmp_path):
    root, proc = _fs(tmp_path)

    cgroup = _prepare(root, proc, memory_max=1536 * 2**20, pids_max=512)

    browser = root / SERVICE / "browser"
    assert cgroup.path == browser
    assert (browser / "memory.max").read_text() == str(1536 * 2**20)
    assert (browser / "memory.swap.max").read_text() == "0"
    assert (browser / "memory.oom.group").read_text() == "1"
    assert (browser / "pids.max").read_text() == "512"
    assert "+memory" in (root / SERVICE / "cgroup.subtree_control").read_text()
    assert "+pids" in (root / SERVICE / "cgroup.subtree_control").read_text()


def test_without_delegation_there_is_no_browser_cgroup(tmp_path):
    root, proc = _fs(tmp_path, leaf="user.slice/session-3.scope")

    with pytest.raises(CgroupUnavailableError, match="DelegateSubgroup=worker"):
        _prepare(root, proc)


def test_kill_writes_cgroup_kill(tmp_path):
    root, proc = _fs(tmp_path)
    cgroup = _prepare(root, proc)

    cgroup.kill()

    assert (cgroup.path / "cgroup.kill").read_text() == "1"


def test_a_leftover_browser_is_killed_when_the_cgroup_is_prepared(tmp_path):
    """A worker that died mid-render leaves its browser behind under a delegated unit."""
    root, proc = _fs(tmp_path)
    browser = root / SERVICE / "browser"
    browser.mkdir()
    (browser / "cgroup.procs").write_text("4242\n")
    (browser / "cgroup.kill").write_text("")

    _prepare(root, proc)

    assert (browser / "cgroup.kill").read_text() == "1"


def test_oom_group_kills_are_read_from_memory_events(tmp_path):
    root, proc = _fs(tmp_path)
    cgroup = _prepare(root, proc)
    (cgroup.path / "memory.events").write_text(
        "low 0\nhigh 0\nmax 12\noom 1\noom_kill 6\noom_group_kill 1\n"
    )

    assert cgroup.oom_group_kills() == 1


def test_the_wrapper_environment_names_the_cgroup(tmp_path):
    root, proc = _fs(tmp_path)
    cgroup = _prepare(root, proc)

    assert cgroup.procs_file == root / SERVICE / "browser" / "cgroup.procs"
