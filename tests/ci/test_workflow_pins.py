"""ci.yml pins its actions, its runner, its cache behaviour and its lock gate on
purpose (#76). Ported from CannObserv/archiver#192 (``c5a7a22``), which took
the cache half from CannObserv/broker#88; assertions only.

**The actions.** Every scanned ``uses:`` is ``owner/action@<40-hex sha> #
vX.Y.Z``. A tag can be moved under us, and Dependabot proposes refs in the
pin's own form, rewriting the SHA and the comment together. That a SHA *is*
its tag needs the network, so it is checked by hand when a pin moves:
``git ls-remote --tags https://github.com/<owner>/<action>``.
``context-cadence.yml`` is generated and excluded (``test_dependabot.py``).

**The cache.** setup-uv v9 flipped ``prune-cache`` to ``false`` (v5.4.2's
``action.yml`` defaults it ``true``). Unpruned, the uv cache a job saves holds
co-core unpacked from the private wheelhouse, and this repo is public: a fork's
pull request can restore a cache saved on ``main``. Dependabot's setup-uv PR
would flip the default with CI green, so each step states it. Every step, not
only those saying ``enable-cache: true``: the default is ``auto``, which caches
on GitHub-hosted runners. ``uv cache prune --ci`` drops pre-built wheels and
keeps wheels *built from source*, so the prune covers co-core only while
uv.lock pins it to a wheel: the lock check below holds that premise.

**The runner.** ``ubuntu-24.04``, not ``ubuntu-latest``: CI matches the host's
distro. ``ubuntu-latest`` moves to 26.04 between 2026-10-19 and 2026-11-19
(actions/runner-images#14748). When the host moves, CI moves with it: on the
host, ``test_runner_image_matches_the_host`` fails until it does.

**The lock gate.** ``uv sync --locked``, not ``--frozen``: ``--frozen`` skips
the check that uv.lock still matches pyproject.toml, which is the half of a
bump PR's gate that proves the new lock resolves. The deploy lines in
docs/COMMANDS.md stay ``--frozen`` on purpose: production never re-resolves.
"""

import re
import socket
import tomllib
from pathlib import Path

import pytest
import yaml

from tests.test_deploy import HOST

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
CI = WORKFLOWS / "ci.yml"
GENERATED = WORKFLOWS / "context-cadence.yml"
LOCK = REPO_ROOT / "uv.lock"
OS_RELEASE = Path("/etc/os-release")

# A `uses:` line as written, comment included: the parsed YAML drops comments.
_USES_LINE = re.compile(r"^\s*(?:-\s+)?uses:\s*(\S+)(.*)$")
# `<40-hex SHA> # vX.Y.Z`, the form Dependabot keeps in step.
_SHA_PIN = re.compile(r"^[^@]+@[0-9a-f]{40}$")
_VERSION_COMMENT = re.compile(r"^\s+# v\d+\.\d+\.\d+$")


def _jobs() -> dict[str, dict]:
    return yaml.safe_load(CI.read_text(encoding="utf-8"))["jobs"]


def _steps() -> list[dict]:
    return [step for job in _jobs().values() for step in job.get("steps", [])]


def _scanned() -> list[Path]:
    """The workflows Dependabot reads: all but the generated one."""
    return [path for path in sorted(WORKFLOWS.glob("*.y*ml")) if path != GENERATED]


def _uses(node: object) -> list[str]:
    """Every ``uses:`` value in a parsed workflow, at any depth."""
    if isinstance(node, dict):
        found = [node["uses"]] if isinstance(node.get("uses"), str) else []
        return found + [u for value in node.values() for u in _uses(value)]
    if isinstance(node, list):
        return [u for item in node for u in _uses(item)]
    return []


def _uses_lines() -> list[tuple[str, int, str, str]]:
    """Every scanned ``uses:`` line: (file, line, ref, rest of line)."""
    return [
        (path.name, n, m.group(1), m.group(2))
        for path in _scanned()
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if (m := _USES_LINE.match(line))
    ]


def _is_pinned(ref: str, rest: str) -> bool:
    """A SHA pin with its version comment, or a ``./`` action in this repo."""
    return ref.startswith("./") or bool(_SHA_PIN.match(ref) and _VERSION_COMMENT.match(rest))


def test_every_uses_line_is_read():
    """The line scan sees every ``uses:`` the parser does, so none escapes the pin rule."""
    parsed = [u for path in _scanned() for u in _uses(yaml.safe_load(path.read_text("utf-8")))]
    assert sorted(ref for _, _, ref, _ in _uses_lines()) == sorted(parsed), (
        "the line scan and the YAML parser disagree on the `uses:` refs: a quoted "
        "ref, a flow-style step or a `uses:` line inside a `run:` block. Write each "
        "step's `uses:` unquoted, on its own line, so the pin rule can read it"
    )


def test_every_action_is_sha_pinned_with_its_version():
    bad = [
        f"{name}:{n}: {ref}{rest}"
        for name, n, ref, rest in _uses_lines()
        if not _is_pinned(ref, rest)
    ]
    assert not bad, (
        "pin each action as `owner/action@<40-hex sha> # vX.Y.Z`: a tag can move, "
        "and Dependabot keeps the SHA and its comment in step. Offending lines:\n  "
        + "\n  ".join(bad)
    )


def test_pin_rule_rejects_a_tag_and_accepts_a_local_action():
    """``./`` actions live in this repo: no tag to move, nothing to pin."""
    assert _is_pinned("./.github/actions/setup", "")
    assert not _is_pinned("actions/checkout@v7", "")
    assert not _is_pinned("actions/checkout@" + "0" * 40, "")
    assert _is_pinned("actions/checkout@" + "0" * 40, " # v7.0.1")


def test_every_setup_uv_step_prunes_the_cache_it_saves():
    """co-core's private source stays out of a public repo's Actions cache."""
    steps = [s for s in _steps() if s.get("uses", "").split("@")[0] == "astral-sh/setup-uv"]
    assert steps, "no setup-uv step found in ci.yml: the derivation broke"
    bad = [s.get("with", {}) for s in steps if s.get("with", {}).get("prune-cache") is not True]
    assert not bad, f"setup-uv steps without `prune-cache: true`: {bad}"


def _wheelhouse_packages_without_a_wheel(lock: Path) -> list[str]:
    packages = tomllib.loads(lock.read_text(encoding="utf-8"))["package"]
    return [
        p["name"]
        for p in packages
        if p.get("source", {}).get("registry") == ".wheelhouse" and not p.get("wheels")
    ]


def test_every_wheelhouse_package_is_locked_to_a_wheel():
    """The prune's premise: a package installed from its wheel is pruned."""
    assert _wheelhouse_packages_without_a_wheel(LOCK) == []


def test_wheel_check_fails_on_an_sdist_only_lock(tmp_path):
    lock = tmp_path / "uv.lock"
    lock.write_text(
        '[[package]]\nname = "co-core"\nversion = "9.9.9"\n'
        'source = { registry = ".wheelhouse" }\n'
        'sdist = { path = "co_core-9.9.9.tar.gz" }\n'
    )
    assert _wheelhouse_packages_without_a_wheel(lock) == ["co-core"]


def _runners() -> set[str]:
    return {job["runs-on"] for job in _jobs().values()}


def test_ci_runs_on_one_pinned_image():
    runners = _runners()
    assert len(runners) == 1, runners
    assert not runners.pop().endswith("-latest")


def _os_version_id(os_release: Path) -> str:
    found = re.search(r'^VERSION_ID="?([^"\n]+)"?$', os_release.read_text(), re.MULTILINE)
    assert found, f"{os_release} names no VERSION_ID"
    return found[1]


def test_runner_image_matches_the_host():
    if socket.gethostname() != HOST:
        pytest.skip(f"not {HOST}")
    assert _runners() == {f"ubuntu-{_os_version_id(OS_RELEASE)}"}


def test_os_version_id_reads_a_quoted_and_a_bare_value(tmp_path):
    os_release = tmp_path / "os-release"
    os_release.write_text('NAME="Ubuntu"\nVERSION_ID="26.04"\n')
    assert _os_version_id(os_release) == "26.04"
    os_release.write_text("VERSION_ID=24.04\n")
    assert _os_version_id(os_release) == "24.04"


def test_every_uv_sync_checks_the_lock():
    """``--locked`` errors on a stale lock; ``--frozen`` installs it unchecked."""
    syncs = [
        line
        for step in _steps()
        for line in step.get("run", "").splitlines()
        if re.match(r"\s*uv sync\b", line)
    ]
    assert syncs, "no `uv sync` in ci.yml: the derivation broke"
    bad = [s.strip() for s in syncs if "--locked" not in s.split()]
    assert not bad, f"`uv sync` without --locked: {bad}"
