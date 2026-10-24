"""The SocratiCode server pin holds at every site that carries it (#152).

Two things launch a SocratiCode server on this host. The driver — the daily
health hook, `index`, `verify` — prefers the pre-install under
``~/.socraticode/pin``. The session's server is the plugin's, launched as
``npx -y --prefer-online ${SOCRATICODE_SPEC:-socraticode@latest}``.

The settings ``env`` block *declares* that spec but does not reach the launch:
Claude Code expands the plugin's args before it merges the block
(gregoryfoster/skills#332, measured on co-watcher and co-notifier). VS Code's
machine-scoped ``claudeCode.environmentVariables`` *delivers* it, being in
``claude``'s environment from the start. Before #152 this repo had only the
block: the running session's ``claude`` carried no ``SOCRATICODE_SPEC`` — the
state #332 measured launching ``@latest`` — while every check reading the
variable from its own environment called it pinned.

A spec that floats installs at session start on any day the package moves —
the 1.2 G cold-install peak `TestServerLaunchCost` exists to keep off this host,
whose 8 GiB and 4 G of swap since #99 are headroom rather than immunity: the
worker runs beside every session (docs/COMMANDS.md). A spec that differs from the pre-install puts
two builds on one shared store. And a pinned spec whose npx entry is cold moves
that same install to the next session start.

So a re-pin changes five things together — the block, docs/SOCRATICODE.md, the
pre-install, the machine setting, the warm npx entry — and these fail until all
five agree. The last three are VM-local: CI has none of them and skips, which
is why they are compared here rather than trusted. Ported from
CannObserv/notifier's ``tests/deploy/test_socraticode_pin.py`` (notifier#87,
notifier#134).
"""

import json
import os
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SETTINGS = REPO_ROOT / ".claude" / "settings.json"
SOCRATICODE_DOC = REPO_ROOT / "docs" / "SOCRATICODE.md"

# The plugin's live launch expands this; unset, it resolves to @latest.
SPEC_VARIABLE = "SOCRATICODE_SPEC"

# A literal release, as the doc and the block spell it.
EXACT_SPEC = re.compile(r"socraticode@(\d+\.\d+\.\d+)")

# VS Code's machine-scoped settings on this remote host. The extension sets
# `claudeCode.environmentVariables` in `claude`'s environment at startup, which
# is when the plugin's args are expanded.
VSCODE_SERVER = Path.home() / ".vscode-server"
VSCODE_MACHINE_SETTINGS = VSCODE_SERVER / "data" / "Machine" / "settings.json"
VSCODE_ENV_SETTING = "claudeCode.environmentVariables"


def pinned_install_version() -> str | None:
    """The pre-install's version, found where mcp-driver.mjs looks; None if absent.

    SOCRATICODE_PIN_DIR, else ~/.socraticode/pin — an empty value falls back
    too, as the driver's `||` does.
    """
    pin_dir = Path(os.environ.get("SOCRATICODE_PIN_DIR") or "~/.socraticode/pin").expanduser()
    manifest = pin_dir / "node_modules" / "socraticode" / "package.json"
    if not manifest.exists():
        return None
    return json.loads(manifest.read_text())["version"]


def npx_cached_versions() -> set[str]:
    """SocratiCode releases in npx's cache, which `npx -y` reuses without installing.

    npx keys each entry by its requested specs under ``<cache>/_npx/<hash>``;
    the cache is ``npm_config_cache`` when set, else ``~/.npm``.
    """
    cache = Path(os.environ.get("npm_config_cache") or "~/.npm").expanduser()
    return {
        json.loads(manifest.read_text())["version"]
        for manifest in cache.glob("_npx/*/node_modules/socraticode/package.json")
    }


def vscode_launch_spec() -> str | None:
    """SOCRATICODE_SPEC as the extension hands it to `claude` at startup; None if unset."""
    if not VSCODE_MACHINE_SETTINGS.exists():
        return None
    variables = json.loads(VSCODE_MACHINE_SETTINGS.read_text()).get(VSCODE_ENV_SETTING, [])
    return next((v["value"] for v in variables if v.get("name") == SPEC_VARIABLE), None)


@pytest.fixture(scope="module")
def session_spec() -> str:
    """The declared spec; its literal shape is `TestServerLaunchCost`'s to pin."""
    spec = json.loads(SETTINGS.read_text()).get("env", {}).get(SPEC_VARIABLE)
    assert spec is not None and EXACT_SPEC.fullmatch(spec), (
        f"{SPEC_VARIABLE}={spec!r} in {SETTINGS.name} is not socraticode@<major>.<minor>.<patch>"
    )
    return spec


def test_session_spec_matches_the_driver_pin(session_spec: str) -> None:
    """One build per host: the session and the driver write the same shared store."""
    pinned = pinned_install_version()
    if pinned is None:
        pytest.skip("no SocratiCode pre-install on this machine")
    assert session_spec == f"socraticode@{pinned}", (
        f"{SPEC_VARIABLE}={session_spec!r} but the driver's pre-install is {pinned} — "
        "re-pin per docs/SOCRATICODE.md"
    )


def test_session_spec_is_warm_in_the_npx_cache(session_spec: str) -> None:
    """The plugin's `npx -y --prefer-online <spec>` must find its entry, not install it.

    Skips where there is no pre-install, as in CI: a host without one is not
    running this pin.
    """
    if pinned_install_version() is None:
        pytest.skip("no SocratiCode pre-install on this machine")
    version = EXACT_SPEC.fullmatch(session_spec).group(1)
    cached = npx_cached_versions()
    assert version in cached, (
        f"no npx cache entry holds socraticode {version} (cached: {sorted(cached)}); "
        "the next session start installs it — warm it per docs/SOCRATICODE.md"
    )


def test_socraticode_doc_names_only_the_session_spec(session_spec: str) -> None:
    """The doc says what the session launches; a re-pin that skips it leaves it lying.

    Every literal spec in it, not one: a re-pin that updates the first mention
    and misses a later one would otherwise pass.
    """
    doc = SOCRATICODE_DOC.read_text()
    named = {match.group(0) for match in EXACT_SPEC.finditer(doc)}
    assert named == {session_spec}, (
        f"{SOCRATICODE_DOC.name} names {sorted(named)}; the session launches {session_spec}"
    )


def test_vscode_launches_claude_with_the_session_spec(session_spec: str) -> None:
    """The block declares the pin; this is what delivers it (gregoryfoster/skills#332).

    VM-local: skips only where no VS Code server is installed, as in CI — on
    this VM a missing setting is the defect, and it was the state until #152.
    """
    if not VSCODE_SERVER.exists():
        pytest.skip("no VS Code server on this machine")
    delivered = vscode_launch_spec()
    assert delivered == session_spec, (
        f"{VSCODE_MACHINE_SETTINGS} gives claude {SPEC_VARIABLE}={delivered!r} at startup; "
        f"{SETTINGS.name} declares {session_spec!r}"
    )
