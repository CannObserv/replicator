"""The co-core pin is one specifier, written the same way everywhere it is named (#153).

The three packages move together: `co-core-aio` and `co-core-sync` each require
`co-core` at their own minor, so a skewed specifier fails at resolve time at
best. The prose sites are worse. AGENTS.md, docs/DEPLOYMENT.md and the
SocratiCode manifest each name the current pin, nothing reads them, and a
bump that forgets one stays green while an agent is told the wrong minor. The
manifest is the costliest to leave stale: its description is what semantic
search ranks first (#19).
"""

import json
import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGES = ("co-core", "co-core-aio", "co-core-sync")
_REQ = re.compile(r"^(?P<name>[A-Za-z0-9_.-]+)(?:\[[^\]]*\])?(?P<spec>.*)$")


def co_core_specifiers() -> dict[str, str]:
    """Map each co-core package to its specifier in pyproject.toml."""
    deps = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    found = {}
    for dep in deps:
        m = _REQ.match(dep.replace(" ", ""))
        if m and m["name"] in PACKAGES:
            found[m["name"]] = m["spec"]
    return found


def the_pin() -> str:
    specs = co_core_specifiers()
    assert set(specs) == set(PACKAGES), f"pyproject.toml pins {sorted(specs)}"
    return specs["co-core"]


def test_the_three_packages_share_one_specifier():
    specs = co_core_specifiers()
    assert set(specs) == set(PACKAGES)
    assert len(set(specs.values())) == 1, specs


def test_the_pin_is_one_minor():
    m = re.fullmatch(r">=0\.(\d+)\.\d+,<0\.(\d+)", the_pin())
    assert m, the_pin()
    assert int(m[2]) == int(m[1]) + 1, the_pin()


def test_agents_md_names_the_pin():
    assert f"Pin the current minor — `{the_pin()}`" in (REPO_ROOT / "AGENTS.md").read_text()


def test_deployment_md_names_the_pin():
    text = (REPO_ROOT / "docs" / "DEPLOYMENT.md").read_text()
    assert f"Pin the current minor — `{the_pin()}`, on all three" in text


def test_the_manifest_names_the_pin():
    manifest = json.loads((REPO_ROOT / ".socraticodecontextartifacts.json").read_text())
    descriptions = [
        a["description"] for a in manifest["artifacts"] if "co-core-sync pins" in a["description"]
    ]
    assert len(descriptions) == 1, "one manifest artifact describes the co-core pins"
    assert f"pins ({the_pin()} since" in descriptions[0]
