"""co-core resolves from the co-pypi index, Path C (#183), and from nowhere else.

Design: docs/plans/2026-11-10-co-pypi-index-design.md. The index is
``explicit``: uv consults it only for a package whose ``[tool.uv.sources]``
entry names it, so a PyPI package of the same name can never shadow co-core,
and co-core can never be looked for on PyPI. Every file the lock records from
it carries a sha256, which ``uv sync`` checks on install.
"""

import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet

REPO = Path(__file__).resolve().parent.parent
PYPROJECT = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
LOCK = tomllib.loads((REPO / "uv.lock").read_text(encoding="utf-8"))

INDEX_NAME = "co-pypi"
INDEX_URL = "https://us-west1-python.pkg.dev/co-gcs/co-pypi/simple/"
CO_CORE = {"co-core", "co-core-aio", "co-core-sync"}


def _uv() -> dict:
    return PYPROJECT["tool"]["uv"]


def _from_index() -> list[dict]:
    return [p for p in LOCK["package"] if p.get("source", {}).get("registry") == INDEX_URL]


def test_the_index_is_declared_once_explicit_and_byte_exact():
    assert _uv()["index"] == [{"name": INDEX_NAME, "url": INDEX_URL, "explicit": True}]


def test_find_links_is_gone():
    assert "find-links" not in _uv()


def test_each_co_core_package_is_declared_directly_and_sourced_from_the_index():
    declared = {Requirement(r).name for r in PYPROJECT["project"]["dependencies"]}
    assert CO_CORE <= declared
    assert _uv()["sources"] == {name: {"index": INDEX_NAME} for name in CO_CORE}


def test_the_lock_takes_exactly_co_core_from_the_index():
    assert {p["name"] for p in _from_index()} == CO_CORE


def test_every_file_from_the_index_is_hashed_and_there_is_a_wheel():
    """The hash is the integrity check; the wheel is the CI cache prune's premise
    (tests/ci/test_workflow_pins.py)."""
    for package in _from_index():
        files = [package["sdist"], *package["wheels"]] if "sdist" in package else package["wheels"]
        assert package["wheels"], package["name"]
        for f in files:
            assert f["url"].startswith("https://us-west1-python.pkg.dev/co-gcs/co-pypi/"), f
            assert f["hash"].startswith("sha256:") and len(f["hash"]) == len("sha256:") + 64, f


def test_the_locked_versions_sit_inside_the_pin():
    """The index lists nothing past 0.44.0 [measured 2026-11-12, #184]; the pin's
    upper bound keeps the next minor out until the change that moves it."""
    pins = {
        (r := Requirement(spec)).name: r.specifier
        for spec in PYPROJECT["project"]["dependencies"]
        if Requirement(spec).name in CO_CORE
    }
    for package in _from_index():
        assert package["version"] in pins[package["name"]], package
        assert package["version"] not in SpecifierSet(">=0.45"), package


def test_no_other_source_kind_names_a_wheelhouse():
    assert not [p["name"] for p in LOCK["package"] if ".wheelhouse" in str(p.get("source"))]
