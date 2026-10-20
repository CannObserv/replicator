"""The co-core import-closure diff: ``scripts/co_core_closure_diff.py`` (#145).

DEPLOYMENT.md requires a diff of co-core's whole import closure before every pin
move, and #140 and #136 each rebuilt the walker to get one. The properties that
matter are the traps those two found: a parent package ``__init__`` reached only
transitively, ``from pkg import submodule``, relative imports, imports under
``TYPE_CHECKING``, and a module that joins the closure between versions. And a
verdict that tells a docstring or ``__version__`` edit from a code change.

The synthetic wheels are built in ``tmp_path``, so none of this needs a
``.wheelhouse``. The one real-wheel check skips where the wheels aren't synced.
"""

import zipfile
from pathlib import Path

import pytest

from scripts.co_core_closure_diff import (
    Edge,
    MissingWheelError,
    build_report,
    closure,
    load_wheels,
    main,
    root_imports,
    verdict,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ["co_core", "co_core_aio"]


def _wheel(wheelhouse: Path, package: str, version: str, files: dict, requires=()) -> None:
    """Write ``<package>-<version>-py3-none-any.whl`` holding ``files`` and a METADATA."""
    path = wheelhouse / f"{package}-{version}-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as z:
        for name, source in files.items():
            z.writestr(name, source)
        meta = "Metadata-Version: 2.1\nName: x\n" + "".join(
            f"Requires-Dist: {r}\n" for r in requires
        )
        z.writestr(f"{package}-{version}.dist-info/METADATA", meta)


def _core(version: str, *, v2: bool) -> dict:
    """A fake ``co_core`` carrying one of each trap, at either version."""
    changes = (
        '"""Models."""\n'
        "import json\n"
        "import pydantic\n"
        "from co_core.extract.canonical import canon\n"
        "from ..util import hashing\n"
        + ("from co_core.models import policy\n" if v2 else "import co_core.legacy\n")
        + "class State(pydantic.BaseModel):\n"
        "    host: str\n" + ("    generation: int | None = None\n" if v2 else "")
    )
    return {
        "co_core/__init__.py": f'"""Core."""\n__version__ = "{version}"\n',
        "co_core/models/__init__.py": "",
        "co_core/models/changes.py": changes,
        # In both wheels, imported only at v2: it joins the closure.
        "co_core/models/policy.py": "X = 1\n",
        # Imported only at v1: it leaves.
        "co_core/legacy.py": "Y = 2\n",
        # Reached only through changes.py's import of extract.canonical.
        "co_core/extract/__init__.py": "from . import config\n",
        "co_core/extract/canonical.py": "def canon(): pass\n",
        "co_core/extract/config.py": "Z = 3\n",
        "co_core/extract/unused.py": "W = 4\n",
        "co_core/util/__init__.py": "",
        "co_core/util/hashing.py": "def digest(): return 1\n",
        # Docstring changes at v2, code doesn't.
        "co_core/bus/__init__.py": (
            "from typing import TYPE_CHECKING\n"
            "from importlib import import_module\n"
            "CONST = 1\n"
            "if TYPE_CHECKING:\n"
            "    from co_core.bus.gcs import Store\n"
            "def __getattr__(name):\n"
            "    return getattr(import_module(f'{__name__}.gcs'), name)\n"
        ),
        "co_core/bus/streams.py": (
            f'"""Streams{" (now documented)" if v2 else ""}."""\n'
            "def group():\n"
            f'    """Group{" name" if v2 else ""}."""\n'
            "    return 1\n"
        ),
        "co_core/bus/gcs.py": "import google.cloud.storage\nclass Store: pass\n",
        "co_core/lazy.py": "",
        "co_core/guarded.py": "",
    }


def _aio(version: str) -> dict:
    return {
        "co_core_aio/__init__.py": (
            f'__version__ = "{version}"\n'
            "def connect():\n"
            "    import co_core.lazy\n"
            "try:\n"
            "    import co_core.guarded\n"
            "except ImportError:\n"
            "    pass\n"
        ),
    }


@pytest.fixture
def repo(tmp_path):
    """A repo with roots ``src`` and ``scripts`` and a two-version wheelhouse."""
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "app.py").write_text(
        "import fastapi\n"
        "from co_core.models import changes\n"
        "from co_core.bus import streams, CONST\n"
        "from .sibling import thing\n"
    )
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "tool.py").write_text("import co_core_aio\n")
    wh = tmp_path / ".wheelhouse"
    wh.mkdir()
    _wheel(wh, "co_core", "1.0.0", _core("1.0.0", v2=False), ["pydantic>=2"])
    _wheel(wh, "co_core", "1.1.0", _core("1.1.0", v2=True), ["pydantic>=2", "httpx<1"])
    _wheel(wh, "co_core_aio", "1.0.0", _aio("1.0.0"), ["co-core<1.1,>=1.0"])
    _wheel(wh, "co_core_aio", "1.1.0", _aio("1.1.0"), ["co-core<1.2,>=1.1"])
    return tmp_path


def _closure(repo: Path, version: str):
    wheels = load_wheels(repo / ".wheelhouse", PACKAGES, version)
    seeds = root_imports([repo / "src", repo / "scripts"])
    return closure(wheels.files, PACKAGES, seeds)


# --- the walk -----------------------------------------------------------------


def test_root_imports_are_absolute_names_only(repo):
    seeds = root_imports([repo / "src"])
    assert Edge("co_core.models") in seeds
    # `from co_core.bus import streams`: a module only if the wheel has one.
    assert Edge("co_core.bus.streams", definite=False) in seeds
    assert not any(e.name.startswith(("sibling", ".")) for e in seeds)


def test_every_parent_init_is_in_the_closure(repo):
    files = _closure(repo, "1.0.0").files
    assert {"co_core/__init__.py", "co_core/models/__init__.py"} <= files


def test_a_package_reached_only_transitively_brings_its_init_and_its_imports(repo):
    files = _closure(repo, "1.0.0").files
    assert "co_core/extract/__init__.py" in files
    assert "co_core/extract/canonical.py" in files
    assert "co_core/extract/config.py" in files  # `from . import config` in the __init__
    assert "co_core/extract/unused.py" not in files


def test_from_pkg_import_name_is_a_module_edge_only_when_name_is_a_module(repo):
    files = _closure(repo, "1.0.0").files
    assert "co_core/bus/streams.py" in files
    assert not any("CONST" in f for f in files)


def test_relative_imports_resolve_against_the_importing_package(repo):
    assert "co_core/util/hashing.py" in _closure(repo, "1.0.0").files


def test_type_checking_function_body_and_guarded_imports_all_count(repo):
    files = _closure(repo, "1.0.0").files
    assert "co_core/bus/gcs.py" in files  # under `if TYPE_CHECKING:`
    assert "co_core/lazy.py" in files  # inside a function body
    assert "co_core/guarded.py" in files  # inside try/except ImportError


def test_imports_cross_between_the_named_packages(repo):
    assert "co_core_aio/__init__.py" in _closure(repo, "1.0.0").files


def test_each_version_is_walked_separately(repo):
    old, new = _closure(repo, "1.0.0").files, _closure(repo, "1.1.0").files
    assert "co_core/legacy.py" in old and "co_core/legacy.py" not in new
    assert "co_core/models/policy.py" in new and "co_core/models/policy.py" not in old


def test_third_party_imports_stop_the_walk_and_are_listed(repo):
    third = _closure(repo, "1.0.0").third_party
    assert third == {"google", "pydantic"}  # not json (stdlib), not fastapi (a root's)


def test_dynamic_imports_are_listed_not_followed(repo):
    dynamic = _closure(repo, "1.0.0").dynamic
    assert len(dynamic) == 1
    path, line, call = dynamic[0]
    assert (path, line) == ("co_core/bus/__init__.py", 7)
    assert call.startswith("import_module(")


def test_a_file_that_does_not_parse_is_listed(repo):
    files = _core("1.0.0", v2=False)
    files["co_core/util/hashing.py"] = "def digest(:\n"
    wheels = {**files, **_aio("1.0.0")}
    c = closure({k: v.encode() for k, v in wheels.items()}, PACKAGES, root_imports([repo / "src"]))
    assert "co_core/util/hashing.py" in c.unparsed


def test_an_import_of_a_module_the_wheel_lacks_is_unresolved(repo):
    files = {k: v.encode() for k, v in {**_core("1.0.0", v2=False), **_aio("1.0.0")}.items()}
    del files["co_core/extract/canonical.py"]
    seeds = [Edge("co_core.models.changes"), Edge("co_core.gone")]
    c = closure(files, PACKAGES, [*seeds, Edge("co_core.bus.CONST", definite=False)])
    assert c.unresolved == frozenset({"co_core.extract.canonical", "co_core.gone"})


def test_nothing_is_unresolved_when_every_module_exists(repo):
    assert _closure(repo, "1.0.0").unresolved == frozenset()


def test_a_namespace_package_is_not_unresolved():
    files = {"co_core/__init__.py": b"", "co_core/ns/mod.py": b""}  # ns/ has no __init__
    c = closure(files, PACKAGES, [Edge("co_core.ns"), Edge("co_core.ns.mod")])
    assert c.unresolved == frozenset()
    assert c.files == frozenset({"co_core/__init__.py", "co_core/ns/mod.py"})


def test_a_relative_import_past_the_top_level_resolves_to_nothing():
    files = {"co_core/__init__.py": b"", "co_core/x.py": b"from ..pydantic import BaseModel\n"}
    assert closure(files, PACKAGES, [Edge("co_core.x")]).third_party == frozenset()


# --- verdicts -----------------------------------------------------------------


def test_equal_bytes_are_identical():
    assert verdict(b"x = 1\n", b"x = 1\n") == "identical"


def test_docstrings_and_comments_alone_are_docstrings_only():
    old = b'"""A."""\ndef f():\n    """B."""\n    return 1  # one\nclass C:\n    """D."""\n'
    new = b'"""A2."""\ndef f():\n    """B2."""\n    return 1  # two\nclass C:\n    """D2."""\n'
    assert verdict(old, new) == "docstrings-only"


def test_a_docstring_that_is_the_whole_body_strips_cleanly():
    assert verdict(b'def f():\n    """A."""\n', b'def f():\n    """B."""\n') == "docstrings-only"


def test_a_version_bump_is_version_only_even_with_a_docstring_edit():
    old = b'"""Core."""\n__version__ = "1.0.0"\n'
    new = b'"""Core, now longer."""\n__version__ = "1.1.0"\n'
    assert verdict(old, new) == "version-only"


def test_anything_else_is_changed():
    assert verdict(b"x = 1\n", b"x = 2\n") == "changed"
    assert verdict(b'__version__ = "1"\nx = 1\n', b'__version__ = "2"\nx = 2\n') == "changed"


def test_a_file_at_one_version_only_is_added_or_removed():
    assert verdict(None, b"x = 1\n") == "added"
    assert verdict(b"x = 1\n", None) == "removed"


# --- the report ---------------------------------------------------------------


def _report(repo: Path, **kw) -> str:
    return build_report(
        "1.0.0",
        "1.1.0",
        roots=[repo / "src", repo / "scripts"],
        packages=PACKAGES,
        wheelhouse=repo / ".wheelhouse",
        **kw,
    )


def test_the_report_counts_and_lists_every_file_that_is_not_identical(repo):
    report = _report(repo)
    assert "15 files at 1.0.0, 15 at 1.1.0" in report
    for line in [
        "- `version-only` co_core/__init__.py",
        "- `version-only` co_core_aio/__init__.py",
        "- `docstrings-only` co_core/bus/streams.py",
        "- `changed` co_core/models/changes.py",
        "- `added` co_core/models/policy.py",
        "- `removed` co_core/legacy.py",
    ]:
        assert line in report
    assert "co_core/util/hashing.py" not in report  # identical files aren't listed


def test_the_report_gives_the_requires_dist_delta_per_package(repo):
    report = _report(repo)
    assert "- co_core: added `httpx<1`" in report
    assert "- co_core_aio: removed `co-core<1.1,>=1.0`; added `co-core<1.2,>=1.1`" in report


def test_the_report_names_unresolved_and_unparsed_only_when_there_are_any(repo):
    assert "Unresolved" not in _report(repo) and "Not parsed" not in _report(repo)
    wh = repo / ".wheelhouse"
    files = _core("1.2.0", v2=True)
    del files["co_core/extract/canonical.py"]
    files["co_core/util/hashing.py"] = "def digest(:\n"
    _wheel(wh, "co_core", "1.2.0", files)
    _wheel(wh, "co_core_aio", "1.2.0", _aio("1.2.0"))
    report = build_report("1.1.0", "1.2.0", roots=[repo / "src"], packages=PACKAGES, wheelhouse=wh)
    assert "**Unresolved imports** (at 1.2.0): `co_core.extract.canonical`." in report
    assert "**Not parsed, imports not followed** (at 1.2.0): `co_core/util/hashing.py`." in report


def test_the_report_lists_third_party_and_dynamic_imports(repo):
    report = _report(repo)
    assert "`google`, `pydantic`" in report
    assert "co_core/bus/__init__.py:7" in report


def test_a_dynamic_import_that_only_moved_lines_is_listed_once(repo):
    wh = repo / ".wheelhouse"
    files = _core("1.2.0", v2=True)
    files["co_core/bus/__init__.py"] = "\n\n" + files["co_core/bus/__init__.py"]
    _wheel(wh, "co_core", "1.2.0", files)
    _wheel(wh, "co_core_aio", "1.2.0", _aio("1.2.0"))
    report = build_report("1.1.0", "1.2.0", roots=[repo / "src"], packages=PACKAGES, wheelhouse=wh)
    sites = [line for line in report.splitlines() if "import_module" in line]
    assert sites == ["- co_core/bus/__init__.py:9 `import_module(f'{__name__}.gcs')`"]


def test_roots_are_reported_in_the_order_given(repo):
    report = build_report(
        "1.0.0",
        "1.1.0",
        roots=[repo / "src", repo / "scripts"],
        packages=PACKAGES,
        wheelhouse=repo / ".wheelhouse",
    )
    assert report.index(str(repo / "src")) < report.index(str(repo / "scripts"))


def test_diff_adds_a_code_level_diff_of_each_changed_file_only(repo):
    plain, with_diff = _report(repo), _report(repo, show_diff=True)
    assert "```diff" not in plain
    assert "+    generation: int | None = None" in with_diff
    assert "co_core/bus/streams.py" not in with_diff.split("```diff", 1)[1]  # docstrings only
    assert "Models." not in with_diff  # docstrings never reach the diff


def test_a_missing_wheel_names_the_sync_script(repo):
    with pytest.raises(MissingWheelError, match="sync_wheelhouse.py"):
        load_wheels(repo / ".wheelhouse", PACKAGES, "9.9.9")


# --- the CLI ------------------------------------------------------------------


def _main(repo, *args):
    return main(
        [*args, "--root", str(repo / "src"), "--root", str(repo / "scripts")]
        + ["--package", "co_core", "--package", "co-core-aio"]
        + ["--wheelhouse", str(repo / ".wheelhouse")]
    )


def test_main_writes_the_report_and_exits_zero_whatever_it_found(repo, capsys):
    assert _main(repo, "1.0.0", "1.1.0") == 0
    assert "`changed` co_core/models/changes.py" in capsys.readouterr().out


def test_main_takes_hyphenated_package_names(repo, capsys):
    assert _main(repo, "1.0.0", "1.1.0") == 0  # co-core-aio above
    assert "co_core_aio/__init__.py" in capsys.readouterr().out


def test_main_exits_two_on_a_missing_wheel(repo, capsys):
    assert _main(repo, "1.0.0", "9.9.9") == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "co_core-9.9.9-py3-none-any.whl" in captured.err
    assert "scripts/sync_wheelhouse.py" in captured.err


def test_main_exits_two_on_a_corrupt_wheel(repo, capsys):
    (repo / ".wheelhouse" / "co_core-1.1.0-py3-none-any.whl").write_bytes(b"truncated")
    assert _main(repo, "1.0.0", "1.1.0") == 2
    err = capsys.readouterr().err
    assert "co_core-1.1.0-py3-none-any.whl" in err
    assert "scripts/sync_wheelhouse.py" in err


def test_main_exits_two_on_a_missing_root(repo, capsys):
    rc = main(["1.0.0", "1.1.0", "--root", str(repo / "nope")])
    assert rc == 2
    assert "nope" in capsys.readouterr().err


def test_main_exits_two_on_a_usage_error(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["1.0.0"])
    assert exc.value.code == 2


# --- the real wheels ----------------------------------------------------------

_REAL = ["co_core", "co_core_aio", "co_core_sync"]
_REAL_WHEELS = [
    REPO_ROOT / ".wheelhouse" / f"{p}-{v}-py3-none-any.whl"
    for p in _REAL
    for v in ("0.36.0", "0.37.0")
]


@pytest.mark.skipif(
    not all(w.exists() for w in _REAL_WHEELS),
    reason="0.36.0/0.37.0 wheels not synced (scripts/sync_wheelhouse.py)",
)
def test_reproduces_136_for_0_36_to_0_37():
    """#136's result: 57 files at each version, six not identical.

    Seeds from the live ``src/`` and ``scripts/``, so a change to Replicator's
    co-core imports can move the 57. Re-measure then; that's the signal.
    """
    roots = [REPO_ROOT / "src", REPO_ROOT / "scripts"]
    seeds = root_imports(roots)
    old = load_wheels(REPO_ROOT / ".wheelhouse", _REAL, "0.36.0")
    new = load_wheels(REPO_ROOT / ".wheelhouse", _REAL, "0.37.0")
    old_c, new_c = closure(old.files, _REAL, seeds), closure(new.files, _REAL, seeds)
    assert (len(old_c.files), len(new_c.files)) == (57, 57)
    verdicts = {f: verdict(old.files.get(f), new.files.get(f)) for f in old_c.files | new_c.files}
    assert {f: v for f, v in verdicts.items() if v != "identical"} == {
        "co_core/__init__.py": "version-only",
        "co_core_aio/__init__.py": "version-only",
        "co_core_sync/__init__.py": "version-only",
        "co_core/pure/adapters/bus/envelope.py": "changed",
        "co_core/pure/adapters/bus/streams.py": "changed",
        "co_core/pure/models/changes.py": "changed",
    }
