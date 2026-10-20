"""Diff co-core's import closure between two wheel versions — every pin move (#145).

A pin move is safe when no module the service actually loads changed under it,
and a module list is not that set. #140 found ``changes.py``'s import of
``extract.canonical`` loading the whole ``pure/extract`` package, which no import
line here names. So this walks the closure from the roots instead, through every
module of the named packages it reaches, and compares the two versions file by
file::

    uv run --no-project python scripts/co_core_closure_diff.py 0.36.0 0.37.0 [--diff]

The output is Markdown for the ``pyproject.toml`` pin comment: the file count at
each version, one line per file that isn't ``identical``, the Requires-Dist delta,
the closure's third-party imports and any dynamic import it can't follow.

**The walk.** Each version is walked on its own, so a module that joins or leaves
the closure shows as ``added`` or ``removed``, not ``changed``. Importing
``a.b.c`` runs ``a/__init__`` and ``a/b/__init__``, so every parent is in.
``from pkg import name`` is a module edge when ``pkg/name`` is a module in the
wheel. Every static import counts: under ``TYPE_CHECKING``, inside
``try/except ImportError``, in a function body. Over-including is cheap and
missing a module isn't. co-core-sync's lazy ``__getattr__`` re-exports are the
case in point: their ``import_module`` targets are named again only under
``TYPE_CHECKING``. A dynamic import is listed and never followed.

**The verdicts.** ``identical`` (bytes), ``docstrings-only`` (ASTs equal with
module, class and function docstrings stripped; comments and formatting never
reach the AST), ``version-only`` (equal once ``__version__`` is masked too),
``changed``, and ``added`` / ``removed``.

Nothing in it is Replicator's: roots, packages and the wheelhouse are flags, and
the defaults are this repo's. A ``--package`` names both the import package and
the wheel's distribution, which co-core's three share. It reads the wheels in
place with ``zipfile`` and installs nothing, and it's standard library only, so
``uv run --no-project`` works too. The wheel is what the venv installs, so it's
the wheel that's read, never the sdist.

Exit codes: ``0`` a report was written, whatever it found · ``2`` a usage error,
a missing root, or a wheel missing from the wheelhouse.
"""

from __future__ import annotations

import argparse
import ast
import difflib
import sys
import zipfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ROOTS = ["src", "scripts"]
DEFAULT_PACKAGES = ["co_core", "co_core_aio", "co_core_sync"]
DEFAULT_WHEELHOUSE = ".wheelhouse"


class MissingWheelError(Exception):
    """A wheel the diff needs isn't in the wheelhouse."""


@dataclass(frozen=True)
class Wheels:
    """One version's ``.py`` files across the packages, and each one's Requires-Dist."""

    version: str
    files: dict[str, bytes]
    requires: dict[str, frozenset[str]]


@dataclass(frozen=True)
class Edge:
    """A dotted name an import may load.

    ``definite`` is False for ``pkg.name`` out of ``from pkg import name``: a
    module only if the wheel has one, an attribute otherwise. A definite name
    with no file in the wheel is an import that fails.
    """

    name: str
    definite: bool = True


@dataclass(frozen=True)
class Closure:
    """The files one version's walk reached, and where the walk stopped."""

    files: frozenset[str]
    third_party: frozenset[str]
    dynamic: tuple[tuple[str, int, str], ...]  # (path, line, call)
    unparsed: frozenset[str] = frozenset()  # in the closure, imports not followed
    unresolved: frozenset[str] = frozenset()  # definite imports with no file


def _normalize(package: str) -> str:
    """Wheels and imports use underscores; pins use hyphens."""
    return package.replace("-", "_")


def wheel_path(wheelhouse: Path, package: str, version: str) -> Path:
    return wheelhouse / f"{_normalize(package)}-{version}-py3-none-any.whl"


def load_wheels(wheelhouse: Path, packages: Sequence[str], version: str) -> Wheels:
    """Read every package's wheel at ``version``, in place."""
    missing = [p for p in packages if not wheel_path(wheelhouse, p, version).is_file()]
    if missing:
        names = ", ".join(str(wheel_path(wheelhouse, p, version)) for p in missing)
        raise MissingWheelError(
            f"missing from the wheelhouse: {names}. Run scripts/sync_wheelhouse.py first."
        )
    files: dict[str, bytes] = {}
    requires: dict[str, frozenset[str]] = {}
    for package in packages:
        with zipfile.ZipFile(wheel_path(wheelhouse, package, version)) as z:
            for name in z.namelist():
                if name.endswith(".py") and ".dist-info/" not in name:
                    files[name] = z.read(name)
                elif name.endswith(".dist-info/METADATA"):
                    lines = z.read(name).decode().splitlines()
                    requires[_normalize(package)] = frozenset(
                        line.removeprefix("Requires-Dist:").strip()
                        for line in lines
                        if line.startswith("Requires-Dist:")
                    )
    return Wheels(version, files, requires)


def _parse(source: bytes) -> ast.Module | None:
    try:
        return ast.parse(source)
    except SyntaxError:
        return None


def _from_import(target: str, node: ast.ImportFrom) -> list[Edge]:
    """``from target import a, b``: ``target`` for sure, each name maybe."""
    return [Edge(target)] + [
        Edge(f"{target}.{a.name}", definite=False) for a in node.names if a.name != "*"
    ]


def _imported_names(tree: ast.AST, module: str, is_package: bool) -> list[Edge]:
    """Every dotted name an import in ``tree`` might load, relative ones resolved."""
    names: list[Edge] = []
    package = module.split(".") if is_package else module.split(".")[:-1]
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(Edge(alias.name) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                if node.level - 1 >= len(package):  # past the top: Python raises
                    continue
                base = package[: len(package) - (node.level - 1)]
                parts = base + ([node.module] if node.module else [])
                if not parts:
                    continue
                target = ".".join(parts)
            else:
                target = node.module or ""
            names.extend(_from_import(target, node))
    return names


def root_imports(roots: Iterable[Path]) -> list[Edge]:
    """The absolute imports of every ``.py`` under the roots: the walk's seeds.

    A root's relative imports name the root's own modules, never a package's.
    """
    seeds: list[Edge] = []
    for root in roots:
        for path in sorted(Path(root).rglob("*.py")):
            tree = _parse(path.read_bytes())
            if tree is None:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    seeds.extend(Edge(alias.name) for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
                    seeds.extend(_from_import(node.module, node))
    return seeds


def _module_file(files: dict[str, bytes], module: str) -> str | None:
    stem = module.replace(".", "/")
    for candidate in (f"{stem}.py", f"{stem}/__init__.py"):
        if candidate in files:
            return candidate
    return None


def _dynamic_calls(tree: ast.AST, path: str, source: bytes) -> list[tuple[str, int, str]]:
    """Each ``importlib.import_module`` / ``__import__`` call: path, line and source."""
    found = []
    text = source.decode(errors="replace")
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if name in ("import_module", "__import__"):
            call = ast.get_source_segment(text, node) or name
            found.append((path, node.lineno, " ".join(call.split())))
    return found


def closure(files: dict[str, bytes], packages: Sequence[str], seeds: Iterable[Edge]) -> Closure:
    """Walk from the seeds through every module of ``packages`` they reach."""
    tops = {_normalize(p) for p in packages}
    seen: set[str] = set()
    third_party: set[str] = set()
    dynamic: list[tuple[str, int, str]] = []
    unparsed: set[str] = set()
    unresolved: set[str] = set()
    todo = list(seeds)
    while todo:
        edge = todo.pop()
        if edge.name.split(".")[0] not in tops:
            continue
        if edge.definite and _module_file(files, edge.name) is None:
            # A namespace package (PEP 420) is a directory with no file to read.
            stem = edge.name.replace(".", "/") + "/"
            if not any(path.startswith(stem) for path in files):
                unresolved.add(edge.name)
        parts = edge.name.split(".")
        for depth in range(1, len(parts) + 1):
            module = ".".join(parts[:depth])
            path = _module_file(files, module)
            if path is None or path in seen:
                continue
            seen.add(path)
            tree = _parse(files[path])
            if tree is None:
                unparsed.add(path)
                continue
            dynamic.extend(_dynamic_calls(tree, path, files[path]))
            imported = _imported_names(tree, module, path.endswith("/__init__.py"))
            # Only a package file's imports are the closure's; a root's aren't.
            third_party.update(
                top
                for top in (e.name.split(".")[0] for e in imported)
                if top and top not in tops and top not in sys.stdlib_module_names
            )
            todo.extend(imported)
    return Closure(
        frozenset(seen),
        frozenset(third_party),
        tuple(sorted(dynamic)),
        frozenset(unparsed),
        frozenset(unresolved),
    )


class _Mask(ast.NodeTransformer):
    """Strip docstrings and, with ``version``, mask every ``__version__`` value."""

    def __init__(self, version: bool) -> None:
        self.version = version

    def _strip(self, node):
        self.generic_visit(node)
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
        return node

    visit_Module = visit_ClassDef = visit_FunctionDef = visit_AsyncFunctionDef = _strip

    def visit_Assign(self, node):
        if self.version and any(
            isinstance(t, ast.Name) and t.id == "__version__" for t in node.targets
        ):
            node.value = ast.Constant(None)
        return node

    def visit_AnnAssign(self, node):
        if self.version and isinstance(node.target, ast.Name) and node.target.id == "__version__":
            node.value = ast.Constant(None)
        return node


def _masked(source: bytes, version: bool) -> ast.Module | None:
    tree = _parse(source)
    return None if tree is None else _Mask(version).visit(tree)


def verdict(old: bytes | None, new: bytes | None) -> str:
    """One file's verdict between two versions."""
    if old is None:
        return "added"
    if new is None:
        return "removed"
    if old == new:
        return "identical"
    for version, label in ((False, "docstrings-only"), (True, "version-only")):
        a, b = _masked(old, version), _masked(new, version)
        if a is not None and b is not None and ast.dump(a) == ast.dump(b):
            return label
    return "changed"


def _code_diff(path: str, old: bytes, new: bytes) -> str:
    """A unified diff of the docstring-stripped code, so only code shows."""
    a, b = _masked(old, False), _masked(new, False)
    old_lines = (ast.unparse(a) if a else old.decode(errors="replace")).splitlines()
    new_lines = (ast.unparse(b) if b else new.decode(errors="replace")).splitlines()
    body = "\n".join(
        difflib.unified_diff(old_lines, new_lines, f"a/{path}", f"b/{path}", lineterm="")
    )
    return f"```diff\n{body}\n```"


def _ticked(names: Iterable[str]) -> str:
    return ", ".join(f"`{n}`" for n in sorted(names)) or "none"


def build_report(
    old_version: str,
    new_version: str,
    *,
    roots: Sequence[Path],
    packages: Sequence[str],
    wheelhouse: Path,
    show_diff: bool = False,
) -> str:
    """The Markdown report for one pin move."""
    packages = [_normalize(p) for p in packages]
    old = load_wheels(wheelhouse, packages, old_version)
    new = load_wheels(wheelhouse, packages, new_version)
    seeds = root_imports(roots)
    old_c, new_c = closure(old.files, packages, seeds), closure(new.files, packages, seeds)

    verdicts = {}
    for path in sorted(old_c.files | new_c.files):
        verdicts[path] = verdict(
            old.files.get(path) if path in old_c.files else None,
            new.files.get(path) if path in new_c.files else None,
        )
    moved = {p: v for p, v in verdicts.items() if v != "identical"}

    out = [
        f"## Import closure, {old_version} → {new_version}",
        "",
        f"Roots {', '.join(f'`{r}`' for r in roots)}; packages {_ticked(packages)}.",
        "",
        f"{len(old_c.files)} files at {old_version}, {len(new_c.files)} at {new_version}. "
        f"{len(moved)} not identical:",
        "",
    ]
    out += [f"- `{v}` {p}" for p, v in moved.items()] or ["- none"]

    out += ["", "**Requires-Dist:**", ""]
    for package in packages:
        removed = old.requires.get(package, frozenset()) - new.requires.get(package, frozenset())
        added = new.requires.get(package, frozenset()) - old.requires.get(package, frozenset())
        parts = [f"removed {_ticked(removed)}"] if removed else []
        parts += [f"added {_ticked(added)}"] if added else []
        out.append(f"- {package}: {'; '.join(parts) or 'unchanged'}")

    for version, c in ((old_version, old_c), (new_version, new_c)):
        if c.unresolved:
            out.append(f"\n**Unresolved imports** (at {version}): {_ticked(c.unresolved)}.")
        if c.unparsed:
            out.append(
                f"\n**Not parsed, imports not followed** (at {version}): {_ticked(c.unparsed)}."
            )

    out += ["", f"**Third-party imports** (at {new_version}): {_ticked(new_c.third_party)}."]
    if old_c.third_party != new_c.third_party:
        out.append(f"At {old_version}: {_ticked(old_c.third_party)}.")

    # Keyed by file and call, not line: a site that only moved is one site.
    old_sites = {(path, call): line for path, line, call in old_c.dynamic}
    new_sites = {(path, call): line for path, line, call in new_c.dynamic}
    if old_sites or new_sites:
        out += ["", "**Dynamic imports, not followed:**", ""]
        for key in sorted(old_sites.keys() | new_sites.keys()):
            path, call = key
            only = (
                ""
                if key in old_sites and key in new_sites
                else f" ({new_version} only)"
                if key in new_sites
                else f" ({old_version} only)"
            )
            line = new_sites.get(key, old_sites.get(key))
            out.append(f"- {path}:{line} `{call}`{only}")

    if show_diff:
        changed = [p for p, v in moved.items() if v == "changed"]
        for path in changed:
            out += ["", f"### {path}", "", _code_diff(path, old.files[path], new.files[path])]
    return "\n".join(out) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])  # -OO: None
    parser.add_argument("old", help="the version pinned now, e.g. 0.36.0")
    parser.add_argument("new", help="the version to move to, e.g. 0.37.0")
    parser.add_argument("--root", action="append", help=f"repeatable; default {DEFAULT_ROOTS}")
    parser.add_argument(
        "--package", action="append", help=f"repeatable; default {DEFAULT_PACKAGES}"
    )
    parser.add_argument("--wheelhouse", default=DEFAULT_WHEELHOUSE)
    parser.add_argument("--diff", action="store_true", help="add each changed file's code diff")
    args = parser.parse_args(argv)

    roots = [Path(r) for r in (args.root or DEFAULT_ROOTS)]
    absent = [str(r) for r in roots if not r.is_dir()]
    if absent:
        print(f"no such root: {', '.join(absent)}", file=sys.stderr)
        return 2
    try:
        report = build_report(
            args.old,
            args.new,
            roots=roots,
            packages=args.package or DEFAULT_PACKAGES,
            wheelhouse=Path(args.wheelhouse),
            show_diff=args.diff,
        )
    except MissingWheelError as exc:
        print(exc, file=sys.stderr)
        return 2
    sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
