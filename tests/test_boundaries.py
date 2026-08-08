"""Executable half of the boundaries charter.

Reasoning lives in
[`docs/contracts/replicator-boundaries.md`](../docs/contracts/replicator-boundaries.md) —
read it before deleting an assertion here. The charter's rule is that Replicator
owns the mechanics of acquiring bytes and holding them briefly, and never owns
why, when, or what they mean. Every assertion below is one way that rule stops
being true one defensible commit at a time.

Two conventions worth knowing before editing:

**The detectors are themselves tested.** ``test_the_*_detector_*`` cases run each
scan against synthetic violating source. A structural test that quietly walks
zero files, or a substring check that stops matching after a refactor, passes
forever while enforcing nothing — which is worse than no test, because the
charter then cites it. The corpus scans also assert their own file list is
non-empty for the same reason.

**Scans are AST-based, never grep.** The vocabulary scan in particular reads
identifiers and string literals only, skipping comments and docstrings: prose
uses these words legitimately (``both tasks watch one stop event``), and a test
whose first tripper is an English sentence is a test that gets deleted rather
than heeded. The regression it exists to catch always arrives as a field name, a
parameter, or a dict key.
"""

import ast
import tomllib
from pathlib import Path

import pytest
from starlette.routing import Route, WebSocketRoute

from src.api.main import app
from src.core.config import Settings
from src.storage.local import LocalBlobStore

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
WORKER = SRC / "worker"
UNIT = REPO / "deploy" / "replicator.service"
LOCK = REPO / "uv.lock"


def _python_files(root: Path) -> list[Path]:
    """Every module under ``root``, sorted for a stable failure message."""
    return sorted(root.rglob("*.py"))


def _parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


# --------------------------------------------------------------------------
# 1. No database
# --------------------------------------------------------------------------

# Charter test 1: Replicator's state is content-addressed on disk, in memory, or
# in the broker. Anything else is a database whatever it is called. Third-party
# names are checked against the lock rather than the installed environment —
# a dirty local venv must not be able to make this pass.
PERSISTENCE_DISTRIBUTIONS = frozenset(
    {
        "alembic",
        "aiosqlite",
        "asyncpg",
        "motor",
        "peewee",
        "psycopg",
        "psycopg2",
        "psycopg2-binary",
        "psycopg-binary",
        "pymongo",
        "sqlalchemy",
        "sqlmodel",
        "tortoise-orm",
    }
)

# The stdlib half. sqlite3 is the obvious one; shelve and dbm are the same thing
# with less ceremony, and pickle is how a cache becomes a file nobody calls a
# database. None has a legitimate use in a service whose entire durable state is
# rebuildable — if one acquires it, this list is the place to argue about it.
PERSISTENCE_MODULES = frozenset({"sqlite3", "shelve", "dbm", "pickle"})


def _locked_distributions() -> set[str]:
    """Every distribution name in ``uv.lock``, normalized."""
    lock = tomllib.loads(LOCK.read_text())
    return {package["name"].lower().replace("_", "-") for package in lock["package"]}


def _imported_modules(tree: ast.Module) -> set[str]:
    """Top-level module names imported anywhere in ``tree``."""
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            modules.add(node.module.split(".")[0])
    return modules


def test_no_persistence_dependency_resolves_in_the_lock():
    assert not PERSISTENCE_DISTRIBUTIONS & _locked_distributions()


def test_no_module_imports_stdlib_persistence():
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {
        path.relative_to(REPO).as_posix(): sorted(
            PERSISTENCE_MODULES & _imported_modules(_parse(path))
        )
        for path in files
        if PERSISTENCE_MODULES & _imported_modules(_parse(path))
    }
    assert not offenders


def test_the_import_detector_sees_a_planted_import():
    tree = ast.parse("import sqlite3\nfrom dbm import gnu\n")

    assert PERSISTENCE_MODULES & _imported_modules(tree) == {"sqlite3", "dbm"}


# --------------------------------------------------------------------------
# 2. No domain vocabulary
# --------------------------------------------------------------------------

# Charter test 3: anything needing these words belongs to the issuer. The bare
# verb "watch" is deliberately absent — it is ordinary English, and scoping this
# scan to identifiers is what lets the domain noun stay.
DOMAIN_TOKENS = frozenset({"info_source", "info_item", "watched_item", "tenant", "aspect"})

# The one carve-out, and it is one *field* wide, not one word wide (#28). co-core
# 0.8.0 makes ``info_source_id`` required on the command and on both facts, so
# Replicator must name it to copy it across. What the charter still forbids is
# *understanding* it — see ``test_the_echoed_domain_key_is_never_interpreted``,
# which is the assertion that makes this allowlist safe to have written.
#
# ECHOED_TOKEN is the DOMAIN_TOKENS entry the exemption suppresses;
# ECHOED_NAME is the only identifier it may be suppressed *for*. The two are
# separate because the token scan matches substrings: exempting the token alone
# would let ``info_source_policy`` or ``info_sources`` into an allowlisted module
# under cover of the field that earned the carve-out (CR #2).
ECHOED_TOKEN = "info_source"
ECHOED_NAME = "info_source_id"

# Exactly the three modules on the emit path: the two publishers, and the report
# dataclass that carries the value between them. Deliberately not a directory
# glob — ``src/worker/`` also holds the pacer, the sweep and the policy reader,
# none of which has any business naming a domain object.
DOMAIN_ECHO_MODULES = frozenset(
    {"src/worker/handler.py", "src/worker/reporter.py", "src/worker/loop.py"}
)

# Where the echo may never spread, asserted against the allowlist itself rather
# than against the source. A future change that needs the domain key in the
# settings table or in a blob path would reach for this list first, and adding a
# module here is the moment the charter is actually being edited.
DOMAIN_ECHO_FORBIDDEN = ("src/core/config.py", "src/storage/", "src/api/")


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """Ids of the ``Constant`` nodes that are docstrings, so the scan can skip them."""
    skip: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        first = node.body[0] if node.body else None
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            skip.add(id(first.value))
    return skip


def _walk_pruning_calls(node: ast.AST) -> list[ast.AST]:
    """``node``'s subtree, stopping at every nested ``Call``.

    Used only by the positional-argument check, and it is what keeps that check
    from flagging the emit path itself. ``_publish(pub, topic,
    BlobAvailableEvent(info_source_id=...), command=command)`` passes a *call* as
    its third positional argument, and scanning that argument's whole subtree
    would reach the legitimate keyword echo nested inside it.

    Pruning loses nothing: the outer ``ast.walk`` visits every nested ``Call`` in
    its own right, so each call's positional arguments are still checked — just
    against that call rather than against its enclosing one.
    """
    found: list[ast.AST] = []
    stack: list[ast.AST] = [node]
    while stack:
        current = stack.pop()
        found.append(current)
        stack.extend(
            child for child in ast.iter_child_nodes(current) if not isinstance(child, ast.Call)
        )
    return found


def _surface_of(nodes: list[ast.AST], skip: set[int]) -> set[str]:
    """Identifiers and non-docstring string literals across ``nodes``."""
    surface: set[str] = set()
    for node in nodes:
        if isinstance(node, ast.Name):
            surface.add(node.id)
        elif isinstance(node, ast.Attribute):
            surface.add(node.attr)
        elif isinstance(node, ast.arg):
            surface.add(node.arg)
        elif isinstance(node, ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            surface.add(node.name)
        elif isinstance(node, ast.keyword) and node.arg:
            surface.add(node.arg)
        elif (
            isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in skip
        ):
            surface.add(node.value)
    return surface


def _vocabulary_surface(tree: ast.AST) -> set[str]:
    """Every identifier and non-docstring string literal in ``tree``.

    String literals are in scope alongside identifiers because domain leakage
    arrives as a dict key or a log field (``detail={"info_source_id": ...}``) at
    least as often as it arrives as an attribute — and that form is the one a
    reviewer skims past.
    """
    return _surface_of(list(ast.walk(tree)), _docstring_nodes(tree))


def _tokens_in(surface: set[str]) -> set[str]:
    """Which ``DOMAIN_TOKENS`` appear anywhere in ``surface``."""
    lowered = [text.lower() for text in surface]
    return {token for token in DOMAIN_TOKENS if any(token in text for text in lowered)}


def _domain_hits(tree: ast.AST) -> set[str]:
    """Tokens from ``DOMAIN_TOKENS`` appearing anywhere in the vocabulary surface."""
    return _tokens_in(_vocabulary_surface(tree))


def _argument_hits(argument: ast.AST) -> set[str]:
    """Domain tokens in one call argument, not counting nested calls.

    A call passed *as* an argument contributes nothing here — it is visited on
    its own turn by the enclosing walk, which is what keeps a legitimate nested
    keyword echo from being read as its caller's read. That is what makes the
    emit path's ``_publish(pub, topic, Event(info_source_id=…), command=…)``
    clean at both levels.
    """
    if isinstance(argument, ast.Call):
        return set()
    return _tokens_in(_surface_of(_walk_pruning_calls(argument), set()))


def _interpreting_hits(tree: ast.AST) -> set[str]:
    """Domain tokens appearing where the code is *reading* the value, not carrying it.

    A verbatim echo is three shapes and no more: an annotated field declaration,
    an attribute read, and a **keyword** argument. Every other position implies
    the value meant something to Replicator —

    - ``Compare`` / ``BoolOp`` and the test of an ``If`` / ``While`` / ``IfExp``:
      a branch on the domain.
    - ``Subscript`` and a ``Dict`` **key**: a lookup keyed by it, or the table
      being built for one — a routing map, a per-source counter, a policy cache.
    - ``Call`` arguments: positional (``registry.get(id)``, ``seen.add(id)``,
      ``Path(root, id)``) and every keyword **except** ``info_source_id=``, which
      is exactly the echo — ``BlobAvailableEvent(info_source_id=…)`` — and
      flagging it would force the allowlist to be deleted rather than obeyed.
      Exempting keywords wholesale was CR #11's hole: ``client.get(name=id)``
      reads the value under a different parameter name, and redis-py's async API
      is keyword-friendly enough that this is how it would really be written.
    - ``BinOp`` and ``JoinedStr``: concatenation and f-strings. Every use anyone
      has proposed for one builds either a Redis key or a filesystem path, which
      is domain *state* under another name.

    The first four of these were added in CR #1, after a probe found the original
    scan caught three of nine realistic violations while the charter cited it as
    the guard that made the vocabulary carve-out safe. A test the charter cites
    and that does not hold is worse than no test.

    Scoped to the whole of ``src/``, not to the allowlist, so a module that is
    not permitted to name the token at all still cannot interpret it in
    prose-shaped ways the other scan would miss.
    """
    hits: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare | ast.BoolOp | ast.Subscript | ast.JoinedStr | ast.BinOp):
            hits |= _domain_hits(node)
        elif isinstance(node, ast.If | ast.While | ast.IfExp):
            hits |= _domain_hits(node.test)
        elif isinstance(node, ast.Match):
            hits |= _domain_hits(node.subject)
        elif isinstance(node, ast.Dict):
            for key in node.keys:
                # ``{**other}`` has a None key and spreads a mapping rather than
                # naming one, so there is nothing to read here.
                if key is not None:
                    hits |= _domain_hits(key)
        elif isinstance(node, ast.Call):
            for argument in node.args:
                hits |= _argument_hits(argument)
            for keyword in node.keywords:
                # ``keyword.arg`` is None for ``**mapping``, which names nothing
                # and so is never the echo.
                if keyword.arg != ECHOED_NAME:
                    hits |= _argument_hits(keyword.value)
    return hits


def _unechoed_domain_names(tree: ast.AST) -> set[str]:
    """Names in ``tree`` that carry ``ECHOED_TOKEN`` but are not the echoed field.

    The exemption is for one field. ``info_source_policy``, ``info_sources`` and
    ``info_source_cache`` all contain the token, and a substring exemption would
    admit every one of them into an allowlisted module — a per-InfoSource map
    arriving under cover of the field that earned the carve-out (CR #2).

    ``info_source_id`` itself is excluded, as is anything that merely *contains*
    it in a longer name, which would be a different field wearing its prefix.
    """
    return {
        text
        for text in _vocabulary_surface(tree)
        if ECHOED_TOKEN in text.lower() and text.lower() != ECHOED_NAME
    }


def test_no_module_names_a_domain_concept():
    """The load-bearing one.

    A domain field on the wire is the regression no type checker and no reviewer
    reliably catches, because it always arrives looking reasonable: one optional
    field, one small settings table, and Replicator has a domain model.

    ``ECHOED_TOKEN`` is exempted **only** in ``DOMAIN_ECHO_MODULES``, only when
    every name carrying it is exactly ``ECHOED_NAME``, and only for itself: an
    allowlisted module that also named a tenant or a watched item is offending on
    that, not on the echo.
    """
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {}
    for path in files:
        name = path.relative_to(REPO).as_posix()
        tree = _parse(path)
        hits = _domain_hits(tree)
        if name in DOMAIN_ECHO_MODULES and not _unechoed_domain_names(tree):
            hits -= {ECHOED_TOKEN}
        if hits:
            offenders[name] = sorted(hits)
    assert not offenders


def test_the_echoed_domain_key_is_never_interpreted():
    """What makes the allowlist a carve-out rather than a hole (#28).

    The charter's rule is not "do not say ``info_source_id``" — the wire contract
    now requires saying it. The rule is that Replicator never learns what it
    means. Naming it to copy it across is mechanics; branching on it, keying on
    it, or building a string out of it is the domain model arriving one
    defensible commit at a time, which is the failure this file exists to catch.
    """
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {
        path.relative_to(REPO).as_posix(): sorted(hits)
        for path in files
        if (hits := _interpreting_hits(_parse(path)))
    }
    assert not offenders


def test_the_echo_allowlist_cannot_reach_config_or_storage():
    """The allowlist is the file a future change edits *first*, so it is guarded.

    Adding a module here is how the domain key would acquire a settings entry, a
    blob path segment, or an HTTP surface — each a different way of holding
    domain state, and each one this assertion names before the code exists.
    """
    for forbidden in DOMAIN_ECHO_FORBIDDEN:
        assert not [module for module in DOMAIN_ECHO_MODULES if module.startswith(forbidden)]


def test_every_echo_module_exists():
    """An allowlist entry that no longer names a file exempts nothing and hides
    that it exempts nothing — the stale-allowlist failure mode."""
    for module in DOMAIN_ECHO_MODULES:
        assert (REPO / module).is_file(), module


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("def f(info_source_id): ...", id="parameter"),
        pytest.param("command.watched_item_id", id="attribute"),
        pytest.param('detail = {"info_item": 1}', id="dict-key"),
        pytest.param("class TenantScope: ...", id="class-name"),
    ],
)
def test_the_vocabulary_detector_sees_a_planted_domain_name(source):
    assert _domain_hits(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param('"""Both tasks watch one stop event."""', id="module-docstring"),
        pytest.param(
            "def f():\n    '''Watch the tenant aspect of nothing.'''\n    return 1",
            id="function-docstring",
        ),
        pytest.param("# info_source_id belongs to the issuer\nx = 1", id="comment"),
    ],
)
def test_the_vocabulary_detector_ignores_prose(source):
    """Prose is where these words are legitimate, and where a false positive kills the test."""
    assert not _domain_hits(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param('if command.info_source_id == "x": ...', id="branch"),
        pytest.param("if command.info_source_id: ...", id="bare-truthiness"),
        pytest.param("policy = table[command.info_source_id]", id="lookup"),
        pytest.param('key = f"replicator:{command.info_source_id}"', id="key-building"),
        pytest.param("ok = enabled and command.info_source_id", id="bool-op"),
        pytest.param(
            "match command.info_source_id:\n    case _:\n        pass", id="match-subject"
        ),
        # The six CR #1 found missing. Each is a way the echoed value becomes
        # state: a table keyed by it, a lookup through one, a Redis key, a path.
        pytest.param("routes = {command.info_source_id: handler}", id="dict-literal-key"),
        pytest.param("policy = registry.get(command.info_source_id)", id="get-lookup"),
        pytest.param("seen.add(command.info_source_id)", id="membership-call"),
        pytest.param('key = "replicator:" + command.info_source_id', id="concatenation"),
        pytest.param("p = Path(root, command.info_source_id)", id="path-building"),
        pytest.param("counts[host] += weights[command.info_source_id]", id="nested-lookup"),
        # CR #11: the same hole reached by keyword syntax. redis-py's async API is
        # keyword-friendly enough that this is how it would actually get written.
        pytest.param("v = client.get(name=command.info_source_id)", id="keyword-lookup"),
        pytest.param("await redis.set(name=command.info_source_id, value=1)", id="keyword-setter"),
        pytest.param('f(**{"info_source_id": command.info_source_id})', id="splatted-mapping"),
    ],
)
def test_the_interpretation_detector_sees_a_planted_read(source):
    assert _interpreting_hits(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("info_source_id: str", id="field-declaration"),
        pytest.param("Event(info_source_id=command.info_source_id)", id="keyword-echo"),
        pytest.param("x = command.info_source_id", id="plain-read"),
        # The emit path in full: a model built inside a positional argument, whose
        # own keyword is the echo. Both halves must stay clean.
        pytest.param(
            "_publish(pub, topic, Event(info_source_id=command.info_source_id), command=command)",
            id="emit-path",
        ),
    ],
)
def test_the_interpretation_detector_passes_a_verbatim_echo(source):
    """The three shapes the emit path actually uses. A detector that flagged these
    would force the allowlist to be deleted rather than obeyed."""
    assert not _interpreting_hits(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("self.info_source_policy = {}", id="policy-map"),
        pytest.param("for x in self.info_sources: pass", id="collection"),
        pytest.param("def f(info_source_url): ...", id="adjacent-field"),
        pytest.param('cache = {"info_source_cache": 1}', id="string-literal"),
    ],
)
def test_the_exemption_detector_sees_a_name_that_is_not_the_echoed_field(source):
    """CR #2: the carve-out is one field wide.

    Each of these contains ``info_source`` and would have been exempt under a
    substring-only exemption — including inside an allowlisted module, where the
    interpretation scan does not reach a bare assignment or a ``for`` target.
    """
    assert _unechoed_domain_names(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("info_source_id: str", id="field-declaration"),
        pytest.param("Event(info_source_id=command.info_source_id)", id="keyword-echo"),
    ],
)
def test_the_exemption_detector_passes_the_echoed_field(source):
    assert not _unechoed_domain_names(ast.parse(source))


# --------------------------------------------------------------------------
# 3. Ingress is read-only
# --------------------------------------------------------------------------

# The charter rejects an inbound admin HTTP API by name. What that forbids is a
# route accepting state-changing input; FastAPI's three introspection endpoints
# are read-only and describe the one real route, so they are allowlisted rather
# than switched off — a dev/prod docs flag would make the app under test differ
# from the app that ships, for a surface no deployment serves.
ALLOWED_PATHS = frozenset({"/health", "/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"})
READ_ONLY_METHODS = frozenset({"GET", "HEAD"})


def _routes(node, prefix: str = "") -> set[tuple[str, str]]:
    """Every (path, method) pair reachable from ``node``, flattened and prefixed.

    Recursive because ``/health`` is registered through ``include_router`` and so
    appears in ``app.routes`` as a router rather than a ``Route`` — under FastAPI
    0.141 an ``_IncludedRouter``, which carries its children on
    ``original_router`` and its mount prefix on ``include_context``. A flat
    comparison against ``app.routes`` fails *and* never sees ``/health``, which
    is the shape of test that looks strict and enforces nothing.

    Both container shapes are handled (``routes`` for an app or ``Mount``,
    ``original_router`` for an included one) so this survives FastAPI moving
    between them. ``test_the_route_walk_sees_a_planted_write_route`` is what
    reports it if the walk ever stops descending.

    **Two ways this used to fail open, both fixed in CR #1 and both now planted
    as tests.** A ``Mount`` carries its prefix on ``.path`` rather than on an
    ``include_context``, so a sub-app mounted at ``/admin`` reported its
    children's bare paths — ``GET /admin/health`` passed the allowlist as
    ``/health``. And a ``WebSocketRoute`` is not a ``Route`` and has no
    ``methods``, so it fell through to the container branch, found no children,
    and contributed nothing at all. Mounting a sub-app is the most plausible
    shape an admin API would actually arrive in, which is exactly why the walk
    has to see it.
    """
    if isinstance(node, Route):
        return {(prefix + node.path, method) for method in node.methods or {"GET"}}
    if isinstance(node, WebSocketRoute):
        # No methods of its own, and a socket is a write surface by construction:
        # named so it fails the read-only assertion rather than vanishing.
        return {(prefix + node.path, "WEBSOCKET")}

    context = getattr(node, "include_context", None)
    prefix += getattr(context, "prefix", "") or ""
    # A Mount's own prefix. The app root and an _IncludedRouter have no `path`,
    # so this is additive for them; for a Mount it is the whole point.
    prefix += getattr(node, "path", "") or ""
    children = getattr(node, "routes", None)
    if children is None:
        children = getattr(getattr(node, "original_router", None), "routes", ())
    return {pair for child in children for pair in _routes(child, prefix)}


def test_ingress_is_read_only():
    """No route accepts state-changing input.

    The charter rejects an inbound admin HTTP API; what that forbids is a write
    surface, not self-description. Both halves are asserted because either alone
    is weak: an allowlist of paths would admit a ``POST /health``, and a
    method check alone would admit a read-only config dump.
    """
    surface = _routes(app)

    assert ("/health", "GET") in surface, "the route walk stopped seeing /health"
    assert {path for path, _ in surface} <= ALLOWED_PATHS
    assert {method for _, method in surface} <= READ_ONLY_METHODS


def test_the_route_walk_sees_a_planted_write_route():
    from fastapi import APIRouter, FastAPI

    probe = FastAPI()
    router = APIRouter()
    router.post("/policy")(lambda: None)
    probe.include_router(router, prefix="/admin")

    assert ("/admin/policy", "POST") in _routes(probe)


def test_the_route_walk_sees_through_a_mounted_sub_app():
    """The shape an admin API would actually arrive in (CR #1).

    A sub-app is the natural way to add one — it carries its own router, its own
    docs, its own everything — and before this the walk reported its children
    without the mount prefix. A ``GET /admin/health`` returning the worker's
    configuration passed both assertions as ``/health``.
    """
    from fastapi import FastAPI

    sub = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    sub.get("/health")(lambda: {"config": "everything"})
    probe = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    probe.mount("/admin", sub)

    surface = _routes(probe)

    assert ("/admin/health", "GET") in surface
    assert not {path for path, _ in surface} <= ALLOWED_PATHS


def test_the_route_walk_sees_a_websocket():
    """Not a ``Route``, no ``methods`` — it used to contribute nothing at all.

    A socket is a write surface by construction, so it is named with a method of
    its own rather than left to fail the path allowlist alone: a websocket at
    ``/health`` would otherwise be invisible on both assertions.
    """
    from fastapi import FastAPI

    probe = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    @probe.websocket("/health")
    async def _socket(websocket): ...

    surface = _routes(probe)

    assert ("/health", "WEBSOCKET") in surface
    assert not {method for _, method in surface} <= READ_ONLY_METHODS


# --------------------------------------------------------------------------
# 4. No locally-defined wire models
# --------------------------------------------------------------------------


def _classes_declaring(tree: ast.Module, field: str) -> set[str]:
    """Classes in ``tree`` with an annotated or assigned attribute named ``field``.

    An AST check on class bodies, deliberately not a grep: ``event_type`` appears
    twice in ``src/worker/loop.py`` legitimately — once in a comment and once
    reading a co-core model's own field — and a grep that cries wolf on those
    gets replaced by nothing.
    """
    declared: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for statement in node.body:
            targets: list[ast.expr] = []
            if isinstance(statement, ast.AnnAssign):
                targets = [statement.target]
            elif isinstance(statement, ast.Assign):
                targets = list(statement.targets)
            if any(isinstance(t, ast.Name) and t.id == field for t in targets):
                declared.add(node.name)
    return declared


def test_no_wire_payload_is_defined_here():
    """Every payload shape comes from co-core.

    The rule governs payload **shapes**, not producer-owned token vocabularies:
    ``src/core/errors.py::FailureReason`` is a local ``StrEnum`` of ``reason``
    tokens and stays local by design, because co-core types that field as a
    plain ``str`` rather than a ``Literal`` so a producer adding a token cannot
    crash an older ``extra="ignore"`` consumer.
    """
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {
        path.relative_to(REPO).as_posix(): sorted(declared)
        for path in files
        if (declared := _classes_declaring(_parse(path), "event_type"))
    }
    assert not offenders


def test_the_wire_model_detector_sees_a_planted_field():
    tree = ast.parse("class Thing(BaseModel):\n    event_type: str = 'thing'\n")

    assert _classes_declaring(tree, "event_type") == {"Thing"}


def test_the_wire_model_detector_ignores_reading_the_field():
    tree = ast.parse(
        "class Handler:\n    def go(self, command):\n        return command.event_type\n"
    )

    assert not _classes_declaring(tree, "event_type")


# --------------------------------------------------------------------------
# 5. No issuer SDK
# --------------------------------------------------------------------------

# A client library for a sibling repo is how domain vocabulary arrives wholesale
# rather than one field at a time. co-core is the shared contract layer and is
# not one of these: it is owned by no issuer.
ISSUER_DISTRIBUTIONS = frozenset(
    {"archiver", "archiver-client", "notifier", "notifier-client", "watcher", "watcher-client"}
)


def test_no_sibling_repo_client_resolves_in_the_lock():
    assert not ISSUER_DISTRIBUTIONS & _locked_distributions()


# --------------------------------------------------------------------------
# 6. Config surface
# --------------------------------------------------------------------------

# Named exemption, not an oversight: the systemd unit's ExecStartPre stamps
# BUILD_ID generically across the cluster's services, so prefixing it here would
# mean every unit needs a per-service variable name for one git SHA. Documented
# in AGENTS.md and in the charter's config taxonomy.
UNPREFIXED_SETTINGS = frozenset({"build_id"})


def test_every_setting_is_replicator_prefixed():
    offenders = {
        name: field.validation_alias
        for name, field in Settings.model_fields.items()
        if name not in UNPREFIXED_SETTINGS
        and not str(field.validation_alias).startswith("REPLICATOR_")
    }
    assert not offenders


def test_the_exemption_stays_a_short_list():
    """An exemption list that grows is the prefix convention ending quietly."""
    assert UNPREFIXED_SETTINGS == {"build_id"}
    assert Settings.model_fields["build_id"].validation_alias == "BUILD_ID"


def test_settings_reads_no_file_of_its_own():
    """Env files are loaded by systemd or the developer, never by this process.

    ``env_file`` would make the service's configuration depend on the working
    directory it was launched from — and on the repo ``.env``, which holds
    org-wide PATs the worker has no use for.
    """
    assert Settings.model_config.get("env_file") is None


# Import must not read configuration and must not touch the network. Deliberately
# a denylist rather than an allowlist of permitted calls: building a logger, a
# router, or a Field default at import is ordinary and an allowlist would have to
# grow for each, which is how a structural test becomes a rubber stamp. The one
# import-time file read in the repo — `pkg_version("replicator")` in src/api —
# is metadata, not configuration, and lives in the surface no deployment serves.
IMPORT_TIME_FORBIDDEN = frozenset(
    {
        "Settings",
        "get_settings",
        "getenv",
        "open",
        "read_text",
        "read_bytes",
        "gethostname",
        "connect",
        "Redis",
        "from_url",
    }
)


def _import_time_calls(tree: ast.Module) -> set[str]:
    """Names called while the module is being imported.

    Module scope and class bodies both execute on import; function bodies do
    not, so the walk stops at them. Scanning them anyway is what made an earlier
    version of this test report ``Settings`` for ``get_settings``'s own body.
    """
    called: set[str] = set()
    stack: list[ast.AST] = list(tree.body)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda):
            continue
        if isinstance(node, ast.Call):
            func = node.func
            called.add(func.id if isinstance(func, ast.Name) else getattr(func, "attr", ""))
        stack.extend(ast.iter_child_nodes(node))
    return called


def test_no_module_reads_configuration_or_the_network_at_import_time():
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {
        path.relative_to(REPO).as_posix(): sorted(hits)
        for path in files
        if (hits := IMPORT_TIME_FORBIDDEN & _import_time_calls(_parse(path)))
    }
    assert not offenders


def test_the_import_time_detector_scans_class_bodies_but_not_functions():
    planted = ast.parse("class C:\n    x = open('f')\n")
    deferred = ast.parse("def f():\n    return open('f')\n")

    assert "open" in _import_time_calls(planted)
    assert "open" not in _import_time_calls(deferred)


# --------------------------------------------------------------------------
# 7. The deployed process has no ingress
# --------------------------------------------------------------------------

# The assertion above covers src/api, which no deployment serves. This one covers
# the process that does: replicator.service runs the worker, and the worker binds
# no port. Without it the charter's ingress invariant could hold forever while an
# admin listener grows inside src/worker/ — the exact thing the rejected fourth
# channel is about.
SERVER_MODULES = frozenset(
    {"fastapi", "uvicorn", "starlette", "aiohttp", "flask", "http", "socketserver", "wsgiref"}
)


def test_the_worker_imports_no_server_framework():
    files = _python_files(WORKER)
    assert files, f"no modules found under {WORKER} — the scan is a no-op"

    offenders = {
        path.relative_to(REPO).as_posix(): sorted(SERVER_MODULES & _imported_modules(_parse(path)))
        for path in files
        if SERVER_MODULES & _imported_modules(_parse(path))
    }
    assert not offenders


def test_the_unit_starts_the_worker_and_binds_no_port():
    unit = UNIT.read_text()

    assert "src.worker.main" in unit
    assert "uvicorn" not in unit
    assert "--port" not in unit


# --------------------------------------------------------------------------
# 8. The tracked violation
# --------------------------------------------------------------------------


def test_a_blob_uri_is_still_host_local(tmp_path):
    """Characterization, not endorsement — see the charter's "Known violation".

    ``file://`` means every ``content.blobs`` consumer must share Replicator's
    filesystem: a data-plane coupling in a service otherwise reached only through
    the broker, and a constraint on the issuer's deployment topology the issuer
    never agreed to. Pinned so #7's object-store backend flips a written line
    rather than quietly satisfying an unstated one.
    """
    store = LocalBlobStore(tmp_path)

    uri = store.store(b"bytes", "0" * 64, "text/plain")

    assert uri.startswith("file://")
