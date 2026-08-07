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

# The one carve-out, and it is a *word*, not a waiver (#28). co-core 0.8.0 makes
# ``info_source_id`` required on the command and on both facts, so Replicator
# must name it to copy it across. What the charter still forbids is
# *understanding* it — see ``test_the_echoed_domain_key_is_never_interpreted``,
# which is the assertion that makes this allowlist safe to have written.
ECHOED_TOKEN = "info_source"

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


def _vocabulary_surface(tree: ast.AST) -> set[str]:
    """Every identifier and non-docstring string literal in ``tree``.

    String literals are in scope alongside identifiers because domain leakage
    arrives as a dict key or a log field (``detail={"info_source_id": ...}``) at
    least as often as it arrives as an attribute — and that form is the one a
    reviewer skims past.
    """
    skip = _docstring_nodes(tree)
    surface: set[str] = set()
    for node in ast.walk(tree):
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


def _domain_hits(tree: ast.AST) -> set[str]:
    """Tokens from ``DOMAIN_TOKENS`` appearing anywhere in the vocabulary surface."""
    lowered = [text.lower() for text in _vocabulary_surface(tree)]
    return {token for token in DOMAIN_TOKENS if any(token in text for text in lowered)}


def _interpreting_hits(tree: ast.AST) -> set[str]:
    """Domain tokens appearing where the code is *reading* the value, not carrying it.

    A verbatim echo is three shapes and no more: an annotated field declaration,
    an attribute read, and a keyword argument. Every other position implies the
    value meant something to Replicator —

    - ``Compare`` / ``BoolOp`` and the test of an ``If`` / ``While`` / ``IfExp``:
      a branch on the domain.
    - ``Subscript``: a lookup keyed by it — a routing table, a per-source counter,
      a policy map.
    - ``JoinedStr``: an f-string. Every use anyone has proposed for one builds
      either a Redis key or a filesystem path, which is domain *state* under
      another name.

    Scoped to the whole of ``src/``, not to the allowlist, so a module that is
    not permitted to name the token at all still cannot interpret it in prose-shaped
    ways the other scan would miss.
    """
    hits: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare | ast.BoolOp | ast.Subscript | ast.JoinedStr):
            hits |= _domain_hits(node)
        elif isinstance(node, ast.If | ast.While | ast.IfExp):
            hits |= _domain_hits(node.test)
        elif isinstance(node, ast.Match):
            hits |= _domain_hits(node.subject)
    return hits


def test_no_module_names_a_domain_concept():
    """The load-bearing one.

    A domain field on the wire is the regression no type checker and no reviewer
    reliably catches, because it always arrives looking reasonable: one optional
    field, one small settings table, and Replicator has a domain model.

    ``ECHOED_TOKEN`` is exempted **only** in ``DOMAIN_ECHO_MODULES``, and only
    when it is the sole hit: an allowlisted module that also named a tenant or a
    watched item is offending on that, not on the echo.
    """
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {}
    for path in files:
        name = path.relative_to(REPO).as_posix()
        hits = _domain_hits(_parse(path))
        if name in DOMAIN_ECHO_MODULES:
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
        pytest.param("policy = table[command.info_source_id]", id="lookup"),
        pytest.param('key = f"replicator:{command.info_source_id}"', id="key-building"),
        pytest.param("ok = enabled and command.info_source_id", id="bool-op"),
        pytest.param(
            "match command.info_source_id:\n    case _:\n        pass", id="match-subject"
        ),
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
    ],
)
def test_the_interpretation_detector_passes_a_verbatim_echo(source):
    """The three shapes the emit path actually uses. A detector that flagged these
    would force the allowlist to be deleted rather than obeyed."""
    assert not _interpreting_hits(ast.parse(source))


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
