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
from src.storage.gcs import GcsBlobStore
from src.storage.local import LocalBlobStore
from tests.storage.test_gcs import FakeBucket, FakeClient

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

# The carve-outs, each one *field* wide rather than one word wide (#28, #29). A
# key maps a ``DOMAIN_TOKENS`` entry the exemption suppresses to the single
# identifier it may be suppressed *for*. The two halves are separate because the
# token scan matches substrings: exempting the token alone would let
# ``info_source_policy`` or ``info_sources`` into an allowlisted module under
# cover of the field that earned the carve-out (CR #2).
#
# ``info_source_id`` — co-core 0.8.0 makes it required on ``content.fetch``'s
# command and on both its facts.
#
# ``info_item_rep_spec_id`` — co-core 0.9.4 makes it required on
# ``ContentReplicateCommand`` and on both replicate facts (#29). It carries the
# ``info_item`` token, which no exemption previously covered, so the replicate
# emit path could not name it at all. Granted on exactly ``info_source_id``'s
# terms and no wider: this is the assignment *row* id, opaque here, and the
# adjacent ``info_item_id`` — the real domain key — stays refused.
#
# What the charter still forbids in both cases is *understanding* the value —
# see ``test_the_echoed_domain_key_is_never_interpreted``, which is the assertion
# that makes this allowlist safe to have written. Adding an entry here is the
# moment the charter is being edited, and it belongs in
# ``docs/contracts/replicator-boundaries.md`` in the same change.
ECHOED_FIELDS = {"info_source": "info_source_id", "info_item": "info_item_rep_spec_id"}
ECHOED_NAMES = frozenset(ECHOED_FIELDS.values())

# Exactly the modules on an emit path: the publishers, and the report dataclasses
# that carry the values between them. Deliberately not a directory glob —
# ``src/worker/`` also holds the pacer, the sweep, the policy reader, the alias
# table and the replicate handler, none of which has any business naming a domain
# object. That ``replicate.py`` is *absent* is the load-bearing part: the
# replicate handler decides what to do with a command and must reach none of
# these fields to do it (#29).
DOMAIN_ECHO_MODULES = frozenset(
    {
        "src/worker/handler.py",
        "src/worker/reporter.py",
        "src/worker/loop.py",
        "src/worker/replicate_reporter.py",
    }
)

# Where the echo may never spread, asserted against the allowlist itself rather
# than against the source. A future change that needs the domain key in the
# settings table or in a blob path would reach for this list first, and adding a
# module here is the moment the charter is actually being edited.
DOMAIN_ECHO_FORBIDDEN = ("src/core/config.py", "src/storage/", "src/api/")


def _docstring_nodes(tree: ast.AST) -> set[int]:
    """Ids of the ``Constant`` nodes that are docstrings, so the scan can skip them.

    Two kinds. The first is the conventional one: a module's, class's or
    function's leading string. The second is the **attribute docstring** (PEP
    258) — a bare string statement following an assignment, which is how every
    member of ``FailureReason`` and ``ReplicateReason`` documents itself. Those
    are prose by every measure a reader applies and are skipped for the reason
    the vocabulary scan is AST-based at all: a test whose first tripper is an
    English sentence gets deleted rather than heeded (#29).
    """
    skip: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list):
            continue
        for index, statement in enumerate(body):
            if not (
                isinstance(statement, ast.Expr)
                and isinstance(statement.value, ast.Constant)
                and isinstance(statement.value.value, str)
            ):
                continue
            # Position 0 is the classic docstring; anything later counts only
            # when it follows an assignment, which is what makes it an attribute
            # docstring rather than a stray expression.
            if index == 0 or isinstance(body[index - 1], ast.Assign | ast.AnnAssign):
                skip.add(id(statement.value))
    return skip


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


def _unechoed_names_for(tree: ast.AST, token: str) -> set[str]:
    """Names in ``tree`` that carry ``token`` but are not the field it is exempt for.

    Each exemption is for one field. ``info_source_policy``, ``info_sources`` and
    ``info_source_cache`` all contain their token, as do ``info_items`` and
    ``info_item_id``, and a substring exemption would admit every one of them
    into an allowlisted module — a per-InfoSource map, or a table keyed by the
    real InfoItem id, arriving under cover of the field that earned the carve-out
    (CR #2).
    """
    echoed = ECHOED_FIELDS[token]
    return {
        text
        for text in _vocabulary_surface(tree)
        if token in text.lower() and text.lower() != echoed
    }


def _unechoed_domain_names(tree: ast.AST) -> set[str]:
    """Every such name, across all the carve-outs."""
    return set().union(*(_unechoed_names_for(tree, token) for token in ECHOED_FIELDS))


def _exempted_hits(module: str, tree: ast.AST) -> set[str]:
    """``module``'s domain-token hits, after whatever its allowlisting subtracts.

    The arithmetic is **per token**, which is the whole of what generalizing from
    one carve-out to two decided. A token is suppressed only inside an
    allowlisted module and only when every name carrying it is exactly the field
    that earned it, so one abused token cannot ride in behind a well-behaved one
    and an abused token does not suppress the report of its innocent neighbour.
    """
    hits = _domain_hits(tree)
    if module not in DOMAIN_ECHO_MODULES:
        return hits
    return hits - {token for token in ECHOED_FIELDS if not _unechoed_names_for(tree, token)}


def _parents(tree: ast.AST) -> dict[int, ast.AST]:
    """Each node's parent, by id. ``ast`` does not record them and the scan needs them."""
    parent: dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parent[id(child)] = node
    return parent


def _is_echoed_value(node: ast.AST, parent: dict[int, ast.AST], name: str) -> bool:
    """Whether this occurrence is the value of the keyword named for ``name`` itself.

    ``Event(info_source_id=command.info_source_id)`` — the one position from
    which the value can only travel onward, because a keyword argument named for
    the field it fills cannot also be a lookup key or a branch.

    The keyword must be named for **this** field, not merely for some echoed
    field. ``Event(info_source_id=command.info_item_rep_spec_id)`` is not an echo
    at all: it reads one correlator to populate another, which is deciding what
    the value means. A membership test over ``ECHOED_NAMES`` would pass it, and
    that hole opens the moment there is more than one carve-out.
    """
    context = parent.get(id(node))
    return isinstance(context, ast.keyword) and context.arg == name


def _echo_violations(tree: ast.AST) -> list[str]:
    """Every occurrence of the echoed name that is **not** one of its legal shapes.

    Inverted from the deny-list this replaces (CR #16). That one enumerated the
    positions in which reading the value was forbidden — ``Compare``,
    ``Subscript``, ``BinOp``, positional call arguments, and so on — and three
    consecutive review rounds found positions it had not enumerated: six in the
    first (dict keys, ``.get()``, concatenation, path building, …), one in the
    second (keyword arguments under a different parameter name), two in the third
    (``assert`` and a comprehension filter). Each fix was correct and each left
    the next gap unknown until somebody probed again, while the charter cited the
    scan as *the* guard making the vocabulary carve-out safe. A deny-list can only
    ever be as complete as its last probe.

    So this asks the opposite question. A verbatim echo has exactly four legal
    shapes and no more:

    1. an annotated field declaration — ``info_source_id: str``;
    2. a parameter declaration — a function that carries the value through;
    3. the keyword itself — ``info_source_id=``;
    4. the value of that keyword — ``…=command.info_source_id``.

    Everything else is flagged, including shapes nobody has thought of yet: a
    subscript, a branch, an f-string, a set element, a comparison, a string
    literal spelling the name as a dict key or log field. Exhaustive by
    construction, so it needs no further enumeration.

    Only ``ECHOED_NAME`` is in scope, because it is the only domain name allowed
    to appear in ``src/`` at all — every other token in ``DOMAIN_TOKENS`` is
    refused outright by ``test_no_module_names_a_domain_concept``, in every
    position, which is a strictly stronger rule than this one.
    """
    parent = _parents(tree)
    skip = _docstring_nodes(tree)
    violations: list[str] = []
    for node in ast.walk(tree):
        # Shapes 1-3: declarations. Their *uses* are still checked below, so
        # allowing a parameter here cannot smuggle a lookup past the scan.
        if isinstance(node, ast.arg) and node.arg in ECHOED_NAMES:
            continue
        if isinstance(node, ast.keyword) and node.arg in ECHOED_NAMES:
            continue
        if isinstance(node, ast.Name) and node.id in ECHOED_NAMES:
            context = parent.get(id(node))
            if isinstance(context, ast.AnnAssign) and context.target is node:
                continue
            if not _is_echoed_value(node, parent, node.id):
                violations.append(f"line {node.lineno}: {node.id} used as a bare name")
        elif isinstance(node, ast.Attribute) and node.attr in ECHOED_NAMES:
            if not _is_echoed_value(node, parent, node.attr):
                violations.append(f"line {node.lineno}: .{node.attr} read outside the echo")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in skip
            and (spelled := _tokens_in({node.value}) & set(ECHOED_FIELDS))
        ):
            # A string literal spelling one is a dict key or a log field — the
            # form a reviewer skims past, per the vocabulary scan's own note.
            violations.append(f"line {node.lineno}: {sorted(spelled)} in a string literal")
    return violations


def test_no_module_names_a_domain_concept():
    """The load-bearing one.

    A domain field on the wire is the regression no type checker and no reviewer
    reliably catches, because it always arrives looking reasonable: one optional
    field, one small settings table, and Replicator has a domain model.

    An ``ECHOED_FIELDS`` token is exempted **only** in ``DOMAIN_ECHO_MODULES``,
    only when every name carrying it is exactly the field it is exempt for, and
    only for itself: an allowlisted module that also named a tenant or a watched
    item is offending on that, not on the echo. ``_exempted_hits`` owns that
    arithmetic and is pinned separately by
    ``test_the_exemption_arithmetic_is_per_token``.
    """
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {}
    for path in files:
        name = path.relative_to(REPO).as_posix()
        if hits := _exempted_hits(name, _parse(path)):
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
        path.relative_to(REPO).as_posix(): violations
        for path in files
        if (violations := _echo_violations(_parse(path)))
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
        # CR #15: two more branch positions the deny-list had not enumerated.
        # Under the inverted scan they need no rule of their own — they are simply
        # not one of the four legal shapes.
        pytest.param("assert command.info_source_id", id="assert"),
        pytest.param("xs = [p for p in ps if p.info_source_id]", id="comprehension-filter"),
        # Shapes nobody enumerated, kept as evidence that the inversion holds
        # without being told about them.
        pytest.param("seen = {command.info_source_id}", id="set-literal"),
        pytest.param("del table[command.info_source_id]", id="delete"),
        pytest.param("raise KeyError(command.info_source_id)", id="raise-argument"),
        pytest.param('log("...", extra={"info_source_id": x})', id="log-field"),
        pytest.param("x = command.info_source_id", id="bound-to-a-local"),
        # The second carve-out (#29) is policed by the same four shapes. co-core
        # 0.9.4 makes info_item_rep_spec_id required on ContentReplicateCommand
        # and on both replicate facts, so it earns a carve-out on exactly the
        # terms info_source_id did — and inherits every one of these refusals.
        pytest.param("if command.info_item_rep_spec_id: ...", id="replicate-branch"),
        pytest.param("row = table[command.info_item_rep_spec_id]", id="replicate-lookup"),
        pytest.param('k = f"rep:{command.info_item_rep_spec_id}"', id="replicate-key-building"),
        pytest.param("x = command.info_item_rep_spec_id", id="replicate-bound-to-a-local"),
        # Cross-wiring: each echoed field may fill only the keyword named for
        # itself. Reading one to populate the other is not carrying a value
        # through, it is deciding what it means — and a keyword-position
        # membership test would wave it past.
        pytest.param(
            "Event(info_source_id=command.info_item_rep_spec_id)", id="cross-wired-keyword"
        ),
    ],
)
def test_the_echo_detector_sees_a_planted_read(source):
    """Every one of these is *not* one of the four legal shapes, which is the only
    question the inverted scan asks."""
    assert _echo_violations(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("info_source_id: str", id="field-declaration"),
        pytest.param("Event(info_source_id=command.info_source_id)", id="keyword-echo"),
        # A helper that carries the value through: the parameter declares it, and
        # the only thing done with it is fill the keyword named for it.
        pytest.param(
            "def build(info_source_id):\n    return Event(info_source_id=info_source_id)",
            id="carried-through-a-parameter",
        ),
        # The emit path in full: a model built inside a positional argument, whose
        # own keyword is the echo. Both halves must stay clean.
        pytest.param(
            "_publish(pub, topic, Event(info_source_id=command.info_source_id), command=command)",
            id="emit-path",
        ),
        # The second carve-out, in the same four shapes (#29).
        pytest.param("info_item_rep_spec_id: str", id="replicate-field-declaration"),
        pytest.param(
            "Event(info_item_rep_spec_id=command.info_item_rep_spec_id)",
            id="replicate-keyword-echo",
        ),
        # Both echoes on one model, which is what the replicate facts actually
        # look like: three correlators copied across, none of them read.
        pytest.param(
            "Event(info_item_rep_spec_id=c.info_item_rep_spec_id, info_source_id=c.info_source_id)",
            id="replicate-emit-path",
        ),
    ],
)
def test_the_echo_detector_passes_a_verbatim_echo(source):
    """The four legal shapes. A detector that flagged these would force the
    allowlist to be deleted rather than obeyed.

    ``x = command.info_source_id`` is deliberately **not** here: binding the value
    to a local is the first half of doing something with it, and under the
    inverted rule a change that needs one has to argue with this test rather than
    slip past it.
    """
    assert not _echo_violations(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("self.info_source_policy = {}", id="policy-map"),
        pytest.param("for x in self.info_sources: pass", id="collection"),
        pytest.param("def f(info_source_url): ...", id="adjacent-field"),
        pytest.param('cache = {"info_source_cache": 1}', id="string-literal"),
        # The second carve-out is one field wide on exactly the same terms (#29).
        # ``info_item_id`` is the dangerous one: it is the real domain key the
        # assignment row points at, one underscore-separated step from the field
        # that earned the exemption, and holding a table of them is precisely the
        # domain model the charter refuses.
        pytest.param("def f(info_item_id): ...", id="replicate-adjacent-field"),
        pytest.param("for x in self.info_items: pass", id="replicate-collection"),
        pytest.param("self.info_item_cache = {}", id="replicate-cache"),
        pytest.param('d = {"info_item_slug": 1}', id="replicate-string-literal"),
    ],
)
def test_the_exemption_detector_sees_a_name_that_is_not_the_echoed_field(source):
    """CR #2: each carve-out is one field wide.

    Each of these contains an exempted *token* and would have been exempt under a
    substring-only exemption — including inside an allowlisted module, where the
    interpretation scan does not reach a bare assignment or a ``for`` target.
    """
    assert _unechoed_domain_names(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("info_source_id: str", id="field-declaration"),
        pytest.param("Event(info_source_id=command.info_source_id)", id="keyword-echo"),
        pytest.param("info_item_rep_spec_id: str", id="replicate-field-declaration"),
        pytest.param(
            "Event(info_item_rep_spec_id=command.info_item_rep_spec_id)",
            id="replicate-keyword-echo",
        ),
    ],
)
def test_the_exemption_detector_passes_the_echoed_field(source):
    assert not _unechoed_domain_names(ast.parse(source))


@pytest.mark.parametrize(
    ("module", "source", "expected"),
    [
        # An allowlisted module echoing both fields is clean: each token is
        # suppressed by the field that earned it.
        pytest.param(
            "src/worker/reporter.py",
            "Event(info_item_rep_spec_id=c.info_item_rep_spec_id, info_source_id=c.info_source_id)",
            set(),
            id="both-echoes-in-an-allowlisted-module",
        ),
        # The same source outside the allowlist keeps both hits. The carve-out is
        # a property of the module, not of the spelling.
        pytest.param(
            "src/worker/pacing.py",
            "Event(info_item_rep_spec_id=c.info_item_rep_spec_id, info_source_id=c.info_source_id)",
            {"info_item", "info_source"},
            id="the-same-echo-outside-the-allowlist",
        ),
        # Each carve-out stands or falls **on its own**. A module smuggling an
        # ``info_items`` collection loses the ``info_item`` exemption and keeps
        # the ``info_source`` one, so the offender report names the token that is
        # actually being abused rather than both.
        pytest.param(
            "src/worker/loop.py",
            "self.info_items = {}\nEvent(info_source_id=c.info_source_id)",
            {"info_item"},
            id="one-abused-token-does-not-forfeit-the-other",
        ),
        # An unrelated domain token is never exempt anywhere.
        pytest.param(
            "src/worker/handler.py",
            "tenant = 1",
            {"tenant"},
            id="an-unexempted-token-in-an-allowlisted-module",
        ),
    ],
)
def test_the_exemption_arithmetic_is_per_token(module, source, expected):
    """What the allowlist actually subtracts, asserted directly (#29).

    Before the second carve-out this arithmetic was a single ``hits -= {TOKEN}``
    inline in the module scan, and with one exemption there was nothing to get
    wrong. With two there is: suppressing the whole exempted set whenever *any*
    echoed field is clean would let an abused token ride in behind a well-behaved
    one, and suppressing nothing when any is abused would misreport which token
    is the problem. Neither shows up in the real-source scan while ``src/`` is
    clean, which is exactly why it is pinned here.
    """
    assert _exempted_hits(module, ast.parse(source)) == expected


# --------------------------------------------------------------------------
# 2b. The alias is a key, never a value (#29)
# --------------------------------------------------------------------------

# The replicate contract's charter check calls for this one by name: "an AST scan
# asserting every ``credentials_alias`` occurrence is a lookup key or a resolver
# argument — the mirror of the existing scan that keeps ``info_source_id`` echoed
# and never interpreted — plus the assertion that no payload field feeds a
# provider client's credential."
#
# The two halves guard different failures. Reading the alias as a *value* is how
# Replicator would start deciding what a destination means — the domain model
# arriving through a field the charter never listed. Feeding a payload field to a
# credential parameter is T1's line: no secret travels, and the way that gets
# broken is not a `secret` field appearing on the wire but an existing field
# being handed to a client that treats it as one.
ALIAS_NAME = "credentials_alias"

# Where the alias may be named at all. It is one module wide on purpose: the
# handler is the only place it appears, and `aliases.py` is absent deliberately —
# it deals in *bindings*, and it names the alias only as a dict key on data it
# read from host config, never off a command.
#
# **The handler uses it for two lookups, not one** (#29 CR #47). It resolves the
# binding — where bytes may land — and then selects that binding's writer, which
# is a live `AsyncGcsDriver` holding a credential resolved from ADC at startup.
# That second lookup is inside T1 rather than an exception to it: the alias
# selects an object the *host* built, and nothing about the credential comes off
# the message. Both tables are keyed identically, which is why `binding.alias`
# was dropped (CR #39) — one key, one derivation.
ALIAS_MODULES = frozenset({"src/worker/replicate.py"})

# Parameter names that hand a value to a provider client as a credential. A
# payload field reaching one of these is T1 broken, whatever it is called.
CREDENTIAL_PARAMS = frozenset(
    {"credentials", "credential", "token", "key", "secret", "password", "api_key", "access_key"}
)


# Calls that *look up* by alias rather than doing something with it. Named
# explicitly, because the first cut allowed any call taking the alias positionally
# — under which ``seen.add(command.credentials_alias)`` was legal, and building a
# set of seen aliases is exactly the state this invariant exists to prevent.
LOOKUP_CALLS = frozenset({"resolve", "get"})


def _is_lookup(node: ast.AST, parent: dict[int, ast.AST]) -> bool:
    """Whether this occurrence is naming a binding rather than being a value.

    **The receiver is not checked**, and that is a known bound on this scan
    rather than an oversight (CR #47): `aliases.resolve(...)` and
    `writers.get(...)` are both legal today, and matching on the receiver's name
    would pin two local variable names into a charter test. So
    `anything.get(command.credentials_alias)` passes here — including, one day, a
    `credential_map`. `CREDENTIAL_PARAMS` is the invariant that catches *that*
    shape, from the other direction: what matters is not which dict was indexed
    but whether a payload field reaches a parameter a client reads as a secret.
    """
    context = parent.get(id(node))
    if isinstance(context, ast.Subscript) and context.slice is node:
        return True
    if isinstance(context, ast.Call) and node in context.args:
        func = context.func
        return isinstance(func, ast.Attribute) and func.attr in LOOKUP_CALLS
    return False


def _alias_violations(tree: ast.AST) -> list[str]:
    """Every ``credentials_alias`` occurrence that is not a lookup or a parameter.

    Inverted like the echo scan, and for the same reason: a deny-list of forbidden
    positions is only ever as complete as its last probe. The legal shapes are

    1. the argument of a resolver call — ``aliases.resolve(command.credentials_alias)``;
    2. a parameter or annotated declaration carrying it through.

    Everything else is flagged, including an f-string, a comparison, a subscript,
    a dict key, or a bare assignment to a local — each of which is a step toward
    treating the alias as data rather than as a selector.
    """
    parent = _parents(tree)
    skip = _docstring_nodes(tree)
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.arg) and node.arg == ALIAS_NAME:
            continue
        if isinstance(node, ast.Attribute) and node.attr == ALIAS_NAME:
            if not _is_lookup(node, parent):
                violations.append(f"line {node.lineno}: .{ALIAS_NAME} used as a value")
        elif isinstance(node, ast.Name) and node.id == ALIAS_NAME:
            context = parent.get(id(node))
            if isinstance(context, ast.AnnAssign) and context.target is node:
                continue
            if not _is_lookup(node, parent):
                violations.append(f"line {node.lineno}: {ALIAS_NAME} used as a bare name")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in skip
            and ALIAS_NAME in node.value
        ):
            violations.append(f"line {node.lineno}: {ALIAS_NAME!r} in a string literal")
    return violations


def _credential_feeds(tree: ast.AST) -> list[str]:
    """Every call that hands a *payload* attribute to a credential-shaped keyword.

    ``client(credentials=command.credentials_alias)`` is the shape T1 forbids, and
    it is the one a well-meaning change makes: the alias is right there, it is
    called "credentials_alias", and passing it looks like wiring rather than like
    putting a secret on the wire.
    """
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg in CREDENTIAL_PARAMS and isinstance(keyword.value, ast.Attribute):
                violations.append(
                    f"line {node.lineno}: {keyword.arg}= is fed from .{keyword.value.attr}"
                )
    return violations


def test_the_alias_is_a_key_never_a_value():
    """The replicate charter check, made mechanical (#29).

    ``credentials_alias`` selects a binding the operator provisioned. The moment
    it is compared, formatted, or stored, Replicator has started to interpret a
    payload field — which is charter test 3 failing through a field that is not
    in ``DOMAIN_TOKENS`` and never will be.
    """
    files = _python_files(SRC)
    assert files, f"no modules found under {SRC} — the scan is a no-op"

    offenders = {}
    for path in files:
        name = path.relative_to(REPO).as_posix()
        violations = _alias_violations(_parse(path))
        if violations and name not in ALIAS_MODULES:
            offenders[name] = ["names the alias at all"] + violations
        elif violations:
            offenders[name] = violations
    assert not offenders


def test_no_payload_field_feeds_a_credential_parameter():
    """T1's line, asserted where it would actually be crossed.

    No secret travels on the wire — but the way that guarantee breaks is not a new
    field on the payload, it is an existing field handed to a client that treats
    it as one. Scoped to every module because the offending call would live
    wherever the first provider writer does.
    """
    offenders = {
        path.relative_to(REPO).as_posix(): feeds
        for path in _python_files(SRC)
        if (feeds := _credential_feeds(_parse(path)))
    }
    assert not offenders


def test_the_alias_modules_list_names_files_that_exist():
    for module in ALIAS_MODULES:
        assert (REPO / module).is_file(), module


@pytest.mark.parametrize(
    "source",
    [
        pytest.param('if command.credentials_alias == "primary": ...', id="comparison"),
        pytest.param('k = f"alias:{command.credentials_alias}"', id="f-string"),
        pytest.param("seen.add(command.credentials_alias)", id="stored"),
        pytest.param("alias = command.credentials_alias", id="bound-to-a-local"),
        pytest.param('d = {"credentials_alias": 1}', id="string-literal"),
        pytest.param("x = [command.credentials_alias]", id="list-element"),
    ],
)
def test_the_alias_detector_sees_a_planted_read(source):
    assert _alias_violations(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("binding = aliases.resolve(command.credentials_alias)", id="resolver-call"),
        pytest.param("binding = table[command.credentials_alias]", id="subscript-lookup"),
        pytest.param("def f(credentials_alias): ...", id="parameter"),
    ],
)
def test_the_alias_detector_passes_a_lookup(source):
    assert not _alias_violations(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param("Client(credentials=command.credentials_alias)", id="alias-as-credential"),
        pytest.param("Client(token=command.some_field)", id="any-payload-field-as-token"),
        pytest.param("build(secret=command.credentials_alias)", id="secret-kwarg"),
    ],
)
def test_the_credential_detector_sees_a_planted_feed(source):
    assert _credential_feeds(ast.parse(source))


def test_the_credential_detector_passes_a_host_resolved_credential():
    """The shape T1 *allows*: the client resolves its own credential locally, and
    the binding names only where to write."""
    source = "Client(project=binding.project).bucket(binding.bucket)"

    assert not _credential_feeds(ast.parse(source))


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


def test_a_blob_uri_is_host_local_on_the_default_backend(tmp_path):
    """Characterization, not endorsement — see the charter's "Known violation".

    ``file://`` means every ``content.blobs`` consumer must share Replicator's
    filesystem: a data-plane coupling in a service otherwise reached only through
    the broker, and a constraint on the issuer's deployment topology the issuer
    never agreed to.

    #7 built the way out and did **not** take it. The object store exists and
    satisfies the same seam, but `REPLICATOR_BLOB_BACKEND` still defaults to
    `local`, so what a bare deployment announces is unchanged and the violation
    is still the live one. It closes when an operator flips the environment
    (Phase C) and Watcher can read the new scheme (CannObserv/watcher#275) —
    not when the code lands.

    Kept as a pin rather than deleted for exactly that reason: a charter that
    said "resolved" while every running worker still announced `file://` would
    be the decorative document its own section warns about.
    """
    store = LocalBlobStore(tmp_path)

    uri = store.store(b"bytes", "0" * 64, "text/plain")

    assert uri.startswith("file://")
    assert Settings().blob_backend == "local"


def test_the_object_store_backend_announces_a_host_independent_uri():
    """The other half of the pin: the way out exists and is one variable away.

    Asserted so the charter's "Known violation" section can say *what closes it*
    and be checked on that, rather than only on the violation still standing. A
    line that describes a remedy nobody can execute is the same decorative
    failure one step later.
    """
    store = GcsBlobStore(
        "a-temp-bucket", prefix="blobs", client=FakeClient(FakeBucket("a-temp-bucket"))
    )

    uri = store.uri_for("0" * 64)

    assert uri.startswith("gs://")
    assert "a-temp-bucket" in uri
