"""The `replicator:cmd:*` keys are the whole non-stream footprint (#80).

`docs/CONVENTIONS.md` answers broker#1's last open question — what those keys
guard, what reads them, what a cold start does without them, and why a change
bus is the right place for them. The broker repo links *at that section*
rather than restating it, because it deliberately holds no application logic
and so cannot keep a copy true.

Three of the four answers are claims about this code, which means they rot
silently: a second key pattern, a `GET` where an `EXISTS` was, a renamed
segment, or a changed TTL default all leave the prose reading plausibly and
naming something that is no longer true — in another repo's inventory and, via
broker#2, in an ACL. This file is the executable half.

**The scan is AST-based, and the convention it rests on is enforced rather than
assumed.** Bus clients are injection-only (see CONVENTIONS.md), so every direct
Redis command in `src/` is spelled `client.<cmd>(...)`; everything else reaches
the broker through a co-core driver, which speaks streams only. That receiver
name is the scan's whole reach, which makes it the bypass: a module taking
`redis: Redis` writes whatever keys it likes and every assertion here still
passes. So `test_every_redis_handle_is_named_client` scans the *annotations*
too and requires the name — a new handle must join the scanned population
before it can be used. `test_the_scan_*` drives both scanners against synthetic
violating source, for the reason `test_boundaries.py` gives: a structural scan
that quietly matches nothing passes forever while enforcing nothing.
"""

import ast
from pathlib import Path

import pytest

from src.core.config import Settings
from src.worker.loop import DEDUPE_KEY_PREFIX, FETCH_SPEC, REPLICATE_SPEC

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
CONVENTIONS = REPO / "docs" / "CONVENTIONS.md"

# The heading the broker's inventory links to. Renaming it breaks that link
# silently, so it is pinned here rather than left to prose.
HEADING = "## The `replicator:cmd:*` keys"

# Commands that address a *stream*, plus the connection-lifecycle call. What is
# left after these is by definition the non-stream keyspace this file is about,
# so a newly used command lands in the assertion instead of a skip list.
STREAM_COMMANDS = frozenset(
    {
        "xadd",
        "xack",
        "xautoclaim",
        "xgroup_create",
        "xinfo_groups",
        "xlen",
        "xpending",
        "xpending_range",
        "xrange",
        "xread",
        "xreadgroup",
        "xrevrange",
        "xtrim",
    }
)
LIFECYCLE_COMMANDS = frozenset({"aclose", "close", "ping"})


def _relative(path: Path) -> str:
    """Repo-relative where it can be, absolute for the synthetic-source cases."""
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


def redis_call_sites(root: Path) -> list[tuple[str, int, str, str]]:
    """Every `client.<cmd>(...)` under `root`, as (file, line, cmd, first arg).

    The first argument is carried because *which key* a command addresses is
    half the claim: `exists(dedupe_key)` and `exists(some_other_key)` are the
    same command name and different footprints.
    """
    sites: list[tuple[str, int, str, str]] = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            receiver = node.func.value
            if not isinstance(receiver, ast.Name) or receiver.id != "client":
                continue
            first = ast.unparse(node.args[0]) if node.args else ""
            sites.append((_relative(path), node.lineno, node.func.attr, first))
    return sites


def redis_handle_names(root: Path) -> list[tuple[str, int, str]]:
    """Every `Redis`-annotated binding under `root`, as (file, line, name).

    Parameters and annotated assignments both, so `self._client: Redis` is as
    visible as `client: Redis`. Return annotations are not bindings and are
    skipped — nothing can be called on them without first being named.
    """
    handles: list[tuple[str, int, str]] = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.arg) and node.annotation is not None:
                if "Redis" in ast.unparse(node.annotation):
                    handles.append((_relative(path), node.lineno, node.arg))
            elif isinstance(node, ast.AnnAssign) and "Redis" in ast.unparse(node.annotation):
                handles.append((_relative(path), node.lineno, ast.unparse(node.target)))
    return handles


def documented_section() -> str:
    """The `## The `replicator:cmd:*` keys` section of CONVENTIONS.md."""
    text = CONVENTIONS.read_text(encoding="utf-8")
    assert HEADING in text, (
        f"{CONVENTIONS.relative_to(REPO)} no longer carries {HEADING!r} — "
        "the broker's keyspace inventory links to that heading (#80)"
    )
    body = text.split(HEADING, 1)[1]
    return body.split("\n## ", 1)[0]


def test_the_non_stream_keyspace_is_the_dedupe_key_and_nothing_else() -> None:
    """Every direct Redis command that is not a stream command names a dedupe key.

    This is the claim the broker's inventory rests on, and the one an ordinary
    feature breaks: a rate-limit counter, a cached policy, a lock. Any of them
    would put a second pattern on a broker whose ACL grants one.
    """
    non_stream = [
        site
        for site in redis_call_sites(SRC)
        if site[2] not in STREAM_COMMANDS and site[2] not in LIFECYCLE_COMMANDS
    ]
    assert non_stream, "the scan found no non-stream commands at all — it has stopped working"
    assert {site[3] for site in non_stream} == {"dedupe_key"}, (
        f"a non-stream Redis key beyond the dedupe key: {non_stream}. "
        f"Update {HEADING!r} in docs/CONVENTIONS.md and tell broker#1 (#80)."
    )


def test_the_documented_command_set_is_the_one_the_code_uses() -> None:
    """`SET` and `EXISTS`, both named in the doc — broker#2 grants from this list."""
    used = {site[2] for site in redis_call_sites(SRC) if site[3] == "dedupe_key"}
    assert used == {"set", "exists"}, f"the dedupe key's command surface changed: {used}"
    section = documented_section()
    for command in used:
        assert command.upper() in section, (
            f"the dedupe key is addressed with {command.upper()} and the section does not say so"
        )


def test_the_documented_key_shape_matches_the_code() -> None:
    """Prefix and both segments, so a third command stream forces a doc edit."""
    section = documented_section()
    assert DEDUPE_KEY_PREFIX in section
    for spec in (FETCH_SPEC, REPLICATE_SPEC):
        assert spec.dedupe_key("<command_id>") in section, (
            f"the {spec.label} stream's key shape is not in the section"
        )


def test_the_documented_ttl_is_the_shipped_default() -> None:
    """The window the broker observed — a doc that says a day must mean 86400."""
    section = documented_section()
    default = Settings.model_fields["dedupe_ttl_seconds"].default
    assert str(default) in section, f"the section does not name the {default}s default TTL"


def test_every_redis_handle_is_named_client() -> None:
    """The convention `redis_call_sites` reaches through, asserted rather than trusted.

    A handle spelled anything else is invisible to every other test in this
    file, so the section could go on claiming one key pattern while a second
    was already on the broker — the failure mode a guard has instead of a bug.
    """
    handles = redis_handle_names(SRC)
    assert handles, "no Redis-annotated binding found at all — the scan has stopped working"
    misnamed = [handle for handle in handles if handle[2] != "client"]
    assert not misnamed, (
        f"a Redis handle not named `client`: {misnamed}. Every direct command in src/ has "
        "to be reachable by this file's scan before docs/CONVENTIONS.md can claim what the "
        "non-stream footprint is (#80)."
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("await client.get(dedupe_key)", ("get", "dedupe_key")),
        ("await client.set(rate_key, 1)", ("set", "rate_key")),
        ("await client.xadd(topic, fields)", ("xadd", "topic")),
    ],
)
def test_the_scan_sees_a_command_this_repo_does_not_use(
    tmp_path: Path, source: str, expected: tuple[str, str]
) -> None:
    """The detector, tested against source that violates what it guards."""
    module = tmp_path / "fake.py"
    module.write_text(f"async def f(client, dedupe_key, rate_key, topic, fields):\n    {source}\n")
    sites = redis_call_sites(tmp_path)
    assert [(site[2], site[3]) for site in sites] == [expected]


def test_the_scan_ignores_a_call_on_something_that_is_not_the_client(tmp_path: Path) -> None:
    """`store.exists(...)` is a blob store, not the broker — receiver is the filter."""
    module = tmp_path / "fake.py"
    module.write_text("def f(store, fingerprint):\n    return store.exists(fingerprint)\n")
    assert redis_call_sites(tmp_path) == []


def test_the_handle_scan_sees_a_redis_parameter_under_another_name(tmp_path: Path) -> None:
    """The bypass finding 1 closed: a handle the call scan would never look at."""
    module = tmp_path / "fake.py"
    module.write_text(
        "class C:\n    _pool: Redis\n\nasync def f(redis: Redis, n: int) -> None:\n    ...\n"
    )
    assert [(name, line > 0) for _, line, name in redis_handle_names(tmp_path)] == [
        ("_pool", True),
        ("redis", True),
    ]
