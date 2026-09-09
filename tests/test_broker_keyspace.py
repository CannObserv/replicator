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

**The scan is AST-based and its receiver is the convention, not a guess.** Bus
clients are injection-only (see CONVENTIONS.md), so every direct Redis command
in `src/` is spelled `client.<cmd>(...)` on a parameter named `client`;
everything else reaches the broker through a co-core driver, which speaks
streams only. `test_the_scan_*` runs the scanner against synthetic source, for
the reason `test_boundaries.py` gives: a structural scan that quietly matches
nothing passes forever while enforcing nothing.
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
