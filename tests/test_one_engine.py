"""Nothing here runs Redis's own binaries by name: one engine, the broker's (#256).

The broker has run Ubuntu's Valkey 7.2 since CannObserv/broker#85. Once this host's
`redis-server` and `redis-tools` are purged (#256 step 6), anything still running
`redis-server` or `redis-cli` by name stops working. A fixture or CI guard that
skips when its binary is absent does not fail: it goes quiet. So this scans what
runs:

- shell, units and workflows, comments aside;
- the fenced code blocks of the runbooks;
- Python string literals that are exactly a binary's name, the shape a spawn or a
  ``shutil.which`` takes.

Prose naming the old binaries is history, not a call, and is not scanned.
"""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

_NAME = re.compile(r"\bredis-(?:server|cli)\b")
_PY_LITERAL = re.compile(r"""["']redis-(?:server|cli)["']""")
_FENCE = re.compile(r"^\s*```")

_ALLOWED = {
    # The resolver: redis-cli is its fallback when valkey-cli is absent.
    "scripts/rcli.sh": "the client resolver",
    # Names both clients in its missing-client message.
    "scripts/check_redis_floor.sh": "the missing-client message",
    # Stub the fallback client by name, on a PATH holding no real one.
    "tests/test_rcli.py": "stubs the fallback",
    "tests/test_check_redis_floor.py": "stubs the fallback",
    # A workflow snippet the timeout guard parses, never run.
    "tests/ci/test_workflow_timeouts.py": "fixture text",
    "tests/test_one_engine.py": "this file",
}


def _shell_lines(text: str) -> list[tuple[int, str]]:
    return [
        (n, line)
        for n, line in enumerate(text.splitlines(), start=1)
        if not line.lstrip().startswith("#")
    ]


def _fenced_lines(text: str) -> list[tuple[int, str]]:
    lines, inside = [], False
    for n, line in enumerate(text.splitlines(), start=1):
        if _FENCE.match(line):
            inside = not inside
            continue
        if inside and not line.lstrip().startswith("#"):
            lines.append((n, line))
    return lines


def engine_name_sites(repo: Path) -> list[tuple[str, int, str]]:
    """Every place under ``repo`` that would run a Redis binary by name."""
    candidates = [
        *(repo / "scripts").rglob("*.sh"),
        *(repo / "deploy").rglob("*"),
        *(repo / ".github" / "workflows").glob("*.yml"),
        *(repo / "tests").rglob("*.py"),
        *(repo / "scripts").rglob("*.py"),
        *(p for p in (repo / "docs").rglob("*.md") if "plans" not in p.relative_to(repo).parts),
        *repo.glob("*.md"),
    ]
    sites = []
    for path in sorted(p for p in candidates if p.is_file()):
        relative = path.relative_to(repo).as_posix()
        if relative in _ALLOWED:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        if path.suffix == ".py":
            numbered = [
                (n, line)
                for n, line in enumerate(text.splitlines(), start=1)
                if _PY_LITERAL.search(line)
            ]
        elif path.suffix == ".md":
            numbered = [(n, line) for n, line in _fenced_lines(text) if _NAME.search(line)]
        else:
            numbered = [(n, line) for n, line in _shell_lines(text) if _NAME.search(line)]
        sites += [(relative, n, line.strip()) for n, line in numbered]
    return sites


def test_nothing_runs_redis_s_own_binaries_by_name() -> None:
    assert engine_name_sites(REPO) == []


def test_the_scan_sees_each_shape(tmp_path: Path) -> None:
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "a.sh").write_text("# redis-cli in a comment\nredis-cli PING\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "t.py").write_text('which("redis-server")\n"""prose redis-server"""\n')
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "R.md").write_text(
        "prose `redis-cli`\n```bash\n# redis-cli comment\nredis-server --port 1\n```\n"
    )

    assert [(f, n) for f, n, _ in engine_name_sites(tmp_path)] == [
        ("docs/R.md", 4),
        ("scripts/a.sh", 2),
        ("tests/t.py", 1),
    ]
