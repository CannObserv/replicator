"""Behaviour of scripts/rcli.sh - redis-cli with the credential kept off argv (#127).

`redis-cli -u redis://user:pw@host` puts the password in the process's argv,
and `/proc/<pid>/cmdline` is readable by every local user while the call runs.
broker#47 set the rule: `REDISCLI_AUTH` plus `--user`, never the password on a
command line. The helper strips the URL's userinfo and hands the rest to `-u`,
so TLS, host, port and db stay redis-cli's own parsing.

Verified against redis-cli 7.0.15 (#127): `-u redis://svc@host` sends `svc` as
the *password*, and `-u redis://svc:@host` sends an empty one that overrides
`REDISCLI_AUTH` - so only the userinfo-free URL plus `--user` works.
"""

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
HELPER = REPO / "scripts" / "rcli.sh"

_SECRET = "s3cr3t"


def _recording_redis_cli(tmp_path: Path) -> Path:
    """A `redis-cli` that records its argv, one per line, and its `REDISCLI_AUTH`.

    The auth file holds `unset` when the variable is absent, so "not passed" and
    "passed empty" stay distinct - redis-cli sends `AUTH ""` for the second.
    """
    binder = tmp_path / "bin"
    binder.mkdir()
    (binder / "redis-cli").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "%s\\n" "$@" > "{tmp_path}/argv"\n'
        f'printf "%s" "${{REDISCLI_AUTH-unset}}" > "{tmp_path}/auth"\n'
        "echo PONG\n"
    )
    (binder / "redis-cli").chmod(0o755)
    return binder


def _rcli(tmp_path: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    bindir = _recording_redis_cli(tmp_path)
    return subprocess.run(
        ["bash", "-c", f'. "{HELPER}"; rcli "$@"', "rcli", *args],
        env={"PATH": f"{bindir}:/usr/bin:/bin", **env},
        text=True,
        capture_output=True,
    )


def _argv(tmp_path: Path) -> list[str]:
    return (tmp_path / "argv").read_text().splitlines()


def _auth(tmp_path: Path) -> str:
    return (tmp_path / "auth").read_text()


@pytest.mark.parametrize(
    ("url", "argv", "auth"),
    [
        pytest.param(
            f"redis://replicator:{_SECRET}@broker:6379/0",
            ["--user", "replicator", "-u", "redis://broker:6379/0"],
            _SECRET,
            id="user-and-password",
        ),
        pytest.param(
            # redis-py reads an empty username as "none" and sends a one-argument
            # AUTH, which is the default user; omitting --user does the same.
            f"redis://:{_SECRET}@broker:6379/0",
            ["-u", "redis://broker:6379/0"],
            _SECRET,
            id="password-only",
        ),
        pytest.param(
            "redis://replicator@broker:6379/0",
            ["--user", "replicator", "-u", "redis://broker:6379/0"],
            "unset",
            id="user-only",
        ),
        pytest.param(
            # Both redis-cli and redis-py percent-decode the userinfo.
            "redis://re%70licator:s3%40cr%3At@broker:6379/0",
            ["--user", "replicator", "-u", "redis://broker:6379/0"],
            "s3@cr:t",
            id="percent-encoded",
        ),
        pytest.param(
            # A `%` that starts no escape is kept, as urllib's unquote keeps it;
            # so is a backslash, which must not become an escape of its own.
            "redis://replicator:a%ZZb%4%41\\n@broker:6379/0",
            ["--user", "replicator", "-u", "redis://broker:6379/0"],
            "a%ZZb%4A\\n",
            id="stray-percent-and-backslash",
        ),
        pytest.param(
            f"rediss://replicator:{_SECRET}@broker:6380/2",
            ["--user", "replicator", "-u", "rediss://broker:6380/2"],
            _SECRET,
            id="tls-scheme-kept",
        ),
        pytest.param(
            "redis://localhost:6379/0",
            ["-u", "redis://localhost:6379/0"],
            "unset",
            id="no-credential",
        ),
        pytest.param(
            # redis-py reads any query argument as a connection kwarg, password
            # included; redis-cli ignores the query, so it is never passed on.
            f"redis://broker:6379/0?password={_SECRET}",
            ["-u", "redis://broker:6379/0"],
            _SECRET,
            id="query-password",
        ),
        pytest.param(
            # The userinfo wins where it says something, as in redis-py's
            # parse_url; the query fills only what it left out.
            f"redis://replicator@broker:6379/0?username=other&password={_SECRET}",
            ["--user", "replicator", "-u", "redis://broker:6379/0"],
            _SECRET,
            id="userinfo-over-query",
        ),
        pytest.param(
            f"redis://:{_SECRET}@broker:6379/0?password=other",
            ["-u", "redis://broker:6379/0"],
            _SECRET,
            id="userinfo-password-over-query",
        ),
        pytest.param(
            # redis-py lets a query `db` override the path's; -n after -u does too.
            "redis://broker:6379/1?db=3",
            ["-u", "redis://broker:6379/1", "-n", "3"],
            "unset",
            id="query-db-overrides-path",
        ),
        pytest.param(
            "redis://broker:6379/0?password=a+b%2Bc",
            ["-u", "redis://broker:6379/0"],
            "a b+c",
            id="query-plus-is-space",
        ),
        pytest.param(
            "rediss://broker:6380/0?ssl_cert_reqs=none#frag",
            ["-u", "rediss://broker:6380/0"],
            "unset",
            id="other-query-dropped",
        ),
    ],
)
def test_the_credential_reaches_redis_cli_through_the_environment(
    tmp_path: Path, url: str, argv: list[str], auth: str
) -> None:
    result = _rcli(tmp_path, {"REPLICATOR_REDIS_URL": url}, "PING")

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert _argv(tmp_path) == [*argv, "PING"]
    assert _auth(tmp_path) == auth


@pytest.mark.parametrize(
    "url",
    [f"redis://replicator:{_SECRET}@broker/0", f"redis://broker/0?password={_SECRET}"],
    ids=["userinfo", "query"],
)
def test_the_password_is_never_on_the_command_line(tmp_path: Path, url: str) -> None:
    _rcli(tmp_path, {"REPLICATOR_REDIS_URL": url}, "PING")

    assert not any(_SECRET in arg for arg in _argv(tmp_path))


def test_an_inherited_redis_cli_auth_does_not_outlive_a_credential_free_url(
    tmp_path: Path,
) -> None:
    """The URL is the whole credential, as it is for the worker."""
    _rcli(
        tmp_path,
        {"REPLICATOR_REDIS_URL": "redis://localhost:6379/0", "REDISCLI_AUTH": "stale"},
        "PING",
    )

    assert _auth(tmp_path) == "unset"


def test_an_unset_url_is_refused_rather_than_defaulted(tmp_path: Path) -> None:
    """An operator with no env loaded must not be pointed at localhost quietly."""
    result = _rcli(tmp_path, {}, "PING")

    assert result.returncode != 0
    assert "REPLICATOR_REDIS_URL" in result.stderr
    assert not (tmp_path / "argv").exists()


def test_a_url_with_no_scheme_is_refused_without_running_redis_cli(tmp_path: Path) -> None:
    """redis-cli refuses it too, but only after starting with it in argv (#127 CR 7).
    The refusal does not quote the URL, which may carry the password."""
    result = _rcli(
        tmp_path, {"REPLICATOR_REDIS_URL": f"replicator:{_SECRET}@broker:6379/0"}, "PING"
    )

    assert result.returncode == 2
    assert "scheme" in result.stderr
    assert _SECRET not in result.stderr
    assert not (tmp_path / "argv").exists()


def test_redis_cli_s_own_output_and_status_pass_through(tmp_path: Path) -> None:
    result = _rcli(tmp_path, {"REPLICATOR_REDIS_URL": "redis://localhost:6379/0"}, "PING")

    assert result.stdout == "PONG\n"


# --- No credential on a redis-cli command line, anywhere (#127) ---------------
#
# The two sites #127 found both spelled it `redis-cli -u "<variable>"`, and
# neither variable was named after the URL it held, so the guard does not try
# to tell credentialed URLs from clean ones: a `-u` given a quoted, variable or
# URL argument, and any `-a` or `--pass`, is a finding unless its file is
# allowed below, with the reason. Prose naming the flag (`-u <URL>`) is not.

_CREDENTIAL_FLAG = re.compile(
    r"\bredis-cli\b.*?(?:\s-u\s+[\"'$]|\s-u\s+rediss?://|\s-a\s|\s--pass\b)"
)

_ALLOWED = {
    # The one place a URL is handed to -u, after its userinfo is stripped.
    "scripts/rcli.sh": "the helper itself",
    # Its own loopback redis-server, started with no password at all.
    "scripts/rehearse_reconnect.sh": "credential-free loopback broker",
}


def credential_flag_sites(*roots: Path) -> list[tuple[str, int, str]]:
    """Every line under ``roots`` handing redis-cli a URL or password flag."""
    sites: list[tuple[str, int, str]] = []
    for root in roots:
        paths = [root] if root.is_file() else sorted(p for p in root.rglob("*") if p.is_file())
        for path in paths:
            relative = path.relative_to(REPO).as_posix() if path.is_relative_to(REPO) else path.name
            if relative in _ALLOWED or path.suffix == ".py":
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):  # pragma: no cover - none here today
                continue
            for number, line in _logical_lines(text):
                if _CREDENTIAL_FLAG.search(line):
                    sites.append((relative, number, line))
    return sites


def _logical_lines(text: str) -> list[tuple[int, str]]:
    """Lines with shell `\\` continuations joined, each numbered where it starts."""
    joined: list[tuple[int, str]] = []
    start, parts = 0, []
    for number, line in enumerate(text.splitlines(), start=1):
        if not parts:
            start = number
        stripped = line.strip()
        if stripped.endswith("\\"):
            parts.append(stripped[:-1].strip())
            continue
        joined.append((start, " ".join([*parts, stripped])))
        parts = []
    if parts:
        joined.append((start, " ".join(parts)))
    return joined


def test_no_runbook_or_script_puts_a_credential_on_redis_cli_s_command_line() -> None:
    """`docs/plans/` is excluded: dated design records, not instructions to run."""
    roots = [REPO / "scripts", REPO / "deploy", REPO / ".claude" / "hooks"]
    roots += [p for p in (REPO / "docs").rglob("*.md") if "plans" not in p.parts]
    roots += sorted(REPO.glob("*.md"))

    assert credential_flag_sites(*roots) == []


@pytest.mark.parametrize(
    "line",
    [
        'redis-cli -u "${URL}" INFO server',
        "alias rcli='redis-cli --no-auth-warning -u \"$REPLICATOR_REDIS_URL\"'",
        'redis-cli -a "$PW" PING',
        "redis-cli --pass hunter2 PING",
        "redis-cli -u redis://replicator:pw@broker:6379/0 PING",
    ],
)
def test_the_guard_sees_each_spelling(tmp_path: Path, line: str) -> None:
    (tmp_path / "runbook.md").write_text(f"```bash\n{line}\n```\n")

    assert [site[2] for site in credential_flag_sites(tmp_path)] == [line]


def test_the_guard_sees_a_command_continued_across_lines(tmp_path: Path) -> None:
    """Long runbook commands wrap; the flag on the second line is still the call's.
    The finding names the line the command starts on."""
    (tmp_path / "runbook.md").write_text(
        '```bash\nredis-cli --no-auth-warning \\\n  -u "$REPLICATOR_REDIS_URL" PING\n```\n'
    )

    assert [site[1] for site in credential_flag_sites(tmp_path)] == [2]
