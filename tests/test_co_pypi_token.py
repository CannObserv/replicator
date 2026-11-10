"""The co-pypi wrapper (#183): one key variable, an allowlisted environment, and
a token that never reaches output.

Design: docs/plans/2026-11-10-co-pypi-index-design.md, items 1 and 4. The
host's ``GOOGLE_APPLICATION_CREDENTIALS`` is the worker's *writer*, and the
deploy shell also carries org-wide ``GH_TOKEN_*`` PATs, so neither may reach the
wrapped ``uv`` — not merely "not be read".
"""

import hashlib
import io
import os
import re
import stat
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from scripts import co_pypi_token as cp

REPO = Path(__file__).resolve().parent.parent
WRAPPER = REPO / "scripts" / "co-pypi.sh"
TOKEN = "ya29.SECRET-TOKEN-VALUE"
READER = "/etc/replicator/co-pypi-reader.json"
WRITER = "/etc/replicator/the-writer.json"  # a placeholder: test_destinations.py

# What `. scripts/load_env.sh` leaves in a deploy shell, abridged.
LOADED = {
    "PATH": "/usr/local/bin:/usr/bin:/bin",
    "HOME": "/home/exedev",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "XDG_CACHE_HOME": "/home/exedev/.cache",
    "UV_CACHE_DIR": "/home/exedev/.cache/uv",
    "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
    "https_proxy": "http://proxy:3128",
    "VIRTUAL_ENV": "/tmp/ephemeral-env",
    cp.KEY_VAR: READER,
    "GOOGLE_APPLICATION_CREDENTIALS": WRITER,
    "GH_TOKEN": "ghp_replicator",
    "GH_TOKEN_BROKER": "ghp_broker",
    "ANTHROPIC_API_KEY": "sk-ant-x",
    "REPLICATOR_NOTIFY_TOKEN": "notify",
}


class Exec:
    """Records the exec instead of replacing the process."""

    def __init__(self, error: OSError | None = None):
        self.calls: list[tuple[str, list[str], dict[str, str]]] = []
        self.error = error

    def __call__(self, file, args, env):
        self.calls.append((file, args, env))
        if self.error:
            raise self.error


def _mint_from(seen: list[str]):
    def mint(key_path):
        seen.append(key_path)
        return TOKEN

    return mint


# --- the environment -------------------------------------------------------


def test_the_child_environment_is_the_allowlist():
    env = cp.child_environment(LOADED)
    assert env == {
        "PATH": LOADED["PATH"],
        "HOME": LOADED["HOME"],
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "XDG_CACHE_HOME": LOADED["XDG_CACHE_HOME"],
        "UV_CACHE_DIR": LOADED["UV_CACHE_DIR"],
        "SSL_CERT_FILE": LOADED["SSL_CERT_FILE"],
        "https_proxy": LOADED["https_proxy"],
    }


def test_the_writer_and_the_pats_never_reach_uv():
    execvpe = Exec()
    cp.main(["uv", "sync", "--frozen"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe)
    (_, _, env) = execvpe.calls[0]
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in env
    assert not [k for k in env if k.startswith("GH_TOKEN")]
    assert "ANTHROPIC_API_KEY" not in env and "REPLICATOR_NOTIFY_TOKEN" not in env
    assert "VIRTUAL_ENV" not in env, "the outer `uv run`'s env would be synced instead"
    # The reader key's path is the mint's input, not uv's.
    assert cp.KEY_VAR not in env


def test_uv_gets_the_pair_and_its_arguments_unchanged():
    execvpe = Exec()
    assert cp.main(["uv", "lock"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe) is None
    (file, args, env) = execvpe.calls[0]
    assert (file, args) == ("uv", ["uv", "lock"])
    assert env[cp.USERNAME_VAR] == "oauth2accesstoken"
    assert env[cp.PASSWORD_VAR] == TOKEN
    assert TOKEN not in " ".join(args)


# --- the key ---------------------------------------------------------------


def test_the_reader_key_is_minted_never_the_writer():
    seen: list[str] = []
    cp.main(["uv", "lock"], LOADED, mint=_mint_from(seen), execvpe=Exec())
    assert seen == [READER]


def test_no_fallback_to_the_writer_when_the_key_variable_is_unset(capsys):
    seen: list[str] = []
    environ = {k: v for k, v in LOADED.items() if k != cp.KEY_VAR}
    execvpe = Exec()
    assert cp.main(["uv", "lock"], environ, mint=_mint_from(seen), execvpe=execvpe) == 2
    assert seen == [] and execvpe.calls == []
    assert cp.KEY_VAR in capsys.readouterr().err


def test_a_blank_key_variable_is_unset(capsys):
    environ = {**LOADED, cp.KEY_VAR: "  "}
    assert cp.main(["uv", "lock"], environ, mint=_mint_from([]), execvpe=Exec()) == 2


def test_the_mint_reads_the_named_file_and_never_adc(monkeypatch, tmp_path):
    """``from_service_account_file`` takes the path; google-auth's ADC search,
    which would find GOOGLE_APPLICATION_CREDENTIALS, is never entered."""
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", WRITER)
    seen: dict = {}

    class Creds:
        token = TOKEN

        def refresh(self, request):
            seen["refreshed"] = True

    def from_file(path, scopes):
        seen["path"], seen["scopes"] = path, scopes
        return Creds()

    def adc(*a, **k):
        raise AssertionError("google.auth.default() must not run")

    monkeypatch.setattr(cp.service_account.Credentials, "from_service_account_file", from_file)
    monkeypatch.setattr("google.auth.default", adc)
    assert cp.mint_token(READER) == TOKEN
    assert seen == {"path": READER, "scopes": cp.SCOPES, "refreshed": True}


# --- refusals and failures -------------------------------------------------


@pytest.mark.parametrize("argv", [[], ["pip", "install"], ["bash", "-c", "env"], ["/bin/uvx"]])
def test_anything_but_uv_or_wheels_is_refused_before_a_mint(argv, capsys):
    seen: list[str] = []
    assert cp.main(argv, LOADED, mint=_mint_from(seen), execvpe=Exec()) == 2
    assert seen == []
    assert "usage" in capsys.readouterr().err


def test_a_uv_by_path_is_still_uv():
    execvpe = Exec()
    cp.main(["/usr/local/bin/uv", "sync"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe)
    assert execvpe.calls[0][0] == "/usr/local/bin/uv"


def test_a_failed_mint_runs_nothing_and_prints_no_token(capsys):
    def mint(_):
        raise RuntimeError("invalid_grant: account not found")

    execvpe = Exec()
    assert cp.main(["uv", "lock"], LOADED, mint=mint, execvpe=execvpe) == 1
    out = capsys.readouterr()
    assert execvpe.calls == []
    assert "invalid_grant" in out.err and READER in out.err
    assert TOKEN not in out.out + out.err


def test_a_failed_exec_names_the_command_and_prints_no_token(capsys):
    execvpe = Exec(FileNotFoundError(2, "No such file or directory"))
    assert cp.main(["uv", "lock"], LOADED, mint=lambda _: TOKEN, execvpe=execvpe) == 127
    out = capsys.readouterr()
    assert "uv" in out.err
    assert TOKEN not in out.out + out.err


# --- the wheels verb (item 4) ----------------------------------------------


def _wheel(name: str) -> bytes:
    return f"wheel bytes of {name}".encode()


def _page(package: str, files: dict[str, bytes], *, host=cp.INDEX_HOST, bad: str = "") -> bytes:
    links = []
    for name, data in files.items():
        digest = hashlib.sha256(b"tampered" if name == bad else data).hexdigest()
        links.append(
            f'<a href="https://{host}/co-gcs/co-pypi/{package}/{name}#sha256={digest}">{name}</a>'
        )
    return ("<html><body>" + "\n".join(links) + "</body></html>").encode()


def _index(versions=("0.40.1", "0.41.0"), **page_kw):
    """A fake opener over the three packages' pages and their files."""
    files: dict[str, bytes] = {}
    pages: dict[str, bytes] = {}
    for package in cp.PACKAGES:
        mine = {}
        for version in versions:
            for suffix in ("-py3-none-any.whl", ".tar.gz"):
                name = f"{package.replace('-', '_')}-{version}{suffix}"
                mine[name] = _wheel(name)
        files.update(mine)
        pages[f"{cp.INDEX_URL}{package}/"] = _page(package, mine, **page_kw)
    requests: list[urllib.request.Request] = []

    def opener(request):
        requests.append(request)
        url = request.full_url
        if url in pages:
            return io.BytesIO(pages[url])
        name = url.rsplit("/", 1)[-1]
        if name in files:
            return io.BytesIO(files[name])
        raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

    return opener, requests, files


def test_wheels_downloads_the_six_wheels_verified(tmp_path):
    opener, requests, files = _index()
    got = cp.fetch_wheels(TOKEN, ["0.40.1", "0.41.0"], tmp_path, opener=opener)
    names = sorted(p.name for p in got)
    assert names == sorted(n for n in files if n.endswith(".whl"))
    assert sorted(p.name for p in tmp_path.iterdir()) == names, "no sdist, no temp file"
    for path in got:
        assert path.read_bytes() == files[path.name]
    assert all(r.get_header("Authorization", "").startswith("Basic ") for r in requests)


def test_wheels_refuses_a_digest_mismatch_and_leaves_nothing(tmp_path):
    bad = "co_core_aio-0.41.0-py3-none-any.whl"
    opener, _, _ = _index(bad=bad)
    with pytest.raises(cp.FetchError, match=bad):
        cp.fetch_wheels(TOKEN, ["0.40.1", "0.41.0"], tmp_path, opener=opener)
    assert not (tmp_path / bad).exists()
    assert not [p for p in tmp_path.iterdir() if p.name.startswith(".")]


def test_wheels_refuses_a_version_the_index_lacks(tmp_path):
    opener, _, _ = _index(versions=("0.41.0",))
    with pytest.raises(cp.FetchError, match="co_core-0.40.1-py3-none-any.whl"):
        cp.fetch_wheels(TOKEN, ["0.40.1", "0.41.0"], tmp_path, opener=opener)


def test_wheels_never_sends_the_token_to_another_host(tmp_path):
    opener, requests, _ = _index(host="evil.example")
    with pytest.raises(cp.FetchError, match="evil.example"):
        cp.fetch_wheels(TOKEN, ["0.41.0"], tmp_path, opener=opener)
    assert all(r.host == cp.INDEX_HOST for r in requests)


@pytest.mark.parametrize(
    ("newurl", "followed"),
    [
        (f"https://{cp.INDEX_HOST}/artifacts-downloads/x", True),
        ("https://storage.googleapis.com/x", False),
        (f"http://{cp.INDEX_HOST}/x", False),
    ],
)
def test_a_redirect_is_followed_only_on_the_index_host(newurl, followed):
    """The index answers a file with a same-host 307 [measured 2026-11-10], and
    urllib forwards the Authorization header across any redirect."""
    handler = cp.IndexHostRedirect()
    request = urllib.request.Request(f"{cp.INDEX_URL}co-core/", headers={"Authorization": "x"})
    if followed:
        new = handler.redirect_request(request, None, 307, "", {}, newurl)
        assert new is not None and new.full_url == newurl
    else:
        with pytest.raises(urllib.error.HTTPError):
            handler.redirect_request(request, None, 307, "", {}, newurl)


def test_the_wheels_verb_mints_then_fetches(tmp_path):
    calls = []

    def fetch(token, versions, dest):
        calls.append((token, versions, dest))
        return []

    rc = cp.main(
        ["wheels", "0.40.1", "0.41.0", str(tmp_path)], LOADED, mint=lambda _: TOKEN, fetch=fetch
    )
    assert rc == 0
    assert calls == [(TOKEN, ["0.40.1", "0.41.0"], tmp_path)]


@pytest.mark.parametrize(
    "argv", [["wheels"], ["wheels", "0.41.0", "dir"], ["wheels", "a", "b", "c", "d"]]
)
def test_the_wheels_verb_takes_two_versions_and_a_directory(argv, capsys):
    seen: list[str] = []
    assert cp.main(argv, LOADED, mint=_mint_from(seen), fetch=lambda *a: []) == 2
    assert seen == []


def test_a_failed_fetch_prints_no_token(tmp_path, capsys):
    def fetch(token, versions, dest):
        raise cp.FetchError(f"sha256 mismatch for x (url had {cp.INDEX_HOST})")

    rc = cp.main(
        ["wheels", "0.40.1", "0.41.0", str(tmp_path)], LOADED, mint=lambda _: TOKEN, fetch=fetch
    )
    out = capsys.readouterr()
    assert rc == 1
    assert TOKEN not in out.out + out.err


# --- the shell wrapper ------------------------------------------------------


@pytest.fixture
def fake_uv(tmp_path):
    """A `uv` first on PATH that records its argv and environment."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    record = tmp_path / "record"
    uv = bindir / "uv"
    uv.write_text(f'#!/bin/bash\nprintf "%s\\n" "$@" > {record}.argv\nenv > {record}.env\n')
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    return bindir, record


def test_the_shell_wrapper_starts_the_outer_uv_from_the_allowlist(fake_uv):
    bindir, record = fake_uv
    environ = {**LOADED, "PATH": f"{bindir}:{os.environ['PATH']}"}
    subprocess.run(["bash", str(WRAPPER), "uv", "sync", "--frozen"], env=environ, check=True)
    env = dict(
        line.split("=", 1) for line in Path(f"{record}.env").read_text().splitlines() if "=" in line
    )
    argv = Path(f"{record}.argv").read_text().splitlines()
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in env
    assert not [k for k in env if k.startswith("GH_TOKEN")]
    assert "ANTHROPIC_API_KEY" not in env and "VIRTUAL_ENV" not in env
    assert env[cp.KEY_VAR] == READER, "the mint's one input"
    assert argv[:3] == ["run", "--no-project", "--with"]
    assert argv[-4:] == [str(REPO / "scripts" / "co_pypi_token.py"), "uv", "sync", "--frozen"]


def test_the_shell_wrapper_allowlist_matches_the_python_one():
    """Two layers, one list: a variable added to one and not the other would
    reach uv through the gap."""
    text = WRAPPER.read_text()

    def array(name: str) -> set[str]:
        m = re.search(rf"^{name}=\(([^)]*)\)", text, flags=re.MULTILINE)
        assert m, f"{name}=(…) not found in {WRAPPER.name}: the derivation broke"
        return set(m.group(1).split())

    # The shell layer also carries the key variable: it is the mint's input.
    assert array("ALLOWED") == set(cp.ALLOWED) | {cp.KEY_VAR}
    assert array("PREFIXES") == set(cp.ALLOWED_PREFIXES)
