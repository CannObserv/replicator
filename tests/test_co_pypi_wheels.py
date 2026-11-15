"""The co-pypi wheel fetcher: wheels on disk, each sha256-checked against the index.

``scripts/co_pypi_wheels.py`` runs under the co-pypi wrapper, as
``bash scripts/co-pypi.sh uv run --no-project python scripts/co_pypi_wheels.py``,
and reads the credential pair the wrapper hands uv. These tests need nothing
from the repo they sit in: the fetcher is loaded by path, the index is a fake
opener, and each ``pyproject.toml`` is written here. No network, no key.
"""

from __future__ import annotations

import ast
import base64
import hashlib
import http.client
import importlib.util
import io
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

NAME = "co_pypi_wheels.py"
HERE = Path(__file__).resolve().parent
# Beside this test (the skill's assets) or in the repo's scripts/ (installed).
SCRIPT = next(
    (p for p in (HERE / NAME, HERE.parent / "scripts" / NAME) if p.is_file()),
    HERE.parent / "scripts" / NAME,
)
HOST = "pkg.example"
INDEX = f"https://{HOST}/acme/co-pypi/simple/"
USER = "oauth2accesstoken"
TOKEN = "ya29.sentinel-never-printed"
PAIR = {"UV_INDEX_CO_PYPI_USERNAME": USER, "UV_INDEX_CO_PYPI_PASSWORD": TOKEN}


@pytest.fixture(scope="module")
def fw():
    spec = importlib.util.spec_from_file_location("co_pypi_wheels_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pyproject(tmp_path: Path, index: str | None = INDEX) -> Path:
    path = tmp_path / "pyproject.toml"
    body = '[project]\nname = "acme"\n'
    if index is not None:
        body += f'\n[[tool.uv.index]]\nname = "co-pypi"\nurl = "{index}"\nexplicit = true\n'
    path.write_text(body)
    return path


def _index(packages, versions, *, host=HOST, bad="", no_digest=""):
    """A fake opener over each package's PEP 503 page and its files."""
    files: dict[str, bytes] = {}
    pages: dict[str, bytes] = {}
    for page, prefix in packages:
        links = []
        for version in versions:
            for suffix in ("-py3-none-any.whl", ".tar.gz"):
                name = f"{prefix}-{version}{suffix}"
                data = f"bytes of {name}".encode()
                files[name] = data
                digest = hashlib.sha256(b"tampered" if name == bad else data).hexdigest()
                fragment = "" if name == no_digest else f"#sha256={digest}"
                links.append(
                    f'<a href="https://{host}/acme/co-pypi/{page}/{name}{fragment}">{name}</a>'
                )
        pages[f"{INDEX}{page}/"] = ("<html><body>" + "\n".join(links) + "</body></html>").encode()
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


CO_CORE = [("co-core", "co_core"), ("co-core-aio", "co_core_aio")]


def _fetch(fw, tmp_path, opener, packages=("co-core", "co-core-aio"), versions=("0.1.0", "0.2.0")):
    return fw.fetch_wheels(
        INDEX, (USER, TOKEN), list(packages), list(versions), tmp_path / "wheels", opener=opener
    )


# --- the fetch ---------------------------------------------------------------


def test_fetches_each_package_at_each_version_verified(fw, tmp_path):
    opener, requests, files = _index(CO_CORE, ["0.1.0", "0.2.0"])
    got = _fetch(fw, tmp_path, opener)
    want = sorted(n for n in files if n.endswith(".whl"))
    assert sorted(p.name for p in got) == want
    assert sorted(p.name for p in (tmp_path / "wheels").iterdir()) == want, "no sdist, no temp file"
    for path in got:
        assert path.read_bytes() == files[path.name]
    basic = "Basic " + base64.b64encode(f"{USER}:{TOKEN}".encode()).decode()
    assert {r.get_header("Authorization") for r in requests} == {basic}


def test_a_digest_mismatch_names_the_file_and_leaves_nothing(fw, tmp_path):
    bad = "co_core_aio-0.2.0-py3-none-any.whl"
    opener, _, _ = _index(CO_CORE, ["0.1.0", "0.2.0"], bad=bad)
    with pytest.raises(fw.FetchError, match=f"sha256 mismatch for {bad}"):
        _fetch(fw, tmp_path, opener)
    assert not (tmp_path / "wheels" / bad).exists()
    assert not [p for p in (tmp_path / "wheels").iterdir() if p.name.startswith(".")]


def test_a_link_without_a_digest_is_refused(fw, tmp_path):
    bare = "co_core-0.1.0-py3-none-any.whl"
    opener, _, _ = _index(CO_CORE, ["0.1.0"], no_digest=bare)
    with pytest.raises(fw.FetchError, match=bare):
        _fetch(fw, tmp_path, opener, versions=["0.1.0"])
    assert not (tmp_path / "wheels" / bare).exists()


def test_a_version_the_index_lacks_names_the_file(fw, tmp_path):
    opener, _, _ = _index(CO_CORE, ["0.2.0"])
    with pytest.raises(fw.FetchError, match="co_core-0.1.0-py3-none-any.whl"):
        _fetch(fw, tmp_path, opener)


def test_a_package_the_index_lacks_names_its_page(fw, tmp_path):
    opener, _, _ = _index(CO_CORE, ["0.1.0"])
    with pytest.raises(fw.FetchError, match=f"{INDEX}absent/"):
        _fetch(fw, tmp_path, opener, packages=["absent"], versions=["0.1.0"])


@pytest.mark.parametrize(
    ("package", "page", "prefix"),
    [
        ("co-core", "co-core", "co_core"),
        ("co_core_aio", "co-core-aio", "co_core_aio"),
        ("Co.Core--Sync", "co-core-sync", "co_core_sync"),
    ],
)
def test_names_are_normalized(fw, tmp_path, package, page, prefix):
    """The page by PEP 503, the file by the wheel spec: what the closure diff reads."""
    opener, requests, _ = _index([(page, prefix)], ["1.0.0"])
    [got] = _fetch(fw, tmp_path, opener, packages=[package], versions=["1.0.0"])
    assert got == tmp_path / "wheels" / f"{prefix}-1.0.0-py3-none-any.whl"
    assert requests[0].full_url == f"{INDEX}{page}/"


def test_the_token_never_goes_to_another_host(fw, tmp_path):
    opener, requests, _ = _index(CO_CORE, ["0.1.0"], host="evil.example")
    with pytest.raises(fw.FetchError, match="evil.example"):
        _fetch(fw, tmp_path, opener, versions=["0.1.0"])
    assert all(r.host == HOST for r in requests)


@pytest.mark.parametrize(
    ("newurl", "followed"),
    [
        (f"https://{HOST}/artifacts-downloads/x", True),
        ("https://storage.googleapis.com/x", False),
        (f"http://{HOST}/x", False),
    ],
)
def test_a_redirect_is_followed_only_on_the_index_host(fw, newurl, followed):
    """Artifact Registry answers a file with a same-host 307, and urllib copies
    the Authorization header onto any redirect."""
    handler = fw.IndexHostRedirect(HOST)
    request = urllib.request.Request(f"{INDEX}co-core/", headers={"Authorization": "x"})
    if followed:
        new = handler.redirect_request(request, None, 307, "", {}, newurl)
        assert new is not None and new.full_url == newurl
    else:
        with pytest.raises(urllib.error.HTTPError):
            handler.redirect_request(request, None, 307, "", {}, newurl)


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("The read operation timed out"),
        http.client.RemoteDisconnected("Remote end closed connection without response"),
        http.client.IncompleteRead(b"partial"),
        urllib.error.URLError("Name or service not known"),
    ],
    ids=["timeout", "disconnected", "incomplete-read", "dns"],
)
def test_a_dropped_connection_is_a_fetch_error_not_a_traceback(fw, tmp_path, error):
    def opener(request):
        raise error

    with pytest.raises(fw.FetchError, match=HOST):
        _fetch(fw, tmp_path, opener, versions=["0.1.0"])


def test_an_unusable_destination_is_a_fetch_error_naming_it(fw, tmp_path):
    """A file where the directory should be: the error names it, no traceback."""
    opener, _, _ = _index(CO_CORE, ["0.1.0"])
    (tmp_path / "wheels").write_text("not a directory")
    with pytest.raises(fw.FetchError, match=str(tmp_path / "wheels")):
        _fetch(fw, tmp_path, opener, versions=["0.1.0"])


def test_a_page_that_is_not_utf8_is_a_fetch_error_naming_it(fw, tmp_path):
    def opener(request):
        return io.BytesIO(b"\xff\xfe<a href=x>")

    with pytest.raises(fw.FetchError, match=f"{INDEX}co-core/"):
        _fetch(fw, tmp_path, opener, packages=["co-core"], versions=["0.1.0"])


# --- the index URL -----------------------------------------------------------


def test_the_index_is_the_pyprojects_co_pypi_index(fw, tmp_path):
    assert fw.index_url(_pyproject(tmp_path)) == INDEX


def test_a_pyproject_without_the_index_is_a_lookup_error(fw, tmp_path):
    with pytest.raises(LookupError, match="co-pypi"):
        fw.index_url(_pyproject(tmp_path, index=None))
    with pytest.raises(LookupError):
        fw.index_url(tmp_path / "absent.toml")


def test_the_default_pyproject_is_the_checkouts_own(fw):
    assert fw.PYPROJECT == SCRIPT.resolve().parent.parent / "pyproject.toml"


# --- the command -------------------------------------------------------------

ARGV = ["--dest", "DIR", "--version", "0.1.0", "--version", "0.2.0", "co-core", "co-core-aio"]


class Fetch:
    def __init__(self, result=(), error: Exception | None = None):
        self.calls: list[tuple] = []
        self.result, self.error = list(result), error

    def __call__(self, index, auth, packages, versions, dest):
        self.calls.append((index, auth, packages, versions, dest))
        if self.error:
            raise self.error
        return self.result


def test_main_fetches_with_the_wrappers_pair_and_prints_each_path(fw, tmp_path, capsys):
    paths = [tmp_path / "a.whl", tmp_path / "b.whl"]
    fetch = Fetch(result=paths)
    rc = fw.main(ARGV, {**PAIR, "PATH": "/bin"}, fetch=fetch, pyproject=_pyproject(tmp_path))
    out = capsys.readouterr()
    assert rc == 0
    assert fetch.calls == [
        (INDEX, (USER, TOKEN), ["co-core", "co-core-aio"], ["0.1.0", "0.2.0"], Path("DIR"))
    ]
    assert out.out.splitlines() == [str(p) for p in paths]
    assert TOKEN not in out.out + out.err


@pytest.mark.parametrize("missing", sorted(PAIR))
def test_outside_the_wrapper_it_exits_2_and_names_the_wrapper(fw, tmp_path, capsys, missing):
    environ = {k: v for k, v in PAIR.items() if k != missing}
    fetch = Fetch()
    assert fw.main(ARGV, environ, fetch=fetch, pyproject=_pyproject(tmp_path)) == 2
    assert fetch.calls == []
    err = capsys.readouterr().err
    assert "bash scripts/co-pypi.sh uv run --no-project python" in err
    assert TOKEN not in err


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--dest", "DIR", "--version", "0.1.0"],  # no package
        ["--dest", "DIR", "co-core"],  # no version
        ["--version", "0.1.0", "co-core"],  # no dest
    ],
    ids=["nothing", "no-package", "no-version", "no-dest"],
)
def test_usage_errors_exit_2_before_a_fetch(fw, tmp_path, argv):
    fetch = Fetch()
    with pytest.raises(SystemExit) as exc:
        fw.main(argv, PAIR, fetch=fetch, pyproject=_pyproject(tmp_path))
    assert exc.value.code == 2 and fetch.calls == []


def test_no_co_pypi_index_exits_2(fw, tmp_path, capsys):
    fetch = Fetch()
    assert fw.main(ARGV, PAIR, fetch=fetch, pyproject=_pyproject(tmp_path, index=None)) == 2
    assert fetch.calls == [] and "co-pypi" in capsys.readouterr().err


def test_a_failed_fetch_exits_1_names_the_file_and_prints_no_token(fw, tmp_path, capsys):
    fetch = Fetch(error=fw.FetchError("sha256 mismatch for co_core-0.2.0-py3-none-any.whl"))
    rc = fw.main(ARGV, PAIR, fetch=fetch, pyproject=_pyproject(tmp_path))
    out = capsys.readouterr()
    assert rc == 1
    assert "co_core-0.2.0-py3-none-any.whl" in out.err
    assert TOKEN not in out.out + out.err


def test_end_to_end_through_main_with_a_fake_index(fw, tmp_path, capsys):
    """main's own fetch, the opener aside: what a run under the wrapper does."""
    opener, _, files = _index(CO_CORE, ["0.1.0"])

    def fetch(index, auth, packages, versions, dest):
        return fw.fetch_wheels(index, auth, packages, versions, dest, opener=opener)

    argv = ["--dest", str(tmp_path / "w"), "--version", "0.1.0", "co-core", "co-core-aio"]
    assert fw.main(argv, PAIR, fetch=fetch, pyproject=_pyproject(tmp_path)) == 0
    assert sorted(p.name for p in (tmp_path / "w").iterdir()) == sorted(
        n for n in files if n.endswith(".whl")
    )


def test_the_script_runs_as_a_command_and_refuses_without_the_pair(tmp_path):
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path)}
    script = [sys.executable, "-I", str(SCRIPT)]
    assert subprocess.run([*script, "--help"], env=env, capture_output=True).returncode == 0
    result = subprocess.run([*script, *ARGV], env=env, capture_output=True, text=True)
    assert result.returncode == 2
    assert "co-pypi.sh" in result.stderr


# --- built to travel ---------------------------------------------------------


def test_the_fetcher_is_no_one_repos(fw):
    """No repo's name, no repo variable, no repo path: it drops in anywhere."""
    text = SCRIPT.read_text().lower()
    for word in ("replicator", "co_core_closure_diff", "src/"):
        assert word not in text, word


def test_the_fetcher_imports_the_standard_library_alone():
    """It runs under `uv run --no-project`: nothing to install, nothing resolved."""
    tree = ast.parse(SCRIPT.read_text())
    tops = {
        (alias.name if isinstance(node, ast.Import) else node.module or "").split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        for alias in node.names
    } - {"__future__", ""}
    assert tops <= set(sys.stdlib_module_names), tops - set(sys.stdlib_module_names)
