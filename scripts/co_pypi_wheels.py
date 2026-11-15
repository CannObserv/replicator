# scripts/co_pypi_wheels.py — fetch wheel files from the co-pypi index, hash-checked.
"""Download each package's ``py3-none-any`` wheel at each version from co-pypi.

    bash scripts/co-pypi.sh uv run --no-project python scripts/co_pypi_wheels.py \\
        --dest DIR --version OLD --version NEW PACKAGE [PACKAGE ...]

uv has no download command and leaves no ``.whl`` behind: an install, and its
cache, hold unpacked trees. A tool that reads wheel files (an import-closure
diff across a pin move, say) needs the files themselves, so this fetches them.

**Run under the co-pypi wrapper, and it handles no key.** The wrapper mints the
token from the co-pypi-reader key (``CO_PYPI_KEY``, else
``/etc/<[project].name>/co-pypi-reader.json``) and hands
``UV_INDEX_CO_PYPI_USERNAME`` / ``UV_INDEX_CO_PYPI_PASSWORD`` to the uv it
runs, from an allowlisted environment; ``uv run`` hands them on to this. So
this reads the pair uv would, and holds nothing the wrapper doesn't. Run
without the pair, it exits 2 and names the wrapper.

**The index** is ``pyproject.toml``'s ``[[tool.uv.index]]`` named ``co-pypi``:
the name the pair's variables encode. That ``pyproject.toml`` is the one beside
this script's directory, as the wrapper's minter reads it.

**Output:** ``DIR/<name>-<version>-py3-none-any.whl``, ``<name>`` normalized
as a wheel's (``co-core`` → ``co_core``), the index page looked up by PEP 503's.
Each file is checked against the sha256 the index's page gives it, written to
a temp file and renamed into place, so ``DIR`` never holds a partial or
unverified wheel. A wheel already there is replaced. Each written path is
printed, one per line.

**The token goes to the index host alone.** A link off that host is refused,
and a redirect is followed only to https on it: the index answers a file with
a same-host 307, and urllib copies the Authorization header onto any redirect.
The token is never printed, logged, put in argv or written. Standard library
only, so ``uv run --no-project`` resolves nothing.

Exit codes: ``0`` every wheel fetched and verified · ``1`` a fetch failed (a
missing version or package, a sha256 mismatch, the network), naming the file or
URL; files verified before it stay · ``2`` usage, no co-pypi index in
``pyproject.toml``, or not run under the wrapper.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import os
import re
import sys
import tempfile
import tomllib
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit

INDEX_NAME = "co-pypi"
USERNAME_VAR = "UV_INDEX_CO_PYPI_USERNAME"
PASSWORD_VAR = "UV_INDEX_CO_PYPI_PASSWORD"
PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"
WRAPPED = "bash scripts/co-pypi.sh uv run --no-project python scripts/co_pypi_wheels.py"
TIMEOUT = 60


class FetchError(Exception):
    """A wheel could not be fetched or verified. Never carries the token."""


def index_url(pyproject: Path = PYPROJECT) -> str:
    """The URL of *pyproject*'s ``[[tool.uv.index]]`` named ``co-pypi``."""
    try:
        indexes = tomllib.loads(pyproject.read_text())["tool"]["uv"]["index"]
        [url] = [i["url"] for i in indexes if i.get("name") == INDEX_NAME]
    except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError, ValueError) as exc:
        raise LookupError(f"{pyproject} declares no one [[tool.uv.index]] {INDEX_NAME!r}") from exc
    return url if url.endswith("/") else url + "/"


def page_name(package: str) -> str:
    """PEP 503: the index page's name."""
    return re.sub(r"[-_.]+", "-", package).lower()


def wheel_name(package: str, version: str) -> str:
    """The wheel spec's file name: the distribution normalized with underscores."""
    return f"{re.sub(r'[-_.]+', '_', package).lower()}-{version}-py3-none-any.whl"


class IndexHostRedirect(urllib.request.HTTPRedirectHandler):
    """Follow a redirect only to https on *host*."""

    def __init__(self, host: str):
        super().__init__()
        self.host = host

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlsplit(urljoin(req.full_url, newurl))
        if target.scheme != "https" or target.hostname != self.host:
            raise urllib.error.HTTPError(
                req.full_url, code, f"refused a redirect off {self.host}", headers, fp
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class _Links(HTMLParser):
    """Every ``<a href>`` on a PEP 503 page."""

    def __init__(self):
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.hrefs.extend(v for k, v in attrs if k == "href" and v)


def fetch_wheels(
    index: str,
    auth: tuple[str, str],
    packages: Sequence[str],
    versions: Sequence[str],
    dest: Path,
    *,
    opener: Callable[[urllib.request.Request], object] | None = None,
) -> list[Path]:
    """Each package's wheel at each version into *dest*, checked, in order."""
    host = urlsplit(index).hostname
    if opener is None:
        built = urllib.request.build_opener(IndexHostRedirect(host))

        def opener(request):
            return built.open(request, timeout=TIMEOUT)

    header = "Basic " + base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()

    def get(url: str) -> bytes:
        if urlsplit(url).scheme != "https" or urlsplit(url).hostname != host:
            raise FetchError(f"refused {url}: not on {host}")
        request = urllib.request.Request(url, headers={"Authorization": header})
        try:
            with opener(request) as response:
                return response.read()
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            # A status, a timeout or a dropped connection: the URL and the
            # failure, never the request's headers.
            raise FetchError(f"{url}: {type(exc).__name__}: {exc}") from exc

    dest.mkdir(parents=True, exist_ok=True)
    fetched: list[Path] = []
    for package in packages:
        page = f"{index}{page_name(package)}/"
        parser = _Links()
        parser.feed(get(page).decode())
        links = {}
        for href in parser.hrefs:
            url, _, fragment = urljoin(page, href).partition("#")
            links[url.rsplit("/", 1)[-1]] = (url, fragment.removeprefix("sha256="))
        for version in versions:
            name = wheel_name(package, version)
            if name not in links:
                raise FetchError(f"{name} is not on {page}")
            url, digest = links[name]
            data = get(url)
            if not digest or hashlib.sha256(data).hexdigest() != digest:
                raise FetchError(
                    f"sha256 mismatch for {name}: the index says {digest or 'nothing'}"
                )
            fd, tmp = tempfile.mkstemp(dir=dest, prefix=f".{name}.", suffix=".part")
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                os.replace(tmp, dest / name)
            finally:
                Path(tmp).unlink(missing_ok=True)
            fetched.append(dest / name)
    return fetched


def main(
    argv: Sequence[str],
    environ: Mapping[str, str],
    *,
    fetch: Callable[..., Sequence[Path]] = fetch_wheels,
    pyproject: Path = PYPROJECT,
) -> int:
    parser = argparse.ArgumentParser(
        prog="co_pypi_wheels.py",
        description="Fetch py3-none-any wheels from the co-pypi index, sha256-checked."
        f" Run under the wrapper: {WRAPPED} --dest DIR --version V [--version V ...] PACKAGE ...",
    )
    parser.add_argument("--dest", required=True, type=Path, help="the directory to fill")
    parser.add_argument(
        "--version", action="append", required=True, dest="versions", help="repeatable"
    )
    parser.add_argument("packages", nargs="+", metavar="PACKAGE")
    args = parser.parse_args(argv)

    user, password = environ.get(USERNAME_VAR, ""), environ.get(PASSWORD_VAR, "")
    if not user or not password:
        print(
            f"error: no co-pypi credentials ({USERNAME_VAR}, {PASSWORD_VAR}):"
            f" run under the wrapper, {WRAPPED} …",
            file=sys.stderr,
        )
        return 2
    try:
        index = index_url(pyproject)
    except LookupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        fetched = fetch(index, (user, password), args.packages, args.versions, args.dest)
    except FetchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    for path in fetched:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], os.environ))
