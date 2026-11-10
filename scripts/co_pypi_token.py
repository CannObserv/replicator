"""Run one ``uv`` command, or fetch co-core wheels, with a co-pypi access token (#183).

    . scripts/load_env.sh                      # REPLICATOR_CO_PYPI_CREDENTIALS
    bash scripts/co-pypi.sh uv sync --frozen   # the deploy step
    bash scripts/co-pypi.sh uv lock
    bash scripts/co-pypi.sh wheels OLD NEW DIR # the closure diff's wheels

co-core, co-core-aio and co-core-sync resolve from the co-pypi Artifact Registry
index (``pyproject.toml``'s ``[[tool.uv.index]]``). It takes HTTP basic auth: the
username ``oauth2accesstoken`` and an OAuth2 access token as the password, which
uv reads from ``UV_INDEX_CO_PYPI_USERNAME`` / ``UV_INDEX_CO_PYPI_PASSWORD``. The
host has no gcloud, so this mints the token from the read-only key named by
``REPLICATOR_CO_PYPI_CREDENTIALS`` (``co-pypi-reader@``) and ``exec``s ``uv``.
Ported from CannObserv/watcher#358's wrapper, with two departures.

**The writer never reaches uv.** Here ``GOOGLE_APPLICATION_CREDENTIALS`` is the
worker's *writer* (#114), and the deploy shell also carries org-wide
``GH_TOKEN_*`` PATs. So the key variable is the only source, with no ADC
fallback, and ``uv`` starts from an **allowlisted** environment, not the
caller's: ``scripts/co-pypi.sh`` applies the list to the outer ``uv run`` and
:func:`child_environment` again to the wrapped command. CI never runs this; its
WIF step hands the token straight to ``uv sync``.

**The token is never printed, logged, put in argv or written anywhere.** It
lives in this process and the wrapped ``uv``'s environment (an hour at most).
Anything but ``uv`` or ``wheels`` is refused before a token is minted.
**Accepted limit:** whatever that ``uv`` spawns inherits the pair (the
``uv_build`` backend). ``wheels`` sends it to ``urllib``, for the index host
alone, across same-host redirects only (:class:`IndexHostRedirect`).

Exit codes: the wrapped command's own · ``0`` wheels fetched · ``2`` usage, a
refused command or no key variable · ``1`` the mint or a fetch failed · ``127``
the command could not be run.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import os
import sys
import tempfile
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit

from google.auth.transport.requests import Request
from google.oauth2 import service_account

INDEX_URL = "https://us-west1-python.pkg.dev/co-gcs/co-pypi/simple/"
INDEX_HOST = urlsplit(INDEX_URL).hostname
PACKAGES = ("co-core", "co-core-aio", "co-core-sync")
KEY_VAR = "REPLICATOR_CO_PYPI_CREDENTIALS"
USERNAME_VAR = "UV_INDEX_CO_PYPI_USERNAME"
PASSWORD_VAR = "UV_INDEX_CO_PYPI_PASSWORD"
USERNAME = "oauth2accesstoken"
# Artifact Registry's documented scope for a token-as-password read; the
# identity's role, artifactregistry.reader, is what bounds it.
SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
# What uv may see. scripts/co-pypi.sh carries the same two lists, plus KEY_VAR
# (tests/test_co_pypi_token.py holds them equal). VIRTUAL_ENV is left out: the
# outer `uv run` sets it to its ephemeral env.
ALLOWED = frozenset(
    {
        "PATH", "HOME", "USER", "LOGNAME", "LANG", "TERM", "TMPDIR",
        "http_proxy", "https_proxy", "no_proxy", "all_proxy",
        "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY",
    }
)  # fmt: skip
ALLOWED_PREFIXES = ("LC_", "XDG_", "SSL_CERT_", "UV_")
USAGE = "usage: bash scripts/co-pypi.sh uv <command> [args...] | wheels OLD NEW DIR"


class FetchError(Exception):
    """A wheel could not be fetched or verified. Never carries the token."""


def child_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """*environ* cut to the allowlist."""
    return {k: v for k, v in environ.items() if k in ALLOWED or k.startswith(ALLOWED_PREFIXES)}


def mint_token(key_path: str) -> str:
    """An access token for the service account whose key is at *key_path*.

    The path is explicit, so google-auth's ADC search — which would find
    ``GOOGLE_APPLICATION_CREDENTIALS`` — never runs.
    """
    credentials = service_account.Credentials.from_service_account_file(key_path, scopes=SCOPES)
    credentials.refresh(Request())
    if not credentials.token:
        raise RuntimeError("the token endpoint returned no access token")
    return credentials.token


class IndexHostRedirect(urllib.request.HTTPRedirectHandler):
    """Follow a redirect only to https on the index host.

    The index answers a file with a same-host 307 [measured 2026-11-10], and
    urllib copies the ``Authorization`` header onto any redirect.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlsplit(urljoin(req.full_url, newurl))
        if target.scheme != "https" or target.hostname != INDEX_HOST:
            raise urllib.error.HTTPError(
                req.full_url, code, f"refused a redirect off {INDEX_HOST}", headers, fp
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _open(request: urllib.request.Request):
    return urllib.request.build_opener(IndexHostRedirect).open(request, timeout=60)


class _Links(HTMLParser):
    """Every ``<a href>`` on a PEP 503 page."""

    def __init__(self):
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.hrefs.extend(v for k, v in attrs if k == "href" and v)


def _get(opener, url: str, auth: str) -> bytes:
    if urlsplit(url).scheme != "https" or urlsplit(url).hostname != INDEX_HOST:
        raise FetchError(f"refused {url}: not on {INDEX_HOST}")
    try:
        with opener(urllib.request.Request(url, headers={"Authorization": auth})) as response:
            return response.read()
    except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
        # A status, a timeout or a dropped connection (CR 4): each names the URL
        # and the failure, never the request's headers.
        raise FetchError(f"{url}: {type(exc).__name__}: {exc}") from exc


def fetch_wheels(
    token: str,
    versions: Sequence[str],
    dest: Path,
    *,
    opener: Callable[[urllib.request.Request], object] = _open,
) -> list[Path]:
    """Download each package's ``py3-none-any`` wheel at each version into *dest*.

    Each file is checked against the index's ``#sha256=`` before it is renamed
    into place, so *dest* never holds a partial or unverified wheel.
    """
    auth = "Basic " + base64.b64encode(f"{USERNAME}:{token}".encode()).decode()
    dest.mkdir(parents=True, exist_ok=True)
    fetched: list[Path] = []
    for package in PACKAGES:
        page_url = f"{INDEX_URL}{package}/"
        parser = _Links()
        parser.feed(_get(opener, page_url, auth).decode())
        links = {}
        for href in parser.hrefs:
            url, _, fragment = urljoin(page_url, href).partition("#")
            links[url.rsplit("/", 1)[-1]] = (url, fragment.removeprefix("sha256="))
        for version in versions:
            name = f"{package.replace('-', '_')}-{version}-py3-none-any.whl"
            if name not in links:
                raise FetchError(f"{name} is not on {page_url}")
            url, digest = links[name]
            data = _get(opener, url, auth)
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
    mint: Callable[[str], str] = mint_token,
    execvpe: Callable[[str, list[str], dict[str, str]], object] = os.execvpe,
    fetch: Callable[[str, list[str], Path], object] = fetch_wheels,
) -> int | None:
    """Exec *argv* (a ``uv`` command) with the co-pypi pair, or fetch wheels.

    Returns an exit code; on a ``uv`` success *execvpe* replaces the process.
    """
    verb = Path(argv[0]).name if argv else ""
    if verb not in ("uv", "wheels") or (verb == "wheels" and len(argv) != 4):
        print(f"error: co-pypi wraps uv, or fetches wheels. {USAGE}", file=sys.stderr)
        return 2

    key_path = environ.get(KEY_VAR, "").strip()
    if not key_path:
        print(
            f"error: {KEY_VAR} is not set: it names the co-pypi-reader key"
            " (never GOOGLE_APPLICATION_CREDENTIALS, the writer). Load it: . scripts/load_env.sh",
            file=sys.stderr,
        )
        return 2

    try:
        token = mint(key_path)
    except Exception as exc:  # broad by design: any failure here runs nothing
        # The exception is the token endpoint's or the key file's, never the token.
        print(
            f"error: could not mint a co-pypi access token from {KEY_VAR}={key_path}:"
            f" {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 1

    if verb == "wheels":
        try:
            fetched = fetch(token, list(argv[1:3]), Path(argv[3]))
        except FetchError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        for path in fetched or []:
            print(path)
        return 0

    env = child_environment(environ)
    env[USERNAME_VAR] = USERNAME
    env[PASSWORD_VAR] = token
    try:
        execvpe(argv[0], list(argv), env)
    except OSError as exc:
        # Names the command and errno, never `env`.
        print(f"error: could not run {argv[0]}: {exc.strerror or exc}", file=sys.stderr)
        return 127
    return None


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], os.environ))
