"""The renderer: one confined headless Chromium per render (#138; #63 reuses it).

:class:`Renderer` is the seam. :class:`PlaywrightRenderer` is the one implementation: it
launches the pinned ``chromium-headless-shell`` per render, behind three confinements, and
returns a PNG or a :class:`RenderError` the caller classifies. The vocabulary on the wire
is the caller's (``src/worker/capture.py`` for ``content.capture``); this module only says
*what happened to the page*.

The confinements, each from the design doc (``docs/plans/2026-10-25-viewport-capture-design.md``):

- **Egress** — every TCP connection goes through a per-render
  :class:`~src.worker.browser_egress.SocksGuard`, and UDP is shut off with
  :data:`LAUNCH_ARGS`. The first refused connection abandons the render
  (:class:`DestinationRefusedError`), whatever the page was doing.
- **Memory** — ``scripts/browser_exec.sh`` is the executable Playwright launches. It
  joins the delegated ``browser/`` cgroup (:mod:`src.worker.browser_cgroup`) and raises
  its own ``oom_score_adj`` to 500, then becomes the shell.
- **Privilege** — the renderer sandbox is **on** (Playwright's default is
  ``--no-sandbox``), and the browser's environment is a scrubbed handful of variables, so
  nothing from ``/etc/replicator/.env`` reaches Chromium.

**A launch per render, and a driver per render.** Launching the shell costs well under a
second on this host, and a fresh profile per render is what keeps one page's cookies,
cache and service workers out of the next. Starting Playwright's Node driver per render
too means there is no long-lived driver to restart after it dies: the next render
starts a new one. At one render at a time, the half-second it costs is not the bottleneck.

**Playwright's Node driver is not confined (CR 11).** ``async_playwright()`` spawns it
from the worker, so it sits in ``worker/`` at the worker's -900, outside the browser's
memory cap, with the worker's whole environment, credentials included. No page can reach
it: Chromium talks to it over a pipe, and Chromium's own environment is the scrubbed
handful above. Its memory is the PNG it relays, base64, which the post-screenshot size
checks bound.

**Teardown kills before it asks when the render failed.** A render that raised or was
cancelled (the caller's deadline) writes ``cgroup.kill`` first, which does not wait, and
only then closes politely under :data:`CLOSE_TIMEOUT_SECONDS`. That bounds what a
deadline costs the stop budget.
"""

import asyncio
import contextlib
import json
import re
import struct
import tempfile
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

import playwright
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from src.core.errors import PermanentFetchError
from src.core.logging import get_logger
from src.worker.browser_cgroup import BrowserCgroup
from src.worker.browser_egress import SocksGuard
from src.worker.egress import DestinationGuard

logger = get_logger(__name__)

BROWSER_EXEC = Path(__file__).resolve().parents[2] / "scripts" / "browser_exec.sh"

# UDP is the one path the SOCKS guard cannot see. QUIC would carry HTTP/3 straight out;
# without the WebRTC policy the spike's STUN probe reached a loopback canary four times.
# Background networking is Chromium's own update and metrics traffic: it would go through
# the guard, but it is nothing a capture needs. Playwright adds ``<-loopback>`` to the
# proxy bypass list itself, so loopback goes through the guard too.
LAUNCH_ARGS = (
    "--disable-quic",
    "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
    "--disable-background-networking",
)

# How long a polite ``browser.close()`` may take before the cgroup is killed regardless.
CLOSE_TIMEOUT_SECONDS = 5.0
# How long the shell may take to start. Measured at 0.05-0.16 s; this is a hang, not a slow start.
LAUNCH_TIMEOUT_SECONDS = 30.0

# How a navigation's ``net::ERR_*`` reads (#138 CR 15). Network failures are worth another
# attempt, as a fetch's connect, TLS or DNS failure is; behind the guard an unresolvable
# name arrives as ERR_SOCKS_CONNECTION_FAILED. A URL the browser refuses outright
# (ERR_UNSAFE_PORT, measured on :6000) never changes. Anything else (certificates,
# redirect loops, codes not listed) is left to the delivery ceiling: neither retried
# forever on a whole render each time, nor guessed terminal.
_NET_ERROR = re.compile(r"net::(ERR_[A-Z0-9_]+)")
_NETWORK_FAILURES = frozenset(
    {
        "ERR_ADDRESS_UNREACHABLE",
        "ERR_CONNECTION_ABORTED",
        "ERR_CONNECTION_CLOSED",
        "ERR_CONNECTION_FAILED",
        "ERR_CONNECTION_REFUSED",
        "ERR_CONNECTION_RESET",
        "ERR_CONNECTION_TIMED_OUT",
        "ERR_EMPTY_RESPONSE",
        "ERR_INTERNET_DISCONNECTED",
        "ERR_NAME_NOT_RESOLVED",
        "ERR_NAME_RESOLUTION_FAILED",
        "ERR_NETWORK_CHANGED",
        "ERR_PROXY_CONNECTION_FAILED",
        "ERR_SOCKS_CONNECTION_FAILED",
        "ERR_SOCKS_CONNECTION_HOST_UNREACHABLE",
        "ERR_SSL_PROTOCOL_ERROR",
        "ERR_TIMED_OUT",
        "ERR_TUNNEL_CONNECTION_FAILED",
    }
)
_IMPOSSIBLE_NAVIGATIONS = frozenset(
    {
        "ERR_DISALLOWED_URL_SCHEME",
        "ERR_INVALID_REDIRECT",
        "ERR_INVALID_URL",
        "ERR_UNKNOWN_URL_SCHEME",
        "ERR_UNSAFE_PORT",
        # A redirect to file:, data: or chrome: (measured, CR 23).
        "ERR_UNSAFE_REDIRECT",
    }
)

# The document's full height, for refusing an over-tall full_page shot before taking it.
_SCROLL_HEIGHT_JS = (
    "Math.max(document.documentElement ? document.documentElement.scrollHeight : 0,"
    " document.body ? document.body.scrollHeight : 0)"
)
# And its width: a full page is as wide as its overflow (CR 18).
_SCROLL_WIDTH_JS = _SCROLL_HEIGHT_JS.replace("Height", "Width")
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


# --------------------------------------------------------------------------- errors


class RenderError(Exception):
    """What happened to the page, for the caller to put in its own vocabulary."""


class RuntimeUnavailableError(RenderError):
    """The pinned shell is not installed where the runtime says it is."""


class DestinationRefusedError(RenderError):
    """The page, or the navigation, asked for a destination the guard refuses."""

    def __init__(self, refusal: PermanentFetchError) -> None:
        super().__init__(str(refusal))
        self.refusal = refusal


class NavigationFailedError(RenderError):
    """The navigation got no response: DNS, connect, TLS, or nothing within the bound."""


class NavigationImpossibleError(RenderError):
    """The browser will never navigate to this URL: a blocked port, a scheme it won't open."""


class NavigationStatusError(RenderError):
    """The navigation's final response was not a 2xx."""

    def __init__(self, status: int, *, retry_after: str | None = None) -> None:
        super().__init__(f"the navigation answered {status}")
        self.status = status
        self.retry_after = retry_after


class WaitTimeoutError(RenderError):
    """``load``, or the wait condition, was not reached within the bound."""

    def __init__(self, stage: str, seconds: float) -> None:
        super().__init__(f"{stage} not reached within {seconds:g}s")
        self.stage = stage


class InvalidSelectorError(RenderError):
    """The ``selector`` wait named something the browser cannot parse as CSS."""


class PageTooTallError(RenderError):
    """A ``full_page`` shot would be taller than the caller allows."""

    def __init__(self, height: int, limit: int) -> None:
        super().__init__(f"the page is {height}px tall, over the {limit}px limit")
        self.height = height
        self.limit = limit


class PageTooWideError(RenderError):
    """The screenshot came out wider than the caller allows: a full page's overflow (CR 12)."""

    def __init__(self, width: int, limit: int) -> None:
        super().__init__(f"the page is {width}px wide, over the {limit}px limit")
        self.width = width
        self.limit = limit


class BrowserKilledError(RenderError):
    """The kernel group-killed the browser at its cgroup's memory cap.

    Deliberately unclassified by the caller (it rides the delivery ceiling): a page that
    kills the browser once usually kills it again.
    """


# --------------------------------------------------------------------------- request


@dataclass(frozen=True)
class Wait:
    """The condition after ``load``. Validated by the caller; the renderer trusts it."""

    kind: Literal["networkidle", "selector", "delay"]
    selector: str | None = None
    seconds: float | None = None


@dataclass(frozen=True)
class RenderRequest:
    url: str
    width: int
    height: int
    full_page: bool
    wait: Wait | None
    max_wait_seconds: float  # bounds ``commit``, ``load`` and the condition, each
    max_height: int  # a taller ``full_page`` is refused, never clipped
    max_width: int  # so is a wider one: a full page is as wide as its overflow


@dataclass(frozen=True)
class Rendered:
    png: bytes
    image_width: int  # from the PNG's IHDR, not the viewport
    image_height: int
    final_url: str  # the main frame's URL when the screenshot was taken
    status: int  # the navigation's final response
    captured_at: datetime


class Renderer(Protocol):
    async def render(self, request: RenderRequest) -> Rendered: ...


# --------------------------------------------------------------------------- runtime


def pinned_shell(browsers_path: Path) -> Path:
    """The ``chromium-headless-shell`` this Playwright release pins, under ``browsers_path``.

    Read from Playwright's own ``browsers.json`` rather than globbed, so a stale revision
    left by an earlier install is never the one launched.
    """
    manifest = Path(playwright.__file__).parent / "driver" / "package" / "browsers.json"
    browsers = json.loads(manifest.read_text())["browsers"]
    revision = next(b["revision"] for b in browsers if b["name"] == "chromium-headless-shell")
    return (
        browsers_path
        / f"chromium_headless_shell-{revision}"
        / "chrome-headless-shell-linux64"
        / "chrome-headless-shell"
    )


@dataclass(frozen=True)
class BrowserRuntime:
    """Where the shell is, and the cgroup it runs in (``None``: the dev escape hatch)."""

    browsers_path: Path
    cgroup: BrowserCgroup | None

    def executable(self) -> Path:
        shell = pinned_shell(self.browsers_path)
        if not shell.is_file():
            raise RuntimeUnavailableError(
                f"{shell} is missing: install the pinned shell "
                "(docs/plans/2026-10-25-viewport-capture-runbook.md)"
            )
        return shell


# --------------------------------------------------------------------------- renderer


class PlaywrightRenderer:
    """One confined launch per :meth:`render`."""

    def __init__(
        self,
        runtime: BrowserRuntime,
        guard: DestinationGuard,
        *,
        playwright_factory: Callable[[], Any] = async_playwright,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._runtime = runtime
        self._guard = guard
        self._playwright = playwright_factory
        self._clock = clock

    async def render(self, request: RenderRequest) -> Rendered:
        shell = self._runtime.executable()
        cgroup = self._runtime.cgroup
        kills_before = cgroup.oom_group_kills() if cgroup else 0
        with tempfile.TemporaryDirectory(prefix="replicator-render-") as home:
            async with SocksGuard(self._guard) as socks, self._playwright() as pw:
                browser = None
                failed = True
                try:
                    browser = await pw.chromium.launch(**self.launch_kwargs(shell, socks, home))
                    rendered = await _first_of(socks, self._drive(browser, request))
                    failed = False
                    return rendered
                except RenderError:
                    raise
                except PlaywrightError as exc:
                    if cgroup and cgroup.oom_group_kills() > kills_before:
                        raise BrowserKilledError(
                            f"the browser was group-killed at its memory cap ({exc})"
                        ) from exc
                    raise
                finally:
                    await self._teardown(browser, failed=failed)

    def launch_kwargs(self, shell: Path, socks: SocksGuard, home: str) -> dict[str, Any]:
        """Everything handed to ``chromium.launch``; a test pins each confinement here."""
        env = {
            "HOME": home,
            "TMPDIR": home,
            "PATH": "/usr/bin:/bin",
            "REPLICATOR_BROWSER_EXECUTABLE": str(shell),
        }
        if self._runtime.cgroup is not None:
            env["REPLICATOR_BROWSER_CGROUP_PROCS"] = str(self._runtime.cgroup.procs_file)
        return {
            "executable_path": str(BROWSER_EXEC),
            "headless": True,
            "chromium_sandbox": True,
            "args": list(LAUNCH_ARGS),
            "env": env,
            "proxy": {"server": socks.proxy_server},
            "handle_sigint": False,
            "handle_sigterm": False,
            "handle_sighup": False,
            "timeout": LAUNCH_TIMEOUT_SECONDS * 1000,
        }

    async def _drive(self, browser: Any, request: RenderRequest) -> Rendered:
        bound = request.max_wait_seconds
        context = await browser.new_context(
            viewport={"width": request.width, "height": request.height},
            device_scale_factor=1,
            service_workers="block",
            accept_downloads=False,
        )
        page = await context.new_page()
        status = await _navigate(page, request.url, bound)
        await _wait(
            page, "load", bound, lambda: page.wait_for_load_state("load", timeout=ms(bound))
        )
        await _condition(page, request.wait, bound)
        if request.full_page:
            height = int(await page.evaluate(_SCROLL_HEIGHT_JS))
            if height > request.max_height:
                raise PageTooTallError(height, request.max_height)
            width = int(await page.evaluate(_SCROLL_WIDTH_JS))
            if width > request.max_width:
                raise PageTooWideError(width, request.max_width)
        # No screenshot timeout: the caller's whole-render deadline bounds it. A long page
        # took 31 s here, past Playwright's 30 s default.
        png = await page.screenshot(type="png", full_page=request.full_page, timeout=0)
        captured_at = self._clock()
        width, height = png_dimensions(png)
        # The measurement above is the cheap exit; this is the check. A page that grew
        # after it was measured comes out taller than it measured (CR 2).
        if height > request.max_height:
            raise PageTooTallError(height, request.max_height)
        if width > request.max_width:
            raise PageTooWideError(width, request.max_width)
        return Rendered(
            png=png,
            image_width=width,
            image_height=height,
            final_url=page.url,
            status=status,
            captured_at=captured_at,
        )

    async def _teardown(self, browser: Any, *, failed: bool) -> None:
        cgroup = self._runtime.cgroup
        if failed and cgroup is not None:
            _kill(cgroup)
        try:
            if browser is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(browser.close(), CLOSE_TIMEOUT_SECONDS)
        finally:
            # In a finally: the caller's deadline cancels the close, and a cancellation
            # is not an Exception the suppress above swallows (CR 6).
            if cgroup is not None and cgroup.populated():
                _kill(cgroup)


def _kill(cgroup: BrowserCgroup) -> None:
    try:
        cgroup.kill()
    except OSError:
        logger.exception("could not kill the browser cgroup")


def ms(seconds: float) -> float:
    return seconds * 1000


async def _first_of(socks: SocksGuard, drive: Coroutine[Any, Any, Rendered]) -> Rendered:
    """The render, unless the guard refuses a connection first; a refusal always wins."""
    driving = asyncio.ensure_future(drive)
    refused = asyncio.ensure_future(socks.refused.wait())
    try:
        await asyncio.wait({driving, refused}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (driving, refused):
            task.cancel()
        await asyncio.gather(driving, refused, return_exceptions=True)
    if socks.refusal is not None:
        raise DestinationRefusedError(socks.refusal)
    return driving.result()


async def _navigate(page: Any, url: str, bound: float) -> int:
    try:
        response = await page.goto(url, wait_until="commit", timeout=ms(bound))
    except PlaywrightTimeoutError as exc:
        raise NavigationFailedError(f"no response within {bound:g}s") from exc
    except PlaywrightError as exc:
        code = _NET_ERROR.search(str(exc))
        if code is not None and code.group(1) in _NETWORK_FAILURES:
            raise NavigationFailedError(_first_line(exc)) from exc
        if code is not None and code.group(1) in _IMPOSSIBLE_NAVIGATIONS:
            raise NavigationImpossibleError(_first_line(exc)) from exc
        raise
    if response is None:
        raise NavigationFailedError("the navigation produced no response")
    status = response.status
    if not 200 <= status < 300:
        raise NavigationStatusError(status, retry_after=await response.header_value("retry-after"))
    return status


async def _wait(page: Any, stage: str, bound: float, waiting: Callable[[], Any]) -> None:
    try:
        await waiting()
    except PlaywrightTimeoutError as exc:
        raise WaitTimeoutError(stage, bound) from exc


async def _condition(page: Any, wait: Wait | None, bound: float) -> None:
    if wait is None:
        return
    if wait.kind == "networkidle":
        await _wait(
            page, "networkidle", bound,
            lambda: page.wait_for_load_state("networkidle", timeout=ms(bound)),
        )  # fmt: skip
    elif wait.kind == "selector":
        try:
            await _wait(
                page, "selector", bound,
                lambda: page.wait_for_selector(wait.selector, state="visible", timeout=ms(bound)),
            )  # fmt: skip
        except PlaywrightError as exc:
            if _is_selector_syntax(exc):
                raise InvalidSelectorError(_first_line(exc)) from exc
            raise
    elif wait.kind == "delay":
        assert wait.seconds is not None
        await asyncio.sleep(wait.seconds)
    else:  # pragma: no cover - the caller validates kind
        raise ValueError(f"unknown wait kind {wait.kind!r}")


def _is_selector_syntax(exc: PlaywrightError) -> bool:
    message = str(exc)
    return "while parsing css selector" in message or "is not a valid selector" in message


def _first_line(exc: BaseException) -> str:
    return str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__


def png_dimensions(png: bytes) -> tuple[int, int]:
    """Width and height from the IHDR chunk, which the PNG spec puts first."""
    if len(png) < 24 or not png.startswith(_PNG_SIGNATURE) or png[12:16] != b"IHDR":
        raise ValueError("the screenshot is not a PNG")
    width, height = struct.unpack(">II", png[16:24])
    return width, height
