"""The renderer, against a fake Playwright: what it launches, waits for, and tears down (#138).

No browser here — ``tests/worker/test_browser_real.py`` (``-m browser``) runs the pinned
shell. These pin the decisions a real browser would make slow or flaky to test:

- **every confinement is in the launch**: the sandbox on, the UDP flags, the wrapper as
  the executable, the proxy, and an environment carrying nothing of the worker's;
- **a refusal wins**, even over a render that was about to succeed or had already failed;
- **a failed render is killed before it is asked to close**, so a deadline costs nothing
  waiting on a browser that will not answer;
- **each stage's timeout is its own error**, because the caller makes ``wait_timeout``
  terminal and a navigation that never answered transient.
"""

import asyncio
import socket
import struct
import zlib
from datetime import UTC, datetime
from pathlib import Path

import pytest
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from src.core.errors import FailureReason
from src.worker.browser import (
    BROWSER_EXEC,
    LAUNCH_ARGS,
    BrowserKilledError,
    BrowserRuntime,
    DestinationRefusedError,
    DocumentTooLargeError,
    InvalidSelectorError,
    LoadRequest,
    NavigationFailedError,
    NavigationImpossibleError,
    NavigationStatusError,
    NotADocumentError,
    PageTooTallError,
    PageTooWideError,
    PlaywrightRenderer,
    RenderRequest,
    RuntimeUnavailableError,
    Wait,
    WaitTimeoutError,
    pinned_shell,
    png_dimensions,
)
from src.worker.egress import DestinationGuard, blocked_networks

NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)


def _png(width: int, height: int) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
        )

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr) + chunk(b"IEND", b"")


# --------------------------------------------------------------------------- fakes


class FakeResponse:
    def __init__(self, status: int, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self._headers = headers or {}

    async def header_value(self, name: str) -> str | None:
        return self._headers.get(name)


class FakePage:
    """Scripted: each hook is replaced per test; the defaults are a clean 1366x2000 page."""

    def __init__(self, log: list[str]) -> None:
        self.log = log
        self.url = "https://example.org/landed"
        self.scroll_height = 2000
        self.scroll_width = 1366
        # What the screenshot comes out as, when the page changed after it was measured.
        self.shot_size: tuple[int, int] | None = None
        self.goto_hook = None
        self.load_states: dict[str, Exception | None] = {}
        self.selector_error: Exception | None = None
        self.screenshot_error: Exception | None = None
        self.status = 200
        self.headers: dict[str, str] = {}
        self.screenshot_kwargs: dict | None = None
        self.proxy_port: int | None = None
        # The document a load serializes (#63), and what the browser read it as.
        self.html = "<!DOCTYPE html><html><body>rendered é</body></html>"
        self.content_type = "text/html"
        self.document_bytes: int | None = None  # the in-page measure; None = the real size

    async def goto(self, url, *, wait_until, timeout):  # noqa: ASYNC109 - Playwright's signature
        self.log.append(f"goto {url} {wait_until} {timeout:g}")
        if self.goto_hook is not None:
            return await self.goto_hook(self)
        return FakeResponse(self.status, self.headers)

    async def wait_for_load_state(self, state, *, timeout):  # noqa: ASYNC109 - Playwright's
        self.log.append(f"load_state {state} {timeout:g}")
        if error := self.load_states.get(state):
            raise error

    async def wait_for_selector(self, selector, *, state, timeout):  # noqa: ASYNC109 - Playwright's
        self.log.append(f"selector {selector} {state}")
        if self.selector_error:
            raise self.selector_error

    async def evaluate(self, script):
        self.log.append("evaluate")
        if "contentType" in script:
            return self.content_type
        if "TextEncoder" in script:
            size = self.document_bytes
            return len(self.html.encode()) if size is None else size
        return self.scroll_width if "Width" in script else self.scroll_height

    async def content(self):
        self.log.append("content")
        return self.html

    async def screenshot(self, **kwargs):
        self.log.append("screenshot")
        self.screenshot_kwargs = kwargs
        if self.screenshot_error:
            raise self.screenshot_error
        if self.shot_size is not None:
            return _png(*self.shot_size)
        return _png(1366, self.scroll_height if kwargs["full_page"] else 768)


class FakeBrowser:
    def __init__(self, page: FakePage) -> None:
        self.page = page
        self.context_kwargs: dict | None = None
        self.close_hangs = False

    async def new_context(self, **kwargs):
        self.context_kwargs = kwargs
        page = self.page

        class Context:
            async def new_page(self):
                return page

        return Context()

    async def close(self):
        self.page.log.append("close")
        if self.close_hangs:
            await asyncio.sleep(3600)


class FakePlaywright:
    def __init__(self, browser: FakeBrowser) -> None:
        self.browser = browser
        self.launch_kwargs: dict | None = None
        self.chromium = self

    async def launch(self, **kwargs):
        self.launch_kwargs = kwargs
        self.browser.page.proxy_port = int(kwargs["proxy"]["server"].rsplit(":", 1)[1])
        return self.browser

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None


class FakeCgroup:
    def __init__(self, log: list[str], tmp_path: Path) -> None:
        self.log = log
        self.procs_file = tmp_path / "browser" / "cgroup.procs"
        self.kills = 0
        self.alive = False

    def populated(self) -> bool:
        return self.alive

    def kill(self) -> None:
        self.log.append("cgroup.kill")
        self.alive = False

    def oom_group_kills(self) -> int:
        return self.kills


@pytest.fixture
def shell(tmp_path):
    path = pinned_shell(tmp_path / "ms-playwright")
    path.parent.mkdir(parents=True)
    path.write_text("#!/bin/false\n")
    return path


@pytest.fixture
def rig(tmp_path, shell):
    log: list[str] = []
    page = FakePage(log)
    pw = FakePlaywright(FakeBrowser(page))
    cgroup = FakeCgroup(log, tmp_path)
    guard = DestinationGuard(
        blocked=blocked_networks(None),
        resolve=_resolver({"intranet.test": ["10.0.0.7"]}),
    )
    renderer = PlaywrightRenderer(
        BrowserRuntime(tmp_path / "ms-playwright", cgroup),  # type: ignore[arg-type]
        guard,
        playwright_factory=lambda: pw,
        clock=lambda: NOW,
    )

    class Rig:
        pass

    r = Rig()
    r.log, r.page, r.pw, r.cgroup, r.renderer = log, page, pw, cgroup, renderer
    return r


def _resolver(mapping):
    async def resolve(host, port):
        if host not in mapping:
            raise socket.gaierror(socket.EAI_NONAME, "unknown")
        return mapping[host]

    return resolve


def _request(**overrides) -> RenderRequest:
    values = dict(
        url="https://example.org/",
        width=1366,
        height=768,
        full_page=False,
        wait=None,
        max_wait_seconds=30,
        max_height=32768,
        max_width=4096,
    )
    values.update(overrides)
    return RenderRequest(**values)


async def _socks_connect_intranet(page: FakePage) -> None:
    """What a page's subresource does: a SOCKS CONNECT to a private name, through the guard."""
    reader, writer = await asyncio.open_connection("127.0.0.1", page.proxy_port)
    writer.write(b"\x05\x01\x00")
    await reader.readexactly(2)
    host = b"intranet.test"
    writer.write(bytes([5, 1, 0, 3, len(host)]) + host + struct.pack("!H", 443))
    await reader.readexactly(10)
    writer.close()


# --------------------------------------------------------------------------- launch


async def test_every_confinement_is_in_the_launch(rig, shell, monkeypatch):
    monkeypatch.setenv("REPLICATOR_REDIS_URL", "rediss://secret@broker")

    await rig.renderer.render(_request())

    kwargs = rig.pw.launch_kwargs
    assert kwargs["chromium_sandbox"] is True
    assert kwargs["headless"] is True
    assert kwargs["executable_path"] == str(BROWSER_EXEC)
    assert set(LAUNCH_ARGS) <= set(kwargs["args"])
    assert "--disable-quic" in kwargs["args"]
    assert "--force-webrtc-ip-handling-policy=disable_non_proxied_udp" in kwargs["args"]
    assert kwargs["proxy"]["server"].startswith("socks5://127.0.0.1:")
    assert kwargs["handle_sigterm"] is False
    env = kwargs["env"]
    assert set(env) == {
        "HOME",
        "TMPDIR",
        "PATH",
        "REPLICATOR_BROWSER_EXECUTABLE",
        "REPLICATOR_BROWSER_CGROUP_PROCS",
    }
    assert env["REPLICATOR_BROWSER_EXECUTABLE"] == str(shell)
    assert env["REPLICATOR_BROWSER_CGROUP_PROCS"] == str(rig.cgroup.procs_file)
    assert "secret" not in repr(env)


async def test_the_context_is_fresh_and_at_the_requested_viewport(rig):
    await rig.renderer.render(_request(width=800, height=600))

    kwargs = rig.pw.browser.context_kwargs
    assert kwargs["viewport"] == {"width": 800, "height": 600}
    assert kwargs["device_scale_factor"] == 1
    assert kwargs["service_workers"] == "block"
    assert kwargs["accept_downloads"] is False


async def test_without_a_cgroup_the_wrapper_is_told_none(tmp_path, shell):
    log: list[str] = []
    pw = FakePlaywright(FakeBrowser(FakePage(log)))
    renderer = PlaywrightRenderer(
        BrowserRuntime(tmp_path / "ms-playwright", None),
        DestinationGuard(blocked=blocked_networks(None)),
        playwright_factory=lambda: pw,
    )

    await renderer.render(_request())

    assert "REPLICATOR_BROWSER_CGROUP_PROCS" not in pw.launch_kwargs["env"]


async def test_a_missing_runtime_fails_before_anything_launches(tmp_path):
    pw = FakePlaywright(FakeBrowser(FakePage([])))
    renderer = PlaywrightRenderer(
        BrowserRuntime(tmp_path / "nowhere", None),
        DestinationGuard(blocked=blocked_networks(None)),
        playwright_factory=lambda: pw,
    )

    with pytest.raises(RuntimeUnavailableError, match="runbook"):
        await renderer.render(_request())
    assert pw.launch_kwargs is None


def test_the_pinned_shell_is_this_releases_revision(tmp_path):
    import json

    import playwright

    manifest = Path(playwright.__file__).parent / "driver" / "package" / "browsers.json"
    revision = next(
        b["revision"]
        for b in json.loads(manifest.read_text())["browsers"]
        if b["name"] == "chromium-headless-shell"
    )

    assert f"chromium_headless_shell-{revision}" in str(pinned_shell(tmp_path))


# --------------------------------------------------------------------------- the render


async def test_a_clean_render_reports_the_image_not_the_viewport(rig):
    rendered = await rig.renderer.render(_request(full_page=True))

    assert (rendered.image_width, rendered.image_height) == (1366, 2000)
    assert rendered.final_url == "https://example.org/landed"
    assert rendered.status == 200
    assert rendered.captured_at == NOW
    assert rig.page.screenshot_kwargs == {"type": "png", "full_page": True, "timeout": 0}
    assert rig.log[0] == "goto https://example.org/ commit 30000"
    assert rig.log[1] == "load_state load 30000"
    assert rig.log[-1] == "close"


async def test_a_clean_render_closes_politely_and_kills_nothing_left(rig):
    await rig.renderer.render(_request())

    assert "cgroup.kill" not in rig.log


async def test_a_straggler_after_close_is_killed(rig):
    rig.cgroup.alive = True

    await rig.renderer.render(_request())

    assert rig.log[-2:] == ["close", "cgroup.kill"]


async def test_a_deadline_during_the_polite_close_still_kills_the_cgroup(rig):
    """CR 6: the caller's deadline cancels the close; the kill must not be skipped with it."""
    rig.pw.browser.close_hangs = True
    rig.cgroup.alive = True

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await rig.renderer.render(_request())

    assert rig.log[-2:] == ["close", "cgroup.kill"]


@pytest.mark.parametrize("status", [404, 503, 301])
async def test_a_non_2xx_navigation_is_reported_with_its_status(rig, status):
    rig.page.status = status
    rig.page.headers = {"retry-after": "120"}

    with pytest.raises(NavigationStatusError) as failed:
        await rig.renderer.render(_request())

    assert failed.value.status == status
    assert failed.value.retry_after == "120"
    assert "screenshot" not in rig.log


async def test_no_response_within_the_bound_is_a_failed_navigation(rig):
    async def hang(page):
        raise PlaywrightTimeoutError("Timeout 30000ms exceeded.")

    rig.page.goto_hook = hang

    with pytest.raises(NavigationFailedError, match="30s"):
        await rig.renderer.render(_request())


async def test_a_network_error_is_a_failed_navigation(rig):
    async def refused(page):
        raise PlaywrightError("net::ERR_CONNECTION_REFUSED at https://example.org/")

    rig.page.goto_hook = refused

    with pytest.raises(NavigationFailedError, match="ERR_CONNECTION_REFUSED"):
        await rig.renderer.render(_request())


@pytest.mark.parametrize(
    "code",
    ["ERR_SOCKS_CONNECTION_FAILED", "ERR_NAME_NOT_RESOLVED", "ERR_CONNECTION_RESET",
     "ERR_TIMED_OUT", "ERR_SSL_PROTOCOL_ERROR", "ERR_EMPTY_RESPONSE"],
)  # fmt: skip
async def test_a_network_failure_is_a_failed_navigation(rig, code):
    """CR 15: worth retrying, as a fetch's connect, TLS or DNS failure is."""

    async def failed(page):
        raise PlaywrightError(f"Page.goto: net::{code} at https://example.org/")

    rig.page.goto_hook = failed

    with pytest.raises(NavigationFailedError, match=code):
        await rig.renderer.render(_request())


@pytest.mark.parametrize(
    "code",
    ["ERR_UNSAFE_PORT", "ERR_UNKNOWN_URL_SCHEME", "ERR_INVALID_REDIRECT",
     "ERR_UNSAFE_REDIRECT", "ERR_DISALLOWED_URL_SCHEME", "ERR_INVALID_URL"],
)  # fmt: skip
async def test_a_url_the_browser_will_never_open_is_impossible(rig, code):
    """CR 15: deterministic, so never retried; measured ERR_UNSAFE_PORT on :6000."""

    async def failed(page):
        raise PlaywrightError(f"Page.goto: net::{code} at http://example.org:6000/")

    rig.page.goto_hook = failed

    with pytest.raises(NavigationImpossibleError, match=code):
        await rig.renderer.render(_request())


@pytest.mark.parametrize("code", ["ERR_CERT_DATE_INVALID", "ERR_TOO_MANY_REDIRECTS"])
async def test_any_other_net_error_is_left_unclassified(rig, code):
    """CR 15: neither retried forever nor guessed terminal; the delivery ceiling decides."""

    async def failed(page):
        raise PlaywrightError(f"Page.goto: net::{code} at https://example.org/")

    rig.page.goto_hook = failed

    with pytest.raises(PlaywrightError, match=code) as raised:
        await rig.renderer.render(_request())
    assert not isinstance(raised.value, NavigationFailedError | NavigationImpossibleError)


async def test_an_unrecognised_playwright_error_is_left_unclassified(rig):
    async def odd(page):
        raise PlaywrightError("Protocol error: something new")

    rig.page.goto_hook = odd

    with pytest.raises(PlaywrightError, match="something new"):
        await rig.renderer.render(_request())
    assert rig.log[-2:] == ["cgroup.kill", "close"]


async def test_load_unmet_is_a_wait_timeout(rig):
    rig.page.load_states["load"] = PlaywrightTimeoutError("Timeout")

    with pytest.raises(WaitTimeoutError) as failed:
        await rig.renderer.render(_request())

    assert failed.value.stage == "load"


async def test_networkidle_unmet_is_a_wait_timeout(rig):
    rig.page.load_states["networkidle"] = PlaywrightTimeoutError("Timeout")

    with pytest.raises(WaitTimeoutError) as failed:
        await rig.renderer.render(_request(wait=Wait("networkidle")))

    assert failed.value.stage == "networkidle"


async def test_a_selector_wait_asks_for_visible(rig):
    await rig.renderer.render(_request(wait=Wait("selector", selector="#ready")))

    assert "selector #ready visible" in rig.log


async def test_a_selector_never_visible_is_a_wait_timeout(rig):
    rig.page.selector_error = PlaywrightTimeoutError("Timeout")

    with pytest.raises(WaitTimeoutError) as failed:
        await rig.renderer.render(_request(wait=Wait("selector", selector="#never")))

    assert failed.value.stage == "selector"


async def test_an_unparseable_selector_is_its_own_error(rig):
    rig.page.selector_error = PlaywrightError(
        'Unexpected token "<" while parsing css selector "<<". Did you mean to CSS.escape it?'
    )

    with pytest.raises(InvalidSelectorError):
        await rig.renderer.render(_request(wait=Wait("selector", selector="<<")))


async def test_a_delay_waits_after_load(rig, monkeypatch):
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def sleep(seconds, *args):
        slept.append(seconds)
        await real_sleep(0)

    monkeypatch.setattr("src.worker.browser.asyncio.sleep", sleep)

    await rig.renderer.render(_request(wait=Wait("delay", seconds=2.5)))

    assert 2.5 in slept


async def test_an_over_tall_full_page_is_refused_before_the_shot(rig):
    rig.page.scroll_height = 40000

    with pytest.raises(PageTooTallError) as refused:
        await rig.renderer.render(_request(full_page=True, max_height=32768))

    assert (refused.value.height, refused.value.limit) == (40000, 32768)
    assert "screenshot" not in rig.log


async def test_a_page_that_grew_after_it_was_measured_is_still_refused(rig):
    """CR 2: the measurement is a cheap early exit; the PNG's own height is the check."""
    rig.page.scroll_height = 2000
    rig.page.shot_size = (1366, 40000)

    with pytest.raises(PageTooTallError) as refused:
        await rig.renderer.render(_request(full_page=True, max_height=32768))

    assert (refused.value.height, refused.value.limit) == (40000, 32768)


async def test_a_page_wider_than_the_limit_is_refused(rig):
    """CR 12: a full page is as wide as its overflow, not its viewport."""
    rig.page.shot_size = (9000, 2000)

    with pytest.raises(PageTooWideError) as refused:
        await rig.renderer.render(_request(full_page=True, max_width=4096))

    assert (refused.value.width, refused.value.limit) == (9000, 4096)


async def test_an_over_wide_full_page_is_refused_before_the_shot(rig):
    """CR 18: the cheap exit CR 2 kept for height, for width too."""
    rig.page.scroll_width = 1_000_000

    with pytest.raises(PageTooWideError) as refused:
        await rig.renderer.render(_request(full_page=True, max_width=4096))

    assert (refused.value.width, refused.value.limit) == (1_000_000, 4096)
    assert "screenshot" not in rig.log


async def test_a_viewport_shot_does_not_measure_the_page(rig):
    rig.page.scroll_height = 40000

    await rig.renderer.render(_request(full_page=False))

    assert "evaluate" not in rig.log


# --------------------------------------------------------------------------- refusals


async def test_a_refused_subresource_abandons_the_render(rig):
    async def subresource_then_hang(page):
        await _socks_connect_intranet(page)
        await asyncio.sleep(30)

    rig.page.goto_hook = subresource_then_hang

    with pytest.raises(DestinationRefusedError) as refused:
        await asyncio.wait_for(rig.renderer.render(_request()), timeout=5)

    assert refused.value.refusal.reason is FailureReason.DESTINATION_REFUSED
    assert "intranet.test" in str(refused.value)
    assert rig.log[-2:] == ["cgroup.kill", "close"]


async def test_a_refusal_outranks_the_error_it_caused(rig):
    """A refused navigation surfaces as a net error too; the refusal is the cause."""

    async def refused_navigation(page):
        await _socks_connect_intranet(page)
        raise PlaywrightError("net::ERR_TUNNEL_CONNECTION_FAILED")

    rig.page.goto_hook = refused_navigation

    with pytest.raises(DestinationRefusedError):
        await rig.renderer.render(_request())


# --------------------------------------------------------------------------- kills


async def test_a_group_kill_at_the_cap_is_named(rig):
    rig.page.screenshot_error = PlaywrightError("Target page, context or browser has been closed")
    original = rig.page.screenshot

    async def killed(**kwargs):
        rig.cgroup.kills += 1
        return await original(**kwargs)

    rig.page.screenshot = killed

    with pytest.raises(BrowserKilledError, match="memory cap"):
        await rig.renderer.render(_request())


async def test_a_cancelled_render_is_killed_before_it_is_closed(rig):
    async def hang(page):
        await asyncio.sleep(30)

    rig.page.goto_hook = hang

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.2):
            await rig.renderer.render(_request())

    assert rig.log[-2:] == ["cgroup.kill", "close"]


# --------------------------------------------------------------------------- png


def test_png_dimensions_read_the_ihdr():
    assert png_dimensions(_png(1366, 28108)) == (1366, 28108)


@pytest.mark.parametrize("data", [b"", b"GIF89a" + b"\0" * 30, b"\x89PNG\r\n\x1a\n" + b"\0" * 16])
def test_anything_else_is_not_a_png(data):
    with pytest.raises(ValueError, match="not a PNG"):
        png_dimensions(data)


# --------------------------------------------------------------------------- preflight


async def test_preflight_launches_the_confined_shell_once_and_closes_it(rig):
    """Boot's check (#138): the same launch a render makes, and nothing navigated."""
    await rig.renderer.preflight()

    assert rig.pw.launch_kwargs["chromium_sandbox"] is True
    assert rig.pw.launch_kwargs["env"]["REPLICATOR_BROWSER_CGROUP_PROCS"].endswith("cgroup.procs")
    assert rig.log == ["close"]


async def test_preflight_kills_a_straggler_after_close(rig):
    rig.cgroup.alive = True

    await rig.renderer.preflight()

    assert rig.log == ["close", "cgroup.kill"]


async def test_preflight_reports_a_missing_runtime(tmp_path):
    pw = FakePlaywright(FakeBrowser(FakePage([])))
    renderer = PlaywrightRenderer(
        BrowserRuntime(tmp_path / "nowhere", None),
        DestinationGuard(blocked=blocked_networks(None)),
        playwright_factory=lambda: pw,
    )

    with pytest.raises(RuntimeUnavailableError):
        await renderer.preflight()
    assert pw.launch_kwargs is None


async def test_preflight_lets_a_launch_failure_through(rig):
    async def refuse(**kwargs):
        raise PlaywrightError("Target page, context or browser has been closed")

    rig.pw.launch = refuse

    with pytest.raises(PlaywrightError):
        await rig.renderer.preflight()


# --------------------------------------------------------------------------- load (#63)


def _load(**overrides) -> LoadRequest:
    values = dict(url="https://example.org/", user_agent=None, max_wait_seconds=30, max_bytes=1024)
    values.update(overrides)
    return LoadRequest(**values)


async def test_a_load_serializes_the_document_at_the_load_event(rig):
    loaded = await rig.renderer.load(_load(max_wait_seconds=20))

    assert loaded.document == rig.page.html.encode("utf-8")
    assert loaded.media_type == "text/html"
    assert loaded.status == 200
    assert loaded.final_url == "https://example.org/landed"
    assert loaded.retry_after is None
    # commit, then load, each under the request's bound; the document only after load.
    assert rig.log.index("goto https://example.org/ commit 20000") < rig.log.index(
        "load_state load 20000"
    )
    assert rig.log.index("load_state load 20000") < rig.log.index("content")
    assert "screenshot" not in rig.log


async def test_a_load_uses_a_fixed_viewport_and_a_fresh_blocked_context(rig):
    await rig.renderer.load(_load())

    assert rig.pw.browser.context_kwargs == {
        "viewport": {"width": 1280, "height": 720},
        "device_scale_factor": 1,
        "service_workers": "block",
        "accept_downloads": False,
    }


async def test_the_issuers_user_agent_is_the_browsers(rig):
    await rig.renderer.load(_load(user_agent="watcher/0.1.0"))

    assert rig.pw.browser.context_kwargs["user_agent"] == "watcher/0.1.0"


async def test_a_load_launches_inside_every_confinement(rig, shell):
    """The same launch a capture makes: one session, not a copy of it."""
    await rig.renderer.load(_load())

    assert rig.pw.launch_kwargs["chromium_sandbox"] is True
    assert rig.pw.launch_kwargs["env"]["REPLICATOR_BROWSER_EXECUTABLE"] == str(shell)
    assert rig.log[-1] == "close"


@pytest.mark.parametrize("status", [404, 429, 503, 301])
async def test_a_non_2xx_load_returns_its_status_without_waiting_for_load(rig, status):
    """Returned, not raised: the fetch path's own classifier and pacer read it (#63)."""
    rig.page.status = status
    rig.page.headers = {"retry-after": "120"}

    loaded = await rig.renderer.load(_load())

    assert (loaded.status, loaded.retry_after, loaded.document) == (status, "120", b"")
    assert not any(entry.startswith("load_state") for entry in rig.log)
    assert "content" not in rig.log


@pytest.mark.parametrize("content_type", ["application/json", "text/plain", "image/png"])
async def test_a_document_that_is_not_html_is_refused_unserialized(rig, content_type):
    """Chromium's viewer wrapper is not the resource (spike, 2026-11-05)."""
    rig.page.content_type = content_type

    with pytest.raises(NotADocumentError, match=content_type):
        await rig.renderer.load(_load())

    assert "content" not in rig.log


async def test_xhtml_is_a_document(rig):
    rig.page.content_type = "application/xhtml+xml"

    assert (await rig.renderer.load(_load())).media_type == "application/xhtml+xml"


async def test_a_download_is_not_a_document(rig):
    """What ``goto`` raises for a PDF, or for ``Content-Disposition: attachment``."""

    async def download(page):
        raise PlaywrightError("Page.goto: Download is starting")

    rig.page.goto_hook = download

    with pytest.raises(NotADocumentError, match="download"):
        await rig.renderer.load(_load())


async def test_a_document_over_the_ceiling_is_refused_before_it_is_relayed(rig):
    """Measured in the page, inside the cgroup, so the bytes never cross the Node driver."""
    rig.page.document_bytes = 1025

    with pytest.raises(DocumentTooLargeError) as refused:
        await rig.renderer.load(_load(max_bytes=1024))

    assert (refused.value.size, refused.value.limit) == (1025, 1024)
    assert "content" not in rig.log


async def test_a_load_that_never_reaches_load_is_a_wait_timeout(rig):
    rig.page.load_states["load"] = PlaywrightTimeoutError("Timeout 30000ms exceeded.")

    with pytest.raises(WaitTimeoutError):
        await rig.renderer.load(_load())

    assert rig.log[-2:] == ["cgroup.kill", "close"]


async def test_a_load_with_no_response_is_a_failed_navigation(rig):
    async def refused(page):
        raise PlaywrightError("net::ERR_CONNECTION_REFUSED at https://example.org/")

    rig.page.goto_hook = refused

    with pytest.raises(NavigationFailedError):
        await rig.renderer.load(_load())


async def test_a_refused_subresource_abandons_the_load(rig):
    async def intranet(page):
        await _socks_connect_intranet(page)
        await asyncio.sleep(30)

    rig.page.goto_hook = intranet

    with pytest.raises(DestinationRefusedError) as refused:
        await rig.renderer.load(_load())

    assert refused.value.refusal.reason is FailureReason.DESTINATION_REFUSED


async def test_renders_and_loads_never_overlap(rig):
    """One browser at a time in the shared cgroup (#63, operator call 5).

    Not only memory: a failed render's teardown writes ``cgroup.kill``, which takes
    every browser in the group with it.
    """

    async def slow(page):
        page.log.append("goto-start")
        await asyncio.sleep(0.05)
        page.log.append("goto-end")
        return FakeResponse(200)

    rig.page.goto_hook = slow

    await asyncio.gather(rig.renderer.render(_request()), rig.renderer.load(_load()))

    first_close = rig.log.index("close")
    assert rig.log.index("goto-start", rig.log.index("goto-start") + 1) > first_close


@pytest.mark.parametrize("url", ["file:///etc/", "chrome://version", "javascript:alert(1)"])
async def test_the_renderer_itself_never_navigates_to_a_url_the_guard_cannot_see(rig, url):
    """CR 1, at the seam: whatever its caller checked, neither operation goes there."""
    with pytest.raises(NavigationImpossibleError):
        await rig.renderer.load(_load(url=url))
    with pytest.raises(NavigationImpossibleError):
        await rig.renderer.render(_request(url=url))

    assert not any(entry.startswith("goto") for entry in rig.log)
