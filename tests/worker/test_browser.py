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
    InvalidSelectorError,
    NavigationFailedError,
    NavigationStatusError,
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
        return self.scroll_height

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
