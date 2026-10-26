"""The renderer against the pinned shell: the confinements hold in a real browser (#138).

``-m browser``: excluded by default, and **never skipped** when selected. A missing runtime
or an undelegated cgroup fails the run, because a green ``-m browser`` that ran nothing
is exactly the result that hides a confinement gone missing. How to run it here and in CI:
docs/TESTING.md.

The page server is on ``127.0.0.1`` and the guard's table is narrowed to the canary's
``127.0.0.2/32``, so the page is reachable and the canary is the "private" destination.
Every vector the spike tried is here, and each one must (a) never reach the canary and
(b) fail the render as ``destination_refused``. WebRTC is UDP, which SOCKS never sees, so
its test asserts the canary heard nothing and the render *succeeded*: the launch flag,
not the guard, is what stops it.
"""

import asyncio
import os
import socket
from pathlib import Path

import pytest

from src.worker.browser import (
    BrowserKilledError,
    BrowserRuntime,
    DestinationRefusedError,
    InvalidSelectorError,
    NavigationImpossibleError,
    NavigationStatusError,
    PlaywrightRenderer,
    RenderRequest,
    Wait,
    WaitTimeoutError,
    pinned_shell,
)
from src.worker.browser_cgroup import BrowserCgroup, delegated_service_cgroup
from src.worker.egress import DestinationGuard, blocked_networks

pytestmark = pytest.mark.browser

CANARY_HOST = "127.0.0.2"
DEFAULT_BROWSERS_PATH = "/var/lib/replicator/ms-playwright"


# --------------------------------------------------------------------------- servers


class Canary:
    """Counts every TCP connection and UDP datagram that reaches ``127.0.0.2``."""

    def __init__(self) -> None:
        self.tcp = 0
        self.udp = 0


@pytest.fixture
async def canary():
    hits = Canary()

    async def on_tcp(reader, writer):
        hits.tcp += 1
        writer.close()

    class Udp(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            hits.udp += 1

    server = await asyncio.start_server(on_tcp, CANARY_HOST, 0)
    port = server.sockets[0].getsockname()[1]
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(Udp, local_addr=(CANARY_HOST, port))
    hits.port = port
    yield hits
    transport.close()
    server.close()
    await server.wait_closed()


@pytest.fixture
async def site():
    """A tiny HTTP/1.1 server on loopback: ``site.pages[path] = (status, headers, body)``."""
    pages: dict[str, tuple[int, dict[str, str], str]] = {}

    async def serve(reader, writer):
        try:
            request = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()
            return
        path = request.split(b" ", 2)[1].decode()
        status, headers, body = pages.get(path, (404, {}, "not here"))
        data = body.encode()
        head = f"HTTP/1.1 {status} X\r\nContent-Length: {len(data)}\r\nConnection: close\r\n"
        head += "".join(
            f"{k}: {v}\r\n" for k, v in {"Content-Type": "text/html", **headers}.items()
        )
        writer.write(head.encode() + b"\r\n" + data)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    class Site:
        pass

    s = Site()
    s.pages = pages
    s.url = lambda path: f"http://127.0.0.1:{port}{path}"
    yield s
    server.close()
    await server.wait_closed()


# --------------------------------------------------------------------------- runtime


@pytest.fixture(scope="session")
def browsers_path() -> Path:
    path = Path(os.environ.get("REPLICATOR_PLAYWRIGHT_BROWSERS_PATH", DEFAULT_BROWSERS_PATH))
    if not pinned_shell(path).is_file():
        pytest.fail(
            f"no pinned shell at {pinned_shell(path)}: install it with "
            f"PLAYWRIGHT_BROWSERS_PATH={path} uv run playwright install chromium-headless-shell, "
            "or point REPLICATOR_PLAYWRIGHT_BROWSERS_PATH at an install"
        )
    return path


def _enter_worker_leaf() -> None:
    """Do what ``DelegateSubgroup=worker`` does for the unit, inside a delegated scope."""
    own = Path("/sys/fs/cgroup") / Path("/proc/self/cgroup").read_text().split("::", 1)[
        1
    ].strip().lstrip("/")
    if own.name == "worker":
        return
    if not os.access(own / "cgroup.procs", os.W_OK):
        pytest.fail(
            f"{own} is not delegated to this user: run -m browser inside "
            "`systemd-run --user --scope -p Delegate=yes` (docs/TESTING.md)"
        )
    leaf = own / "worker"
    leaf.mkdir(exist_ok=True)
    for pid in (own / "cgroup.procs").read_text().split():
        (leaf / "cgroup.procs").write_text(pid)


@pytest.fixture(scope="session")
def delegated() -> None:
    _enter_worker_leaf()
    assert delegated_service_cgroup() is not None


def _renderer(browsers_path: Path, cgroup: BrowserCgroup | None) -> PlaywrightRenderer:
    guard = DestinationGuard(blocked=blocked_networks([f"{CANARY_HOST}/32"]))
    return PlaywrightRenderer(BrowserRuntime(browsers_path, cgroup), guard)


@pytest.fixture
def renderer(browsers_path, delegated) -> PlaywrightRenderer:
    return _renderer(browsers_path, BrowserCgroup.prepare(memory_max=1536 * 2**20))


def _request(url: str, **overrides) -> RenderRequest:
    values = dict(
        url=url,
        width=800,
        height=600,
        full_page=False,
        wait=None,
        max_wait_seconds=10,
        max_height=32768,
        max_width=4096,
    )
    values.update(overrides)
    return RenderRequest(**values)


# --------------------------------------------------------------------------- egress

VECTORS = {
    "img": '<img src="http://{canary}/x.png">',
    "fetch": '<script>fetch("http://{canary}/api").catch(() => {{}})</script>',
    "websocket": '<script>new WebSocket("ws://{canary}/feed")</script>',
    "beacon": '<script>navigator.sendBeacon("http://{canary}/b", "x")</script>',
    "iframe": '<iframe src="http://{canary}/frame"></iframe>',
    "prefetch": '<link rel="prefetch" href="http://{canary}/next">',
    "ipv6-loopback": '<img src="http://[::1]:{port}/x.png">',
}


@pytest.mark.parametrize("vector", sorted(VECTORS))
async def test_every_vector_is_refused_before_it_reaches_the_canary(renderer, site, canary, vector):
    markup = VECTORS[vector].format(canary=f"{CANARY_HOST}:{canary.port}", port=canary.port)
    site.pages["/"] = (200, {}, f"<html><body>page{markup}</body></html>")
    if vector == "ipv6-loopback":
        # ::1 is not the canary, so widen the table to hold it as well.
        renderer._guard = DestinationGuard(
            blocked=blocked_networks([f"{CANARY_HOST}/32", "::1/128"])
        )

    with pytest.raises(DestinationRefusedError):
        await renderer.render(_request(site.url("/"), wait=Wait("delay", seconds=1)))

    assert (canary.tcp, canary.udp) == (0, 0)


async def test_a_redirect_into_the_refused_range_is_refused(renderer, site, canary):
    site.pages["/"] = (302, {"Location": f"http://{CANARY_HOST}:{canary.port}/"}, "")

    with pytest.raises(DestinationRefusedError):
        await renderer.render(_request(site.url("/")))

    assert canary.tcp == 0


async def test_webrtc_sends_nothing_outside_the_proxy(renderer, site, canary):
    stun = f"stun:{CANARY_HOST}:{canary.port}"
    site.pages["/"] = (
        200,
        {},
        "<script>const pc = new RTCPeerConnection({iceServers: [{urls: '%s'}]});"
        "pc.createDataChannel('x');"
        "pc.createOffer().then(o => pc.setLocalDescription(o));</script>".replace("%s", stun),
    )

    await renderer.render(_request(site.url("/"), wait=Wait("delay", seconds=2)))

    assert canary.udp == 0


# --------------------------------------------------------------------------- render


async def test_a_full_page_capture_measures_the_stored_image(renderer, site):
    site.pages["/"] = (
        200,
        {},
        '<html><body style="margin:0"><div style="height:3000px">tall</div></body></html>',
    )

    rendered = await renderer.render(_request(site.url("/"), full_page=True))

    assert rendered.png.startswith(b"\x89PNG")
    assert (rendered.image_width, rendered.image_height) == (800, 3000)
    assert rendered.final_url == site.url("/")


async def test_a_selector_wait_captures_after_it_appears(renderer, site):
    site.pages["/"] = (
        200,
        {},
        "<script>setTimeout(() => {document.body.innerHTML = '<p id=ready>ok</p>'}, 500)</script>",
    )

    rendered = await renderer.render(
        _request(site.url("/"), wait=Wait("selector", selector="#ready"))
    )

    assert rendered.status == 200


async def test_a_selector_that_never_appears_times_out(renderer, site):
    site.pages["/"] = (200, {}, "<p>nothing</p>")

    with pytest.raises(WaitTimeoutError):
        await renderer.render(
            _request(site.url("/"), wait=Wait("selector", selector="#never"), max_wait_seconds=1)
        )


async def test_an_unparseable_selector_is_recognised(renderer, site):
    site.pages["/"] = (200, {}, "<p>x</p>")

    with pytest.raises(InvalidSelectorError):
        await renderer.render(_request(site.url("/"), wait=Wait("selector", selector="<<")))


async def test_a_404_is_reported_with_its_status(renderer, site):
    with pytest.raises(NavigationStatusError) as failed:
        await renderer.render(_request(site.url("/missing")))

    assert failed.value.status == 404


# --------------------------------------------------------------------------- confinement


def _field(status: str, name: str) -> str:
    for line in status.splitlines():
        if line.startswith(f"{name}:"):
            return line.split(":", 1)[1].strip()
    return ""


def _snapshot(cgroup: BrowserCgroup) -> dict[str, object]:
    """What the browser's processes look like mid-render, read from ``/proc``."""
    seen: dict[str, object] = {}
    pids = [int(p) for p in cgroup.procs_file.read_text().split()]
    seen["pids"] = pids
    # Chromium rewrites a child's argv in place, NULs to spaces: search the whole buffer.
    cmdlines = {p: Path(f"/proc/{p}/cmdline").read_bytes() for p in pids}
    status = {p: Path(f"/proc/{p}/status").read_text() for p in pids}
    seen["no_sandbox"] = any(b"--no-sandbox" in c for c in cmdlines.values())
    renderers = [p for p, c in cmdlines.items() if b"--type=renderer" in c]
    seen["renderers"] = renderers
    seen["seccomp"] = [_field(status[p], "Seccomp") for p in renderers]
    # The namespace sandbox: a renderer sees itself in a nested PID namespace.
    seen["nested"] = [len(_field(status[p], "NSpid").split()) > 1 for p in renderers]
    seen["adj"] = {int(Path(f"/proc/{p}/oom_score_adj").read_text()) for p in pids}
    return seen


async def test_the_browser_runs_sandboxed_capped_and_ranked_below_the_worker(
    browsers_path, delegated, site
):
    cgroup = BrowserCgroup.prepare(memory_max=1536 * 2**20)
    renderer = _renderer(browsers_path, cgroup)
    site.pages["/"] = (200, {}, "<p>hold</p>")
    seen: dict[str, object] = {}

    async def observe():
        await asyncio.sleep(1.0)
        seen.update(_snapshot(cgroup))

    observer = asyncio.create_task(observe())
    await renderer.render(_request(site.url("/"), wait=Wait("delay", seconds=2)))
    await observer

    assert seen["pids"], "no browser process was in the browser cgroup"
    assert seen["no_sandbox"] is False
    assert seen["renderers"], "no renderer process was found"
    assert seen["seccomp"] == ["2"] * len(seen["renderers"]), "a renderer runs without seccomp-bpf"
    assert all(seen["nested"]), "a renderer is outside the namespace sandbox"
    # The wrapper sets 500 on the browser; Chromium then ranks its own children (renderers
    # 300 and up, utilities 200). All of them sit far above the worker's -900.
    assert 500 in seen["adj"]
    assert min(seen["adj"]) >= 200, f"a browser process ranks near the worker: {seen['adj']}"
    assert not cgroup.populated(), "the browser outlived its render"


async def test_a_runaway_page_is_group_killed_and_the_worker_lives(browsers_path, delegated, site):
    cgroup = BrowserCgroup.prepare(memory_max=256 * 2**20)
    renderer = _renderer(browsers_path, cgroup)
    kills_before = cgroup.oom_group_kills()
    site.pages["/"] = (
        200,
        {},
        "<script>const hog = []; setInterval(() => {"
        " for (let i = 0; i < 64; i++) hog.push(new Uint8Array(1 << 20).fill(i)); }, 10);</script>",
    )

    with pytest.raises(BrowserKilledError):
        await renderer.render(
            _request(site.url("/"), wait=Wait("delay", seconds=15), max_wait_seconds=20)
        )

    assert cgroup.oom_group_kills() > kills_before
    assert not cgroup.populated()
    assert os.getpid() in [
        int(p) for p in (cgroup.path.parent / "worker" / "cgroup.procs").read_text().split()
    ]


async def test_a_dns_name_is_resolved_by_the_guard_not_the_browser(browsers_path, delegated, site):
    """``localhost`` resolves to loopback; the guard, holding 127.0.0.0/8, must refuse it."""
    renderer = PlaywrightRenderer(
        BrowserRuntime(browsers_path, BrowserCgroup.prepare(memory_max=1536 * 2**20)),
        DestinationGuard(blocked=blocked_networks(None)),
    )
    port = int(site.url("/").rsplit(":", 1)[1].split("/")[0])

    with pytest.raises(DestinationRefusedError, match="localhost"):
        await renderer.render(_request(f"http://localhost:{port}/"))


def test_the_host_resolves_localhost_to_loopback():
    """The premise of the test above."""
    assert socket.gethostbyname("localhost").startswith("127.")


@pytest.mark.parametrize(
    "location", ["file:///etc/passwd", "data:text/html,hi", "chrome://version"]
)
async def test_a_redirect_to_a_scheme_the_browser_wont_open_is_impossible(renderer, site, location):
    """CR 23: measured as ERR_UNSAFE_REDIRECT, which the contract closes as not_fetchable."""
    site.pages["/"] = (302, {"Location": location}, "")

    with pytest.raises(NavigationImpossibleError, match="ERR_UNSAFE_REDIRECT"):
        await renderer.render(_request(site.url("/")))
