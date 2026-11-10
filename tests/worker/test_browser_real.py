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
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire
from co_core.pure.models.changes import CaptureStoredEvent
from co_core.pure.util.hashing import sha256
from co_core_sync.drivers.blobstore import LocalBlobStore

from src.core.config import get_settings
from src.core.errors import (
    CaptureReason,
    FailureReason,
    PermanentCaptureError,
    PermanentFetchError,
    TransientFetchError,
)
from src.storage.sweeper import BlobUsage
from src.worker.browser import (
    BrowserKilledError,
    BrowserRuntime,
    DestinationRefusedError,
    InvalidSelectorError,
    LoadRequest,
    NavigationImpossibleError,
    NavigationStatusError,
    PlaywrightRenderer,
    RenderRequest,
    Wait,
    WaitTimeoutError,
    pinned_shell,
    png_dimensions,
)
from src.worker.browser_cgroup import BrowserCgroup, delegated_service_cgroup
from src.worker.browser_fetch import BrowserFetcher
from src.worker.capture import build_capture_handler
from src.worker.capture_reporter import build_captured_publisher
from src.worker.egress import DestinationGuard, blocked_networks
from src.worker.handler import build_handler
from src.worker.pacing import HostPacer
from tests.worker.conftest import FakeFetcher, capture_command, command, published_facts

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
    delays: dict[str, float] = {}
    user_agents: list[str] = []

    async def serve(reader, writer):
        try:
            request = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, ConnectionError):
            writer.close()
            return
        path = request.split(b" ", 2)[1].decode()
        for line in request.decode("latin-1").split("\r\n")[1:]:
            name, _, value = line.partition(":")
            if name.lower() == "user-agent":
                user_agents.append(value.strip())
        if path in delays:
            await asyncio.sleep(delays[path])
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
    s.delays = delays
    s.user_agents = user_agents
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


def _reading(pid: int) -> dict[str, object] | None:
    """One browser process, read from ``/proc``; ``None`` once it has exited."""
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
        status = Path(f"/proc/{pid}/status").read_text()
        adj = int(Path(f"/proc/{pid}/oom_score_adj").read_text())
    except (FileNotFoundError, ProcessLookupError):
        return None
    if not cmdline:  # a zombie: what it reads now is not what it ran as
        return None
    # Chromium rewrites a child's argv in place, NULs to spaces: search the whole buffer.
    return {
        "no_sandbox": b"--no-sandbox" in cmdline,
        "renderer": b"--type=renderer" in cmdline,
        "seccomp": _field(status, "Seccomp"),
        # The namespace sandbox: a renderer sees itself in a nested PID namespace.
        "nested": len(_field(status, "NSpid").split()) > 1,
        "adj": adj,
    }


async def _last_readings(cgroup: BrowserCgroup, render: asyncio.Task) -> list[dict[str, object]]:
    """Each browser process's last reading before it exited, polled until the render ends (#186).

    Not the first sight, and not a snapshot. A renderer is listed before it engages
    seccomp-bpf (read ``0``, then ``2`` 6-55 ms later, measured), and the wrapper is in the
    cgroup a moment before it writes its own ``oom_score_adj``. Bounded by the render, not a
    count: a cold first launch shows its renderer seconds late.
    """
    last: dict[int, dict[str, object]] = {}
    while not render.done():
        try:
            pids = [int(p) for p in cgroup.procs_file.read_text().split()]
        except FileNotFoundError:
            pids = []
        for pid in pids:
            if (reading := _reading(pid)) is not None:
                last[pid] = reading
        await asyncio.sleep(0.05)
    return list(last.values())


async def test_the_browser_runs_sandboxed_capped_and_ranked_below_the_worker(
    browsers_path, delegated, site
):
    cgroup = BrowserCgroup.prepare(memory_max=1536 * 2**20)
    renderer = _renderer(browsers_path, cgroup)
    site.pages["/"] = (200, {}, "<p>hold</p>")

    render = asyncio.create_task(
        renderer.render(_request(site.url("/"), wait=Wait("delay", seconds=2)))
    )
    try:
        procs = await _last_readings(cgroup, render)
    finally:
        await render  # an observer that failed must not orphan the browser
    renderers = [p for p in procs if p["renderer"]]
    adj = {p["adj"] for p in procs}

    assert procs, "no browser process was in the browser cgroup"
    assert not any(p["no_sandbox"] for p in procs), "the browser launched with --no-sandbox"
    assert renderers, "no renderer process was found"
    assert [r["seccomp"] for r in renderers] == ["2"] * len(renderers), (
        "a renderer runs without seccomp-bpf"
    )
    assert all(r["nested"] for r in renderers), "a renderer is outside the namespace sandbox"
    # The wrapper sets 500 on the browser; Chromium then ranks its own children (renderers
    # 300 and up, utilities 200). All of them sit far above the worker's -900.
    assert 500 in adj
    assert min(adj) >= 200, f"a browser process ranks near the worker: {adj}"
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


# --------------------------------------------------------------------------- end to end


def _capture_handler(renderer, tmp_path, client):
    """``run()``'s capture handler over the real shell, publishing to ``content.artifacts``."""
    settings = get_settings()
    stop = asyncio.Event()
    return build_capture_handler(
        renderer=renderer,
        store=LocalBlobStore(tmp_path),
        settings=settings,
        pacer=HostPacer(0.0),
        usage=BlobUsage(),
        ceiling_bytes=None,
        complete=build_captured_publisher(client=client),
        stop=stop,
    )


async def test_a_capture_command_lands_a_png_and_its_fact(renderer, site, tmp_path, fake_redis):
    """The whole path but the bus loop (#138): command, real render, store, ``capture_stored``."""
    site.pages["/"] = (200, {}, "<html><body style='margin:0'><main>hello</main></body></html>")
    handle = _capture_handler(renderer, tmp_path, fake_redis)

    await handle(capture_command(url=site.url("/"), width=640, height=480,
                                 wait={"kind": "selector", "selector": "main"}))  # fmt: skip

    ((entry_id, fields),) = await fake_redis.xrange(streams.CONTENT_ARTIFACTS)
    fact = from_wire(
        {k.decode(): v.decode() for k, v in fields.items()},
        topic=streams.CONTENT_ARTIFACTS,
        message_id=entry_id.decode(),
    ).payload
    assert isinstance(fact, CaptureStoredEvent)
    assert (fact.image_width, fact.image_height) == (640, 480)
    assert fact.media_type == "image/png"
    assert fact.final_url == site.url("/")
    png = LocalBlobStore(tmp_path).open(fact.content_fingerprint)
    assert sha256(png) == fact.content_fingerprint
    assert png_dimensions(png) == (640, 480)


async def test_a_refused_subresource_closes_the_command_destination_refused(
    renderer, site, canary, tmp_path, fake_redis
):
    """The contract's reading of the guard: one private request fails the whole capture."""
    site.pages["/"] = (
        200,
        {},
        f'<html><img src="http://{CANARY_HOST}:{canary.port}/x.png"></html>',
    )
    handle = _capture_handler(renderer, tmp_path, fake_redis)

    with pytest.raises(PermanentCaptureError) as refused:
        await handle(capture_command(url=site.url("/")))

    assert refused.value.reason == CaptureReason.DESTINATION_REFUSED
    assert canary.tcp == 0
    assert await fake_redis.xlen(streams.CONTENT_ARTIFACTS) == 0


@pytest.mark.parametrize(
    "location", ["file:///etc/passwd", "data:text/html,hi", "chrome://version"]
)
async def test_a_redirect_to_a_scheme_the_browser_wont_open_is_impossible(renderer, site, location):
    """CR 23: measured as ERR_UNSAFE_REDIRECT, which the contract closes as not_fetchable."""
    site.pages["/"] = (302, {"Location": location}, "")

    with pytest.raises(NavigationImpossibleError, match="ERR_UNSAFE_REDIRECT"):
        await renderer.render(_request(site.url("/")))


# --------------------------------------------------------------------------- browser fetch (#63)

# A page whose meaningful content only exists after its script runs, served as Latin-1
# with a validator: the fact must describe the stored UTF-8 document, not this response.
JS_PAGE = (
    "<html><head><meta charset='iso-8859-1'></head><body><div id='x'>shell</div>"
    "<script>document.getElementById('x').textContent = 'rendered caf\\u00e9';</script>"
    "</body></html>"
)


def _fetch_handler(renderer, tmp_path, client, *, pacer=None):
    """``run()``'s fetch handler with the real shell behind ``render="browser"``.

    The HTTP driver is a recorder: a browser command that reached it would show there.
    """
    http = FakeFetcher()
    settings = get_settings()
    handle = build_handler(
        fetcher=http,
        store=LocalBlobStore(tmp_path),
        client=client,
        settings=settings,
        ceiling_bytes=None,
        pacer=pacer if pacer is not None else HostPacer(0.0),
        browser_fetcher=BrowserFetcher(
            renderer,
            default_wait_seconds=settings.capture_max_wait_seconds,
            max_bytes=settings.max_blob_bytes,
        ),
    )
    return handle, http


async def test_a_browser_fetch_stores_the_rendered_document(renderer, site, tmp_path, fake_redis):
    """The whole byte path over the real shell: the script's text is in the stored bytes."""
    site.pages["/"] = (
        200,
        {"Content-Type": "text/html; charset=ISO-8859-1", "ETag": '"abc"'},
        JS_PAGE,
    )
    handle, http = _fetch_handler(renderer, tmp_path, fake_redis)

    await handle(
        command(url=site.url("/"), render="browser", headers={"user-agent": "watcher/0.1.0"})
    )

    assert http.effects == []
    (fact,) = await published_facts(fake_redis)
    document = LocalBlobStore(tmp_path).open(fact.content_fingerprint)
    assert "rendered café".encode() in document
    assert b"shell" not in document.split(b"<script>")[0]
    assert fact.media_type == "text/html"
    assert fact.content_type_raw == "text/html; charset=utf-8"
    assert (fact.etag, fact.last_modified) == (None, None)
    assert (fact.status_code, fact.final_url) == (200, site.url("/"))
    assert site.user_agents and set(site.user_agents) == {"watcher/0.1.0"}


@pytest.mark.parametrize(
    "headers",
    [{"Content-Type": "application/pdf"}, {"Content-Disposition": "attachment; filename=x"},
     {"Content-Type": "application/json"}],
)  # fmt: skip
async def test_a_download_or_a_non_html_document_is_not_fetchable(
    renderer, site, tmp_path, fake_redis, headers
):
    """Measured: a download raises in ``goto``; JSON renders as Chromium's viewer."""
    site.pages["/"] = (200, headers, '{"a": 1}')
    handle, _ = _fetch_handler(renderer, tmp_path, fake_redis)

    with pytest.raises(PermanentFetchError) as refused:
        await handle(command(url=site.url("/"), render="browser"))

    assert refused.value.reason is FailureReason.NOT_FETCHABLE
    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 0


async def test_a_429s_retry_after_reaches_the_pacer(renderer, site, tmp_path, fake_redis):
    site.pages["/"] = (429, {"Retry-After": "45"}, "slow down")
    pacer = HostPacer(0.0)
    handle, _ = _fetch_handler(renderer, tmp_path, fake_redis, pacer=pacer)

    with pytest.raises(TransientFetchError):
        await handle(command(url=site.url("/"), render="browser"))

    assert 44 < pacer.wait_seconds(site.url("/")) <= 45


async def test_a_refused_subresource_closes_a_browser_fetch(
    renderer, site, canary, tmp_path, fake_redis
):
    site.pages["/"] = (200, {}, f'<html><img src="http://{CANARY_HOST}:{canary.port}/x.png">')
    handle, _ = _fetch_handler(renderer, tmp_path, fake_redis)

    with pytest.raises(PermanentFetchError) as refused:
        await handle(command(url=site.url("/"), render="browser"))

    assert refused.value.reason is FailureReason.DESTINATION_REFUSED
    assert canary.tcp == 0


async def test_concurrent_loads_take_turns_rather_than_killing_each_other(renderer, site):
    """Operator call 5. Without the lock the first teardown's ``cgroup.kill`` takes the
    second, slower load's browser with it: the group is shared, the kill is not scoped."""
    site.pages["/fast"] = (200, {}, "<html><body>fast</body></html>")
    site.pages["/slow"] = (200, {}, '<html><body>slow<img src="/pixel"></body></html>')
    site.pages["/pixel"] = (200, {"Content-Type": "image/gif"}, "GIF89a")
    site.delays["/pixel"] = 1.5

    def request(path: str) -> LoadRequest:
        return LoadRequest(url=site.url(path), user_agent=None, max_wait_seconds=10,
                           max_bytes=2**20)  # fmt: skip

    slow, fast = await asyncio.gather(
        renderer.load(request("/slow")), renderer.load(request("/fast"))
    )

    assert b"slow" in slow.document and b"fast" in fast.document


async def test_a_local_file_is_never_fetched(renderer, tmp_path, fake_redis):
    """CR 1: measured before the fix, Chromium served this file as text/html and the
    handler would have published it on content.blobs."""
    page = tmp_path / "local.html"
    page.write_text("<html><body>on this host</body></html>")
    handle, http = _fetch_handler(renderer, tmp_path / "blobs", fake_redis)

    with pytest.raises(PermanentFetchError) as refused:
        await handle(command(url=page.as_uri(), render="browser"))

    assert refused.value.reason is FailureReason.NOT_FETCHABLE
    assert http.effects == []
    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 0
