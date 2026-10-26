"""The capture loop at startup (#138): off unless an operator turns it on.

On, the group is created at ``0`` (never ``$``), the browser is launched once to
prove it can start, and a failure there degrades capture alone: the group is
still ensured, so no command is skipped, and fetch keeps running.
"""

import asyncio
import json

import pytest

import src.worker.main
from src.core.config import get_settings
from src.worker.browser import RuntimeUnavailableError
from src.worker.main import run

CAPTURE_TOPIC = "replicator.test.capture"


@pytest.fixture(autouse=True)
def _short_policy_poll(monkeypatch, tmp_path, fake_redis):
    monkeypatch.setenv("REPLICATOR_READ_BLOCK_MS", "50")
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)


class FakeRenderer:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.preflights = 0

    async def preflight(self) -> None:
        self.preflights += 1
        if self.error is not None:
            raise self.error

    async def render(self, request):  # pragma: no cover - the loop is stubbed
        raise AssertionError("no render at boot")


def _loops_started(monkeypatch) -> list:
    started: list = []

    async def stub_run_loop(*, spec, **kwargs):
        started.append((spec.label, kwargs))

    monkeypatch.setattr("src.worker.main.run_loop", stub_run_loop)
    return started


def _renderer(monkeypatch, renderer) -> list:
    built: list = []

    def build(settings, guard):
        built.append(guard)
        return renderer

    monkeypatch.setattr("src.worker.main.build_capture_renderer", build)
    return built


def _lines(capsys) -> list[dict]:
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


def _ready(lines) -> dict:
    (ready,) = [line for line in lines if line.get("message") == "worker ready"]
    return ready


async def _group_names(client, topic) -> list[str]:
    groups = await client.xinfo_groups(topic)
    return [g["name"].decode() if isinstance(g["name"], bytes) else g["name"] for g in groups]


async def test_off_by_default_the_capture_stream_is_never_touched(monkeypatch, fake_redis, capsys):
    monkeypatch.delenv("REPLICATOR_CAPTURE_ENABLED", raising=False)
    get_settings.cache_clear()
    built = _renderer(monkeypatch, FakeRenderer())
    started = _loops_started(monkeypatch)

    await run(asyncio.Event(), capture_topic=CAPTURE_TOPIC)

    assert not await fake_redis.exists(CAPTURE_TOPIC)
    assert built == []
    assert "content.capture" not in [label for label, _ in started]
    assert _ready(_lines(capsys))["capture"] == "disabled"


async def test_on_it_creates_its_group_at_zero_and_consumes(monkeypatch, fake_redis, capsys):
    """``0``, not ``$``: Replicator cannot prove the stream empty, and an entry
    written before the group would otherwise be skipped with no fact."""
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()
    await fake_redis.xadd(CAPTURE_TOPIC, {"early": "entry"})
    renderer = FakeRenderer()
    built = _renderer(monkeypatch, renderer)
    started = _loops_started(monkeypatch)

    await run(asyncio.Event(), capture_topic=CAPTURE_TOPIC)

    assert await _group_names(fake_redis, CAPTURE_TOPIC) == ["replicator.capture"]
    (group,) = await fake_redis.xinfo_groups(CAPTURE_TOPIC)
    assert group["last-delivered-id"] in (b"0-0", "0-0")
    assert renderer.preflights == 1
    (guard,) = built
    assert guard is not None
    labels = {label: kwargs for label, kwargs in started}
    assert labels["content.capture"]["group"] == "replicator.capture"
    ready = _ready(_lines(capsys))
    assert ready["capture"] == "enabled"
    assert ready["capture_group"] == "replicator.capture"
    assert ready["capture_consumer"] == "replicator-capture-1"


async def test_fetch_and_capture_share_one_pacer(monkeypatch, fake_redis):
    """An origin's tolerance is one number however it is asked (#138)."""
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()
    _renderer(monkeypatch, FakeRenderer())
    _loops_started(monkeypatch)
    pacers: dict[str, object] = {}
    real_fetch = src.worker.main.build_handler
    real_capture = src.worker.main.build_capture_handler

    def fetch(**kwargs):
        pacers["fetch"] = kwargs["pacer"]
        return real_fetch(**kwargs)

    def capture(**kwargs):
        pacers["capture"] = kwargs["pacer"]
        return real_capture(**kwargs)

    monkeypatch.setattr("src.worker.main.build_handler", fetch)
    monkeypatch.setattr("src.worker.main.build_capture_handler", capture)

    await run(asyncio.Event(), capture_topic=CAPTURE_TOPIC)

    assert pacers["fetch"] is pacers["capture"]


@pytest.mark.parametrize(
    "error",
    [RuntimeUnavailableError("the shell is missing"), OSError("cgroup.subtree_control")],
)
async def test_a_browser_that_cannot_start_degrades_capture_alone(
    monkeypatch, fake_redis, capsys, error
):
    """The group is still ensured, so nothing is skipped; fetch runs; the journal
    says ``CRITICAL`` and names the runbook; broker's undelivered-age probe alarms."""
    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()
    _renderer(monkeypatch, FakeRenderer(error))
    started = _loops_started(monkeypatch)

    await run(asyncio.Event(), capture_topic=CAPTURE_TOPIC)

    assert await _group_names(fake_redis, CAPTURE_TOPIC) == ["replicator.capture"]
    labels = [label for label, _ in started]
    assert "content.capture" not in labels
    assert "content.fetch" in labels
    lines = _lines(capsys)
    assert _ready(lines)["capture"] == "unavailable"
    (critical,) = [line for line in lines if line.get("level") == "CRITICAL"]
    assert "viewport-capture-runbook" in critical["runbook"]
    assert str(error) in critical["error"]


async def test_a_browser_that_cannot_be_built_degrades_capture_alone(
    monkeypatch, fake_redis, capsys
):
    """``BrowserCgroup.prepare`` refusing (an undelegated unit) is the same verdict."""
    from src.worker.browser_cgroup import CgroupUnavailableError

    monkeypatch.setenv("REPLICATOR_CAPTURE_ENABLED", "true")
    get_settings.cache_clear()

    def refuse(settings, guard):
        raise CgroupUnavailableError("not delegated")

    monkeypatch.setattr("src.worker.main.build_capture_renderer", refuse)
    started = _loops_started(monkeypatch)

    await run(asyncio.Event(), capture_topic=CAPTURE_TOPIC)

    assert "content.capture" not in [label for label, _ in started]
    assert _ready(_lines(capsys))["capture"] == "unavailable"


def test_the_renderer_is_built_against_the_settings_cgroup_and_path(monkeypatch, tmp_path):
    monkeypatch.setenv("REPLICATOR_CAPTURE_CGROUP", "off")
    monkeypatch.setenv("REPLICATOR_PLAYWRIGHT_BROWSERS_PATH", str(tmp_path / "pw"))
    get_settings.cache_clear()
    guard = object()

    renderer = src.worker.main.build_capture_renderer(get_settings(), guard)

    assert renderer._runtime.browsers_path == tmp_path / "pw"
    assert renderer._runtime.cgroup is None
    assert renderer._guard is guard


def test_with_the_cgroup_on_the_renderer_prepares_it_at_the_settings_cap(monkeypatch):
    prepared: dict = {}

    def prepare(**kwargs):
        prepared.update(kwargs)
        return "the cgroup"

    monkeypatch.setattr("src.worker.main.BrowserCgroup.prepare", prepare)
    get_settings.cache_clear()

    renderer = src.worker.main.build_capture_renderer(get_settings(), object())

    assert prepared == {"memory_max": 1536 * 1024 * 1024}
    assert renderer._runtime.cgroup == "the cgroup"


def test_the_capture_override_moves_only_its_own_group(monkeypatch):
    monkeypatch.setenv("REPLICATOR_CAPTURE_CONSUMER_NAME", "replicator-capture-dev")
    get_settings.cache_clear()
    settings = get_settings()

    assert (
        src.worker.main.consumer_name_for(settings, settings.capture_consumer_group)
        == "replicator-capture-dev"
    )
    # Every other group, the default branch included (CR 35): a leak into the default
    # would register fetch's consumer under capture's name.
    for group in (
        settings.consumer_group,
        settings.replicate_consumer_group,
        settings.persist_consumer_group,
    ):
        assert src.worker.main.consumer_name_for(settings, group) != "replicator-capture-dev"
