"""The persist loop at startup (#114 step 6): off unless an operator turns it on.

Off is not caution alone. The loop creates its consumer group at boot, and a
broker that has not granted ``content.persist`` (CannObserv/broker#64) refuses
``XGROUP CREATE`` — the one refusal that does not retry — so a loop started by
default would stop the whole worker, fetch included.
"""

import asyncio
import json

import pytest
from co_core_sync.drivers.blobstore import LocalBlobStore

import src.worker.main
from src.core.config import get_settings
from src.worker.main import run

PERSIST_TOPIC = "replicator.test.persist"


@pytest.fixture(autouse=True)
def _short_policy_poll(monkeypatch):
    """``test_main``'s reason: fakeredis honours ``block`` on the policy reader's
    groupless ``XREAD``, so without this every run waits out a full poll window."""
    monkeypatch.setenv("REPLICATOR_READ_BLOCK_MS", "50")
    get_settings.cache_clear()


def _loops_started(monkeypatch) -> tuple[asyncio.Event, list[str]]:
    """``test_main``'s ``_ended_by_the_loop``, recording each loop's stream label.

    A stop event that is not pre-set, so ``worker ready`` is logged; the stubbed
    loops return at once and end the run.
    """
    labels: list[str] = []

    async def stub_run_loop(*, spec, **kwargs):
        labels.append(spec.label)

    monkeypatch.setattr("src.worker.main.run_loop", stub_run_loop)
    return asyncio.Event(), labels


def _ready_line(capsys) -> dict:
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    (ready,) = [line for line in lines if line.get("message") == "worker ready"]
    return ready


async def test_off_by_default_the_persist_stream_is_never_touched(
    monkeypatch, fake_redis, tmp_path, capsys
):
    """No group, so no ``XGROUP CREATE … MKSTREAM`` — the stream is not even created."""
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.delenv("REPLICATOR_PERSIST_ENABLED", raising=False)
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)

    stop, labels = _loops_started(monkeypatch)

    await run(stop, persist_topic=PERSIST_TOPIC)

    assert not await fake_redis.exists(PERSIST_TOPIC)
    assert "content.persist" not in labels
    assert _ready_line(capsys)["persist"] == "disabled"


async def test_on_it_consumes_with_its_own_group_into_the_permanent_store(
    monkeypatch, fake_redis, tmp_path, capsys
):
    monkeypatch.setenv("REPLICATOR_BLOB_DIR", str(tmp_path / "blobs"))
    monkeypatch.setenv("REPLICATOR_PERSIST_ENABLED", "true")
    monkeypatch.setenv("REPLICATOR_PERMANENT_BUCKET", "a-permanent-bucket")
    get_settings.cache_clear()
    monkeypatch.setattr("src.worker.main.Redis.from_url", lambda *a, **kw: fake_redis)
    permanent = LocalBlobStore(tmp_path / "permanent")
    monkeypatch.setattr("src.worker.main.build_permanent_stores", lambda settings: (permanent,))
    captured: dict = {}
    real = src.worker.main.build_persist_handler

    def capture(**kwargs):
        captured.update(kwargs)
        return real(**kwargs)

    monkeypatch.setattr("src.worker.main.build_persist_handler", capture)

    stop, labels = _loops_started(monkeypatch)

    await run(stop, persist_topic=PERSIST_TOPIC)

    assert "content.persist" in labels
    groups = await fake_redis.xinfo_groups(PERSIST_TOPIC)
    assert [g["name"].decode() if isinstance(g["name"], bytes) else g["name"] for g in groups] == [
        "replicator.persist"
    ]
    assert captured["permanent"] is permanent
    ready = _ready_line(capsys)
    assert ready["persist"] == "enabled"
    assert ready["persist_group"] == "replicator.persist"
    assert ready["persist_consumer"] == "replicator-persist-1"
