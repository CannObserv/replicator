"""The patching gate's in-flight count: ``scripts/inflight.py`` (#131).

The ``patching-hosts`` knob's ``inflight`` line runs it before each apply step
and inside the reboot chain, and the gate passes only on a printed ``0``. So the
two properties that matter are that it counts **this worker's** pending entries
and nobody else's, and that any failure prints something other than ``0``.
"""

from pathlib import Path

import pytest
from co_core.pure.adapters.bus import streams
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from scripts.inflight import count_inflight, main
from src.core.config import Settings


def _faithful(client):
    """``XPENDING`` on an absent stream or group answers ``NOGROUP``, as Redis 7 does.

    fakeredis 2.37.0 returns a short reply instead, which redis-py's parser then
    indexes past (``IndexError``). ``test_redis_answers_nogroup`` holds this to
    the live server.
    """
    xpending = client.xpending

    async def faithful(name, groupname):
        groups = await client.xinfo_groups(name) if await client.exists(name) else []
        if groupname.encode() not in [g["name"] for g in groups]:
            raise ResponseError(f"NOGROUP No such key '{name}' or consumer group '{groupname}'")
        return await xpending(name, groupname)

    client.xpending = faithful
    return client


@pytest.fixture
def fake_redis(fake_redis):
    return _faithful(fake_redis)


async def _deliver(client, topic: str, group: str, consumer: str) -> None:
    """Leave one entry pending under ``consumer``: read, never acked."""
    await client.xgroup_create(topic, group, id="0", mkstream=True)
    await client.xadd(topic, {"k": "v"})
    await client.xreadgroup(group, consumer, {topic: ">"}, count=1)


async def test_no_groups_count_as_nothing_in_flight(fake_redis):
    assert await count_inflight(fake_redis, Settings()) == 0


async def test_an_unacked_fetch_entry_is_in_flight(fake_redis):
    s = Settings()
    await _deliver(fake_redis, streams.CONTENT_FETCH, s.consumer_group, "replicator-fetch-1")
    assert await count_inflight(fake_redis, s) == 1


async def test_another_consumers_entries_are_not_ours(fake_redis):
    """A dead consumer's stale PEL would otherwise hold the gate shut forever."""
    s = Settings()
    await _deliver(fake_redis, streams.CONTENT_FETCH, s.consumer_group, "replicator-fetch-2")
    assert await count_inflight(fake_redis, s) == 0


async def test_the_replicate_group_counts_too(fake_redis):
    s = Settings()
    await _deliver(
        fake_redis, streams.CONTENT_REPLICATE, s.replicate_consumer_group, "replicator-replicate-1"
    )
    assert await count_inflight(fake_redis, s) == 1


async def test_a_name_override_is_the_consumer_counted(fake_redis, monkeypatch):
    monkeypatch.setenv("REPLICATOR_CONSUMER_NAME", "replicator-fetch-7")
    s = Settings()
    await _deliver(fake_redis, streams.CONTENT_FETCH, s.consumer_group, "replicator-fetch-7")
    assert await count_inflight(fake_redis, s) == 1


async def test_persist_is_read_only_when_enabled(fake_redis, monkeypatch):
    """A disabled loop has no group, and the broker may not grant its key (broker#64)."""
    group = Settings().persist_consumer_group
    await _deliver(fake_redis, streams.CONTENT_PERSIST, group, "replicator-persist-1")
    assert await count_inflight(fake_redis, Settings()) == 0

    monkeypatch.setenv("REPLICATOR_PERSIST_ENABLED", "true")
    monkeypatch.setenv("REPLICATOR_PERMANENT_BUCKET", "scratch-bucket")
    assert await count_inflight(fake_redis, Settings()) == 1


@pytest.mark.integration
async def test_redis_answers_nogroup(real_redis):
    """What ``count_inflight`` skips, and the fake above mimics."""
    with pytest.raises(ResponseError, match="^NOGROUP"):
        await real_redis.xpending("replicator.itest.inflight-absent", "nobody")


def _env_file(tmp_path: Path, *lines: str) -> Path:
    path = tmp_path / "env"
    path.write_text("\n".join(lines) + "\n")
    return path


def test_main_prints_the_count(tmp_path, capsys, fake_redis_factory):
    env = _env_file(tmp_path, "REPLICATOR_REDIS_URL=redis://broker:6379/0")
    assert main(["--env-file", str(env)], client_factory=fake_redis_factory) == 0
    assert capsys.readouterr().out == "0\n"


def test_main_reads_the_env_file(tmp_path, capsys, fake_redis_factory):
    """The reboot chain runs it through ``runuser`` with no session environment."""
    env = _env_file(tmp_path, "REPLICATOR_REDIS_URL=redis://broker:6379/0")
    seen = []

    def factory(settings):
        seen.append(settings.redis_url)
        return fake_redis_factory(settings)

    main(["--env-file", str(env)], client_factory=factory)
    assert seen == ["redis://broker:6379/0"]


def test_a_missing_env_file_fails_closed(tmp_path, capsys, fake_redis_factory):
    """Without it the defaults point at localhost, and a 0 from there means nothing."""
    code = main(["--env-file", str(tmp_path / "absent")], client_factory=fake_redis_factory)
    out = capsys.readouterr()
    assert code != 0
    assert out.out == ""
    assert "absent" in out.err


def test_a_broker_error_fails_closed(tmp_path, capsys):
    env = _env_file(tmp_path, "REPLICATOR_REDIS_URL=redis://broker:6379/0")

    def factory(settings):
        raise RedisConnectionError("Error connecting to broker:6379")

    code = main(["--env-file", str(env)], client_factory=factory)
    out = capsys.readouterr()
    assert code != 0
    assert out.out == ""
    assert "ConnectionError" in out.err


def test_the_error_line_never_carries_the_credential(tmp_path, capsys):
    env = _env_file(tmp_path, "REPLICATOR_REDIS_URL=redis://replicator:s3cret@broker:6379/0")

    def factory(settings):
        raise RedisConnectionError(f"cannot reach {settings.redis_url}")

    main(["--env-file", str(env)], client_factory=factory)
    assert "s3cret" not in capsys.readouterr().err


@pytest.fixture
def fake_redis_factory():
    from fakeredis.aioredis import FakeRedis

    return lambda settings: _faithful(FakeRedis(decode_responses=False))
