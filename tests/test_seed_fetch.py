"""The MVP command issuer: ``scripts/seed_fetch.py``.

The script is the only thing in the repo that *writes* to ``content.fetch``, and
the live worker fetches whatever lands there for real. Its guard rail therefore
gets as much test attention as its publishing does.

Frames are decoded with co-core's own ``from_wire`` rather than by reading raw
fields, so a producer-side envelope change breaks these tests instead of leaving
the script quietly publishing something the worker cannot decode.
"""

from datetime import UTC, datetime

import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire, to_wire
from co_core.pure.models.changes import BlobAvailableEvent, ContentFetchCommand
from ulid import ULID

from scripts.seed_fetch import (
    ProductionTargetError,
    build_parser,
    guard_production_target,
    last_id,
    main,
    publish,
    watch_for_facts,
)

TOPIC = "replicator.itest.fetch"
BLOBS = "replicator.itest.blobs"
URL = "https://example.test/a"


async def decoded_commands(client, topic: str = TOPIC) -> list[ContentFetchCommand]:
    """Every frame on ``topic``, decoded the way the worker would decode it."""
    commands = []
    for message_id, fields in await client.xrange(topic):
        payload = from_wire(
            {k.decode(): v.decode() for k, v in fields.items()},
            topic=topic,
            message_id=message_id.decode(),
        ).payload
        assert isinstance(payload, ContentFetchCommand)
        commands.append(payload)
    return commands


async def add_fact(client, command_id: str, topic: str = BLOBS) -> bytes:
    """XADD a ``blob_available`` frame as the worker's handler would."""
    return await client.xadd(
        topic,
        to_wire(
            BlobAvailableEvent(
                occurred_at=datetime.now(UTC),
                content_fingerprint="f" * 64,
                blob_uri="file:///tmp/blobs/ff/ff/" + "f" * 64 + ".bin",
                size_bytes=3,
                media_type="text/html",
                url=URL,
                command_id=command_id,
            )
        ),
    )


async def test_publishing_lands_a_decodable_command(fake_redis):
    results = await publish(fake_redis, TOPIC, [URL])

    (command,) = await decoded_commands(fake_redis)
    assert command.url == URL
    assert command.command_id == results[0].command_id


async def test_the_entry_id_reported_is_the_one_redis_assigned(fake_redis):
    """The operator correlates the journal to the stream by this id."""
    (result,) = await publish(fake_redis, TOPIC, [URL])

    ((message_id, _),) = await fake_redis.xrange(TOPIC)
    assert result.bus_message_id == message_id.decode()


async def test_each_url_gets_its_own_ulid_command_id(fake_redis):
    """ULID is the cluster's identifier convention, and one per URL is the point.

    A shared ``command_id`` across a multi-URL run would make the second command
    a dedupe no-op: the worker keys ``replicator:cmd:*`` on exactly this value.
    """
    urls = [URL, "https://example.test/b", "https://example.test/c"]

    results = await publish(fake_redis, TOPIC, urls)

    assert [r.url for r in results] == urls
    assert len({r.command_id for r in results}) == 3
    for result in results:
        assert ULID.from_str(result.command_id)


async def test_publishing_preserves_the_order_the_urls_were_given(fake_redis):
    urls = [URL, "https://example.test/b"]

    await publish(fake_redis, TOPIC, urls)

    assert [c.url for c in await decoded_commands(fake_redis)] == urls


def test_the_live_command_stream_on_the_live_database_is_refused():
    """The one combination the running service picks up: db 0 + content.fetch."""
    with pytest.raises(ProductionTargetError):
        guard_production_target(streams.CONTENT_FETCH, db=0, production=False)


def test_the_live_target_is_allowed_with_an_explicit_opt_in():
    guard_production_target(streams.CONTENT_FETCH, db=0, production=True)


def test_the_command_stream_on_a_scratch_database_is_allowed():
    """No worker is polling db 15 — that stream reaches nothing."""
    guard_production_target(streams.CONTENT_FETCH, db=15, production=False)


def test_a_scratch_stream_on_the_live_database_is_allowed():
    """Nothing consumes ``replicator.itest.*``; the guard is about reach, not db."""
    guard_production_target(TOPIC, db=0, production=False)


@pytest.mark.parametrize(
    "argv",
    [
        ["--topic", TOPIC, URL],
        ["--redis-url", "redis://localhost:6379/15", URL],
        ["--redis-url", "redis://localhost:6379/15", "--topic", TOPIC],
    ],
)
def test_the_target_and_at_least_one_url_are_all_required(argv):
    """No defaults: a no-argument run must never mean "publish to production"."""
    with pytest.raises(SystemExit) as excinfo:
        build_parser().parse_args(argv)

    assert excinfo.value.code == 2


def test_a_dry_run_never_opens_a_connection(capsys):
    """The frame is printed and nothing is contacted — hence an unroutable port."""
    code = main(
        ["--redis-url", "redis://localhost:1/0", "--topic", streams.CONTENT_FETCH, "--dry-run", URL]
    )

    assert code == 0
    assert URL in capsys.readouterr().out


def test_a_refused_target_names_the_flag_that_would_allow_it(capsys):
    """The guard fires before the client is opened, so the port is unroutable too."""
    code = main(["--redis-url", "redis://localhost:1/0", "--topic", streams.CONTENT_FETCH, URL])

    assert code == 2
    assert "--production" in capsys.readouterr().err


async def test_the_last_id_of_an_empty_stream_reads_from_the_beginning(fake_redis):
    assert await last_id(fake_redis, BLOBS) == "0-0"


async def test_the_last_id_of_a_populated_stream_is_its_final_entry(fake_redis):
    await add_fact(fake_redis, "cmd-1")
    final = await add_fact(fake_redis, "cmd-2")

    assert await last_id(fake_redis, BLOBS) == final.decode()


async def test_watching_returns_the_fact_for_the_command_it_was_given(fake_redis):
    start = await last_id(fake_redis, BLOBS)
    await add_fact(fake_redis, "cmd-1")

    (fact,) = await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=1)

    assert fact.command_id == "cmd-1"
    assert fact.url == URL


async def test_watching_ignores_facts_for_other_commands(fake_redis):
    """A shared fact stream carries every issuer's blobs, not just this run's."""
    start = await last_id(fake_redis, BLOBS)
    await add_fact(fake_redis, "someone-elses")
    await add_fact(fake_redis, "cmd-1")

    facts = await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=1)

    assert [f.command_id for f in facts] == ["cmd-1"]


async def test_watching_skips_facts_that_predate_the_run(fake_redis):
    """The start id is captured *before* publishing, and it must actually be used.

    Read from ``0-0`` instead and a stale fact for a reused command_id would be
    reported as this run's result.
    """
    await add_fact(fake_redis, "cmd-1")
    start = await last_id(fake_redis, BLOBS)

    assert await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=0.2) == []


async def test_watching_gives_up_at_the_timeout(fake_redis):
    """A fact that never arrives must not hang an operator's terminal."""
    start = await last_id(fake_redis, BLOBS)

    assert await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=0.2) == []


async def test_watching_leaves_no_consumer_group_behind(fake_redis):
    """``content.blobs`` is Archiver-operated: a stray group's PEL grows forever.

    A plain XREAD reads without joining a group at all, which is what keeps an
    operator tool from leaving state on shared infrastructure.
    """
    start = await last_id(fake_redis, BLOBS)
    await add_fact(fake_redis, "cmd-1")

    await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=1)

    assert await fake_redis.xinfo_groups(BLOBS) == []
