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
from co_core.pure.models.changes import (
    BlobAvailableEvent,
    ContentFetchCommand,
    FetchFailedEvent,
)
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from ulid import ULID

from scripts.seed_fetch import (
    ProductionTargetError,
    SeedResult,
    _report_facts,
    build_parser,
    guard_production_target,
    last_id,
    main,
    publish,
    resolve_blobs_topic,
    resolve_db,
    run,
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


def fact_fields(command_id: str) -> dict[str, str]:
    """The wire frame the worker's handler publishes for a handled command.

    Carries the enriched fetch metadata (#10) as well, because the claim in that
    first line is what the helper is for: a sample missing six of the fields the
    handler actually sends is a quietly wrong model of the wire, and the watch
    output is read against it.
    """
    return to_wire(
        BlobAvailableEvent(
            occurred_at=datetime.now(UTC),
            content_fingerprint="f" * 64,
            blob_uri="file:///tmp/blobs/ff/ff/" + "f" * 64 + ".bin",
            size_bytes=3,
            media_type="text/html",
            url=URL,
            command_id=command_id,
            final_url=URL,
            status_code=200,
            fetched_at=datetime.now(UTC),
            content_type_raw="text/html; charset=utf-8",
            etag='W/"abc-123"',
            last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
        )
    )


async def add_fact(client, command_id: str, topic: str = BLOBS) -> bytes:
    """XADD a ``blob_available`` frame as the worker's handler would."""
    return await client.xadd(topic, fact_fields(command_id))


async def add_failure(client, command_id: str, topic: str = BLOBS) -> bytes:
    """XADD a ``fetch_failed`` frame as the worker's reporter would (#9)."""
    return await client.xadd(
        topic,
        to_wire(
            FetchFailedEvent(
                occurred_at=datetime.now(UTC),
                command_id=command_id,
                url=URL,
                reason="http_status",
                terminal=True,
                status_code=404,
            )
        ),
    )


def seed_args(*argv: str):
    """Parse a seed invocation, with the required target filled in."""
    return build_parser().parse_args(["--redis-url", "redis://fake/15", *argv])


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


async def test_watching_closes_a_command_on_a_failure_fact(fake_redis):
    """#9: the failure is now an outcome, not a timeout the operator waits out.

    ``content.blobs`` carries both outcomes, so the watch has to accept either —
    filtering to ``blob_available`` would render the exact case ``fetch_failed``
    exists to make visible as the silence it replaced.
    """
    start = await last_id(fake_redis, BLOBS)
    await add_failure(fake_redis, "cmd-1")

    (fact,) = await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=1)

    assert isinstance(fact, FetchFailedEvent)
    assert fact.command_id == "cmd-1"
    assert fact.reason == "http_status"


async def test_watching_ignores_a_failure_for_another_issuers_command(fake_redis):
    start = await last_id(fake_redis, BLOBS)
    await add_failure(fake_redis, "someone-elses")
    await add_fact(fake_redis, "cmd-1")

    facts = await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=1)

    assert [f.command_id for f in facts] == ["cmd-1"]


async def test_a_failure_is_reported_and_exits_non_zero(fake_redis, capsys):
    """A closed-with-a-reason command is still a failed seed — but a *named* one.

    The distinction the operator gets that MUST-6's silence never could: the
    reason, rather than "nothing arrived within the timeout".
    """
    start = await last_id(fake_redis, BLOBS)
    await add_failure(fake_redis, "cmd-1")

    code = await _report_facts(
        fake_redis,
        blobs_topic=BLOBS,
        timeout_seconds=1,
        start_id=start,
        results=[SeedResult(command_id="cmd-1", url=URL, bus_message_id="1-1")],
    )

    out = capsys.readouterr().out
    assert code == 1
    assert "fetch_failed" in out
    assert "reason=http_status" in out
    assert "status=404" in out


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("redis://localhost:6379/15", 15),
        # A ?db= query parameter overrides the path, and a path-less or
        # unix-socket URL has no database to read at all — redis-py's own
        # default is 0, which is exactly the one the guard cares about.
        ("redis://localhost:6379/0?db=7", 7),
        ("redis://localhost:6379", 0),
        ("unix:///tmp/redis.sock", 0),
    ],
)
def test_the_database_is_the_one_redis_py_resolved(url, expected):
    """Read from the client, not parsed out of the URL (CR #2)."""
    assert resolve_db(Redis.from_url(url)) == expected


@pytest.mark.parametrize(
    ("topic", "override", "expected"),
    [
        (streams.CONTENT_FETCH, None, streams.CONTENT_BLOBS),
        (TOPIC, None, f"{TOPIC}.blobs"),
        (TOPIC, "somewhere.else", "somewhere.else"),
        (streams.CONTENT_FETCH, "somewhere.else", "somewhere.else"),
    ],
)
def test_the_fact_stream_default_follows_the_command_stream(topic, override, expected):
    """A scratch seed watched the production fact stream and found nothing (CR #5).

    Pairing the default with ``--topic`` makes the scratch case work unattended,
    and matches how the integration fixtures name their streams.
    """
    assert resolve_blobs_topic(topic, override) == expected


async def test_watching_leaves_no_consumer_group_behind(fake_redis):
    """``content.blobs`` is Archiver-operated: a stray group's PEL grows forever.

    A plain XREAD reads without joining a group at all, which is what keeps an
    operator tool from leaving state on shared infrastructure.
    """
    start = await last_id(fake_redis, BLOBS)
    await add_fact(fake_redis, "cmd-1")

    await watch_for_facts(fake_redis, BLOBS, start, {"cmd-1"}, timeout_seconds=1)

    assert await fake_redis.xinfo_groups(BLOBS) == []


@pytest.fixture
def owned_client(fake_redis, monkeypatch) -> list[bool]:
    """Hand ``run()`` the fake broker, and record that it closed it.

    ``run()`` opens its own client — bus clients are injection-only, so the
    script owns one for its run — which is what makes it awkward to test and
    worth testing (CR #2). The returned list is the close receipt.
    """
    closed: list[bool] = []

    async def aclose():
        closed.append(True)

    monkeypatch.setattr(fake_redis, "aclose", aclose)
    monkeypatch.setattr(Redis, "from_url", staticmethod(lambda url, **kwargs: fake_redis))
    return closed


@pytest.fixture
def instant_worker(fake_redis, monkeypatch) -> None:
    """A worker so fast the fact exists the moment the command is published.

    The worst case for the start-id ordering, made deterministic: no sleeping, no
    racing a background task.
    """
    original = fake_redis.xadd

    async def xadd(name, fields, **kwargs):
        message_id = await original(name, fields, **kwargs)
        if name == TOPIC:
            command = from_wire(fields, topic=name, message_id=message_id.decode()).payload
            # from_wire's dispatch table is global, so the payload type is a
            # union until something narrows it — the same check the script makes.
            assert isinstance(command, ContentFetchCommand)
            await original(f"{TOPIC}.blobs", fact_fields(command.command_id))
        return message_id

    monkeypatch.setattr(fake_redis, "xadd", xadd)


async def test_a_run_publishes_and_closes_the_client_it_opened(fake_redis, owned_client):
    code = await run(seed_args("--topic", TOPIC, URL))

    assert code == 0
    assert [command.url for command in await decoded_commands(fake_redis)] == [URL]
    assert owned_client == [True]


async def test_a_refused_target_publishes_nothing_and_still_closes(fake_redis, owned_client):
    """The guard fires before the first XADD, not after a partial run."""
    code = await run(seed_args("--topic", streams.CONTENT_FETCH, URL))

    assert code == 2
    assert await fake_redis.xlen(streams.CONTENT_FETCH) == 0
    assert owned_client == [True]


async def test_a_run_reports_each_command_as_it_is_published(fake_redis, owned_client, capsys):
    """Reported as they go, not in a batch at the end — see the test below."""
    urls = [URL, "https://example.test/b", "https://example.test/c"]

    assert await run(seed_args("--topic", TOPIC, *urls)) == 0

    out = capsys.readouterr().out
    for command in await decoded_commands(fake_redis):
        assert command.command_id in out
        assert command.url in out


async def test_a_failure_partway_through_still_names_what_went_out(
    fake_redis, owned_client, monkeypatch, capsys
):
    """A published command is in flight whether or not the run finished (CR #10).

    The live worker will fetch it. An operator who cannot see its ``command_id``
    cannot correlate the journal, cannot dedupe a retry, and has no way to learn
    which of their URLs is already on the stream. The count on stderr is the
    summary: how much of the run actually landed.
    """
    original = fake_redis.xadd
    attempts: list[str] = []

    async def failing_xadd(name, fields, **kwargs):
        attempts.append(name)
        if len(attempts) == 2:
            raise RedisConnectionError("broker went away mid-loop")
        return await original(name, fields, **kwargs)

    monkeypatch.setattr(fake_redis, "xadd", failing_xadd)
    urls = [URL, "https://example.test/b", "https://example.test/c"]

    code = await run(seed_args("--topic", TOPIC, *urls))

    assert code == 1
    (survivor,) = await decoded_commands(fake_redis)
    captured = capsys.readouterr()
    assert survivor.command_id in captured.out
    assert "1 of 3" in captured.err


async def test_a_watch_that_cannot_read_is_an_error_not_a_traceback(
    fake_redis, owned_client, monkeypatch, capsys
):
    """The commands are already out; only the watching failed, and it says so."""

    async def failing_xread(*args, **kwargs):
        raise RedisConnectionError("broker went away mid-watch")

    monkeypatch.setattr(fake_redis, "xread", failing_xread)

    code = await run(seed_args("--topic", TOPIC, "--watch", "--watch-timeout", "1", URL))

    assert code == 1
    assert f"watching {resolve_blobs_topic(TOPIC, None)} failed" in capsys.readouterr().err


async def test_a_broker_that_fails_before_the_cursor_read_publishes_nothing(
    fake_redis, owned_client, monkeypatch, capsys
):
    """The cursor read is its own step, so its failure says what it cost (CR #15).

    It runs before the first XADD, so nothing went out — which is the operator's
    actionable fact, and the one a "publishing failed after 0 of 1" would have
    buried under the wrong cause.
    """

    async def failing_xrevrange(*args, **kwargs):
        raise RedisConnectionError("broker went away before the seed")

    monkeypatch.setattr(fake_redis, "xrevrange", failing_xrevrange)

    code = await run(seed_args("--topic", TOPIC, "--watch", URL))

    assert code == 1
    assert await fake_redis.xlen(TOPIC) == 0
    assert "nothing was published" in capsys.readouterr().err


async def test_a_watched_run_reports_the_fact_for_the_command_it_published(
    fake_redis, owned_client, instant_worker, capsys
):
    """The start id has to be captured *before* publishing (CR #2).

    The fake worker publishes the fact the instant the command lands. Capture the
    cursor afterwards and it is already past that fact, so the watch would sit
    out its whole timeout waiting for something that arrived while it was looking
    away — and report a working loop as a failure.
    """
    code = await run(seed_args("--topic", TOPIC, "--watch", "--watch-timeout", "2", URL))

    assert code == 0
    out = capsys.readouterr().out
    assert "blob_available" in out
    assert "f" * 64 in out
    # The two enriched fields the line carries (#10). They are on it because an
    # operator checks a live fetch against them — a 203 where a 200 was
    # expected, a redirect nobody knew about — so an unasserted format leaves
    # the one change aimed at an operator's eyes covered by nothing.
    assert "status=200" in out
    assert f"final_url={URL}" in out


async def test_a_watched_run_that_sees_no_fact_reports_the_command_that_is_missing(
    fake_redis, owned_client, capsys
):
    """Nothing consumes the scratch stream here, so the watch runs out of time."""
    code = await run(seed_args("--topic", TOPIC, "--watch", "--watch-timeout", "0.2", URL))

    assert code == 1
    published = (await decoded_commands(fake_redis))[0]
    assert published.command_id in capsys.readouterr().err


def test_an_unusable_redis_url_is_an_error_not_a_traceback(capsys):
    """An operator tool answers with a line and an exit code, not a stack (CR #3)."""
    code = main(["--redis-url", "not-a-url", "--topic", TOPIC, URL])

    assert code == 1
    assert "error:" in capsys.readouterr().err


def test_a_broker_that_refuses_the_connection_is_an_error_not_a_traceback(capsys):
    """Port 1 refuses immediately — the same shape as a wrong --redis-url."""
    code = main(["--redis-url", "redis://localhost:1/15", "--topic", TOPIC, URL])

    assert code == 1
    assert "error:" in capsys.readouterr().err


async def test_published_commands_carry_the_request_options(fake_redis):
    """The only issuer there is, so the only way to exercise #11 on a live worker."""
    await publish(
        fake_redis,
        TOPIC,
        [URL],
        headers={"User-Agent": "watcher/0.1.0"},
        timeout_seconds=2.5,
    )

    (command,) = await decoded_commands(fake_redis)
    assert command.headers == {"User-Agent": "watcher/0.1.0"}
    assert command.timeout_seconds == 2.5


async def test_a_command_without_options_carries_neither_field(fake_redis):
    """Omitted stays omitted: the pre-#11 wire, byte for byte."""
    await publish(fake_redis, TOPIC, [URL])

    (command,) = await decoded_commands(fake_redis)
    assert command.headers is None
    assert command.timeout_seconds is None


def test_repeated_header_flags_collect_into_one_mapping():
    args = seed_args("--topic", TOPIC, "--header", "Accept: text/html", "--header", "X-A: b", URL)

    assert args.headers == {"Accept": "text/html", "X-A": "b"}


def test_a_header_value_may_contain_a_colon():
    """Split on the first colon only — a Referer is the obvious case."""
    args = seed_args("--topic", TOPIC, "--header", "Referer: https://x.test/a", URL)

    assert args.headers == {"Referer": "https://x.test/a"}


@pytest.mark.parametrize("header", ["no-colon", ": novalue"])
def test_a_malformed_header_is_a_usage_error(header):
    with pytest.raises(SystemExit) as excinfo:
        seed_args("--topic", TOPIC, "--header", header, URL)

    assert excinfo.value.code == 2


def test_a_repeated_header_name_is_a_usage_error():
    """Last-wins would discard one silently — the worker refuses the same shape."""
    with pytest.raises(SystemExit) as excinfo:
        seed_args("--topic", TOPIC, "--header", "Accept: a", "--header", "accept: b", URL)

    assert excinfo.value.code == 2


def test_no_header_flag_means_no_headers():
    args = seed_args("--topic", TOPIC, URL)

    assert args.headers is None
    assert args.timeout_seconds is None


def test_a_dry_run_shows_the_options_that_would_travel(capsys):
    code = main(
        [
            "--redis-url",
            "redis://localhost:1/0",
            "--topic",
            TOPIC,
            "--dry-run",
            "--header",
            "User-Agent: watcher/0.1.0",
            "--timeout",
            "5",
            URL,
        ]
    )

    assert code == 0
    printed = capsys.readouterr().out
    assert "watcher/0.1.0" in printed
    assert "5.0" in printed
