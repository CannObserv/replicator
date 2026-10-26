"""``scripts/seed_capture.py``: one ``content.capture`` command onto a scratch stream (#138).

Scratch only. Archiver issues the live stream (archiver#380), and Replicator's
identity may not ``XADD`` there (broker#116), so the live target has no opt-in.
"""

import json

import pytest
from co_core.pure.adapters.bus.envelope import from_wire
from co_core.pure.models.changes import ContentCaptureCommand

from scripts import seed_capture
from scripts.seed_capture import build_command, main, publish, target_refusal

SCRATCH = "redis://localhost:6379/15"


def test_the_command_is_what_the_flags_say():
    args = seed_capture.build_parser().parse_args(
        [
            "--redis-url", SCRATCH, "--topic", "replicator.itest.capture",
            "--width", "800", "--height", "600", "--full-page",
            "--wait", "selector", "--selector", "#main",
            "https://example.test/",
        ]
    )  # fmt: skip

    command = build_command(args)

    assert command.url == "https://example.test/"
    assert (command.viewport.width, command.viewport.height) == (800, 600)
    assert command.full_page is True
    assert (command.wait.kind, command.wait.selector) == ("selector", "#main")
    assert command.info_source_id == seed_capture.SEED_INFO_SOURCE_ID


def test_no_wait_is_the_load_event():
    args = seed_capture.build_parser().parse_args(
        ["--redis-url", SCRATCH, "--topic", "replicator.itest.capture", "https://example.test/"]
    )

    command = build_command(args)

    assert command.wait is None
    assert command.full_page is False
    assert command.command_id != build_command(args).command_id


@pytest.mark.parametrize(
    ("url", "topic"),
    [
        ("redis://localhost:6379/0", "replicator.itest.capture"),
        ("redis://localhost:6379", "replicator.itest.capture"),
        ("redis://broker:6379/15", "replicator.itest.capture"),
        (SCRATCH, "content.capture"),
    ],
)
def test_anything_but_a_scratch_stream_on_a_loopback_scratch_db_is_refused(url, topic):
    assert target_refusal(url, topic) is not None


def test_a_scratch_target_is_accepted():
    assert target_refusal(SCRATCH, "replicator.itest.capture") is None
    assert target_refusal("unix:///run/redis.sock?db=15", "replicator.itest.capture") is None


def test_a_refused_target_exits_2_before_dialling(capsys):
    code = main(["--redis-url", SCRATCH, "--topic", "content.capture", "https://example.test/"])

    assert code == 2
    assert "content.capture" in capsys.readouterr().err


async def test_publishing_lands_a_decodable_command(fake_redis):
    args = seed_capture.build_parser().parse_args(
        ["--redis-url", SCRATCH, "--topic", "replicator.itest.capture", "https://example.test/"]
    )
    command = build_command(args)

    entry_id = await publish(fake_redis, "replicator.itest.capture", command)

    ((got_id, fields),) = await fake_redis.xrange("replicator.itest.capture")
    assert got_id.decode() == entry_id
    decoded = from_wire(
        {k.decode(): v.decode() for k, v in fields.items()},
        topic="replicator.itest.capture",
        message_id=entry_id,
    ).payload
    assert isinstance(decoded, ContentCaptureCommand)
    assert decoded.command_id == command.command_id


def test_main_publishes_and_reports_the_command(monkeypatch, fake_redis, capsys):
    monkeypatch.setattr(seed_capture.Redis, "from_url", lambda *a, **kw: fake_redis)

    code = main(
        ["--redis-url", SCRATCH, "--topic", "replicator.itest.capture", "https://example.test/"]
    )

    assert code == 0
    line = json.loads(capsys.readouterr().out)
    assert line["topic"] == "replicator.itest.capture"
    assert line["command_id"]
    assert line["entry_id"]
