"""Publish one ``content.capture`` command to a scratch stream (#138).

``seed_fetch.py``'s job for the capture loop, and only the scratch half of it:
Archiver issues the live stream (archiver#380), and this host's ``replicator``
identity may not ``XADD`` there (broker#116), so the live target has no opt-in.
Refused before anything is dialled: the stream ``content.capture``, database 0,
and any host that is not loopback.

    uv run python -m scripts.seed_capture \\
        --redis-url redis://localhost:6379/15 --topic replicator.itest.capture \\
        --width 1280 --height 800 --full-page https://example.test/

Point a dev worker at the same database with ``REPLICATOR_CAPTURE_ENABLED=true``
and its ``capture_topic`` moved to the scratch stream (``docs/COMMANDS.md``), then
read ``content.artifacts`` on that database for the outcome.

Exit codes: ``0`` published · ``1`` publishing failed · ``2`` the target was refused.
"""

import argparse
import asyncio
import ipaddress
import json
import sys
from datetime import UTC, datetime

from co_core.effects.bus import BusPublish
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import (
    CaptureViewportEmit,
    CaptureWaitEmit,
    ContentCaptureCommandEmit,
)
from co_core_aio.bus import AsyncBusPublisher
from pydantic import TypeAdapter
from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import RedisError
from ulid import ULID

# A discriminated union, not a class: validated, never called.
_WAIT = TypeAdapter(CaptureWaitEmit)

# Not a real InfoSource id, so the facts a seed run produces read as synthetic.
SEED_INFO_SOURCE_ID = "seed-capture-placeholder"


def target_refusal(url: str, topic: str) -> str | None:
    """Why this harness must not publish to ``url``/``topic``, or ``None``."""
    if topic == streams.CONTENT_CAPTURE:
        return f"{topic} is Archiver's to issue (archiver#380); use a scratch stream"
    kwargs = ConnectionPool.from_url(url).connection_kwargs
    host = kwargs.get("host")
    if host is not None and host != "localhost":
        try:
            loopback = ipaddress.ip_address(host).is_loopback
        except ValueError:
            loopback = False
        if not loopback:
            return f"redis host {host!r} is not loopback: run a scratch redis-server"
    if kwargs.get("db", 0) == 0:
        return "database 0 is the broker's live one: use a scratch database"
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--redis-url", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=800)
    parser.add_argument("--full-page", action="store_true")
    parser.add_argument("--wait", choices=["networkidle", "selector", "delay"])
    parser.add_argument("--selector")
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--info-source-id", default=SEED_INFO_SOURCE_ID)
    parser.add_argument("url")
    return parser


def build_command(args: argparse.Namespace) -> ContentCaptureCommandEmit:
    """A fresh ULID ``command_id`` per call: a reused one is deduped into a no-op."""
    wait = None
    if args.wait is not None:
        fields = {"kind": args.wait, "selector": args.selector, "seconds": args.seconds}
        # Each kind refuses the others' fields, even as None.
        wait = _WAIT.validate_python({k: v for k, v in fields.items() if v is not None})
    return ContentCaptureCommandEmit(
        occurred_at=datetime.now(UTC),
        command_id=str(ULID()),
        url=args.url,
        info_source_id=args.info_source_id,
        viewport=CaptureViewportEmit(width=args.width, height=args.height),
        full_page=args.full_page,
        wait=wait,
    )


async def publish(client: Redis, topic: str, command: ContentCaptureCommandEmit) -> str:
    """XADD the frame; the entry id Redis assigned."""
    result = await AsyncBusPublisher(client).execute(BusPublish(topic, to_wire(command)))
    return result.bus_message_id


async def _run(args: argparse.Namespace) -> int:
    command = build_command(args)
    client = Redis.from_url(args.redis_url)
    try:
        entry_id = await publish(client, args.topic, command)
    except RedisError as exc:
        print(f"publishing failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        await client.aclose()
    print(json.dumps({"topic": args.topic, "command_id": command.command_id, "entry_id": entry_id}))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    refusal = target_refusal(args.redis_url, args.topic)
    if refusal is not None:
        print(f"refused: {refusal}", file=sys.stderr)
        return 2
    return asyncio.run(_run(args))


if __name__ == "__main__":
    sys.exit(main())
