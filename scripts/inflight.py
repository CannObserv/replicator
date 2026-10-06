"""Print how many commands this worker holds unacked — the patching gate (#131).

The ``patching-hosts`` knob's ``inflight`` line runs this before every apply step
and again inside the reboot chain, and the gate passes only on a printed ``0``::

    cd /home/exedev/replicator && .venv/bin/python -m scripts.inflight

**This worker's entries, not the group's.** ``XPENDING``'s summary form names
each consumer's count, and only the names ``consumer_name_for`` gives this
host's loops are summed. A group-wide count would include a dead consumer's
stale entries, which only an ``XAUTOCLAIM`` at ``min_idle_time`` ever clears, and
hold the gate shut until then. The persist group is read only when the loop is
enabled: a disabled loop never created it, and the broker may not grant its key
(broker#64) — a denied probe lands in its ``ACL LOG``.

**It fails closed.** Any error prints nothing on stdout and exits non-zero, so the
gate can never read a failed read as ``0``. That includes a missing env file:
without ``/etc/replicator/.env`` the settings default to a localhost broker, and
a ``0`` from there says nothing about this worker. The reboot chain runs knob
commands through ``runuser`` with no session environment, which is why the file
is read here rather than inherited.

A graceful stop drains the message in flight anyway (``deploy/replicator.service``),
and at-least-once delivery makes an interrupted one a redelivery, not a loss. The
gate is the cheap belt to those braces.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from urllib.parse import urlsplit

from co_core.pure.adapters.bus import streams
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from src.core.bus_client import build_bus_client
from src.core.config import Settings
from src.worker.main import consumer_name_for

DEFAULT_ENV_FILE = "/etc/replicator/.env"


def _groups(settings: Settings) -> list[tuple[str, str]]:
    """Each (stream, group) this host's worker consumes, as ``run`` wires them."""
    groups = [
        (streams.CONTENT_FETCH, settings.consumer_group),
        (streams.CONTENT_REPLICATE, settings.replicate_consumer_group),
    ]
    if settings.persist_enabled:
        groups.append((streams.CONTENT_PERSIST, settings.persist_consumer_group))
    return groups


async def count_inflight(client: Redis, settings: Settings) -> int:
    """Sum this worker's pending entries across the groups it consumes."""
    total = 0
    for topic, group in _groups(settings):
        try:
            summary = await client.xpending(topic, group)
        except ResponseError as exc:
            # No group: nothing was ever delivered from it, so nothing is pending.
            if str(exc).startswith("NOGROUP"):
                continue
            raise
        ours = consumer_name_for(settings, group)
        for consumer in summary.get("consumers") or []:
            name = consumer["name"]
            if isinstance(name, bytes):
                name = name.decode()
            if name == ours:
                total += int(consumer["pending"])
    return total


def _scrub(message: str, redis_url: str) -> str:
    """Drop the broker password from an error line before it reaches a log."""
    password = urlsplit(redis_url).password
    return message.replace(password, "***") if password else message


async def _read(settings: Settings, client_factory: Callable[[Settings], Redis]) -> int:
    client = client_factory(settings)
    try:
        return await count_inflight(client, settings)
    finally:
        await client.aclose()


def main(
    argv: Sequence[str] | None = None,
    *,
    client_factory: Callable[[Settings], Redis] = build_bus_client,
) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env-file", default=DEFAULT_ENV_FILE)
    args = parser.parse_args(argv)

    env_file = Path(args.env_file)
    redis_url = ""
    try:
        # pydantic-settings skips a missing env file silently; this must not.
        env_file.read_text()
        settings = Settings(_env_file=env_file)
        redis_url = settings.redis_url
        count = asyncio.run(_read(settings, client_factory))
    except Exception as exc:  # noqa: BLE001 — every failure must fail the gate
        detail = _scrub(f"{type(exc).__name__}: {exc}", redis_url)
        print(f"inflight: {detail}", file=sys.stderr)
        return 1
    print(count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
