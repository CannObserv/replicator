"""Publish ``content.fetch`` commands — the MVP's command issuer.

Replicator's loop is driven by commands, and until Watcher is cut over (parent
strategy Phase 4) nothing in the cluster issues them. This script is that issuer:
it mints a ULID ``command_id`` per URL and XADDs a ``ContentFetchCommand`` frame
built by co-core's own ``to_wire``. It is not scaffolding — it is also what drives
the live end-to-end test, so it earns a permanent place next to
``sync_wheelhouse.py`` and ``check_redis_floor.sh``. Design:
``docs/plans/2026-07-31-replicator-mvp-open-questions-design.md`` §2.

    uv run python -m scripts.seed_fetch \
        --redis-url redis://localhost:6379/15 \
        --topic replicator.itest.fetch \
        https://example.test/a

**The live worker fetches whatever lands on ``content.fetch``.** A frame added to
that stream on db 0 is picked up by ``replicator.service``, fetched over the
network, and written to the blob directory. So the target is never defaulted —
``--redis-url`` and ``--topic`` are both required, and the one combination that
actually reaches the running service (db 0 *and* ``content.fetch``) additionally
requires ``--production``. A flag rather than a prompt: the script has to stay
usable non-interactively.

``--watch`` tails the fact stream so a human can see the loop close without
hand-writing ``XRANGE``. It accepts **either** outcome — ``blob_available`` or,
since #9, ``fetch_failed`` — because a watch that recognized only success would
report a named, terminal failure as an indistinguishable timeout. It reads with a
plain ``XREAD`` and never joins a consumer group: ``content.blobs`` is
Archiver-operated, and a group left behind by an operator tool accumulates a
pending entries list nothing will ever drain. The stream it watches follows
``--topic`` unless overridden, so seeding a scratch stream watches that stream's
facts rather than production's.

Exit codes: ``0`` published (and, under ``--watch``, every command produced a
blob) · ``1`` the run did not complete — publishing failed, watching failed, a
command was closed by a ``fetch_failed``, or no fact ever arrived · ``2`` the
target was refused. Commands are reported on stdout as they land, so a non-zero
exit never hides a command it saw land — a connection lost between the ``XADD``
and its reply is the one gap, and the "N of M" count on stderr is what marks that
boundary as fuzzy.
"""

import argparse
import asyncio
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from co_core.effects.bus import BusPublish
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire, to_wire
from co_core.pure.models.changes import (
    BlobAvailableEvent,
    ContentFetchCommand,
    FetchFailedEvent,
)
from co_core_aio.bus import AsyncBusPublisher
from redis.asyncio import Redis
from redis.exceptions import RedisError
from ulid import ULID

# How long the watch parks when a blocking read comes back empty. Real Redis
# blocks for the full window and this is never reached; it is insurance against a
# client that ignores `block` (fakeredis does), which would otherwise busy-spin.
# Mirrors src/worker/loop.py::IDLE_SLEEP_SECONDS.
IDLE_SLEEP_SECONDS = 0.05

DEFAULT_WATCH_TIMEOUT_SECONDS = 30.0

# Either outcome of a command. ``content.blobs`` has carried both since #9, so
# "the fact for this command_id" is no longer synonymous with "the blob".
Fact = BlobAvailableEvent | FetchFailedEvent


class ProductionTargetError(RuntimeError):
    """The requested target is the live command stream and no opt-in was given."""


@dataclass(frozen=True)
class SeedResult:
    """One published command: what it asked for, and where it landed."""

    command_id: str
    url: str
    bus_message_id: str


def build_command(
    url: str,
    headers: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
) -> ContentFetchCommand:
    """Mint a command for one URL.

    The ``command_id`` is a ULID — the cluster's identifier convention — minted
    fresh on every call, so it is unique per URL *and* per invocation. The
    worker's dedupe key is ``replicator:cmd:<command_id>``: an id shared across
    the URLs of one run would make every URL after the first a no-op, and an id
    stable across runs would make every run after the first a no-op. Both are
    silent — the worker acks and fetches nothing. See
    ``docs/contracts/content-fetch-issuer-contract.md`` MUST-1.

    ``headers`` / ``timeout_seconds`` apply to every URL in the run and default
    to ``None`` — the omitted-field shape, which is the worker's pre-#11
    behaviour exactly.
    """
    return ContentFetchCommand(
        occurred_at=datetime.now(UTC),
        command_id=str(ULID()),
        url=url,
        headers=headers,
        timeout_seconds=timeout_seconds,
    )


class HeaderAction(argparse.Action):
    """Collect repeated ``--header 'Name: value'`` into a mapping as they parse.

    An action rather than a post-processing step so ``args.headers`` is the final
    mapping everywhere it is read, and a malformed argument exits 2 through
    argparse's own error path instead of a second hand-rolled one.

    Splits on the **first** colon only: a value may contain one (a URL in a
    ``Referer``, a port in a custom header) while a name may not.

    A repeated name is a usage error rather than last-wins, because a dict would
    otherwise swallow one silently — the same reasoning that makes the worker
    refuse a case-collision (#11). What this deliberately does *not* do is
    duplicate the worker's guard list: sending a refused header is how an
    operator exercises the refusal against a live worker, and a script that
    pre-empted it would leave that path testable only in the unit suite.
    """

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,
    ) -> None:
        # The signature is argparse's, widened for every `nargs`. This action
        # declares none, so argparse only ever hands over the single string.
        value = str(values)
        headers = getattr(namespace, self.dest, None) or {}
        name, separator, header_value = value.partition(":")
        name = name.strip()
        if not separator or not name:
            raise argparse.ArgumentError(self, f"{value!r} is not a 'Name: value' header")
        if name.lower() in {existing.lower() for existing in headers}:
            raise argparse.ArgumentError(self, f"{name!r} was given more than once")
        headers[name] = header_value.strip()
        setattr(namespace, self.dest, headers)


def resolve_db(client: Redis) -> int:
    """The database redis-py actually resolved for this client.

    Read from the connection kwargs rather than parsed out of the URL: a ``?db=``
    query parameter overrides the path, and a unix-socket URL has no path to
    inspect at all. Missing means 0 — redis-py's own default. Same reasoning as
    the ``real_redis`` fixture's guard in ``tests/conftest.py``.
    """
    return int(client.connection_pool.connection_kwargs.get("db") or 0)


def guard_production_target(topic: str, *, db: int, production: bool) -> None:
    """Refuse the live command stream unless the caller opted in.

    The gate is the *conjunction*, because that is what determines reach:
    ``content.fetch`` on a scratch database has no consumer, and a scratch topic
    on db 0 is not polled by anything. Only both together put bytes through the
    running service.
    """
    if not (db == 0 and topic == streams.CONTENT_FETCH) or production:
        return
    raise ProductionTargetError(
        f"{topic} on db {db} is the live command stream — the running worker will fetch "
        f"these URLs for real. Pass --production to mean it."
    )


def resolve_blobs_topic(topic: str, override: str | None) -> str:
    """Which fact stream ``--watch`` reads, given the command stream being seeded.

    A fixed ``content.blobs`` default meant a scratch seed watched production's
    facts, found nothing, and exited 1 after the full timeout — a working loop
    reported as a failure (CR #5). Pairing the default with ``--topic`` makes the
    scratch case work unattended, and ``<topic>.blobs`` is the name the
    integration fixtures already use.
    """
    if override is not None:
        return override
    return streams.CONTENT_BLOBS if topic == streams.CONTENT_FETCH else f"{topic}.blobs"


async def publish(
    client: Redis,
    topic: str,
    urls: list[str],
    *,
    on_published: Callable[[SeedResult], None] | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
) -> list[SeedResult]:
    """XADD one command per URL, in the order given.

    ``on_published`` fires per command rather than per run so a caller can report
    incrementally (CR #10). A command that has landed is in flight whether or not
    the rest of the loop succeeds — the live worker will fetch it — so a failure
    on URL two must not swallow the id of URL one.

    ``headers`` / ``timeout_seconds`` are keyword-only and shared by every URL in
    the run: this is the only issuer there is today, so it is also the only way
    to exercise the worker's request-option path against a live broker (#11).
    """
    publisher = AsyncBusPublisher(client)
    results = []
    for url in urls:
        command = build_command(url, headers, timeout_seconds)
        result = await publisher.execute(BusPublish(topic, to_wire(command)))
        published = SeedResult(command.command_id, url, result.bus_message_id)
        results.append(published)
        if on_published is not None:
            on_published(published)
    return results


async def last_id(client: Redis, topic: str) -> str:
    """The newest entry id on ``topic``, or ``0-0`` when it is empty or absent.

    Captured *before* publishing. Reading from the beginning instead would report
    a stale fact as this run's result; capturing afterwards would race a worker
    fast enough to publish before the watch starts, and the watch would hang.
    """
    entries = await client.xrevrange(topic, count=1)
    if not entries:
        return "0-0"
    message_id, _fields = entries[0]
    return _as_str(message_id)


async def watch_for_facts(
    client: Redis,
    topic: str,
    start_id: str,
    command_ids: set[str],
    *,
    timeout_seconds: float,
) -> list[Fact]:
    """Tail ``topic`` until every awaited command has a fact, or time runs out.

    A fact is **either outcome**: ``content.blobs`` carries ``blob_available``
    and, since #9, ``fetch_failed``. Accepting only the success would leave this
    tool timing out on exactly the case the failure fact exists to make visible,
    reporting a named failure as the silence it replaced.

    Returns what it saw — an incomplete list is the caller's signal that the loop
    did not close, not an error here. Frames for other issuers' commands are
    skipped, and one that will not decode is skipped rather than fatal: this is a
    shared stream, and an operator watching their own seed should not be stopped
    by somebody else's malformed entry.

    The deadline is tracked here rather than delegated to ``asyncio.timeout``
    because the remaining budget is also the ``XREAD`` block window, and because
    cancelling mid-read would discard the partial results this returns.
    """
    outstanding = set(command_ids)
    found: list[Fact] = []
    cursor = start_id
    deadline = time.monotonic() + timeout_seconds
    while outstanding:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        response = await client.xread({topic: cursor}, block=max(1, int(remaining * 1000)))
        if not response:
            await asyncio.sleep(min(IDLE_SLEEP_SECONDS, remaining))
            continue
        for _stream, entries in response:
            for message_id, fields in entries:
                cursor = _as_str(message_id)
                fact = _decode_fact(topic, cursor, fields)
                if fact is not None and fact.command_id in outstanding:
                    outstanding.discard(fact.command_id)
                    found.append(fact)
    return found


def _decode_fact(
    topic: str, message_id: str, fields: dict[bytes | str, bytes | str]
) -> Fact | None:
    """Decode one fact frame, or ``None`` if it is not one this run cares about.

    ``from_wire``'s dispatch table is global, so any known event type on this
    stream decodes cleanly into the wrong model — hence the ``isinstance`` check
    rather than trusting the topic. Both fact types pass; a ``content_fetch``
    command misrouted here does not, and neither does an archiver fact that
    happens to share the stream.

    The catch is deliberately broad (CR #7). ``from_wire`` documents
    ``BusMessageAnomaly`` as its failure mode, but the fallback for *any* decode
    failure is the same — skip the frame — and the commands are already published
    by the time the watch runs, so an unanticipated exception here would look
    like a failed seed and invite an operator to re-issue work already in flight.
    """
    try:
        payload = from_wire(
            {_as_str(k): _as_str(v) for k, v in fields.items()},
            topic=topic,
            message_id=message_id,
        ).payload
    except Exception as exc:
        print(f"warning: skipping undecodable frame {message_id}: {exc}", file=sys.stderr)
        return None
    return payload if isinstance(payload, BlobAvailableEvent | FetchFailedEvent) else None


def _as_str(value: bytes | str) -> str:
    """Decode a raw Redis reply (the client is not in ``decode_responses`` mode)."""
    return value.decode() if isinstance(value, bytes) else value


def build_parser() -> argparse.ArgumentParser:
    """The CLI. ``--redis-url`` and ``--topic`` are required, deliberately."""
    parser = argparse.ArgumentParser(
        prog="seed_fetch",
        description="Publish content.fetch commands for one or more URLs.",
    )
    parser.add_argument("urls", nargs="+", help="URLs to issue a fetch command for")
    parser.add_argument(
        "--redis-url",
        required=True,
        help="broker to publish to, e.g. redis://localhost:6379/15 (no default, by design)",
    )
    parser.add_argument(
        "--topic",
        required=True,
        help=f"stream to publish to, e.g. {streams.CONTENT_FETCH} (no default, by design)",
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help=f"allow publishing to {streams.CONTENT_FETCH} on db 0, which the live worker consumes",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the frames that would be published and exit without connecting",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="tail the fact stream until every command has an outcome "
        "(blob_available or fetch_failed)",
    )
    parser.add_argument(
        "--blobs-topic",
        default=None,
        help=(
            f"fact stream --watch reads "
            f"(default: {streams.CONTENT_BLOBS} for {streams.CONTENT_FETCH}, "
            f"otherwise <topic>.blobs)"
        ),
    )
    parser.add_argument(
        "--header",
        action=HeaderAction,
        dest="headers",
        default=None,
        metavar="'Name: value'",
        help="request header to attach to every command; repeatable (#11)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        dest="timeout_seconds",
        help=(
            "per-fetch timeout in seconds for every command (default: the worker's driver default)"
        ),
    )
    parser.add_argument(
        "--watch-timeout",
        type=float,
        default=DEFAULT_WATCH_TIMEOUT_SECONDS,
        help=f"seconds to wait for facts under --watch (default: {DEFAULT_WATCH_TIMEOUT_SECONDS})",
    )
    return parser


def _print_dry_run(
    topic: str,
    urls: list[str],
    headers: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
) -> None:
    """Show the wire frames without publishing them.

    The options ride inside the printed ``payload``, which is the point of
    showing the frame rather than a summary: an operator checking a header
    against what the worker will refuse should read the value that will actually
    travel, after this script's own stripping.
    """
    for url in urls:
        command = build_command(url, headers, timeout_seconds)
        print(f"would publish to {topic}: {to_wire(command)}")


async def run(args: argparse.Namespace) -> int:
    """Open the client, seed, and close it again — the process's exit code.

    Bus clients are injection-only, so the script owns one for its run, the same
    rule ``src/worker/main.py`` follows. This function is that ownership and
    nothing else; the policy lives in ``_seed``.
    """
    try:
        client = Redis.from_url(args.redis_url)
    except (ValueError, RedisError) as exc:
        print(f"error: {args.redis_url} is not a usable Redis URL: {exc}", file=sys.stderr)
        return 1
    try:
        return await _seed(client, args)
    finally:
        await client.aclose()


async def _seed(client: Redis, args: argparse.Namespace) -> int:
    """Publish, optionally watch, and say precisely what happened.

    Failures are answered with a line and an exit code rather than a traceback
    (CR #3), matching ``sync_wheelhouse.py``, the other operator-facing script
    here. Each step that can fail is guarded on its own, because the four
    failures mean four different things to whoever has to act on them: a refused
    target, a broker that died before anything went out, a loop that published
    *some* of its commands, and a watch that could not read facts for commands
    already in flight. One shared handler told an operator "the broker is not
    usable" about a broker that had just accepted a command they were never
    shown (CR #10), or named publishing as the culprit when nothing had been
    published yet (CR #15).
    """
    try:
        guard_production_target(args.topic, db=resolve_db(client), production=args.production)
    except ProductionTargetError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    # Bound before the fallible steps rather than inside them: every name read
    # after a `try` has to be one that no branch can skip past. Mirrors the
    # pre-binding in src/worker/loop.py::run_loop (CR #14).
    blobs_topic = resolve_blobs_topic(args.topic, args.blobs_topic)
    start_id = "0-0"
    published: list[SeedResult] = []

    def report(result: SeedResult) -> None:
        published.append(result)
        print(
            f"published command_id={result.command_id} "
            f"entry_id={result.bus_message_id} topic={args.topic} url={result.url}"
        )

    if args.watch:
        # Before publishing, deliberately: capture it afterwards and a worker
        # fast enough to answer immediately would land its fact behind the
        # cursor, and the watch would time out on a loop that worked.
        try:
            start_id = await last_id(client, blobs_topic)
        except (RedisError, OSError) as exc:
            print(
                f"error: reading {blobs_topic} failed, so nothing was published: {exc}",
                file=sys.stderr,
            )
            return 1

    try:
        await publish(
            client,
            args.topic,
            args.urls,
            on_published=report,
            headers=args.headers,
            timeout_seconds=args.timeout_seconds,
        )
    except (RedisError, OSError) as exc:
        print(
            f"error: publishing to {args.topic} failed after {len(published)} of "
            f"{len(args.urls)} commands: {exc}",
            file=sys.stderr,
        )
        return 1

    if not args.watch:
        return 0
    try:
        return await _report_facts(
            client,
            blobs_topic=blobs_topic,
            timeout_seconds=args.watch_timeout,
            start_id=start_id,
            results=published,
        )
    except (RedisError, OSError) as exc:
        print(f"error: watching {blobs_topic} failed: {exc}", file=sys.stderr)
        return 1


async def _report_facts(
    client: Redis,
    *,
    blobs_topic: str,
    timeout_seconds: float,
    start_id: str,
    results: list[SeedResult],
) -> int:
    """Wait for each command's outcome and print it; non-zero unless all succeeded.

    Three exits, not two: every blob arrived (0), a command was closed with a
    reason (1), or nothing arrived at all (1). The last two share a code because
    both are a failed seed, but they no longer share a *message* — naming the
    reason is the whole of what #9 bought the operator over waiting out a
    timeout and guessing.
    """
    awaited = {result.command_id for result in results}
    facts = await watch_for_facts(
        client, blobs_topic, start_id, awaited, timeout_seconds=timeout_seconds
    )
    failed = set()
    for fact in facts:
        if isinstance(fact, FetchFailedEvent):
            failed.add(fact.command_id)
            print(
                f"fetch_failed command_id={fact.command_id} reason={fact.reason} "
                f"terminal={fact.terminal} status={fact.status_code} url={fact.url}"
            )
            continue
        # status and final_url join the line because they are what an operator
        # checks a live fetch against — a 203 where a 200 was expected, or a
        # redirect nobody knew about. The validators (etag, last_modified) and
        # the raw Content-Type stay off: they exist for an issuer's database,
        # and printing them would bury the two fields worth reading at a glance.
        print(
            f"blob_available command_id={fact.command_id} "
            f"fingerprint={fact.content_fingerprint} size={fact.size_bytes} "
            f"status={fact.status_code} final_url={fact.final_url} uri={fact.blob_uri}"
        )
    missing = awaited - {fact.command_id for fact in facts}
    if missing:
        print(
            f"error: no fact on {blobs_topic} after {timeout_seconds}s "
            f"for: {', '.join(sorted(missing))}",
            file=sys.stderr,
        )
    return 1 if missing or failed else 0


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run; the dry run never opens a connection."""
    args = build_parser().parse_args(argv)
    if args.dry_run:
        _print_dry_run(args.topic, args.urls, args.headers, args.timeout_seconds)
        return 0
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
