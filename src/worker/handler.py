"""The byte path: fetch -> fingerprint -> temp-store -> ``blob_available``.

Fills the seam ``src.worker.loop`` dispatches to. The loop owns a message's fate;
this module's only vocabulary for influencing it is raising —
``PermanentFetchError`` to dead-letter now, ``TransientFetchError`` to leave the
message pending for the next reclaim, and since #17
``CompletedWithoutBlobError`` to close the command with a fact and no DLQ entry.
"""

import asyncio
import math
import re
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import NamedTuple, Protocol

import httpx
from co_core.effects.bus import BusPublish
from co_core.effects.fetch import FetchContent, FetchResult
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import BlobAvailableEvent, ContentFetchCommand
from co_core.pure.util.hashing import sha256
from co_core_aio.bus import AsyncBusPublisher
from redis.asyncio import Redis

from src.core.config import Settings
from src.core.errors import (
    CompletedWithoutBlobError,
    FailureReason,
    PermanentFetchError,
    TransientFetchError,
)
from src.core.logging import get_logger
from src.storage.base import BlobStore
from src.storage.sweeper import BlobUsage
from src.worker.loop import Handler, park
from src.worker.pacing import HostPacer, HostPolicy

logger = get_logger(__name__)

# What a response with no usable Content-Type is stored and announced as.
DEFAULT_MEDIA_TYPE = "application/octet-stream"

# How long a header value may be before the fact refuses to carry it.
#
# The passthrough fields (etag, last_modified, content_type_raw) are strings the
# *origin* chooses, riding onto a broadcast stream nothing trims. A generous
# bound costs nothing real — an ETag is tens of characters, an HTTP-date is 29 —
# and a value past it is not a validator, it is an origin misbehaving.
#
# Over the bound the value is **dropped, not truncated**. A truncated ETag
# replayed in an If-None-Match is a validator that can never match: the origin
# answers 200 every time while the issuer believes it asked conditionally, which
# is strictly worse than having sent no validator at all.
MAX_HEADER_VALUE_LENGTH = 1024

# Non-2xx statuses worth another attempt. 5xx is the origin failing rather than
# refusing; 429 and 408 are it asking for later explicitly. Everything else
# non-2xx is the origin's settled answer — re-fetching a 404 gets another 404,
# so retrying only postpones the dead-letter while holding a PEL slot.
#
# The retry cadence is REPLICATOR_CLAIM_MIN_IDLE_MS, so this is deliberately not
# a tight loop against a struggling origin: raising leaves the message pending
# and the next reclaim brings it back a minute later.
_RETRYABLE_STATUSES = frozenset({408, 429})

# The one non-2xx that is an *answer* rather than a refusal: the origin agreeing
# that the issuer's copy is current. Classified on the status alone — see
# ``_raise_for_status``.
_NOT_MODIFIED = 304

# The statuses that are the origin asking to be asked less often (#25). A 429 is
# it refusing outright, a 503 is it saying it cannot cope; both are evidence about
# this origin's tolerance, which is exactly what the pacer's escalation is for.
#
# Deliberately narrower than ``TransientFetchError``: a 500 or a 504 is transient
# too, but it is an origin bug or a slow upstream rather than a statement about
# request *rate*, and escalating on it would slow a host for a fault more requests
# would not have caused. Keyed on the status here rather than on the exception for
# that reason — the exception type cannot express the distinction.
_RATE_LIMIT_STATUSES = frozenset({429, 503})

# The request headers that make a 304 something Replicator asked for (#11 sends
# them; #17 is what happens when one works). Read off the command as the issuer
# spelled it, folded here, so the check cannot disagree with what went out.
_VALIDATOR_HEADERS = frozenset({"if-none-match", "if-modified-since"})

# How many request headers a command may carry, and how many bytes they may add
# up to on the wire (#11).
#
# Deliberately not MAX_HEADER_VALUE_LENGTH, which reasons about *response*
# passthroughs riding a broadcast stream nothing trims — a different argument
# that happens to land on a similar number. This bound is about what an origin
# will accept: 8 KiB is the common server-side limit (nginx's
# large_client_header_buffers, Apache's LimitRequestFieldSize), so exceeding it
# earns an opaque 400 from the far end. Refusing locally turns that into a named
# reason on a fact the issuer already consumes.
#
# Constants rather than settings: these bound an unauthenticated capability, and
# a per-deployment knob is an invitation to loosen the one thing standing between
# a bus writer and an arbitrary request.
MAX_REQUEST_HEADERS = 32
MAX_REQUEST_HEADER_BYTES = 8192

# Request headers Replicator will not send on an issuer's behalf.
#
# Two groups, refused for different reasons and listed together because the
# refusal is the same. The hop-by-hop set (RFC 9110 §7.6.1) describes a single
# connection, not the request — httpx and h11 own it, and an issuer's value is
# either ignored or corrupts the framing. `host` and `content-length` are not
# hop-by-hop: httpx derives both, and overriding `host` in particular points the
# request at one origin while addressing another (domain fronting) — precisely
# the widening the contract's trust model says the broker, not this list, is
# holding back.
#
# **Refused, not stripped.** Silently dropping one changes what the origin saw
# with nothing the issuer can observe, which is the same failure the
# reject-not-truncate rule exists to prevent, applied to the request side. It is
# also the conservative direction: relaxing a refusal to a strip later stays
# compatible, tightening a strip into a refusal does not.
REFUSED_HEADERS = frozenset(
    {
        "connection",
        "content-length",
        "host",
        "keep-alive",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)

# Every Proxy-* header, refused as a family: they configure the hop rather than
# the request, and enumerating them would date the list.
_REFUSED_HEADER_PREFIX = "proxy-"

# A field name is an RFC 9110 token. Matched against the *folded* name, so the
# alphabetic range is lowercase only.
#
# **`\Z`, never `$`.** Python's `$` also matches immediately before a trailing
# newline, so `^token+$` accepts `"accept\n"` — and httpx passes that straight
# through to the wire as a header name containing a bare LF, which is header
# injection by any other name. `\Z` is the absolute end of the string and the
# only correct anchor for a validator (CR #14). The same trap applies to the
# value pattern below, where `.strip()` happens to hide it; relying on that
# would make one guard's correctness depend on another's ordering.
_HEADER_NAME = re.compile(r"\A[!#$%&'*+\-.^_`|~0-9a-z]+\Z")

# A field value here is printable US-ASCII and SP — narrower than RFC 9110's
# VCHAR / obs-text on both edges, and deliberately so on each.
#
# **obs-text (\x80-\xff) is excluded because httpx cannot send it.** It encodes
# header values as ASCII and raises UnicodeEncodeError, which — unlike the
# LocalProtocolError a CRLF earns — is *not* an httpx.HTTPError, so it would slip
# past _fetch's mapping entirely and land in the loop's unclassified branch:
# retried to the delivery ceiling and finally closed as `handler_error` minutes
# later, instead of the immediate `invalid_request_options` this guard exists to
# produce. RFC 9110 deprecates obs-text anyway (CR #1).
#
# **HTAB (\x09) is excluded even though httpx accepts it.** RFC 9110 permits it
# as internal whitespace, but a tab inside a header value is nobody's intent and
# survives no round trip worth having. Refused on purpose, not by oversight
# (CR #3).
#
# What the guard is really for is CR and LF: a CRLF in a value is request
# splitting, and everything else here is the cheap part of drawing that line.
#
# Anchored `\A`/`\Z` for the reason spelled out on `_HEADER_NAME` above — `$`
# would admit a trailing newline. Unreachable here (the value is stripped before
# it is matched) and fixed anyway: one guard's correctness must not rest on
# another's ordering.
_HEADER_VALUE = re.compile(r"\A[\x20-\x7e]*\Z")


class RequestOptions(NamedTuple):
    """What the command asked the fetch itself to look like (#11).

    ``None`` on both fields is the omitted-field shape and means the driver's own
    defaults, unchanged — the compatibility promise the additive co-core fields
    were designed around (cannobserv#272).
    """

    headers: dict[str, str] | None
    timeout: float | None


class Fetcher(Protocol):
    """The fetch seam — ``co_core_aio.fetch.AsyncFetchDriver`` in production."""

    async def execute(self, effect: FetchContent) -> FetchResult: ...


def build_handler(
    *,
    fetcher: Fetcher,
    store: BlobStore,
    client: Redis,
    settings: Settings,
    usage: BlobUsage | None = None,
    policy: HostPolicy | None = None,
    pacer: HostPacer | None = None,
    park_above_seconds: float | None = None,
    stop: asyncio.Event | None = None,
    blobs_topic: str = streams.CONTENT_BLOBS,
) -> Handler:
    """Wire the byte path into a handler the loop can dispatch to.

    ``usage`` is the blob tree's measured size, shared with the retention task —
    the sweep re-measures it, this handler adds to it, and it is the one thing
    standing between a burst and a full disk on a shared VM. Left unset it is
    private to this handler, which only makes the ceiling later to notice; the
    worker passes the shared instance.

    ``pacer`` is per-host politeness (#12). Built from ``settings`` when not
    injected, deliberately: unwired it fails *open*, and a byte path that
    silently stopped pacing looks exactly like one that is working. Tests inject
    one with a controlled interval and clock.

    ``policy`` is where the per-host numbers come from (#19) — in the worker,
    ``FetchPolicyMap.interval_for``. Passed as a bare callable rather than the
    map itself so this module stays ignorant of ``content.fetch-policy``, the
    same way ``loop.py`` stays ignorant of ``content.blobs``. Ignored when
    ``pacer`` is injected: a caller supplying its own pacer has already decided
    what that pacer consults.

    ``park_above_seconds`` is where a pacing wait stops being slept through and
    starts parking the message. Defaults to the poll window — a wait no longer
    than one blocking read adds nothing to the shutdown latency the unit's
    ``TimeoutStopSec`` is already sized for, and a longer one would hold the
    serial consume path against every other host's commands.

    ``stop`` lets a sleeping handler notice a SIGTERM. Unset, the sleep simply
    runs its course; the bound above is what keeps that from mattering.

    ``blobs_topic`` is a defaulted argument rather than a setting, for the same
    reason ``build_consumer``'s ``topic`` is: the only caller that moves it is a
    live-broker test, which must keep its facts on a scratch stream. A fact
    written to the real ``content.blobs`` during a test would be a genuine
    announcement of a blob under ``tmp_path`` — gone before any consumer reads it.
    """
    publisher = AsyncBusPublisher(client)
    usage = usage if usage is not None else BlobUsage()
    pacer = (
        pacer if pacer is not None else HostPacer(settings.min_host_interval_seconds, policy=policy)
    )
    if park_above_seconds is None:
        park_above_seconds = settings.read_block_ms / 1000
    stop = stop if stop is not None else asyncio.Event()

    async def handle(command: ContentFetchCommand) -> None:
        # Ahead of the ceiling deliberately. Validation is pure and free; the
        # ceiling raise is *transient* and parks the message in the PEL until a
        # sweep frees space. A command that can never succeed must not wait a
        # sweep interval to reach a conclusion available immediately.
        options = _request_options(command, settings.max_fetch_timeout_seconds)
        _raise_for_ceiling(usage, settings.blob_max_total_bytes)
        # Last of the three, and after the ceiling on purpose: spending a wait to
        # reach a check that was going to park the message anyway is a wait the
        # origin never benefits from. The cost of that ordering is that the tree
        # can cross the ceiling *during* a wait — bounded by park_above_seconds,
        # and the ceiling is an inter-sweep estimate either way (CR #9).
        paced_seconds = await _pace(
            pacer, command, stop=stop, park_above_seconds=park_above_seconds
        )
        result = await _fetch(fetcher, command, options)
        # Stamped here rather than at publish: occurred_at is when the fact went
        # onto the bus, which under a reclaim is minutes after the bytes were on
        # the wire. This is the closest the handler can stand to that instant.
        fetched_at = datetime.now(UTC)
        # Folded once, here rather than after the guards, because the escalation
        # below needs Retry-After off a response the classifier is about to raise
        # on. Cheap, and unconditional either way.
        headers = _folded_headers(result)
        # Ahead of the classifier and not inside it: this is a side effect on
        # shared state, while ``_raise_for_status`` is a pure status -> outcome
        # function that #17 gave three arms. Reporting from here also reads the
        # status off the *result*, which is the only place it survives —
        # ``TransientFetchError`` carries no status code, so a call site that
        # waited for the raise could not tell a 429 from a 504.
        _report_rate_limited(pacer, command, result.status_code, headers, now=fetched_at)
        _raise_for_status(result, command)
        _raise_for_size(result, command, settings.max_blob_bytes)
        fingerprint = sha256(result.content)
        media_type = _media_type(headers)
        # Asked before storing, because store's short-circuit does not report
        # which branch it took and counting a re-store would inflate the tree's
        # measured size on every redelivery.
        #
        # Racy either way — two workers can both see a new fingerprint and both
        # count it, and a sweep reaping between the two calls means a genuinely
        # new write goes uncounted. Both drifts are bounded by the sweep
        # interval, after which observe() replaces the estimate outright.
        is_new = not store.exists(fingerprint)
        # Read *before* the store, so the horizon published below can only be
        # earlier than the blob's real one. store() stamps the mtime the sweep
        # measures against, and mtime >= stored_at by however long the write
        # takes; deriving the horizon from a clock read afterwards would put the
        # announced expiry past the real one, which is the direction that leaves
        # a consumer holding a dead blob_uri.
        stored_at = datetime.now(UTC)
        blob_uri = store.store(result.content, fingerprint, media_type)
        if is_new:
            usage.add(len(result.content))
        await _publish(
            publisher,
            blobs_topic,
            BlobAvailableEvent(
                occurred_at=datetime.now(UTC),
                content_fingerprint=fingerprint,
                blob_uri=blob_uri,
                size_bytes=len(result.content),
                media_type=media_type,
                url=command.url,
                command_id=command.command_id,
                # Copied across, never read (#28). The value is opaque to the
                # byte path — nothing here parses it, branches on it, keys on it,
                # or stores it — so the only way to get it wrong is to transform
                # it. tests/test_boundaries.py holds that line mechanically.
                info_source_id=command.info_source_id,
                # When these bytes stop being retrievable at blob_uri
                # (cannobserv#301). Published because the TTL clock runs from the
                # last fetch *reference* — store() touches the mtime on its
                # content-addressed short-circuit — and no consumer can observe
                # that event. The alternative was every consumer re-deriving the
                # horizon from the contract's MUST-7 TTL, hard-coding a retention
                # policy this service owns and starting the clock in the wrong
                # place. The real reap is later still: the sweep only runs every
                # blob_sweep_interval_seconds, so this errs early in both terms.
                blob_expires_at=stored_at + timedelta(seconds=settings.blob_ttl_seconds),
                # The metadata a broadcast consumer cannot recover once fetching
                # lives here rather than in Watcher (cannobserv#271). Every one
                # is optional, and None means "nobody said" — never a stand-in
                # value that would read as an answer.
                #
                # final_url is passed through exactly as the driver reported it,
                # command.url included in the silence: echoing the request would
                # leave an issuer unable to tell "it landed where I asked" from
                # "nobody knows where it landed" (cannobserv#279).
                #
                # `or None` normalizes the one shape the contract says cannot
                # occur — an empty string is neither a URL nor the None an issuer
                # branches on. Unreachable through the http driver, whose value
                # is str(response.url); this keeps a future driver from inventing
                # a third state the issuer has no rule for.
                final_url=result.final_url or None,
                status_code=result.status_code,
                fetched_at=fetched_at,
                content_type_raw=_passthrough(headers, "content-type"),
                etag=_passthrough(headers, "etag"),
                last_modified=_passthrough(headers, "last-modified"),
            ),
            command=command,
        )
        logger.info(
            "stored a blob and published blob_available",
            extra={
                "command_id": command.command_id,
                "content_fingerprint": fingerprint,
                "size_bytes": len(result.content),
                "media_type": media_type,
                # Still the one number nothing else keeps. The fact gained the
                # rest of the fetch metadata in #10 — status, landing URL, and
                # the wire instant — but not how long the origin took to give
                # the bytes up, which stays journal-only.
                "duration_ms": result.duration_ms,
                # Names only, never values. The trust-model paragraph these
                # guards answer to is specifically about an issuer attaching an
                # Authorization header; logging its value would re-open the same
                # exposure one layer down, in a journal a wider set of people
                # read than can write to the bus.
                "request_headers": sorted(options.headers or {}),
                "request_timeout_seconds": options.timeout,
                # Politeness, on the line that already exists rather than one of
                # its own (CR #3): without it a mechanism that caps per-host
                # throughput is absent from the journal, and an operator seeing a
                # slow drain cannot tell "waiting politely" from "origin is
                # slow". A per-fetch datum, correlated with duration_ms above —
                # the *gauge* (how much of the corpus is under pacing) rides
                # _pace's own line instead, so a slowly-changing number is not
                # repeated once per command (CR #13).
                "paced_seconds": paced_seconds,
            },
        )

    return handle


async def _publish(
    publisher: AsyncBusPublisher,
    topic: str,
    event: BlobAvailableEvent,
    *,
    command: ContentFetchCommand,
) -> None:
    """Announce the blob, naming it in the journal if the announcement fails.

    Store-then-publish is deliberate — a crash between the two must never
    announce bytes that are not there — but the reverse gap is what leaks. A
    publish that fails in a way the loop cannot classify as transient walks the
    delivery ceiling into ``content.fetch.dlq``, leaving bytes on disk with no
    fact and no ``command_id`` pointing at them: invisible to the bus, and to any
    operator query that starts from ``content.blobs``.

    This is the one moment the orphan is exactly knowable, so it is recorded
    here rather than reconstructed later by reconciling the tree against the fact
    stream — which would make a *delete* decision depend on another service's
    stream-trimming policy.

    The error is re-raised untouched. The loop, not the handler, decides a
    message's fate, and a ``ResponseError`` reaching the DLQ is the right outcome
    for a publish that is not going to start working — with the carve-out that
    ``OutOfMemoryError`` is a ``ResponseError`` subclass the loop now classifies
    transient (#20), so a broker out of memory retries instead (see
    ``loop._TRANSIENT_ERRORS``).
    """
    try:
        await publisher.execute(BusPublish(topic, to_wire(event)))
    except Exception as exc:
        logger.error(
            "stored a blob but failed to publish blob_available — it is now an orphan",
            extra={
                "command_id": command.command_id,
                "content_fingerprint": event.content_fingerprint,
                "blob_uri": event.blob_uri,
                "size_bytes": event.size_bytes,
                "error": f"{type(exc).__name__}: {exc}",
                # Orphans are reaped as ordinary aged blobs; what matters is that
                # a rising count of these reads as a publishing failure rather
                # than as normal expiry.
                "detail": "no fact references these bytes; they expire on the blob TTL",
            },
        )
        raise


def _request_options(command: ContentFetchCommand, max_timeout: float) -> RequestOptions:
    """Validate the command's per-fetch options and shape them for the driver.

    Every refusal here is a :class:`PermanentFetchError`: an issuer that sent an
    unsendable header will send it again on the next reclaim, and the guards
    exist precisely so the issuer *hears* about it — through a terminal
    ``fetch_failed`` naming ``invalid_request_options`` — rather than receiving
    bytes fetched under conditions it did not ask for.
    """
    return RequestOptions(
        headers=_request_headers(command.headers),
        timeout=_request_timeout(command.timeout_seconds, max_timeout),
    )


def _request_headers(headers: dict[str, str] | None) -> dict[str, str] | None:
    """Fold an issuer's headers to lowercase, refusing anything unsendable.

    **The fold is what makes "issuer wins" true.** ``AsyncFetchDriver`` merges
    ``{"user-agent": DEFAULT_USER_AGENT, **effect.headers}`` — a plain,
    case-sensitive dict — so a capitalized ``User-Agent`` leaves *both* keys in
    the mapping, and httpx does not resolve them: it puts **two** ``User-Agent``
    field lines on the wire, the default first. Which one applies is then the
    origin's decision (RFC 9110 lets it join repeated lines with a comma), so the
    issuer's value is not overridden so much as adulterated. Verified against a
    real driver and a real socket, not inferred. That is exactly the
    fingerprint-continuity case Watcher needs at cutover (watcher#241), so it
    cannot be left to chance.

    A collision the fold would create is refused rather than resolved
    last-wins — discarding one of two headers an issuer deliberately set is the
    invisible change these guards exist to prevent, and "refuse only when the
    values differ" would make the rule depend on the values instead of the shape.

    Surrounding whitespace is dropped from a **value**, not refused: RFC 9110
    excludes OWS from a field value, the same reading ``_passthrough`` applies to
    the response side. A **name** gets no such treatment — RFC 9110 forbids space
    between a field name and its colon, so padding there is malformed rather than
    optional whitespace, and silently trimming it would be the one thing this
    module refuses to do anywhere else: adjust a request instead of refusing it
    (CR #4). A padded name simply fails the token match below.

    ``None`` in, ``None`` out — an omitted field must reach the driver as the
    absence it was, not as an empty mapping some future driver reads as
    "send no headers".
    """
    if headers is None:
        return None
    if len(headers) > MAX_REQUEST_HEADERS:
        raise _invalid_options(f"{len(headers)} headers, over the {MAX_REQUEST_HEADERS} allowed")

    folded: dict[str, str] = {}
    total = 0
    for name, value in headers.items():
        key = name.lower()
        if not _HEADER_NAME.match(key):
            raise _invalid_options(f"{name!r} is not a valid header name")
        # Every refusal names the header as the *issuer* spelled it, not as the
        # fold left it: the message is read by somebody grepping their own
        # publishing code, where `Host` will not be found under `host` (CR #12).
        if key in REFUSED_HEADERS or key.startswith(_REFUSED_HEADER_PREFIX):
            raise _invalid_options(f"{name!r} is not a header Replicator will send")
        if key in folded:
            raise _invalid_options(f"{name!r} was given more than once, differing only in case")
        stripped = value.strip()
        if not _HEADER_VALUE.match(stripped):
            raise _invalid_options(f"{name!r} has a value that cannot be sent verbatim")
        # +4 for the ": " and the CRLF the value costs on the wire, so the bound
        # measures what the origin will measure rather than the payload alone.
        #
        # Characters, counted against a bound stated in *bytes* — exact only
        # because both charsets above are US-ASCII, one byte each. Widen either
        # regex and this silently starts under-counting (CR #6).
        total += len(key) + len(stripped) + 4
        if total > MAX_REQUEST_HEADER_BYTES:
            raise _invalid_options(f"headers exceed the {MAX_REQUEST_HEADER_BYTES}-byte bound")
        folded[key] = stripped
    return folded


def _request_timeout(timeout_seconds: float | None, maximum: float) -> float | None:
    """Validate the command's timeout, or ``None`` for the driver's default.

    Bounded above because the consume path is serial — ``read`` takes
    ``count=1`` and the handler is awaited before the next poll — so a command's
    timeout is a lien on every *other* command in the group, not only its own.
    An unbounded value parks the worker for as long as the issuer likes, through
    the same unauthenticated capability the header guards answer to.

    Refused rather than clamped, for the reason the whole of #11 refuses rather
    than adjusts: a silently shortened timeout produces a failure the issuer
    cannot distinguish from a slow origin.
    """
    if timeout_seconds is None:
        return None
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise _invalid_options(f"timeout_seconds={timeout_seconds!r} is not a duration")
    if timeout_seconds > maximum:
        raise _invalid_options(
            f"timeout_seconds={timeout_seconds} is over the {maximum}-second ceiling"
        )
    return timeout_seconds


def _invalid_options(detail: str) -> PermanentFetchError:
    """The one refusal both option guards raise.

    Built rather than raised so each call site reads as the ``raise`` it is, and
    so the reason cannot drift between them.
    """
    return PermanentFetchError(
        f"command request options refused: {detail}",
        reason=FailureReason.INVALID_REQUEST_OPTIONS,
    )


async def _fetch(
    fetcher: Fetcher, command: ContentFetchCommand, options: RequestOptions
) -> FetchResult:
    """Fetch the command's URL, mapping httpx's failures into the loop's vocabulary.

    httpx's exception hierarchy is disjoint from the builtin ``ConnectionError`` /
    ``TimeoutError`` the loop already treats as transient, so an unmapped
    ``ConnectError`` would land in the loop's *unclassified* branch and consume
    the delivery ceiling — an origin down for a few reclaims could dead-letter a
    perfectly good command.

    ``InvalidURL`` and ``UnsupportedProtocol`` are the exceptions: a URL that is
    not a URL will not become one on the next reclaim.
    """
    try:
        return await fetcher.execute(
            FetchContent(command.url, headers=options.headers, timeout=options.timeout)
        )
    except (httpx.UnsupportedProtocol, httpx.InvalidURL) as exc:
        raise PermanentFetchError(
            f"{command.url} is not fetchable: {exc}", reason=FailureReason.NOT_FETCHABLE
        ) from exc
    except httpx.HTTPError as exc:
        raise TransientFetchError(f"{command.url} failed to fetch: {exc}") from exc


async def _pace(
    pacer: HostPacer,
    command: ContentFetchCommand,
    *,
    stop: asyncio.Event,
    park_above_seconds: float,
) -> float:
    """Give the origin its space before asking it for anything (#12).

    Returns the seconds actually waited, for the success line to report.

    Two ways to spend a wait, split by duration, because neither is correct
    alone on a serial consume path:

    * **Sleep** a short one. The consume path is serial, so a sub-second pause
      costs the group a sub-second pause — the same order as the fetch it is
      about to do, and far cheaper than a reclaim round-trip.
    * **Park** a long one, transiently, so the message returns via
      ``claim_stale`` like a command over the disk ceiling. Sleeping instead
      would hold every *other* host's commands behind this one origin's
      politeness, and hold a SIGTERM behind it too.

    Parking cannot express a wait shorter than ``REPLICATOR_CLAIM_MIN_IDLE_MS``
    (60 s by default), which is the whole reason the sleep branch exists: the
    normal interval is a second, and a park-only implementation would pace every
    host at 1/60th of the rate the cluster runs at today. Silently, and in the
    safe direction, which is what would have made it easy to ship.

    Transient in both directions — a paced command has done nothing wrong, and
    burning its delivery ceiling on politeness would dead-letter perfectly good
    work.
    """
    wait = pacer.wait_seconds(command.url)
    if wait <= 0:
        pacer.record(command.url)
        return 0.0
    if wait > park_above_seconds:
        raise TransientFetchError(
            f"{command.url} is inside its host's {wait:.1f}-second politeness window; "
            f"leaving it for the next reclaim"
        )
    # INFO, and only on the branch that actually waits (CR #13). At DEBUG this
    # was invisible under the root INFO level; on every command it repeated a
    # gauge that changes only when the corpus does. Here it appears exactly when
    # the mechanism acts, which is when an operator wants it, and carries
    # `tracked_hosts` as the periodic-ish gauge that has nowhere better to live —
    # the sweep's line is the other candidate, and it is silent on an idle tree.
    logger.info(
        "waiting out a host's politeness window",
        extra={
            "command_id": command.command_id,
            "wait_seconds": wait,
            "tracked_hosts": pacer.tracked_hosts,
        },
    )
    await park(stop, wait)
    # park returns early on SIGTERM, and an interrupted wait is not an elapsed
    # one — the origin has had no space. The message stays in the PEL, which is
    # where a command interrupted mid-flight belongs anyway.
    if stop.is_set():
        raise TransientFetchError(
            f"{command.url} was still inside its host's politeness window when the worker "
            f"began stopping"
        )
    pacer.record(command.url)
    return wait


def _report_rate_limited(
    pacer: HostPacer,
    command: ContentFetchCommand,
    status_code: int,
    headers: dict[str, str],
    *,
    now: datetime,
) -> None:
    """Tell the pacer this origin asked for later, so its siblings slow down too (#25).

    The gap the Phase 4 cutover opened. Watcher escalated on the 429s its own
    fetch path saw; that path became a publish path, and on this side a 429 was
    only ever a ``TransientFetchError`` — the one command that hit it came back a
    minute later and every *other* command to the same host kept the original
    spacing. Nothing else in the cluster can see this: a 429 produces no fact
    (transient failures are non-terminal by design), so the issuer cannot react
    even in principle.

    A no-op on every other status. Called before ``_raise_for_status`` because the
    classifier does not return on these two, and ordering it first costs nothing
    on the paths that do.
    """
    if status_code not in _RATE_LIMIT_STATUSES:
        return
    retry_after = _retry_after_seconds(headers.get("retry-after"), now)
    interval = pacer.report_rate_limited(command.url, retry_after_seconds=retry_after)
    # WARNING rather than INFO: an origin refusing is an operator-visible
    # condition, and this is the only surface it gets — there is no fact, and the
    # transient raise below is indistinguishable in the journal from a timeout.
    logger.warning(
        "the origin asked to be asked less often; raising this host's interval",
        extra={
            "command_id": command.command_id,
            "url": command.url,
            "status_code": status_code,
            "interval_seconds": interval,
            "retry_after_seconds": retry_after,
        },
    )


def _retry_after_seconds(value: str | None, now: datetime) -> float | None:
    """A ``Retry-After`` header as seconds from ``now``, or ``None`` if unusable.

    Both wire forms, because RFC 9110 §10.2.3 defines both and origins send both:
    ``delay-seconds`` (``Retry-After: 120``) and an HTTP-date (``Retry-After: Wed,
    01 Jan 2025 01:00:00 GMT``). Reading only the first silently discards the
    origin's number on the second and falls back to guessing.

    **Never raises.** An unparseable value is an origin being sloppy, and turning
    that into an unhandled exception here would convert "the origin is busy" into
    an unclassified handler failure retried to the delivery ceiling. ``None`` is
    the same answer the pacer gives an absent header: fall back to the multiplier.

    ``int`` and not ``float`` for the first form: ``delay-seconds`` is ``1*DIGIT``,
    so ``1.5`` and ``1e3`` are not legal and are better read as "no evidence" than
    accepted leniently. A negative or already-past value is returned as-is and the
    pacer treats it as no evidence too — the clamp belongs with the ceiling, in one
    place, rather than half here.

    ``now`` is passed rather than read, because the pacer's clock is monotonic and
    an HTTP-date is wall-clock; the handler is the only layer holding both.
    """
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return float(int(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        # An RFC 5322 ``-0000`` means "zone unknown" and arrives naive, which
        # would raise on subtraction. Everything in this service is UTC.
        when = when.replace(tzinfo=UTC)
    return (when - now).total_seconds()


def _raise_for_ceiling(usage: BlobUsage, ceiling_bytes: int) -> None:
    """Stop fetching once the blob tree has grown past what this deployment holds.

    Backpressure rather than reaping. Freeing space by deleting blobs still
    inside their TTL would convert a local disk problem into a ``blob_uri`` that
    cannot be opened in another repo — the one failure mode with no local
    symptom. Refusing the work instead is visible immediately and loses nothing.

    Transient on purpose: transient failures are exempt from the delivery
    ceiling, so the command stays in the PEL and returns via ``claim_stale``
    once a sweep brings the tree back under. Checked before the fetch, since the
    bytes are resident the moment the driver returns them.
    """
    if usage.is_over(ceiling_bytes):
        raise TransientFetchError(
            f"blob directory holds {usage.total_bytes} bytes, "
            f"at or over the {ceiling_bytes}-byte ceiling"
        )


def _raise_for_size(result: FetchResult, command: ContentFetchCommand, maximum: int) -> None:
    """Refuse a body too large to keep, before it reaches the blob directory."""
    if len(result.content) <= maximum:
        return
    raise PermanentFetchError(
        f"{command.url} returned {len(result.content)} bytes, over the {maximum}-byte ceiling",
        reason=FailureReason.TOO_LARGE,
    )


def _raise_for_status(result: FetchResult, command: ContentFetchCommand) -> None:
    """Turn a captured non-2xx status into the loop's outcome vocabulary.

    ``is_2xx`` and not ``is_success`` is the body-presence predicate: a 304 Not
    Modified reports ``is_success`` while carrying an empty body, and it reaches
    us on the *default* path — httpx only follows the redirects that carry a
    ``Location``. Storing that would content-address the empty string.

    **A 304 is classified on its status alone, and its body is ignored.** RFC 9110
    forbids one, but an origin that sends anyway must not change the outcome —
    branching on the body would make the answer depend on a field the status has
    already settled. The ``is_2xx`` guard above is what keeps those bytes out of
    the store, and that is deliberate rather than incidental: a 304's body is not
    the resource, so content-addressing it would mint a fingerprint for
    something no issuer asked for.

    Three outcomes, not two, since #17: a 304 is neither a failure nor a success
    but a **completed command with no blob**, so it raises the sibling type and
    the loop publishes-and-acks rather than dead-lettering. Everything else
    non-2xx and non-retryable — 412 included, since a failed precondition on a
    GET is the issuer's error — stays ``http_status``.

    The co-core fetch driver captures non-2xx into the result rather than
    raising, so this is the only place a status becomes an outcome.
    """
    if result.is_2xx:
        return
    detail = f"{command.url} returned HTTP {result.status_code}"
    if result.status_code == _NOT_MODIFIED:
        _log_not_modified(command)
        raise CompletedWithoutBlobError(
            detail, reason=FailureReason.NOT_MODIFIED, status_code=result.status_code
        )
    if result.status_code >= 500 or result.status_code in _RETRYABLE_STATUSES:
        raise TransientFetchError(detail)
    # The status rides as a field, not only in the message: it is the one datum a
    # 4xx fetch_failed carries that the reason token cannot express, and it is
    # what an issuer's per-domain backoff branches on (#9).
    raise PermanentFetchError(
        detail, reason=FailureReason.HTTP_STATUS, status_code=result.status_code
    )


def _log_not_modified(command: ContentFetchCommand) -> None:
    """Say whether this 304 was asked for — WARNING when it was not (#17).

    An **unbidden** 304 is an origin behaving oddly, and until #17 that signal was
    carried by accident: every 304 dead-lettered, so it landed on an operator
    surface. Now that the bidden case closes cleanly and, at steady state,
    routinely, the level is the only thing left to carry the distinction — and it
    costs three lines to keep.

    Read off ``command.headers`` rather than the folded request map because the
    fold refuses as well as folds: this runs after a fetch has already gone out,
    and a reporting line must not be able to raise. Case is folded here for the
    same reason ``_request_headers`` folds it — an issuer spelling it
    ``If-None-Match`` asked for this 304 exactly as much as one spelling it
    lowercase.
    """
    validators = sorted({name.lower() for name in (command.headers or {})} & _VALIDATOR_HEADERS)
    context = {
        "command_id": command.command_id,
        "url": command.url,
        "validators": validators,
    }
    if validators:
        logger.info("the origin reports the content unchanged", extra=context)
        return
    logger.warning(
        "the origin reports the content unchanged for a request that sent no validator",
        extra=context,
    )


def _folded_headers(result: FetchResult) -> dict[str, str]:
    """The response headers, keyed lowercase, folded once for every reader.

    Case-folded rather than trusting the names to arrive lowercased. httpx does
    normalize, but ``FetchResult.headers`` is typed as a plain
    ``Mapping[str, str]`` with no such guarantee, and the failure mode of
    assuming it — every response silently typed ``application/octet-stream``,
    every validator silently absent — is quiet enough to survive a long time.

    **Single-valued mapping assumed.** Two names differing only by case collapse
    to the last one seen, where HTTP semantics say repeated field lines are
    comma-joined. Unreachable through the co-core driver, which builds
    ``headers`` from an httpx ``Headers`` already collapsed to one value per
    name, so this is a note rather than a fix: a future driver handing over raw
    multi-value headers would silently discard the earlier ones, and three
    fields depend on this fold now rather than one.
    """
    return {name.lower(): value for name, value in result.headers.items()}


def _passthrough(headers: dict[str, str], name: str) -> str | None:
    """A header's value verbatim, or ``None`` when the origin sent nothing usable.

    Verbatim is the contract, not laziness: ``etag`` and ``last_modified`` are
    replayed unparsed in a conditional GET's ``If-None-Match`` /
    ``If-Modified-Since`` (cannobserv#272), and a parse/re-serialize round trip
    can hand the origin a value it never sent. The ETag's ``W/`` prefix and its
    quotes are part of the value.

    ``None`` and a value are a distinction an issuer branches on, so a blank or
    whitespace-only header reads as absent rather than propagating ``""``.
    Surrounding whitespace is optional whitespace per RFC 9110 and not part of
    the value, so stripping it is not a modification.

    Over :data:`MAX_HEADER_VALUE_LENGTH` the value is dropped — see the constant.
    """
    value = headers.get(name, "").strip()
    if not value or len(value) > MAX_HEADER_VALUE_LENGTH:
        return None
    return value


def _media_type(headers: dict[str, str]) -> str:
    """The response's media type, normalized, or the generic fallback.

    The ``charset`` parameter describes the bytes' *encoding*, not their type, so
    it is dropped: a consumer grouping facts by media type must not see
    ``text/html`` and ``text/html; charset=utf-8`` as two different kinds of
    thing. Case is normalized for the same reason — RFC 9110 makes the type and
    subtype case-insensitive, and origins are inconsistent about it.

    Reads the same bounded value ``_passthrough`` does, so an absurd
    ``Content-Type`` cannot reach the fact through the normalized channel after
    being refused on the raw one. An absent, blank, parameter-only, or dropped
    header falls back rather than propagating an empty ``media_type``.

    This is a *second channel*, not a replacement: ``content_type_raw`` keeps
    what normalization discards, because Watcher stores the verbatim header as an
    observed fact (watcher#168) and reads ``application/octet-stream`` as
    "unknown, guess from the URL" — a meaning the fallback must not manufacture
    on the raw side.
    """
    header = _passthrough(headers, "content-type") or ""
    return header.split(";")[0].strip().lower() or DEFAULT_MEDIA_TYPE
