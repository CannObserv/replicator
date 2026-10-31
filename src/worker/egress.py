"""The destination guard: what a fetch may reach from this host (#95).

``content.fetch`` is an unauthenticated capability — any writer the broker grants
`+xadd` can name a URL, and the bytes that come back are stored in
``co-gcs-blobs`` and announced on ``content.blobs``, where every consumer SA can
read them. So the capability is not "issue an outbound request" but *read
whatever this node's network position reaches, and republish it cluster-wide*.
The broker's grants bound who may pull the trigger; nothing bounded where the
barrel pointed. This module is that bound (#89's decision, option 2).

**Why a transport and not a check in ``_request_options``.** ``follow_redirects``
is ``True`` by default on both ``FetchContent`` and ``AsyncFetchDriver``'s
client, so an origin answering ``302 Location: http://127.0.0.1:9999/`` walks
around any check on the URL the issuer submitted. ``AsyncClient`` calls the
transport once per hop, so a check here is the cheapest thing that sees every
destination actually contacted rather than only the first one requested.

**Why it does not live in co-core.** ``AsyncFetchDriver`` accepts an injected
``httpx.AsyncClient``, so the guard is composed in at
:func:`src.worker.main.run` rather than pushed upstream into a driver three
services share. Replicator's network position is Replicator's fact.

**Resolving here moves where a name failure surfaces**, so this module owns its
classification: a resolution failure is transient and an unencodable hostname is
terminal, because the loop reads the exception *type* and neither is one httpx
would have raised (#95 CR 2). It also moved *when* one surfaces (#100): httpcore
resolves inside its connect timeout, and a resolve ahead of httpcore is ahead of
that too, so the guard carries its own — :data:`RESOLVE_TIMEOUT_SECONDS`.

**An answer the guard cannot check is refused, never passed** (#100). The check
is a loop over the resolved addresses, so an empty answer, or one that is not an
address, would skip it rather than fail it. ``getaddrinfo`` gives neither, but
the resolver is a seam, and the direction a guard fails in must not rest on its
seam's manners.

**The residual, stated rather than closed: DNS rebinding.** The check resolves
and inspects every address, then hands the *name* to the inner transport, which
resolves again — a TOCTOU window an origin controlling its own DNS can aim at.
Closing it means connecting to the pinned address with the ``Host`` header
preserved and certificate verification still keyed to the name, which is
materially more machinery than the threat justifies today: rebinding needs a
hostile origin *and* a bus writer the broker granted aiming a command at it, and
CannObserv/broker#14 bounds the second. The browser's SOCKS guard (#138) shares
:class:`DestinationGuard` and *does* dial the addresses it checked, so the residual
is fetch's alone.

**And how much a fetch may bring back (#104).** :class:`BodyCeilingTransport`
sits inside the guard on the same client, for the same reason it is a transport:
the driver reads every hop's body whole, and this is the one place a body passes
before that. It stops reading at ``REPLICATOR_MAX_BLOB_BYTES``.
"""

import asyncio
import ipaddress
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence

import httpx

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError

# What a fetch may not reach. Every range here is this host, this host's private
# network, the tailnet the bus rides, or an IANA special-purpose range (#161) —
# none of which a public corpus is ever served from, so the operational cost of
# the guard is expected to be exactly zero refusals.
#
# ``100.64.0.0/10`` is the sharp one: CGNAT is where Tailscale assigns node
# addresses, so this is the range that stops one bus participant from being made
# to read another's surfaces.
DEFAULT_BLOCKED_DESTINATIONS: tuple[str, ...] = (
    "0.0.0.0/8",  # "this host on this network" — RFC 1122
    "10.0.0.0/8",  # RFC 1918
    "100.64.0.0/10",  # CGNAT — the tailnet
    "127.0.0.0/8",  # loopback, the whole /8 and not just .0.1
    "169.254.0.0/16",  # link-local, including the cloud metadata address
    "172.16.0.0/12",  # RFC 1918
    "192.0.0.0/24",  # IETF protocol assignments, incl. DNS64 discovery — RFC 6890 §2.1
    "192.0.2.0/24",  # documentation, TEST-NET-1 — RFC 5737
    "192.88.99.0/24",  # 6to4 relay anycast, deprecated — RFC 7526
    "192.168.0.0/16",  # RFC 1918
    "198.18.0.0/15",  # benchmarking, and some providers' internal networks — RFC 2544
    "198.51.100.0/24",  # documentation, TEST-NET-2 — RFC 5737
    "203.0.113.0/24",  # documentation, TEST-NET-3 — RFC 5737
    "224.0.0.0/4",  # multicast
    "240.0.0.0/4",  # reserved
    "::/128",  # unspecified
    "::1/128",  # loopback
    # Local-use NAT64 — RFC 8215. A row, not an unwrap: where the IPv4 address sits
    # depends on the prefix length the site chose (RFC 6052 §2.2). Every fixed-position
    # form is unwrapped in _embedded instead (#156).
    "64:ff9b:1::/48",
    "100::/64",  # discard-only — RFC 6666
    # The next two sit in the IETF's 2001::/23, as Teredo's 2001::/32 does; Teredo is
    # unwrapped in _embedded, not refused, so the /23 itself is no row (#161).
    "2001:2::/48",  # benchmarking — RFC 5180
    "2001:20::/28",  # ORCHIDv2 — RFC 7343
    "2001:db8::/32",  # documentation — RFC 3849
    "3fff::/20",  # documentation — RFC 9637
    "5f00::/16",  # SRv6 SIDs — RFC 9602
    "fc00::/7",  # unique local
    "fe80::/10",  # link-local
    "ff00::/8",  # multicast
)

# How long the guard waits on one name (#100). Above 5 s deliberately: that is
# glibc's per-try default (``resolv.conf`` ``timeout:5``), and a cap equal to it
# fails exactly the resolve a single dropped packet makes slow — the one libc
# finishes on its second try. A constant rather than a setting, as Watcher's copy
# of this guard has it. Inside the handler's whole-fetch deadline (#104), which is
# the term the unit's stop budget counts (tests/test_deploy.py).
RESOLVE_TIMEOUT_SECONDS = 10.0

Address = ipaddress.IPv4Address | ipaddress.IPv6Address
Network = ipaddress.IPv4Network | ipaddress.IPv6Network
Resolver = Callable[[str, int], Awaitable[Sequence[str]]]


def blocked_networks(configured: Iterable[str] | None) -> tuple[Network, ...]:
    """Parse the guard's range table, defaulting to :data:`DEFAULT_BLOCKED_DESTINATIONS`.

    **``None`` means the deny set, never an empty one.** The table is carried in
    env — the config taxonomy's first row, facts about *this host* — and the
    failure mode of anything carried in env is an env file that does not load.
    An operator widening the guard does it by naming the ranges they want,
    visibly, in the file; there is deliberately no flag that spells "off" in one
    token, because that token is what a hurried afternoon reaches for and
    nothing afterwards reads it back.
    """
    values = DEFAULT_BLOCKED_DESTINATIONS if configured is None else tuple(configured)
    return tuple(ipaddress.ip_network(value, strict=True) for value in values)


async def _getaddrinfo(host: str, port: int) -> Sequence[str]:
    """Resolve ``host`` to every address it answers with.

    Through the running loop's ``getaddrinfo`` rather than ``socket``'s, for the
    reason every ``BlobStore`` call goes through ``asyncio.to_thread``: the
    consume path is serial, and a blocking resolve parks the worker rather than
    only this command.
    """
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    # typeshed types a sockaddr's first element ``str | int`` to cover every
    # family; for IPv4 and IPv6 it is the address string (#100 CR 6).
    return [str(info[4][0]) for info in infos]


class DestinationGuard:
    """The destination check, for every caller that opens a connection for an issuer (#138).

    Two callers since #138: :class:`GuardedTransport`, once per fetch hop, and the
    browser's SOCKS guard (``src/worker/browser_egress.py``), once per connection a
    rendered page opens. One check rather than two copies, because the copy that
    drifts is the one nobody tests against a redirect.

    :meth:`vet` **returns the addresses it checked**, so a caller that opens the
    socket itself can connect to exactly those. The SOCKS guard does, which closes
    the rebinding window this module names as its residual for that caller; the
    httpx transport hands the *name* on and keeps the residual.
    """

    def __init__(
        self,
        *,
        blocked: tuple[Network, ...],
        resolve: Resolver = _getaddrinfo,
        resolve_timeout: float = RESOLVE_TIMEOUT_SECONDS,
    ) -> None:
        self._blocked = blocked
        self._resolve = resolve
        self._resolve_timeout = resolve_timeout

    async def vet(self, host: str, port: int, *, label: str | None = None) -> list[Address]:
        """Every address ``host`` answers with, or the refusal for the first blocked one.

        ``label`` is what the refusal names: the URL for a fetch hop, else the
        ``host:port`` asked for — an IPv6 literal bracketed, since its last group
        would otherwise read as the port.
        """
        addresses = await self._addresses(host, port)
        for address in addresses:
            for checked in (address, *_embedded(address)):
                network = _containing(checked, self._blocked)
                if network is None:
                    continue
                spelled = address if checked == address else f"{address} (embedding {checked})"
                raise PermanentFetchError(
                    f"{label or _host_port(host, port)} resolves to {spelled}, inside the refused "
                    f"range {network} — Replicator does not fetch its own host, its private "
                    f"network, the tailnet, or an IANA special-purpose range",
                    reason=FailureReason.DESTINATION_REFUSED,
                )
        return addresses

    async def _addresses(self, host: str, port: int) -> list[Address]:
        """The addresses to check — the literal itself when the URL names one.

        A URL naming an address must not take the resolver path at all: DNS is
        not consulted for a literal, so a guard that resolves first would be
        asking a question with no answer and would have to decide what an
        unresolvable host means. It means nothing here; the address is already
        in hand.
        """
        try:
            literal = _address(host)
        except ValueError:
            return await self._resolve_or_classify(host, port)
        return [literal]

    async def _resolve_or_classify(self, host: str, port: int) -> list[Address]:
        """Resolve, translating every way resolution fails into the loop's terms.

        **Resolving here moved where a name failure surfaces, and the loop
        classifies by type** (#95 CR 2). Before this guard, an unresolvable host
        failed inside httpx as a ``ConnectError`` — an ``httpx.HTTPError``,
        which ``_fetch`` maps to ``TransientFetchError`` and the loop retries
        indefinitely. A bare ``socket.gaierror`` in its place is none of the
        things the loop recognises: not an ``httpx.HTTPError``, not a builtin
        ``ConnectionError``, not in ``_TRANSIENT_ERRORS``. It would reach
        ``_handle_unclassified`` and dead-letter a good command at the delivery
        ceiling because its origin's DNS was briefly unavailable.

        The failures are kept apart rather than caught together, because only
        some of them will answer differently next time: a name that does not
        resolve today may tomorrow, while a label too long to encode is as
        unfetchable on the next reclaim as on this one.

        **The deadline abandons the wait, not the resolve** (#100).
        ``loop.getaddrinfo`` runs in the default executor, and cancelling the
        await leaves that thread to finish on libc's schedule — its full retry
        budget, which the ``search`` line in ``resolv.conf`` can double. A
        transient failure does not wait for a reclaim before the loop takes the
        *next* command, so against a dead nameserver the abandoned threads of
        consecutive commands overlap: a few at once, each outliving its attempt
        by libc's remaining budget, never an unbounded number (#100 CR 2). They hold
        workers of the pool every ``BlobStore`` call's ``asyncio.to_thread`` and
        asyncio's own resolve for a broker reconnect draw on —
        ``min(32, cpus + 4)``, six on this VM.
        """
        try:
            async with asyncio.timeout(self._resolve_timeout):
                answer = await self._resolve(host, port)
        except TimeoutError as exc:
            raise TransientFetchError(
                f"{host} did not resolve within {self._resolve_timeout}s"
            ) from exc
        except socket.gaierror as exc:
            raise TransientFetchError(f"{host} could not be resolved: {exc}") from exc
        except UnicodeError as exc:
            raise PermanentFetchError(
                f"{host} is not an encodable hostname: {exc}",
                reason=FailureReason.NOT_FETCHABLE,
            ) from exc
        return _checkable(host, answer)


class GuardedTransport(httpx.AsyncBaseTransport):
    """Refuse a request whose destination resolves into a blocked range.

    Composition rather than a subclass of ``AsyncHTTPTransport``: the inner
    transport is what a test replaces to assert the refusal happened *before*
    anything left the host, which is the promise the issuer contract makes about
    every other guard on this path. The check is :class:`DestinationGuard`'s.
    """

    def __init__(
        self,
        inner: httpx.AsyncBaseTransport,
        *,
        blocked: tuple[Network, ...],
        resolve: Resolver = _getaddrinfo,
        resolve_timeout: float = RESOLVE_TIMEOUT_SECONDS,
    ) -> None:
        self._inner = inner
        self._guard = DestinationGuard(
            blocked=blocked, resolve=resolve, resolve_timeout=resolve_timeout
        )

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        await self._guard.vet(url.host, url.port or _default_port(url), label=str(url))
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


class BodyCeilingTransport(httpx.AsyncBaseTransport):
    """Stop reading a body at the blob ceiling, rather than after its last byte (#104).

    ``AsyncFetchDriver`` returns ``response.content``, so the whole body was in
    memory before ``_raise_for_size`` measured it: ``REPLICATOR_MAX_BLOB_BYTES``
    decided what was *kept*, not what was *read*. Here, on every hop, a body is
    counted as it arrives. Beside the destination guard for the guard's reason —
    the driver takes an injected client, so this needs no change to the driver
    three services share.

    **A 2xx over the ceiling is refused ``too_large``**, at the chunk that crosses
    it, or before any byte at all when ``Content-Length`` already says so. The
    same reason ``_raise_for_size`` gives, earlier, so an issuer sees no change.

    **Anything else is cut short at the ceiling, not refused.** A non-2xx body is
    never stored or announced — its status is the outcome, and
    ``_raise_for_status`` reads nothing else — so refusing it would put
    ``too_large`` on a body nobody asked to keep, and turn a 503's retry or a
    404's ``http_status`` into a different answer. A 304's ``Content-Length`` may
    describe the representation it is not sending (RFC 9110 §8.6), which is the
    other reason the declared length is read on a 2xx only.

    **What is counted is bytes on the wire.** httpx decodes a ``Content-Encoding``
    after the transport, so a compressed body's decoded size is still checked
    only by ``_raise_for_size``, after the read: a compression bomb is bounded by
    this ceiling on the wire and by nothing before decoding. Deflate's worst-case
    expansion is a fraction of a percent, so an incompressible body within that of
    the ceiling can be refused here although it would have been kept.
    """

    def __init__(self, inner: httpx.AsyncBaseTransport, *, max_bytes: int) -> None:
        self._inner = inner
        self._max_bytes = max_bytes

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        stream = response.stream
        if not isinstance(stream, httpx.AsyncByteStream):  # pragma: no cover - async transports
            raise TypeError(f"an async transport answered with a {type(stream).__name__}")
        kept = response.is_success
        if kept:
            declared = _declared_length(response.headers)
            if declared is not None and declared > self._max_bytes:
                await response.aclose()
                raise _too_large(request.url, f"declares {declared} bytes", self._max_bytes)
        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            stream=_CeilingStream(stream, self._max_bytes, refusing=request.url if kept else None),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self._inner.aclose()


class _CeilingStream(httpx.AsyncByteStream):
    """A response body that ends at ``max_bytes`` — by refusing, or by stopping.

    ``refusing`` is the URL to name in a ``too_large`` refusal, or ``None`` to cut
    the body short instead. Closing it closes the origin's stream, and an origin
    stream closed part-way is a connection httpcore drops rather than pools.
    """

    def __init__(
        self, inner: httpx.AsyncByteStream, max_bytes: int, *, refusing: httpx.URL | None
    ) -> None:
        self._inner = inner
        self._max_bytes = max_bytes
        self._refusing = refusing

    async def __aiter__(self) -> AsyncIterator[bytes]:
        seen = 0
        async for chunk in self._inner:
            seen += len(chunk)
            if seen <= self._max_bytes:
                yield chunk
                continue
            if self._refusing is not None:
                raise _too_large(self._refusing, f"sent {seen} bytes and more", self._max_bytes)
            yield chunk[: len(chunk) - (seen - self._max_bytes)]
            return

    async def aclose(self) -> None:
        await self._inner.aclose()


def _declared_length(headers: httpx.Headers) -> int | None:
    """``Content-Length`` as a number, or ``None`` when absent or not ``1*DIGIT``."""
    value = headers.get("content-length", "").strip()
    return int(value) if value.isascii() and value.isdigit() else None


def _too_large(url: httpx.URL, observed: str, maximum: int) -> PermanentFetchError:
    return PermanentFetchError(
        f"{url} {observed}, over the {maximum}-byte ceiling — stopped reading",
        reason=FailureReason.TOO_LARGE,
    )


def _host_port(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _default_port(url: httpx.URL) -> int:
    return 443 if url.scheme == "https" else 80


def _checkable(host: str, answer: Sequence[str]) -> list[Address]:
    """The resolver's answer as addresses the guard can check, or a refusal (#100).

    **Both refusals close a hole the loop above would otherwise leave open.** The
    guard refuses from *inside* a loop over the answer, so an empty answer runs
    it zero times and passes the request unchecked; and before #100 an answer
    that did not parse reached ``_containing`` as ``None`` — "in no blocked
    range" — and passed too. Neither comes from ``getaddrinfo``; both can come
    from a resolver seam that is not ``getaddrinfo``.

    Transient, not terminal: the command is sound and the resolver is not, so
    the command waits in the PEL for the answer to change — or the resolver to be
    fixed — rather than dead-lettering a URL that was never the problem.

    **Emptiness is judged on the list, not on the answer** (#100 CR 7).
    ``not <generator>`` is ``False`` whatever it yields, so testing the answer
    let an empty iterator through the same zero-trip loop; ``or ()`` keeps a
    ``None`` answer on this refusal rather than a ``TypeError``.
    """
    try:
        addresses = [_address(value) for value in answer or ()]
    except ValueError as exc:
        raise TransientFetchError(
            f"{host} resolved to something that is not an address: {exc}"
        ) from exc
    if not addresses:
        raise TransientFetchError(f"{host} resolved to no addresses")
    return addresses


def _address(value: str) -> Address:
    """Parse ``value`` as the destination it names; ``ValueError`` if it names none.

    An IPv4-mapped IPv6 address (``::ffff:127.0.0.1``) is the same destination
    spelled differently, and the ``/8`` above would not hold it — so it is
    unmapped here, once, for both the literal and the resolved path. The rest of its
    class is checked, not unmapped: :func:`_embedded`.
    """
    parsed = ipaddress.ip_address(value)
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        return parsed.ipv4_mapped
    return parsed


# The IPv6 forms that carry an IPv4 address in their low 32 bits under a fixed prefix.
_LOW_32_PREFIXES = (
    ipaddress.IPv6Network("64:ff9b::/96"),  # NAT64 well-known prefix — RFC 6052 §2.1
    ipaddress.IPv6Network("::ffff:0:0:0/96"),  # SIIT IPv4-translated — RFC 2765 §2.1
    ipaddress.IPv6Network("::/96"),  # IPv4-compatible, deprecated — RFC 4291 §2.5.5.1
)
# ISATAP's interface id, ``0000:5efe`` or ``0200:5efe`` (u bit), under any prefix — RFC 5214 §6.1.
_ISATAP_IID, _U_BIT = 0x00005EFE, 0x02000000


def _embedded(address: Address) -> tuple[ipaddress.IPv4Address, ...]:
    """The IPv4 addresses ``address`` carries at a fixed position, to check beside it (#156).

    ``_containing`` compares versions, so ``64:ff9b::a00:7`` is never tested against
    ``10.0.0.0/8`` — yet a NAT64 translator delivers it to ``10.0.0.7``. Each form here
    puts its IPv4 address at a position fixed by its RFC; local-use NAT64 does not, and
    is a row in :data:`DEFAULT_BLOCKED_DESTINATIONS` instead.

    **Checked, never substituted.** :meth:`DestinationGuard.vet` returns the address
    itself and the SOCKS guard dials it: as IPv4, a public NAT64 destination is
    unreachable from an IPv6-only host, and 6to4's embedded address is the site's
    relay router, not the host behind it. The mapped form *is* substituted, in
    :func:`_address`, because it names the same destination.

    **An unwrap rather than a row per prefix**, because ``REPLICATOR_BLOCKED_DESTINATIONS``
    replaces the table: a host that sets it keeps this check, and a public NAT64 or
    6to4 destination stays reachable.

    Not covered: an operator-chosen prefix — a NAT64 network-specific prefix, 6rd, IVI,
    MAP — whose position cannot be known without the prefix.
    """
    if not isinstance(address, ipaddress.IPv6Address):
        return ()
    value = int(address)
    low_32 = ipaddress.IPv4Address(value & 0xFFFFFFFF)
    found: list[ipaddress.IPv4Address] = []
    if any(address in prefix for prefix in _LOW_32_PREFIXES):
        found.append(low_32)
    if address.sixtofour is not None:  # bits 16–48 — RFC 3056
        found.append(address.sixtofour)
    if address.teredo is not None:  # server, and the client deobfuscated — RFC 4380 §4
        found.extend(address.teredo)
    if (value >> 32) & 0xFFFFFFFF & ~_U_BIT == _ISATAP_IID:
        found.append(low_32)
    return tuple(found)


def _containing(address: Address, blocked: tuple[Network, ...]) -> Network | None:
    """The first blocked range holding ``address``, or ``None``.

    Takes an address already parsed, so it has no answer to give about one that
    is not — that question is asked, and refused, at the resolver's boundary in
    :func:`_checkable`. This function answered it with ``None`` until #100: an
    arm #95 CR 9 called unreachable, and which failed open.
    """
    for network in blocked:
        if address.version == network.version and address in network:
            return network
    return None
