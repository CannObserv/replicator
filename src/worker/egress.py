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
would have raised (CR 2). It also moved *when* one surfaces (#100): httpcore
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
CannObserv/broker#14 bounds the second.
"""

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable, Sequence

import httpx

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError

# What a fetch may not reach. Every range here is either this host, this host's
# private network, or the tailnet the bus rides — none of which a public corpus
# is ever served from, so the operational cost of the guard is expected to be
# exactly zero refusals.
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
    "192.168.0.0/16",  # RFC 1918
    "224.0.0.0/4",  # multicast
    "240.0.0.0/4",  # reserved
    "::/128",  # unspecified
    "::1/128",  # loopback
    "fc00::/7",  # unique local
    "fe80::/10",  # link-local
    "ff00::/8",  # multicast
)

# How long the guard waits on one name (#100). Above 5 s deliberately: that is
# glibc's per-try default (``resolv.conf`` ``timeout:5``), and a cap equal to it
# fails exactly the resolve a single dropped packet makes slow — the one libc
# finishes on its second try. A constant rather than a setting, as Watcher's copy
# of this guard has it; the unit's stop budget counts it (tests/test_deploy.py).
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
    # family; for IPv4 and IPv6 it is the address string (CR 6).
    return [str(info[4][0]) for info in infos]


class GuardedTransport(httpx.AsyncBaseTransport):
    """Refuse a request whose destination resolves into a blocked range.

    Composition rather than a subclass of ``AsyncHTTPTransport``: the inner
    transport is what a test replaces to assert the refusal happened *before*
    anything left the host, which is the promise the issuer contract makes about
    every other guard on this path.
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
        self._blocked = blocked
        self._resolve = resolve
        self._resolve_timeout = resolve_timeout

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await self._refuse_blocked_destination(request.url)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()

    async def _refuse_blocked_destination(self, url: httpx.URL) -> None:
        host = url.host
        for address in await self._addresses(host, url.port or _default_port(url)):
            network = _containing(address, self._blocked)
            if network is not None:
                raise PermanentFetchError(
                    f"{url} resolves to {address}, inside the refused range {network} — "
                    f"Replicator does not fetch its own host, its private network, or the "
                    f"tailnet",
                    reason=FailureReason.DESTINATION_REFUSED,
                )

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
        classifies by type** (CR 2). Before this guard, an unresolvable host
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
        by libc's remaining budget, never an unbounded number (CR 2). They hold
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
    """
    if not answer:
        raise TransientFetchError(f"{host} resolved to no addresses")
    try:
        return [_address(value) for value in answer]
    except ValueError as exc:
        raise TransientFetchError(
            f"{host} resolved to something that is not an address: {exc}"
        ) from exc


def _address(value: str) -> Address:
    """Parse ``value`` as the destination it names; ``ValueError`` if it names none.

    An IPv4-mapped IPv6 address (``::ffff:127.0.0.1``) is the same destination
    spelled differently, and the ``/8`` above would not hold it — so it is
    unmapped here, once, for both the literal and the resolved path.
    """
    parsed = ipaddress.ip_address(value)
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        return parsed.ipv4_mapped
    return parsed


def _containing(address: Address, blocked: tuple[Network, ...]) -> Network | None:
    """The first blocked range holding ``address``, or ``None``.

    Takes an address already parsed, so it has no answer to give about one that
    is not — that question is asked, and refused, at the resolver's boundary in
    :func:`_checkable`. This function answered it with ``None`` until #100: an
    arm CR 9 called unreachable, and which failed open.
    """
    for network in blocked:
        if address.version == network.version and address in network:
            return network
    return None
