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
would have raised (CR 2).

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
    return [info[4][0] for info in infos]


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
    ) -> None:
        self._inner = inner
        self._blocked = blocked
        self._resolve = resolve

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

    async def _addresses(self, host: str, port: int) -> Sequence[str]:
        """The addresses to check — the literal itself when the URL names one.

        A URL naming an address must not take the resolver path at all: DNS is
        not consulted for a literal, so a guard that resolves first would be
        asking a question with no answer and would have to decide what an
        unresolvable host means. It means nothing here; the address is already
        in hand.
        """
        try:
            ipaddress.ip_address(host)
        except ValueError:
            return await self._resolve_or_classify(host, port)
        return [host]

    async def _resolve_or_classify(self, host: str, port: int) -> Sequence[str]:
        """Resolve, translating the two failures resolution has into the loop's.

        **Resolving here moved where a name failure surfaces, and the loop
        classifies by type** (CR 2). Before this guard, an unresolvable host
        failed inside httpx as a ``ConnectError`` — an ``httpx.HTTPError``,
        which ``_fetch`` maps to ``TransientFetchError`` and the loop retries
        indefinitely. A bare ``socket.gaierror`` in its place is none of the
        things the loop recognises: not an ``httpx.HTTPError``, not a builtin
        ``ConnectionError``, not in ``_TRANSIENT_ERRORS``. It would reach
        ``_handle_unclassified`` and dead-letter a good command at the delivery
        ceiling because its origin's DNS was briefly unavailable.

        The two failures are kept apart rather than caught together, because
        only one of them will answer differently next time: a name that does not
        resolve today may tomorrow, while a label too long to encode is as
        unfetchable on the next reclaim as on this one.
        """
        try:
            return await self._resolve(host, port)
        except socket.gaierror as exc:
            raise TransientFetchError(f"{host} could not be resolved: {exc}") from exc
        except UnicodeError as exc:
            raise PermanentFetchError(
                f"{host} is not an encodable hostname: {exc}",
                reason=FailureReason.NOT_FETCHABLE,
            ) from exc


def _default_port(url: httpx.URL) -> int:
    return 443 if url.scheme == "https" else 80


def _containing(address: str, blocked: tuple[Network, ...]) -> Network | None:
    """The first blocked range holding ``address``, or ``None``.

    An address that does not parse is not silently allowed: it cannot have come
    from ``getaddrinfo``, so it came from somewhere this function does not model
    and the safe reading of an unmodelled destination is to let the transport
    below fail on it rather than to claim it was checked.
    """
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:  # pragma: no cover - getaddrinfo does not produce these
        return None
    # An IPv4-mapped IPv6 address (::ffff:127.0.0.1) is the same destination
    # spelled differently, and the /8 above would not hold it.
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
        parsed = parsed.ipv4_mapped
    for network in blocked:
        if parsed.version == network.version and parsed in network:
            return network
    return None
