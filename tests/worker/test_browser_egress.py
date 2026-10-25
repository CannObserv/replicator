"""The browser's SOCKS guard: what a rendered page may reach from this host (#138).

A rendered page opens connections nobody asked for by URL — subresources, scripts,
WebSockets, beacons — so the browser is pointed at a per-render SOCKS5 listener and every
connection it opens is vetted by the same :class:`DestinationGuard` a fetch hop is.

The tests that matter most:

- **a refused CONNECT never opens an upstream socket, and is remembered**, because the
  capture fails as a whole on the first refusal (the design's operator call 1), so the
  renderer must be able to ask afterwards;
- **the guard connects to the address it checked**, never re-resolving, which is the
  rebinding window ``egress.py`` leaves open for fetch;
- **only CONNECT**: BIND and UDP ASSOCIATE would be a way round the check.

The ranges here are narrowed to ``10.0.0.0/8`` so a loopback upstream can stand in for the
public internet; the default table's loopback refusal is ``test_egress.py``'s to pin.
"""

import asyncio
import socket
import struct

import pytest

from src.core.errors import FailureReason, PermanentFetchError
from src.worker.browser_egress import SocksGuard
from src.worker.egress import DestinationGuard, blocked_networks


def _destination_guard(mapping: dict[str, list[str]], calls: list[str] | None = None):
    async def resolve(host: str, port: int) -> list[str]:
        if calls is not None:
            calls.append(host)
        if host not in mapping:
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        return mapping[host]

    return DestinationGuard(blocked=blocked_networks(["10.0.0.0/8"]), resolve=resolve)


@pytest.fixture
async def upstream():
    """An echo server on loopback, counting the connections it accepted."""
    accepted: list[int] = []

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.append(1)
        while data := await reader.read(4096):
            writer.write(data)
            await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    yield port, accepted
    server.close()
    await server.wait_closed()


async def _socks_connect(
    port: int, atyp: int, address: bytes, dst_port: int, *, cmd: int = 1
) -> tuple[int, asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"\x05\x01\x00")
    assert await reader.readexactly(2) == b"\x05\x00"
    writer.write(bytes([5, cmd, 0, atyp]) + address + struct.pack("!H", dst_port))
    head = await reader.readexactly(4)
    assert head[0] == 5
    bound = {1: 4, 4: 16}[head[3]]
    await reader.readexactly(bound + 2)
    return head[1], reader, writer


def _name(host: str) -> bytes:
    return bytes([len(host)]) + host.encode()


async def test_a_public_name_is_relayed(upstream):
    up_port, accepted = upstream
    async with SocksGuard(_destination_guard({"public.test": ["127.0.0.1"]})) as guard:
        rep, reader, writer = await _socks_connect(guard.port, 3, _name("public.test"), up_port)
        assert rep == 0
        writer.write(b"ping")
        assert await reader.readexactly(4) == b"ping"
        writer.close()

    assert accepted == [1]
    assert guard.refusal is None


async def test_a_refused_name_opens_nothing_and_is_remembered(upstream):
    up_port, accepted = upstream
    async with SocksGuard(_destination_guard({"intranet.test": ["10.0.0.7"]})) as guard:
        rep, _, writer = await _socks_connect(guard.port, 3, _name("intranet.test"), up_port)
        writer.close()

    assert rep == 2  # connection not allowed by ruleset
    assert accepted == []
    assert isinstance(guard.refusal, PermanentFetchError)
    assert guard.refusal.reason is FailureReason.DESTINATION_REFUSED
    assert "intranet.test" in str(guard.refusal)
    assert guard.refused.is_set()


async def test_a_refused_literal_is_refused_without_resolving(upstream):
    up_port, _ = upstream
    calls: list[str] = []
    async with SocksGuard(_destination_guard({}, calls)) as guard:
        rep, _, writer = await _socks_connect(guard.port, 1, socket.inet_aton("10.9.9.9"), up_port)
        writer.close()

    assert rep == 2
    assert calls == []
    assert guard.refusal is not None


async def test_the_first_refusal_is_the_one_kept(upstream):
    up_port, _ = upstream
    mapping = {"a.test": ["10.0.0.1"], "b.test": ["10.0.0.2"]}
    async with SocksGuard(_destination_guard(mapping)) as guard:
        for host in ("a.test", "b.test"):
            _, _, writer = await _socks_connect(guard.port, 3, _name(host), up_port)
            writer.close()

    assert "a.test" in str(guard.refusal)


async def test_the_guard_connects_to_the_address_it_checked(upstream):
    """One resolve per CONNECT: the socket goes to the vetted answer, not a fresh one."""
    up_port, accepted = upstream
    calls: list[str] = []
    async with SocksGuard(_destination_guard({"public.test": ["127.0.0.1"]}, calls)) as guard:
        rep, _, writer = await _socks_connect(guard.port, 3, _name("public.test"), up_port)
        writer.close()

    assert rep == 0
    assert calls == ["public.test"]
    assert accepted == [1]


async def test_an_unreachable_first_address_falls_through_to_the_next(upstream):
    """This host has no IPv6 route: an AAAA-first answer must not cost the render."""
    up_port, accepted = upstream
    mapping = {"dual.test": ["::1", "127.0.0.1"]}
    async with SocksGuard(_destination_guard(mapping)) as guard:
        rep, _, writer = await _socks_connect(guard.port, 3, _name("dual.test"), up_port)
        writer.close()

    assert rep == 0  # ::1 has nothing on up_port; 127.0.0.1 does
    assert accepted == [1]


async def test_an_unresolvable_name_is_unreachable_not_refused(upstream):
    up_port, _ = upstream
    async with SocksGuard(_destination_guard({})) as guard:
        rep, _, writer = await _socks_connect(guard.port, 3, _name("nowhere.test"), up_port)
        writer.close()

    assert rep == 4  # host unreachable
    assert guard.refusal is None


@pytest.mark.parametrize("cmd", [2, 3], ids=["bind", "udp-associate"])
async def test_only_connect_is_served(upstream, cmd):
    up_port, accepted = upstream
    async with SocksGuard(_destination_guard({"public.test": ["127.0.0.1"]})) as guard:
        rep, _, writer = await _socks_connect(guard.port, 3, _name("public.test"), up_port, cmd=cmd)
        writer.close()

    assert rep == 7  # command not supported
    assert accepted == []


async def test_a_client_offering_no_unauthenticated_method_is_turned_away():
    async with SocksGuard(_destination_guard({})) as guard:
        reader, writer = await asyncio.open_connection("127.0.0.1", guard.port)
        writer.write(b"\x05\x01\x02")  # username/password only
        assert await reader.readexactly(2) == b"\x05\xff"
        writer.close()


async def test_the_listener_is_loopback_and_closes_with_the_render():
    async with SocksGuard(_destination_guard({})) as guard:
        port = guard.port
        assert guard.host == "127.0.0.1"

    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", port)


async def test_closing_the_guard_ends_open_relays(upstream):
    up_port, _ = upstream
    async with SocksGuard(_destination_guard({"public.test": ["127.0.0.1"]})) as guard:
        _, reader, _writer = await _socks_connect(guard.port, 3, _name("public.test"), up_port)

    assert await asyncio.wait_for(reader.read(), timeout=2) == b""


async def test_the_proxy_server_flag_names_the_listener():
    async with SocksGuard(_destination_guard({})) as guard:
        assert guard.proxy_server == f"socks5://127.0.0.1:{guard.port}"
