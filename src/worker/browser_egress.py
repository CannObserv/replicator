"""The browser's egress guard: a per-render SOCKS5 listener (#138).

A fetch makes the requests it is asked to make; a rendered page makes whatever its
scripts ask for. So the browser is launched with ``--proxy-server`` pointed here, and
every TCP connection its network service opens — subresources, workers, WebSockets,
beacons, prefetches, redirect hops — arrives as a SOCKS ``CONNECT`` and is vetted by the
same :class:`~src.worker.egress.DestinationGuard` a fetch hop is. The spike behind the
design (``docs/plans/2026-10-25-viewport-capture-design.md``) ran every one of those
through a proxy with zero canary hits; UDP is the launcher's to shut off (QUIC and
non-proxied WebRTC), because SOCKS sees only TCP.

**It connects to the address it checked.** Chromium sends the hostname (``ATYP`` 3) and
does not resolve it, so the guard resolves once, checks every answer, then dials those
answers in order. Nothing re-resolves, which closes the rebinding window ``egress.py``
names as its residual — for this caller.

**A refusal is remembered, and the first one wins.** The capture fails as a whole on any
refused connection (operator call 1 on #138), so the renderer races the page against
:attr:`SocksGuard.refused` and reads :attr:`SocksGuard.refusal` afterwards. A name that
will not resolve is *not* a refusal: the reply is "host unreachable" and the page carries
on without that subresource, as it would on any network.

**No authentication, by design.** Chromium does no SOCKS auth, and the listener grants
nothing a local process lacks: public internet, through the guard. It binds
``127.0.0.1:0`` and lives exactly as long as one render, so a refusal belongs to one
capture. **The accepted residual (CR 10):** any local process that finds the port can
``CONNECT`` to a blocked address during that render and fail the capture as
``destination_refused``. It reaches nothing new that way, only disrupts one capture;
checking the peer (``SO_PEERCRED``) would close it at the cost of a second identity check.
"""

import asyncio
import contextlib
import ipaddress
import socket
import struct
from types import TracebackType
from typing import Self

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.core.logging import get_logger
from src.worker.egress import DestinationGuard

logger = get_logger(__name__)

# How long a client has to finish the greeting and request, and how long one dial may take.
# Both are per connection and inside the render's own deadline, which is what bounds the
# capture; these only stop one stalled socket from holding a relay slot to the end of it.
HANDSHAKE_TIMEOUT_SECONDS = 10.0
CONNECT_TIMEOUT_SECONDS = 10.0

_VERSION = 5
_NO_AUTH = 0x00
_NO_ACCEPTABLE = 0xFF
_CONNECT = 0x01
_ATYP_V4, _ATYP_NAME, _ATYP_V6 = 0x01, 0x03, 0x04

# RFC 1928 §6 reply codes.
_SUCCEEDED = 0x00
_GENERAL_FAILURE = 0x01
_NOT_ALLOWED = 0x02
_HOST_UNREACHABLE = 0x04
_COMMAND_NOT_SUPPORTED = 0x07
_ATYP_NOT_SUPPORTED = 0x08

_RELAY_CHUNK = 64 * 1024


class _HandshakeError(Exception):
    """The client spoke something other than the SOCKS5 we serve; the reply is ``rep``."""

    def __init__(self, rep: int | None) -> None:
        super().__init__(rep)
        self.rep = rep


class SocksGuard:
    """One render's SOCKS5 listener, vetting every ``CONNECT`` (``async with``)."""

    host = "127.0.0.1"

    def __init__(
        self,
        guard: DestinationGuard,
        *,
        handshake_timeout: float = HANDSHAKE_TIMEOUT_SECONDS,
        connect_timeout: float = CONNECT_TIMEOUT_SECONDS,
    ) -> None:
        self._guard = guard
        self._handshake_timeout = handshake_timeout
        self._connect_timeout = connect_timeout
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self.refusal: PermanentFetchError | None = None
        self.refused = asyncio.Event()

    @property
    def port(self) -> int:
        if self._server is None:
            raise RuntimeError("the SOCKS guard is not listening")
        return self._server.sockets[0].getsockname()[1]

    @property
    def proxy_server(self) -> str:
        """The value for Chromium's ``--proxy-server`` (Playwright's ``proxy.server``)."""
        return f"socks5://{self.host}:{self.port}"

    async def __aenter__(self) -> Self:
        self._server = await asyncio.start_server(self._serve, self.host, 0)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._server is not None:
            self._server.close()
        for writer in list(self._writers):
            writer.close()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._server is not None:
            await self._server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        self._writers.add(writer)
        try:
            await self._handle(reader, writer)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self._tasks.discard(task)
            self._writers.discard(writer)
            writer.close()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            host, port = await asyncio.wait_for(
                self._handshake(reader, writer), self._handshake_timeout
            )
        except _HandshakeError as exc:
            if exc.rep is not None:
                await self._reply(writer, exc.rep)
            return
        except TimeoutError:
            return

        try:
            addresses = await self._guard.vet(host, port)
        except PermanentFetchError as exc:
            # Only a destination fails the capture. A name the resolver cannot encode
            # is ``not_fetchable``, a broken subresource rather than a private one (CR 4).
            if exc.reason is not FailureReason.DESTINATION_REFUSED:
                await self._reply(writer, _HOST_UNREACHABLE)
                return
            if self.refusal is None:
                self.refusal = exc
                logger.warning("capture connection refused", extra={"destination": host})
            self.refused.set()
            await self._reply(writer, _NOT_ALLOWED)
            return
        except TransientFetchError:
            await self._reply(writer, _HOST_UNREACHABLE)
            return

        upstream = await self._dial(addresses, port)
        if upstream is None:
            await self._reply(writer, _HOST_UNREACHABLE)
            return
        up_reader, up_writer = upstream
        self._writers.add(up_writer)
        try:
            await self._reply(writer, _SUCCEEDED)
            await _relay(reader, writer, up_reader, up_writer)
        finally:
            self._writers.discard(up_writer)
            up_writer.close()

    async def _handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> tuple[str, int]:
        version, count = await reader.readexactly(2)
        if version != _VERSION:
            raise _HandshakeError(None)
        methods = await reader.readexactly(count)
        if _NO_AUTH not in methods:
            writer.write(bytes([_VERSION, _NO_ACCEPTABLE]))
            await writer.drain()
            raise _HandshakeError(None)
        writer.write(bytes([_VERSION, _NO_AUTH]))
        await writer.drain()

        version, command, _reserved, atyp = await reader.readexactly(4)
        if version != _VERSION:
            raise _HandshakeError(_GENERAL_FAILURE)
        if atyp == _ATYP_V4:
            host = socket.inet_ntop(socket.AF_INET, await reader.readexactly(4))
        elif atyp == _ATYP_V6:
            host = socket.inet_ntop(socket.AF_INET6, await reader.readexactly(16))
        elif atyp == _ATYP_NAME:
            (length,) = await reader.readexactly(1)
            # ASCII, passed through: Chromium has already punycoded the name (IDNA2008,
            # non-transitional), which Python's IDNA2003 codec refuses to decode (CR 3).
            try:
                host = (await reader.readexactly(length)).decode("ascii")
            except UnicodeDecodeError as exc:
                raise _HandshakeError(_GENERAL_FAILURE) from exc
        else:
            raise _HandshakeError(_ATYP_NOT_SUPPORTED)
        (port,) = struct.unpack("!H", await reader.readexactly(2))
        if command != _CONNECT:
            raise _HandshakeError(_COMMAND_NOT_SUPPORTED)
        return host, port

    async def _dial(
        self, addresses: list[ipaddress.IPv4Address | ipaddress.IPv6Address], port: int
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
        for address in addresses:
            with contextlib.suppress(OSError, TimeoutError):
                return await asyncio.wait_for(
                    asyncio.open_connection(str(address), port), self._connect_timeout
                )
        return None

    @staticmethod
    async def _reply(writer: asyncio.StreamWriter, rep: int) -> None:
        writer.write(bytes([_VERSION, rep, 0, _ATYP_V4, 0, 0, 0, 0, 0, 0]))
        with contextlib.suppress(ConnectionError):
            await writer.drain()


async def _relay(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    up_reader: asyncio.StreamReader,
    up_writer: asyncio.StreamWriter,
) -> None:
    """Copy both ways until either side closes."""

    async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError):
            while data := await src.read(_RELAY_CHUNK):
                dst.write(data)
                await dst.drain()
        with contextlib.suppress(OSError):
            if dst.can_write_eof():
                dst.write_eof()

    downstream = asyncio.ensure_future(pipe(up_reader, writer))
    upstream = asyncio.ensure_future(pipe(reader, up_writer))
    try:
        await asyncio.wait({downstream, upstream}, return_when=asyncio.FIRST_COMPLETED)
        # One side finished; give the other a moment to drain its half, then stop it.
        await asyncio.wait({downstream, upstream}, timeout=CONNECT_TIMEOUT_SECONDS)
    finally:
        for task in (downstream, upstream):
            task.cancel()
        await asyncio.gather(downstream, upstream, return_exceptions=True)
