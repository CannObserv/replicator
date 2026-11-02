"""A TLS error after the handshake, through the worker's own client (#173).

httpcore maps a handshake's ``ssl.SSLError`` to ``ConnectError``, and not one a
later read or write raises: anyio converts only the EOF and syscall cases, so a
record that fails its MAC mid-response reaches ``_fetch`` as the raw
``ssl.SSLError`` — not an ``httpx.HTTPError``, and not the builtin
``ConnectionError`` the loop retries. Left there it lands in the loop's
unclassified branch, the one ``UnicodeEncodeError`` would have reached before
``_HEADER_VALUE``: retried to the delivery ceiling, then closed ``handler_error``.

Measured on a real socket for #173, with a forged record before the status line
and after part of the body. A mock here, because a forged record needs a TLS
origin and the guard refuses loopback; the mock raises from the same two places.

Through ``build_fetch_client``'s stack rather than a fake fetcher, so the test
also shows neither the guard nor the ceiling's stream wrapper turns the error into
something else on the way.
"""

import ssl

import httpx
import pytest
from co_core_aio.fetch import AsyncFetchDriver

from src.core.config import get_settings
from src.core.errors import TransientFetchError
from src.worker.main import build_fetch_client
from tests.worker.conftest import command, published_facts

# A public literal, so the guard checks the address without a resolve.
PUBLIC_URL = "https://93.184.216.34/page"


def bad_record() -> ssl.SSLError:
    """The error a forged record raised on the real socket, as OpenSSL names it."""
    return ssl.SSLError(1, "[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC] decryption failed")


class BadRecordBody(httpx.AsyncByteStream):
    """A body whose second record fails its MAC, after the first arrived intact."""

    async def __aiter__(self):
        yield b"<html>"
        raise bad_record()


def answer_before_the_status_line(request: httpx.Request) -> httpx.Response:
    raise bad_record()


def answer_mid_body(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, headers={"content-type": "text/html"}, stream=BadRecordBody())


@pytest.mark.parametrize(
    "answer",
    [answer_before_the_status_line, answer_mid_body],
    ids=["before-the-status-line", "mid-body"],
)
async def test_a_tls_error_after_the_handshake_is_transient(
    handler, fake_redis, monkeypatch, answer
):
    """A peer's broken record is the network's fault for now, not the command's forever."""
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", lambda: httpx.MockTransport(answer))

    async with build_fetch_client(get_settings()) as client:
        with pytest.raises(TransientFetchError, match="BAD_RECORD_MAC") as caught:
            await handler(AsyncFetchDriver(client))(command(url=PUBLIC_URL))

    assert isinstance(caught.value.__cause__, ssl.SSLError)
    assert await published_facts(fake_redis) == []
