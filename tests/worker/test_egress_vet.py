"""The destination check itself, as a seam a second caller can use (#138).

The browser's SOCKS guard (``src/worker/browser_egress.py``) asks the same question the
fetch transport asks, so the answer has to come from one place: two copies of the range
check would drift, and the copy that drifted would be the one nobody tests against a
redirect. ``DestinationGuard.vet`` is that place, and it returns the addresses it checked
so a caller can connect to exactly those (the SOCKS guard does, closing the rebinding
window ``egress.py`` names as its residual).
"""

import ipaddress
import re
import socket

import pytest

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.worker.egress import DestinationGuard, blocked_networks


def _resolver(mapping: dict[str, list[str]]):
    async def resolve(host: str, port: int) -> list[str]:
        if host not in mapping:
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        return mapping[host]

    return resolve


def _guard(mapping: dict[str, list[str]]) -> DestinationGuard:
    return DestinationGuard(blocked=blocked_networks(None), resolve=_resolver(mapping))


async def test_a_public_name_returns_every_address_it_checked():
    guard = _guard({"example.org": ["93.184.215.14", "2606:2800:21f:cb07:6820:80da:af6b:8b2c"]})

    addresses = await guard.vet("example.org", 443)

    assert addresses == [
        ipaddress.ip_address("93.184.215.14"),
        ipaddress.ip_address("2606:2800:21f:cb07:6820:80da:af6b:8b2c"),
    ]


async def test_one_blocked_answer_beside_a_public_one_refuses_the_name():
    guard = _guard({"rebind.example": ["93.184.215.14", "127.0.0.1"]})

    with pytest.raises(PermanentFetchError) as refused:
        await guard.vet("rebind.example", 80)

    assert refused.value.reason is FailureReason.DESTINATION_REFUSED


async def test_a_literal_is_checked_without_the_resolver():
    guard = _guard({})  # any resolve would raise

    with pytest.raises(PermanentFetchError) as refused:
        await guard.vet("10.1.2.3", 80)

    assert refused.value.reason is FailureReason.DESTINATION_REFUSED
    assert await guard.vet("93.184.215.14", 80) == [ipaddress.ip_address("93.184.215.14")]


async def test_a_mapped_ipv6_literal_is_unmapped_before_the_check():
    with pytest.raises(PermanentFetchError):
        await _guard({}).vet("::ffff:127.0.0.1", 80)


async def test_an_embedding_ipv6_address_is_refused_by_what_it_embeds():
    with pytest.raises(PermanentFetchError, match=re.escape("refused range 10.0.0.0/8 ")):
        await _guard({}).vet("64:ff9b::a00:7", 80)


async def test_an_embedding_ipv6_address_is_returned_as_itself_not_unwrapped():
    """The SOCKS guard dials what ``vet`` returns (#156).

    The embedded address is checked, never substituted. Dialled as IPv4,
    ``64:ff9b::5db8:d822`` would be unreachable from an IPv6-only NAT64 host, and a
    6to4 address would reach the site's relay router instead of the host behind it.
    """
    for address in ("64:ff9b::5db8:d822", "2002:5db8:d822::1"):
        assert await _guard({}).vet(address, 443) == [ipaddress.ip_address(address)]


async def test_an_ipv6_literal_is_bracketed_in_the_refusal():
    """``2002:a9fe:a9fe::1:80`` names a different address; the port needs brackets to read."""
    with pytest.raises(PermanentFetchError) as refused:
        await _guard({}).vet("2002:a9fe:a9fe::1", 80)

    assert str(refused.value).startswith("[2002:a9fe:a9fe::1]:80 resolves to ")


async def test_an_unresolvable_name_is_transient():
    with pytest.raises(TransientFetchError):
        await _guard({}).vet("nowhere.invalid", 80)


async def test_the_refusal_names_what_the_caller_says_was_asked_for():
    guard = _guard({"intranet.example": ["192.168.1.5"]})

    with pytest.raises(PermanentFetchError, match="wss://intranet.example/feed"):
        await guard.vet("intranet.example", 443, label="wss://intranet.example/feed")
