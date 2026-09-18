"""The destination guard: what a fetch may reach from this host (#95).

Every test here is a way the guard stops holding. The three that matter most are
not the per-range ones — those are arithmetic — but:

- **the redirect hop**, because the obvious implementation (a check on
  ``command.url`` in ``_request_options``) passes every other test in this file
  and is walked around by a ``302``;
- **every resolved address**, because a name that answers with one public and one
  loopback address is the cheap half of DNS rebinding;
- **unset means the deny set**, because the failure mode of a guard carried in
  env is an env file that does not load.

The decision and the three tests it was run through: #89. The scope: #95.
"""

import socket

import httpx
import pytest

from src.core.errors import (
    FailureReason,
    PermanentError,
    PermanentFetchError,
    TransientError,
    TransientFetchError,
)
from src.worker.egress import DEFAULT_BLOCKED_DESTINATIONS, GuardedTransport, blocked_networks


def _never_called(request: httpx.Request) -> httpx.Response:  # pragma: no cover - asserts
    raise AssertionError(f"a request left the host for {request.url}")


def _resolver(mapping: dict[str, list[str]]):
    """A stand-in for ``getaddrinfo``, so no test depends on real DNS."""

    async def resolve(host: str, port: int) -> list[str]:
        return mapping[host]

    return resolve


def _guard(mapping: dict[str, list[str]], *, inner=None, blocked=None) -> GuardedTransport:
    return GuardedTransport(
        inner or httpx.MockTransport(_never_called),
        blocked=blocked if blocked is not None else blocked_networks(None),
        resolve=_resolver(mapping),
    )


@pytest.mark.parametrize(
    ("label", "address"),
    [
        ("loopback", "127.0.0.1"),
        ("loopback, not .0.1", "127.99.4.2"),
        ("IPv6 loopback", "::1"),
        ("RFC 1918 /8", "10.0.0.7"),
        ("RFC 1918 /12", "172.16.31.9"),
        ("RFC 1918 /16", "192.168.1.1"),
        ("CGNAT — the tailnet", "100.101.102.103"),
        ("link-local", "169.254.169.254"),
        ("IPv6 link-local", "fe80::1"),
        ("unique local", "fd00::1"),
        ("unspecified", "0.0.0.0"),
    ],
)
async def test_a_blocked_address_is_refused_before_the_request_leaves(label, address):
    """Refused, and refused *before* the inner transport is reached.

    The MockTransport raises if it is ever called, so this asserts the
    contract's promise — "a refused command is refused before any request goes
    out" — rather than only the refusal.
    """
    transport = _guard({"target.invalid": [address]})
    request = httpx.Request("GET", "http://target.invalid/x")

    with pytest.raises(PermanentFetchError) as caught:
        await transport.handle_async_request(request)

    assert caught.value.reason is FailureReason.DESTINATION_REFUSED
    assert address in str(caught.value), label


async def test_a_public_address_passes():
    inner = httpx.MockTransport(lambda request: httpx.Response(200, content=b"ok"))
    transport = _guard({"example.gov": ["93.184.216.34"]}, inner=inner)

    response = await transport.handle_async_request(httpx.Request("GET", "http://example.gov/"))

    assert response.status_code == 200


async def test_a_literal_address_is_checked_without_resolution():
    """A URL naming an address needs no DNS, and must not get a free pass.

    The resolver here raises for every name, so a guard that resolves before
    recognising a literal fails this rather than silently allowing it.
    """

    async def resolve(host: str, port: int) -> list[str]:  # pragma: no cover - asserts
        raise AssertionError(f"resolved {host}, which is already an address")

    transport = GuardedTransport(
        httpx.MockTransport(_never_called), blocked=blocked_networks(None), resolve=resolve
    )

    with pytest.raises(PermanentFetchError):
        await transport.handle_async_request(httpx.Request("GET", "http://127.0.0.1:9999/"))


async def test_every_resolved_address_is_checked_not_only_the_first():
    """The cheap half of DNS rebinding: one public answer beside a loopback one.

    A guard that checks ``addresses[0]`` passes every other test in this file.
    """
    transport = _guard({"split.invalid": ["93.184.216.34", "127.0.0.1"]})

    with pytest.raises(PermanentFetchError):
        await transport.handle_async_request(httpx.Request("GET", "http://split.invalid/"))


async def test_the_guard_runs_on_the_redirect_hop():
    """An origin that 302s to loopback is refused at the second hop.

    **This is the test that distinguishes a transport-level guard from a
    pre-flight check on the command's URL.** The submitted URL is public and
    passes; the hop it redirects to is the one that matters, and only a check
    inside the transport sees it — ``AsyncClient`` calls the transport once per
    hop.
    """
    seen: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://127.0.0.1:9999/"})

    transport = _guard({"example.gov": ["93.184.216.34"]}, inner=httpx.MockTransport(respond))

    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        with pytest.raises(PermanentFetchError):
            await client.get("http://example.gov/")

    assert seen == ["http://example.gov/"], "the second hop must not reach the inner transport"


def test_unset_means_the_deny_set_not_an_empty_one():
    """The failure mode of a guard carried in env is an env file that does not load.

    ``None`` in — the field unset — must yield the compiled deny set. An
    operator widening the guard does it by naming ranges, visibly, in the file;
    there is deliberately no flag that spells "off" in one token.
    """
    assert blocked_networks(None) == blocked_networks(DEFAULT_BLOCKED_DESTINATIONS)
    assert blocked_networks(None), "an unset guard is not an absent guard"


def test_an_operator_list_replaces_the_default():
    networks = blocked_networks(("10.0.0.0/8",))

    assert [str(net) for net in networks] == ["10.0.0.0/8"]


async def test_a_widened_list_lets_a_dev_host_reach_loopback():
    """The escape hatch is the list itself, not a second setting.

    A dev worker fetching its own ``/health`` names the narrower set; that the
    guard is weakened is then a fact about the env file rather than about a
    boolean nobody greps for.
    """
    inner = httpx.MockTransport(lambda request: httpx.Response(200))
    transport = _guard({}, inner=inner, blocked=blocked_networks(("10.0.0.0/8",)))

    response = await transport.handle_async_request(httpx.Request("GET", "http://127.0.0.1:8001/"))

    assert response.status_code == 200


def test_the_refusal_is_terminal():
    """A private destination is private again on the next reclaim.

    Terminality is structural here, as everywhere in this module: the guard
    raises ``PermanentFetchError``, which the loop closes and reports, rather
    than ``TransientFetchError``, which would consume the delivery ceiling and
    dead-letter four reclaims later for a command whose answer will not change.
    """
    error = PermanentFetchError("x", reason=FailureReason.DESTINATION_REFUSED)

    assert isinstance(error, PermanentError)
    assert not isinstance(error, TransientError)
    assert error.reason == "destination_refused"


async def test_an_ipv4_mapped_address_is_the_address_it_maps():
    """``::ffff:127.0.0.1`` is loopback spelled as IPv6.

    It is not inside ``127.0.0.0/8`` — that network is IPv4 and this address is
    not — so a guard comparing versions naively lets the most obvious bypass in
    the file straight through.
    """
    transport = _guard({"mapped.invalid": ["::ffff:127.0.0.1"]})

    with pytest.raises(PermanentFetchError, match="127.0.0.1"):
        await transport.handle_async_request(httpx.Request("GET", "http://mapped.invalid/"))


async def test_the_default_resolver_is_the_one_the_worker_runs_with():
    """The production path, exercised rather than assumed.

    Every other test injects a resolver, which would leave the real one — the
    one the worker actually uses — covered by nothing. ``localhost`` is resolved
    by the stub resolver in libc, not by the network, so this stays hermetic.
    """
    transport = GuardedTransport(httpx.MockTransport(_never_called), blocked=blocked_networks(None))

    with pytest.raises(PermanentFetchError):
        await transport.handle_async_request(httpx.Request("GET", "http://localhost:9999/"))


async def test_a_resolution_failure_is_transient_not_unclassified():
    """CR 2: the guard resolves before httpx does, and that moved the failure.

    Before the guard, a name that would not resolve failed *inside* httpx as a
    ``ConnectError`` — an ``httpx.HTTPError``, which ``_fetch`` maps to
    ``TransientFetchError`` and the loop retries indefinitely. Resolving first
    puts a bare ``socket.gaierror`` in its place, which is not an
    ``httpx.HTTPError``, not a builtin ``ConnectionError``, and not in the
    loop's ``_TRANSIENT_ERRORS`` — so it would reach the unclassified branch and
    dead-letter a good command at the delivery ceiling. Exactly the regression
    ``_fetch``'s own docstring exists to prevent.
    """

    async def resolve(host: str, port: int) -> list[str]:
        raise socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")

    transport = GuardedTransport(
        httpx.MockTransport(_never_called), blocked=blocked_networks(None), resolve=resolve
    )

    with pytest.raises(TransientFetchError, match="could not be resolved"):
        await transport.handle_async_request(httpx.Request("GET", "http://nx.invalid/"))


async def test_an_unencodable_hostname_is_terminal():
    """A label too long for IDNA will be too long on the next reclaim too.

    ``getaddrinfo`` raises ``UnicodeError`` rather than ``gaierror`` for these,
    and a ``ValueError`` subclass would otherwise take the same unclassified
    path finding 2 is about — but retrying it forever is the wrong answer, so
    the two are separated at the raise site rather than lumped together.
    """

    async def resolve(host: str, port: int) -> list[str]:
        raise UnicodeError("label empty or too long")

    transport = GuardedTransport(
        httpx.MockTransport(_never_called), blocked=blocked_networks(None), resolve=resolve
    )

    with pytest.raises(PermanentFetchError) as caught:
        await transport.handle_async_request(httpx.Request("GET", "http://toolong.invalid/"))

    assert caught.value.reason is FailureReason.NOT_FETCHABLE
