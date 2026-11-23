"""The destination guard: what a fetch may reach from this host (#95).

Every test here is a way the guard stops holding. The four that matter most are
not the per-range ones — those are arithmetic — but:

- **the redirect hop**, because the obvious implementation (a check on
  ``command.url`` in ``_request_options``) passes every other test in this file
  and is walked around by a ``302``;
- **every resolved address**, because a name that answers with one public and one
  loopback address is the cheap half of DNS rebinding;
- **unset means the deny set**, because the failure mode of a guard carried in
  env is an env file that does not load;
- **an answer the guard cannot check is refused**, because the check is a loop
  over the answer, and an empty one, of any shape, or one that is not an
  address, would skip the loop rather than fail it — the check not raced, as
  rebinding (the module's stated residual) races it, but never run (#100).

The decision and the three tests it was run through: #89. The scope: #95, and
#100 for the resolver seam's failures.
"""

import asyncio
import ipaddress
import logging
import re
import socket
from types import MappingProxyType

import httpx
import pytest
from co_core.effects.fetch import FetchContent
from co_core_aio.fetch import AsyncFetchDriver
from pydantic import SecretStr

from src.core.errors import (
    FailureReason,
    PermanentError,
    PermanentFetchError,
    TransientError,
    TransientFetchError,
)
from src.worker.credentials import FetchCredential, FetchCredentialTable, Resolved
from src.worker.egress import (
    DEFAULT_BLOCKED_DESTINATIONS,
    RESOLVE_TIMEOUT_SECONDS,
    BodyCeilingTransport,
    CommandScopeTransport,
    GuardedTransport,
    blocked_networks,
    command_scope,
    never_storing_cookies,
)
from src.worker.loop import poll_once
from src.worker.reporter import build_failure_reporter
from tests.worker.conftest import GROUP, TOPIC, command, make_command, process_one


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
    than ``TransientFetchError``, which is exempt from the delivery ceiling — it
    would retry at every reclaim, indefinitely, for a command whose answer will
    not change, and the issuer would never be told why (#100 CR 3). That exemption is
    exactly why the guard's *unanswerable* resolves are transient (#100): there,
    waiting in the PEL is the point.
    """
    error = PermanentFetchError("x", reason=FailureReason.DESTINATION_REFUSED)

    assert isinstance(error, PermanentError)
    assert not isinstance(error, TransientError)
    assert error.reason == "destination_refused"


@pytest.mark.parametrize(
    ("label", "url", "mapping"),
    [
        ("resolved", "http://mapped.invalid/", {"mapped.invalid": ["::ffff:127.0.0.1"]}),
        ("literal", "http://[::ffff:127.0.0.1]:9999/", {}),
    ],
)
async def test_an_ipv4_mapped_address_is_the_address_it_maps(label, url, mapping):
    """``::ffff:127.0.0.1`` is loopback spelled as IPv6.

    It is not inside ``127.0.0.0/8`` — that network is IPv4 and this address is
    not — so a guard comparing versions naively lets the most obvious bypass in
    the file straight through.

    **Both paths, because since #100 each unmaps on its own** (#100 CR 1). ``_address``
    runs on the URL's literal and on every resolved answer, where ``_containing``
    once did it for both; restoring the literal path's pre-#100 spelling,
    ``ipaddress.ip_address(host)``, reopens the literal case alone. The empty
    mapping makes resolving the literal a ``KeyError`` rather than a pass.
    """
    transport = _guard(mapping)

    with pytest.raises(PermanentFetchError, match="127.0.0.1"):
        await transport.handle_async_request(httpx.Request("GET", url))


# --- IPv6 forms that embed an IPv4 address (#156) ----------------------------
#
# The mapped form above is one of a class. Each row is an IPv6 address carrying a
# blocked IPv4 address, and the range the refusal must name — the IPv4 one, so a
# refusal for some other reason (a future IPv6 row, say) fails the test rather
# than passing it.
EMBEDDING_A_BLOCKED_ADDRESS = [
    ("NAT64 well-known prefix, RFC 6052", "64:ff9b::a00:7", "10.0.0.0/8"),
    ("NAT64 well-known prefix, loopback", "64:ff9b::7f00:1", "127.0.0.0/8"),
    ("local-use NAT64, RFC 8215", "64:ff9b:1::a00:7", "64:ff9b:1::/48"),
    ("IPv4-compatible, RFC 4291", "::7f00:1", "127.0.0.0/8"),
    ("SIIT IPv4-translated, RFC 2765", "::ffff:0:a00:7", "10.0.0.0/8"),
    ("6to4, RFC 3056", "2002:7f00:1::", "127.0.0.0/8"),
    ("6to4, the metadata address", "2002:a9fe:a9fe::1", "169.254.0.0/16"),
    ("Teredo client, RFC 4380", "2001:0:4136:e378:8000:ffff:f5ff:fff8", "10.0.0.0/8"),
    ("Teredo server", "2001:0:a00:7:8000:ffff:a247:27dd", "10.0.0.0/8"),
    ("ISATAP, RFC 5214", "2600:1f18::5efe:a00:7", "10.0.0.0/8"),
    ("ISATAP, u bit set", "2600:1f18::200:5efe:a00:7", "10.0.0.0/8"),
    ("NAT64 well-known prefix, benchmarking (#161)", "64:ff9b::c612:1", "198.18.0.0/15"),
]


def _refused_in(network: str) -> str:
    """The refusal naming ``network`` — escaped, since a range's dots are regex wildcards."""
    return re.escape(f"refused range {network} ")


# The same forms carrying a public address, which the guard must let through.
EMBEDDING_A_PUBLIC_ADDRESS = [
    ("NAT64 well-known prefix", "64:ff9b::5db8:d822"),
    ("IPv4-compatible", "::5db8:d822"),
    ("SIIT IPv4-translated", "::ffff:0:5db8:d822"),
    ("6to4", "2002:5db8:d822::1"),
    ("Teredo", "2001:0:4136:e378:8000:ffff:a247:27dd"),
    ("ISATAP", "2600:1f18::200:5efe:5db8:d822"),
]


@pytest.mark.parametrize("path", ["literal", "resolved"])
@pytest.mark.parametrize(
    ("label", "address", "refused"),
    EMBEDDING_A_BLOCKED_ADDRESS,
    ids=[row[0] for row in EMBEDDING_A_BLOCKED_ADDRESS],
)
async def test_an_ipv6_address_embedding_a_blocked_one_is_refused(path, label, address, refused):
    """An IPv6 address carrying ``10.0.0.7`` reaches ``10.0.0.7`` through a translator.

    ``_containing`` compares versions, so none of these is ever tested against the
    IPv4 row holding what it embeds. Latent on this host — no global IPv6 route —
    and a bypass of #89/#95 on one with NAT64/DNS64 or a 6to4 relay (#156).
    """
    url, mapping = (
        (f"http://[{address}]:9999/", {})
        if path == "literal"
        else ("http://embedded.invalid/", {"embedded.invalid": [address]})
    )

    with pytest.raises(PermanentFetchError, match=_refused_in(refused)) as caught:
        await _guard(mapping).handle_async_request(httpx.Request("GET", url))

    assert caught.value.reason is FailureReason.DESTINATION_REFUSED
    assert address in str(caught.value), label


@pytest.mark.parametrize("path", ["literal", "resolved"])
@pytest.mark.parametrize(
    ("label", "address"), EMBEDDING_A_PUBLIC_ADDRESS, ids=[r[0] for r in EMBEDDING_A_PUBLIC_ADDRESS]
)
async def test_an_ipv6_address_embedding_a_public_one_passes(path, label, address):
    """Unwrapping refuses by what is embedded, so a public NAT64 or 6to4 destination stays
    reachable — the issue's reason for preferring it to a table row per prefix."""
    url, mapping = (
        (f"http://[{address}]/", {})
        if path == "literal"
        else ("http://embedded.example/", {"embedded.example": [address]})
    )
    inner = httpx.MockTransport(lambda request: httpx.Response(200))

    response = await _guard(mapping, inner=inner).handle_async_request(httpx.Request("GET", url))

    assert response.status_code == 200, label


@pytest.mark.parametrize(
    ("label", "address", "refused"),
    [
        ("ISATAP under link-local", "fe80::5efe:5db8:d822", "fe80::/10"),
        ("ISATAP under ULA", "fd00::200:5efe:5db8:d822", "fc00::/7"),
        ("local-use NAT64, its row", "64:ff9b:1::5db8:d822", "64:ff9b:1::/48"),
    ],
)
async def test_the_ipv6_address_itself_is_still_checked(label, address, refused):
    """Unwrapping adds a check; it does not replace the IPv6 one.

    The ISATAP rows unwrap to a public address inside a blocked IPv6 range, so a
    change that substituted the embedded address — as ``_address`` does for the
    mapped form — would pass them. The local-use row pins the table: its prefix is
    refused even where the low 32 bits are public.
    """
    with pytest.raises(PermanentFetchError, match=_refused_in(refused)):
        await _guard({}).handle_async_request(httpx.Request("GET", f"http://[{address}]/"))


async def test_an_operator_table_cannot_drop_the_unwrap():
    """``REPLICATOR_BLOCKED_DESTINATIONS`` *replaces* the table, so an IPv6 row per
    translator prefix would vanish from any host that sets it. The unwrap holds whatever
    the table says: an operator naming only ``10.0.0.0/8`` still refuses its NAT64 spelling.
    """
    transport = _guard({}, blocked=blocked_networks(("10.0.0.0/8",)))

    with pytest.raises(PermanentFetchError, match=_refused_in("10.0.0.0/8")):
        await transport.handle_async_request(httpx.Request("GET", "http://[64:ff9b::a00:7]/"))


async def test_a_redirect_to_an_embedded_literal_is_refused_at_the_hop():
    """The redirect hop is where an issuer's public URL becomes an IPv6 literal."""

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "http://[64:ff9b::7f00:1]:9999/"})

    transport = _guard({"example.gov": ["93.184.216.34"]}, inner=httpx.MockTransport(respond))

    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        with pytest.raises(PermanentFetchError, match=_refused_in("127.0.0.0/8")):
            await client.get("http://example.gov/")


# --- The IANA special-purpose ranges (#161) -----------------------------------
#
# One row per range the operator chose, each address off the network's first one, and
# the /15 and /20 and /28 rows at their top end so a narrower row fails them.
IANA_SPECIAL_PURPOSE = [
    ("IETF protocol assignments, RFC 6890", "192.0.0.170", "192.0.0.0/24"),
    ("TEST-NET-1, RFC 5737", "192.0.2.7", "192.0.2.0/24"),
    ("TEST-NET-2, RFC 5737", "198.51.100.7", "198.51.100.0/24"),
    ("TEST-NET-3, RFC 5737", "203.0.113.7", "203.0.113.0/24"),
    ("benchmarking, RFC 2544", "198.19.255.1", "198.18.0.0/15"),
    ("6to4 relay anycast, RFC 7526", "192.88.99.1", "192.88.99.0/24"),
    ("discard-only, RFC 6666", "100::7", "100::/64"),
    ("IPv6 benchmarking, RFC 5180", "2001:2::7", "2001:2::/48"),
    ("IPv6 documentation, RFC 3849", "2001:db8::7", "2001:db8::/32"),
    ("IPv6 documentation, RFC 9637", "3fff:fff::7", "3fff::/20"),
    ("ORCHIDv2, RFC 7343", "2001:2f::7", "2001:20::/28"),
    ("SRv6 SIDs, RFC 9602", "5f00::7", "5f00::/16"),
]


@pytest.mark.parametrize("path", ["literal", "resolved"])
@pytest.mark.parametrize(
    ("label", "address", "refused"), IANA_SPECIAL_PURPOSE, ids=[r[0] for r in IANA_SPECIAL_PURPOSE]
)
async def test_an_iana_special_purpose_address_is_refused(path, label, address, refused):
    """No public corpus is served from a documentation, benchmarking or protocol range,
    so a refusal costs nothing. What one catches is a provider using such a range inside
    the network this host reaches — ``198.18.0.0/15`` above all.
    """
    host = f"[{address}]" if ":" in address else address
    url, mapping = (
        (f"http://{host}:9999/", {})
        if path == "literal"
        else ("http://special.invalid/", {"special.invalid": [address]})
    )

    with pytest.raises(PermanentFetchError, match=_refused_in(refused)) as caught:
        await _guard(mapping).handle_async_request(httpx.Request("GET", url))

    assert caught.value.reason is FailureReason.DESTINATION_REFUSED
    assert address in str(caught.value), label


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
    """#95 CR 2: the guard resolves before httpx does, and that moved the failure.

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


async def test_an_empty_answer_is_transient_not_a_pass():
    """#100: an answer with no addresses must not skip the check.

    The guard refuses by iterating the addresses, so an empty answer runs the
    loop zero times and hands the request to the inner transport unchecked.
    ``getaddrinfo`` raises rather than answering empty, but the resolver is an
    injected seam — a cache or a pinned-address resolver may legitimately
    answer ``[]`` — and a guard must not depend on its seam's good manners for
    the direction it fails in. Transient, beside ``gaierror``: an empty answer
    may be a full one on the next reclaim.
    """
    transport = _guard({"empty.invalid": []})

    with pytest.raises(TransientFetchError, match="no addresses"):
        await transport.handle_async_request(httpx.Request("GET", "http://empty.invalid/"))


@pytest.mark.parametrize(
    ("label", "answer"),
    [
        ("an exhausted generator", lambda: (value for value in [])),
        ("an empty iterator", lambda: iter(())),
        ("None", lambda: None),
    ],
)
async def test_an_empty_answer_of_any_shape_is_transient_not_a_pass(label, answer):
    """#100 CR 7: emptiness is judged on what the loop iterates, not on what came back.

    ``not <generator>`` is ``False`` whatever it will yield, so a guard testing
    the resolver's return value lets an empty iterator through the same zero-trip
    loop the empty list used to. Typed ``Sequence`` or not, the seam's shape is
    one more of the manners this guard must not rest on.
    """

    async def resolve(host: str, port: int):
        return answer()

    transport = GuardedTransport(
        httpx.MockTransport(_never_called), blocked=blocked_networks(None), resolve=resolve
    )

    with pytest.raises(TransientFetchError, match="no addresses"):
        await transport.handle_async_request(httpx.Request("GET", "http://empty.invalid/"))


@pytest.mark.parametrize(
    ("label", "answer"),
    [
        ("a hostname", "localhost"),
        ("an empty string", ""),
    ],
)
async def test_an_answer_that_is_not_an_address_is_refused_not_passed(label, answer):
    """#100: the arm #95 CR 9 called unreachable failed *open*.

    ``_containing`` answered ``None`` — "in no blocked range" — for anything it
    could not parse, so a resolver answering with a name rather than an address
    let the request through. Unreachable through ``getaddrinfo``, but not through
    the seam. It fails closed now, and transiently: the command is sound and the
    resolver is broken, so the command waits for the fix rather than dead-letters.
    """
    transport = _guard({"named.invalid": [answer]})

    with pytest.raises(TransientFetchError, match="not an address"):
        await transport.handle_async_request(httpx.Request("GET", "http://named.invalid/"))


async def test_a_resolve_that_never_answers_is_bounded():
    """#100: resolving ahead of httpx also moved the resolve out of its timeout.

    httpcore resolves inside the connect timeout; this guard resolves before
    httpcore is reached, so nothing but libc's own retries bounded it — on a
    serial consume path, a parked resolve is a parked worker. Transient, because
    a nameserver that dropped this query may answer the next one.
    """

    async def resolve(host: str, port: int) -> list[str]:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")  # pragma: no cover

    transport = GuardedTransport(
        httpx.MockTransport(_never_called),
        blocked=blocked_networks(None),
        resolve=resolve,
        resolve_timeout=0.01,
    )

    with pytest.raises(TransientFetchError, match="did not resolve within"):
        await transport.handle_async_request(httpx.Request("GET", "http://blackholed.invalid/"))


def test_the_resolve_cap_survives_one_libc_retry():
    """Why the cap is not Watcher's 5 s (#100).

    glibc's per-try timeout defaults to 5 s (``resolv.conf`` ``timeout:5``), so
    a 5 s cap fails exactly the resolve a single dropped UDP packet makes slow:
    the one libc would have finished on its second try, a little after 5 s.
    Here that costs a ``REPLICATOR_CLAIM_MIN_IDLE_MS`` reclaim for a name that
    was resolving fine.

    **Room for a whole second try, not a hair over the first** (#100 CR 5): the
    retry only *starts* at 5 s, so a cap of 5.001 s clears the first try's
    timeout and still fails the resolve it exists to let through.
    """
    glibc_per_try_seconds = 5.0

    assert RESOLVE_TIMEOUT_SECONDS >= 2 * glibc_per_try_seconds


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


async def test_a_refusal_survives_the_driver_and_reaches_the_handler(handler):
    """#95 CR 3: the refusal has to cross httpx's stack and ``_fetch``'s except clauses.

    Every other test here calls the transport directly, which proves the range
    arithmetic and nothing about the path the exception actually travels. In
    between sit ``AsyncClient``'s redirect loop and ``_fetch``, whose
    ``except httpx.HTTPError`` would reclassify anything it caught as
    *transient* — a refusal that landed there would retry to the delivery
    ceiling and dead-letter as ``handler_error``, telling the issuer nothing
    about why. So this drives the **real** ``AsyncFetchDriver`` over a guarded
    client and asserts the reason the contract's refusal row promises.

    Finding 2 in this same review is why the seam is worth a test rather than an
    argument: it is where the guard already broke once.
    """
    reached = []

    def inner(request: httpx.Request) -> httpx.Response:  # pragma: no cover - asserts
        reached.append(str(request.url))
        return httpx.Response(200)

    client = httpx.AsyncClient(
        transport=GuardedTransport(
            httpx.MockTransport(inner),
            blocked=blocked_networks(None),
            resolve=_resolver({"shelley.invalid": ["127.0.0.1"]}),
        ),
        follow_redirects=True,
    )
    async with client:
        with pytest.raises(PermanentFetchError) as caught:
            await handler(AsyncFetchDriver(client))(command(url="http://shelley.invalid:9999/"))

    assert caught.value.reason is FailureReason.DESTINATION_REFUSED
    assert reached == [], "the refusal must precede the request, not follow it"


@pytest.mark.parametrize(
    ("label", "host"),
    [
        ("decimal", "2130706433"),
        ("short form", "127.1"),
    ],
)
async def test_an_obfuscated_literal_still_reaches_the_guard(label, host):
    """#95 CR 7: these are loopback, and they are refused for a non-obvious reason.

    ``ipaddress.ip_address`` **rejects** both spellings — it takes dotted quads
    only — so they are not recognised as literals and fall through to the
    resolver, where the platform parses them the way a browser would and the
    guard catches the address that comes back.

    That is correct by accident of layering rather than by design, which is
    exactly why it is pinned: a change that tried harder to parse a literal — to
    skip a resolve, say — would turn both into a bypass with every other test in
    this file still green.
    """
    transport = _guard({host: ["127.0.0.1"]})

    with pytest.raises(PermanentFetchError):
        await transport.handle_async_request(httpx.Request("GET", f"http://{host}/"))

    with pytest.raises(ValueError):
        # The fact the refusal above rests on, asserted rather than assumed.
        ipaddress.ip_address(host)


def test_an_octal_literal_is_refused_before_the_guard_sees_it():
    """The third obfuscated form never reaches this module, and that is fine.

    httpx refuses ``0177.0.0.1`` at URL construction — ``InvalidURL``, which
    ``_fetch`` already maps to a terminal ``not_fetchable``. Recorded here
    beside its two siblings so a reader does not conclude the guard handles all
    three, and so the day httpx stops refusing it, this test says where the
    coverage went.
    """
    with pytest.raises(httpx.InvalidURL):
        httpx.Request("GET", "http://0177.0.0.1/")


# --- The body ceiling (#104) --------------------------------------------------
#
# REPLICATOR_MAX_BLOB_BYTES decided what was *kept*: the driver read the whole
# body into memory and the handler measured it afterwards. These are the ways a
# transport that stops reading at the ceiling could still read past it, or stop
# something it should have let through.

CEILING = 25


class Source(httpx.AsyncByteStream):
    """An origin's body, one chunk at a time, recording how much was asked of it."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks
        self.sent = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self._chunks:
            self.sent += 1
            yield chunk

    async def aclose(self) -> None:
        self.closed = True


def _capped(source: Source, *, status: int = 200, headers: dict[str, str] | None = None):
    """A ``BodyCeilingTransport`` over an origin answering ``status`` with ``source``."""
    inner = httpx.MockTransport(
        lambda request: httpx.Response(status, headers=headers or {}, stream=source)
    )
    return BodyCeilingTransport(inner, max_bytes=CEILING)


async def _read(transport: httpx.AsyncBaseTransport) -> httpx.Response:
    async with httpx.AsyncClient(transport=transport) as client:
        return await client.get("https://example.gov/doc.pdf")


async def test_a_body_streamed_past_the_ceiling_is_refused_at_the_first_byte_over():
    """``too_large`` at the chunk that crosses, with the rest never asked for."""
    source = Source([b"x" * 10] * 100)

    with pytest.raises(PermanentFetchError) as raised:
        await _read(_capped(source))

    assert raised.value.reason is FailureReason.TOO_LARGE
    assert source.sent == 3
    assert source.closed


async def test_a_body_declared_over_the_ceiling_is_refused_before_a_byte_is_read():
    """A ``Content-Length`` over the ceiling is the answer already; reading it would be waste."""
    source = Source([b"x" * 10] * 100)

    with pytest.raises(PermanentFetchError) as raised:
        await _read(_capped(source, headers={"content-length": "1000"}))

    assert raised.value.reason is FailureReason.TOO_LARGE
    assert source.sent == 0
    assert source.closed


async def test_a_body_at_the_ceiling_is_delivered_whole():
    source = Source([b"x" * 10, b"x" * 10, b"x" * 5])

    response = await _read(_capped(source, headers={"content-length": str(CEILING)}))

    assert response.content == b"x" * CEILING


async def test_an_unparseable_declared_length_is_left_to_the_count():
    """A header the origin garbled is not evidence either way; the bytes still are."""
    source = Source([b"x" * 10] * 100)

    with pytest.raises(PermanentFetchError):
        await _read(_capped(source, headers={"content-length": "lots"}))

    assert source.sent == 3


async def test_a_non_2xx_body_past_the_ceiling_is_cut_short_rather_than_refused():
    """Its status is the outcome, and ``_raise_for_status`` must still get to read it.

    A non-2xx body is never stored or announced, so ``too_large`` would name a
    body nobody asked to keep — and turn a 503's retry, or a 404's
    ``http_status``, into a different terminal answer. It is still not read past
    the ceiling, which is the memory half of #104.
    """
    source = Source([b"x" * 10] * 100)

    response = await _read(_capped(source, status=503))

    assert response.status_code == 503
    assert len(response.content) == CEILING
    assert source.sent == 3


async def test_a_304_declaring_a_large_representation_is_not_refused():
    """RFC 9110 lets a 304 carry the *representation's* ``Content-Length``, and no body."""
    source = Source([])

    response = await _read(_capped(source, status=304, headers={"content-length": "1000000"}))

    assert response.status_code == 304


async def test_the_refusal_reaches_the_handler_through_the_real_driver():
    """Raised inside httpx's read and out through ``AsyncFetchDriver`` unwrapped.

    httpx maps only its own exceptions, and the driver catches none, so the
    refusal arrives at ``_fetch`` as the ``PermanentFetchError`` it left as — not
    an ``httpx.HTTPError`` that ``_fetch`` would call transient.
    """
    source = Source([b"x" * 10] * 100)
    guard = _guard({"example.gov": ["93.184.216.34"]}, inner=_capped(source))

    async with httpx.AsyncClient(transport=guard) as client:
        with pytest.raises(PermanentFetchError) as raised:
            await AsyncFetchDriver(client).execute(FetchContent("https://example.gov/a"))

    assert raised.value.reason is FailureReason.TOO_LARGE


@pytest.fixture
async def endless_origin():
    """A local origin that answers with a body that never ends.

    ``Connection: close`` and no ``Content-Length``, so only the count can stop
    it. Yields the URL and an event the handler sets when its socket dies, which
    is what shows the read stopped rather than the whole body arriving first.
    """

    def serve(status_line: bytes):
        stopped = asyncio.Event()

        async def endless(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(status_line + b"\r\nConnection: close\r\n\r\n")
                while True:
                    writer.write(b"x" * 1024)
                    await writer.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                stopped.set()
                writer.close()

        return endless, stopped

    servers = []

    async def start(status_line: bytes):
        handler, stopped = serve(status_line)
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        servers.append(server)
        port = server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}/endless", stopped

    try:
        yield start
    finally:
        for server in servers:
            server.close()
            await server.wait_closed()


async def test_an_endless_body_from_a_real_origin_is_refused_and_its_connection_dropped(
    endless_origin,
):
    """Over real httpcore, where a stream closed part-way is a connection dropped.

    Loopback, so the bare transport rather than the guard, which would refuse it
    first.
    """
    url, stopped = await endless_origin(b"HTTP/1.1 200 OK")
    transport = BodyCeilingTransport(httpx.AsyncHTTPTransport(), max_bytes=64 * 1024)

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(PermanentFetchError) as raised:
            await client.get(url)
    await asyncio.wait_for(stopped.wait(), timeout=5)

    assert raised.value.reason is FailureReason.TOO_LARGE


async def test_an_endless_non_2xx_body_from_a_real_origin_is_cut_short(endless_origin):
    """CR 2: the other half of the ceiling, against a response that really exists.

    Truncation abandons a live body rather than raising through it, and what
    makes that safe is httpcore dropping a part-read connection instead of
    pooling it — which a mock stream has no way to show. The status still
    reaches ``_raise_for_status``, which is the whole reason this path is not a
    refusal.
    """
    url, stopped = await endless_origin(b"HTTP/1.1 503 Service Unavailable")
    ceiling = 64 * 1024
    transport = BodyCeilingTransport(httpx.AsyncHTTPTransport(), max_bytes=ceiling)

    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get(url)
    await asyncio.wait_for(stopped.wait(), timeout=5)

    assert response.status_code == 503
    assert len(response.content) == ceiling


# --- The command scope (#216): cookies live for one command's redirect chain ---------


def _scoped_client(origin) -> httpx.AsyncClient:
    """The worker's stack minus the guard and the ceiling: scope over a mock origin."""
    return httpx.AsyncClient(
        transport=CommandScopeTransport(httpx.MockTransport(origin)),
        cookies=never_storing_cookies(),
        follow_redirects=True,
    )


def _cookie_origin(seen: list[tuple[str, str | None]]):
    """``/bounce`` sets a cookie on a 302 to ``/land``; ``/plant`` sets one on a 200."""

    def origin(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, request.headers.get("cookie")))
        if request.url.path == "/bounce":
            return httpx.Response(
                302, headers={"location": "/land", "set-cookie": "sid=abc; Path=/"}
            )
        if request.url.path == "/plant":
            return httpx.Response(200, headers={"set-cookie": "planted=1; Path=/"})
        return httpx.Response(200, text="ok")

    return origin


async def test_a_cookie_set_on_a_redirect_reaches_the_next_hop():
    """Today's behaviour, kept: a session minted on a 302 is sent on the hop it points at."""
    seen: list[tuple[str, str | None]] = []

    async with _scoped_client(_cookie_origin(seen)) as client:
        with command_scope():
            await AsyncFetchDriver(client).execute(FetchContent("https://a.example/bounce"))

    assert seen == [("/bounce", None), ("/land", "sid=abc")]


async def test_a_cookie_from_one_command_never_reaches_the_next():
    """The behaviour change (operator call 1A): the worker-lifetime jar is gone.

    Before, one command's ``Set-Cookie`` was replayed on every later command to the
    same host, so a credentialed fetch's session would ride an anonymous one.
    """
    seen: list[tuple[str, str | None]] = []

    async with _scoped_client(_cookie_origin(seen)) as client:
        driver = AsyncFetchDriver(client)
        with command_scope():
            await driver.execute(FetchContent("https://a.example/plant"))
        with command_scope():
            await driver.execute(FetchContent("https://a.example/later"))

    assert seen[-1] == ("/later", None)


async def test_the_client_jar_stores_nothing():
    """The per-command cookies are the only ones: the client's jar never fills."""
    async with _scoped_client(_cookie_origin([])) as client:
        with command_scope():
            await AsyncFetchDriver(client).execute(FetchContent("https://a.example/bounce"))

        assert len(client.cookies.jar) == 0


async def test_a_hop_outside_any_command_scope_is_refused():
    """Fails closed: nothing reaches the wire without a scope to read it against."""
    async with _scoped_client(_never_called) as client:
        with pytest.raises(RuntimeError, match="outside a command scope"):
            await client.get("https://a.example/")


async def test_the_scope_is_gone_after_the_command_even_when_it_raises():
    with pytest.raises(ValueError), command_scope():
        raise ValueError("the fetch failed")

    async with _scoped_client(_never_called) as client:
        with pytest.raises(RuntimeError, match="outside a command scope"):
            await client.get("https://a.example/")


async def test_the_request_handed_down_is_never_mutated():
    """httpx builds the next hop from the request it handed in, headers and all.

    So whatever the transport adds must go on a copy, or it rides every later hop
    including a cross-origin one [measured 2026-11-23, httpx 0.28.1].
    """
    seen: list[httpx.Request] = []

    def origin(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    transport = CommandScopeTransport(httpx.MockTransport(origin))
    handed = httpx.Request("GET", "https://a.example/", headers={"accept": "text/html"})
    with command_scope() as scope:
        scope.cookies.set("sid", "abc", domain="a.example")
        await transport.handle_async_request(handed)

    assert seen[0].headers["cookie"] == "sid=abc"
    assert seen[0].headers["accept"] == "text/html"
    assert "cookie" not in handed.headers


async def test_an_issuers_own_cookie_header_is_sent_as_given():
    """As httpx's jar did: a ``Cookie`` already on the request is left alone."""
    seen: list[tuple[str, str | None]] = []

    async with _scoped_client(_cookie_origin(seen)) as client:
        with command_scope() as scope:
            scope.cookies.set("sid", "abc", domain="a.example")
            await client.get("https://a.example/land", headers={"cookie": "mine=1"})

    assert seen == [("/land", "mine=1")]


async def test_every_command_the_handler_runs_has_its_own_scope(handler):
    """The handler enters the scope, so the worker's client never refuses a real command.

    And it is a fresh one each time: the second command carries none of the first's cookies.
    """
    seen: list[tuple[str, str | None]] = []

    async with _scoped_client(_cookie_origin(seen)) as client:
        handle = handler(AsyncFetchDriver(client))
        await handle(command(command_id="cmd-1", url="https://a.example/plant"))
        await handle(command(command_id="cmd-2", url="https://a.example/later"))

    assert seen == [("/plant", None), ("/later", None)]


# --- Credentials per hop (#216, D3): in scope or not sent, and never by mutation -------

VALUE = "planted-egress-value-c4d2"  # never a real credential; a tripwire for leaks


def _resolved(header="x-api-key", hosts=("api.leg.wa.gov",), reference="fetch-wa-leg-api"):
    return Resolved(
        reference,
        FetchCredential(
            header=header, hosts=frozenset(hosts), value=SecretStr(VALUE), value_file="/v"
        ),
    )


def _recording_origin(seen: list[httpx.Request], routes: dict[str, httpx.Response] | None = None):
    def origin(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        for path, response in (routes or {}).items():
            if request.url.path == path:
                return response
        return httpx.Response(200, text="ok")

    return origin


async def _get(origin, url: str, *credentials: Resolved) -> httpx.Response:
    async with _scoped_client(origin) as client:
        with command_scope(credentials):
            return await AsyncFetchDriver(client).execute(FetchContent(url))


async def test_an_in_scope_credential_is_sent_on_every_hop():
    seen: list[httpx.Request] = []
    origin = _recording_origin(seen, {"/a": httpx.Response(302, headers={"location": "/b"})})

    await _get(origin, "https://api.leg.wa.gov/a", _resolved())

    assert [r.url.path for r in seen] == ["/a", "/b"]
    assert [r.headers.get("x-api-key") for r in seen] == [VALUE, VALUE]


async def test_a_cross_origin_redirect_is_refused_before_the_hop_is_sent():
    """C2, C3: the hop is refused, not sent bare. The other host receives nothing at all."""
    seen: list[httpx.Request] = []
    origin = _recording_origin(
        seen, {"/a": httpx.Response(302, headers={"location": "https://other.example/land"})}
    )

    with pytest.raises(PermanentFetchError) as raised:
        await _get(origin, "https://api.leg.wa.gov/a", _resolved())

    assert raised.value.reason is FailureReason.CREDENTIAL_REFUSED
    assert [str(r.url) for r in seen] == ["https://api.leg.wa.gov/a"]
    assert "other.example" in str(raised.value) and "fetch-wa-leg-api" in str(raised.value)
    assert VALUE not in str(raised.value)


@pytest.mark.parametrize(
    "location",
    [
        pytest.param("http://api.leg.wa.gov/b", id="https-to-http"),
        pytest.param("https://api.leg.wa.gov:8443/b", id="another-port"),
        pytest.param("https://evil.api.leg.wa.gov/b", id="a-subdomain"),
    ],
)
async def test_a_redirect_off_the_scheme_port_or_host_is_refused(location):
    seen: list[httpx.Request] = []
    origin = _recording_origin(seen, {"/a": httpx.Response(302, headers={"location": location})})

    with pytest.raises(PermanentFetchError) as raised:
        await _get(origin, "https://api.leg.wa.gov/a", _resolved())

    assert raised.value.reason is FailureReason.CREDENTIAL_REFUSED
    assert len(seen) == 1


async def test_a_first_hop_over_http_is_refused_at_the_transport_too():
    """The handler checks the submitted URL; the transport doesn't rely on it."""
    with pytest.raises(PermanentFetchError) as raised:
        await _get(_never_called, "http://api.leg.wa.gov/a", _resolved())

    assert raised.value.reason is FailureReason.CREDENTIAL_REFUSED


@pytest.mark.parametrize(
    ("url", "hosts"),
    [
        pytest.param("https://Bücher.example/a", ("xn--bcher-kva.example",), id="idna"),
        pytest.param("https://api.leg.wa.gov./a", ("api.leg.wa.gov",), id="trailing-dot"),
        pytest.param("https://API.LEG.WA.GOV/a", ("api.leg.wa.gov",), id="upper-case"),
    ],
)
async def test_the_hop_host_is_compared_in_its_canonical_form(url, hosts):
    seen: list[httpx.Request] = []

    await _get(_recording_origin(seen), url, _resolved(hosts=hosts))

    assert seen[0].headers["x-api-key"] == VALUE


async def test_the_unicode_host_is_never_what_is_compared():
    """``url.host`` is IDNA-decoded; a binding written in unicode would never match it."""
    with pytest.raises(PermanentFetchError):
        await _get(_never_called, "https://Bücher.example/a", _resolved(hosts=("bücher.example",)))


async def test_every_credential_must_admit_the_hop():
    seen: list[httpx.Request] = []

    with pytest.raises(PermanentFetchError) as raised:
        await _get(
            _recording_origin(seen),
            "https://api.leg.wa.gov/a",
            _resolved(),
            _resolved(header="x-other", hosts=("other.example",), reference="fetch-other"),
        )

    assert raised.value.reason is FailureReason.CREDENTIAL_REFUSED
    assert "fetch-other" in str(raised.value)
    assert seen == []


async def test_the_credential_never_lands_on_the_request_httpx_holds():
    """httpx builds the next hop from the handed request: the credential must not be on it."""
    seen: list[httpx.Request] = []
    transport = CommandScopeTransport(httpx.MockTransport(_recording_origin(seen)))
    handed = httpx.Request("GET", "https://api.leg.wa.gov/a")

    with command_scope((_resolved(),)):
        await transport.handle_async_request(handed)

    assert seen[0].headers["x-api-key"] == VALUE
    assert "x-api-key" not in handed.headers


async def test_a_cookie_credential_keeps_the_session_minted_on_top_of_it():
    """``<credential>; <jar>``: the origin's session cookie survives the redirect chain."""
    seen: list[httpx.Request] = []
    origin = _recording_origin(
        seen,
        {"/a": httpx.Response(302, headers={"location": "/b", "set-cookie": "sid=abc; Path=/"})},
    )

    await _get(origin, "https://api.leg.wa.gov/a", _resolved(header="cookie"))

    assert [r.headers.get("cookie") for r in seen] == [VALUE, f"{VALUE}; sid=abc"]


async def test_an_anonymous_scope_sends_what_a_plain_client_sends():
    """No credential, no cookie: the hop goes out header for header as it did before #216."""
    plain: list[httpx.Request] = []
    scoped: list[httpx.Request] = []

    async with httpx.AsyncClient(transport=httpx.MockTransport(_recording_origin(plain))) as client:
        await AsyncFetchDriver(client).execute(FetchContent("https://a.example/x"))
    await _get(_recording_origin(scoped), "https://a.example/x")

    assert list(scoped[0].headers.multi_items()) == list(plain[0].headers.multi_items())
    assert scoped[0].url == plain[0].url


async def test_out_of_scope_and_in_a_refused_range_is_credential_refused():
    """Operator call 5: the scope check is pure and outermost, so it answers before DNS."""
    resolved_hosts: list[str] = []

    async def resolve(host: str, port: int) -> list[str]:  # pragma: no cover - asserts
        resolved_hosts.append(host)
        return ["127.0.0.1"]

    client = httpx.AsyncClient(
        transport=CommandScopeTransport(
            GuardedTransport(
                httpx.MockTransport(_never_called), blocked=blocked_networks(None), resolve=resolve
            )
        ),
        cookies=never_storing_cookies(),
    )
    async with client:
        with command_scope((_resolved(),)), pytest.raises(PermanentFetchError) as raised:
            await client.get("https://shelley.invalid/")

    assert raised.value.reason is FailureReason.CREDENTIAL_REFUSED
    assert resolved_hosts == []


async def test_a_hop_refusal_reaches_the_handler_through_the_real_driver(handler):
    """Out through httpx's redirect loop and ``_fetch`` unchanged: terminal, not transient."""
    seen: list[httpx.Request] = []
    origin = _recording_origin(
        seen, {"/a": httpx.Response(302, headers={"location": "https://other.example/"})}
    )
    table = FetchCredentialTable(MappingProxyType({"fetch-wa-leg-api": _resolved().credential}))

    async with _scoped_client(origin) as client:
        with pytest.raises(PermanentFetchError) as raised:
            await handler(AsyncFetchDriver(client), credentials=table)(
                command(
                    url="https://api.leg.wa.gov/a",
                    credential_headers={"X-Api-Key": "fetch-wa-leg-api"},
                )
            )

    assert raised.value.reason is FailureReason.CREDENTIAL_REFUSED
    assert len(seen) == 1 and seen[0].headers["x-api-key"] == VALUE


async def test_no_value_in_the_journal_when_the_wire_fails_mid_fetch(
    fake_redis, consumer, settings, handler, caplog
):
    """A forced, unclassified failure after the credential is attached: the traceback the
    loop logs names the error, and never the value the request carried."""

    def exploding(request: httpx.Request) -> httpx.Response:
        raise ValueError(f"the origin broke on {request.url}")

    table = FetchCredentialTable(MappingProxyType({"fetch-wa-leg-api": _resolved().credential}))
    await fake_redis.xadd(
        TOPIC,
        make_command(
            command_id="cmd-boom",
            url="https://api.leg.wa.gov/a",
            credential_headers={"X-Api-Key": "fetch-wa-leg-api"},
        ),
    )
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]

    async with _scoped_client(exploding) as client:
        with caplog.at_level("DEBUG"):
            await process_one(
                fake_redis,
                consumer,
                settings,
                message,
                handler(AsyncFetchDriver(client), credentials=table),
                reporter=build_failure_reporter(client=fake_redis),
            )

    assert any(r.exc_info or r.exc_text for r in caplog.records), "no traceback was logged"
    journal = caplog.text + "".join(
        logging.Formatter().formatException(r.exc_info) for r in caplog.records if r.exc_info
    )
    assert "the origin broke" in journal
    assert VALUE not in journal
    assert VALUE not in str(await fake_redis.xrange("content.blobs"))
