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

import asyncio
import ipaddress
import socket

import httpx
import pytest
from co_core_aio.fetch import AsyncFetchDriver

from src.core.errors import (
    FailureReason,
    PermanentError,
    PermanentFetchError,
    TransientError,
    TransientFetchError,
)
from src.worker.egress import (
    DEFAULT_BLOCKED_DESTINATIONS,
    RESOLVE_TIMEOUT_SECONDS,
    GuardedTransport,
    blocked_networks,
)
from tests.worker.conftest import command


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
    not change, and the issuer would never be told why (CR 3). That exemption is
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

    **Both paths, because since #100 each unmaps on its own** (CR 1). ``_address``
    runs on the URL's literal and on every resolved answer, where ``_containing``
    once did it for both; restoring the literal path's pre-#100 spelling,
    ``ipaddress.ip_address(host)``, reopens the literal case alone. The empty
    mapping makes resolving the literal a ``KeyError`` rather than a pass.
    """
    transport = _guard(mapping)

    with pytest.raises(PermanentFetchError, match="127.0.0.1"):
        await transport.handle_async_request(httpx.Request("GET", url))


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
        ("a hostname", "localhost"),
        ("an empty string", ""),
    ],
)
async def test_an_answer_that_is_not_an_address_is_refused_not_passed(label, answer):
    """#100: the arm CR 9 called unreachable failed *open*.

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
    """
    glibc_per_try_seconds = 5.0

    assert RESOLVE_TIMEOUT_SECONDS > glibc_per_try_seconds


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
    """CR 3: the refusal has to cross httpx's stack and ``_fetch``'s except clauses.

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
    """CR 7: these are loopback, and they are refused for a non-obvious reason.

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
