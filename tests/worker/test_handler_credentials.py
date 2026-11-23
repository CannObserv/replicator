"""The handler's half of fetch credential references (#216, D4 steps 1 and 2).

``credential_headers`` maps a header name to a ``fetch-<role>`` reference. Before the
ceiling, the pacer or any byte on the wire, the handler:

1. guards the names with the ``headers`` rules, folded **with** ``headers``, so a
   name in both maps (co-core E3) or twice by case (E4) is a duplicate; refuses E5's
   names and any credential under ``render="browser"`` (E1). All
   ``invalid_request_options``.
2. resolves each reference against the host's frozen table: unknown or off the rule is
   ``credential_unknown``; the wrong header for its binding, or a submitted URL
   outside every binding's scope, is ``credential_refused``; the byte bound, measured
   over the resolved values, is ``invalid_request_options``.

Every refusal is terminal, before the fetch, and names the header and the reference,
never a value. The per-hop half (redirects) is ``test_egress.py``'s.
"""

from types import MappingProxyType

import httpx
import pytest
from co_core.pure.models.changes import FetchFailedEvent
from pydantic import SecretStr

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.storage.sweeper import BlobUsage
from src.worker import egress
from src.worker.credentials import FetchCredential, FetchCredentialTable, Resolved
from src.worker.headers import MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS
from src.worker.loop import Outcome, poll_once
from src.worker.reporter import build_failure_reporter
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    FakeFetcher,
    command,
    decoded_facts,
    make_command,
    process_one,
)

VALUE = "planted-handler-value-91ab"  # never a real credential; a tripwire for leaks
URL = "https://api.leg.wa.gov/bills"


def credential(header="x-api-key", hosts=("api.leg.wa.gov",), value=VALUE) -> FetchCredential:
    return FetchCredential(
        header=header, hosts=frozenset(hosts), value=SecretStr(value), value_file="/v"
    )


def table(**bindings: FetchCredential) -> FetchCredentialTable:
    return FetchCredentialTable(
        MappingProxyType({name.replace("_", "-"): cred for name, cred in bindings.items()})
    )


WA = table(fetch_wa_leg_api=credential())


class ScopeRecordingFetcher(FakeFetcher):
    """A ``FakeFetcher`` that also records the command scope its fetch ran under.

    Reads the transport's ``ContextVar`` directly: the scope is what the transport
    will read on every hop, so it is what the handler is responsible for.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.scopes: list[egress.CommandScope | None] = []

    async def execute(self, effect):
        self.scopes.append(egress._COMMAND_SCOPE.get())
        return await super().execute(effect)


async def refused(handler, cmd, reason, *, credentials=WA, **build) -> PermanentFetchError:
    fetcher = FakeFetcher()
    with pytest.raises(PermanentFetchError) as caught:
        await handler(fetcher, credentials=credentials, **build)(cmd)
    assert caught.value.reason is reason
    assert fetcher.effects == [], "refused before the fetch, not after"
    assert VALUE not in str(caught.value)
    return caught.value


# --- None and {} are today's path ---------------------------------------------------


@pytest.mark.parametrize("credential_headers", [None, {}])
async def test_no_credentials_is_todays_path(handler, credential_headers):
    fetcher = ScopeRecordingFetcher()

    await handler(fetcher, credentials=WA)(command(credential_headers=credential_headers))

    assert fetcher.effect.headers is None
    [scope] = fetcher.scopes
    assert scope is not None and scope.credentials == ()


async def test_a_resolved_credential_rides_the_scope_and_never_the_effect(handler):
    """The value goes to the transport through the scope; ``FetchContent`` never holds it."""
    fetcher = ScopeRecordingFetcher()

    await handler(fetcher, credentials=WA)(
        command(url=URL, credential_headers={"X-Api-Key": "fetch-wa-leg-api"})
    )

    assert fetcher.effect.headers is None
    [scope] = fetcher.scopes
    assert scope.credentials == (Resolved("fetch-wa-leg-api", WA.resolve("fetch-wa-leg-api")),)
    assert VALUE not in repr(fetcher.effect)


async def test_the_scope_is_reset_after_the_fetch(handler):
    await handler(ScopeRecordingFetcher(), credentials=WA)(
        command(url=URL, credential_headers={"X-Api-Key": "fetch-wa-leg-api"})
    )

    assert egress._COMMAND_SCOPE.get() is None


# --- Step 1: the name guards, folded with headers -----------------------------------


@pytest.mark.parametrize(
    ("credential_headers", "headers", "names"),
    [
        pytest.param({"X Api Key": "fetch-wa-leg-api"}, None, "X Api Key", id="not-a-token"),
        pytest.param({"Host": "fetch-wa-leg-api"}, None, "Host", id="refused"),
        pytest.param(
            {"Proxy-Authorization": "fetch-wa-leg-api"}, None, "Proxy-Authorization", id="proxy"
        ),
        pytest.param(
            {"X-Api-Key": "fetch-wa-leg-api"}, {"x-api-key": "plain"}, "X-Api-Key", id="E3"
        ),
        pytest.param(
            {"X-Api-Key": "fetch-wa-leg-api", "x-api-key": "fetch-wa-leg-api"},
            None,
            "x-api-key",
            id="E4",
        ),
        pytest.param({"User-Agent": "fetch-wa-leg-api"}, None, "User-Agent", id="E5-ua"),
        pytest.param({"If-None-Match": "fetch-wa-leg-api"}, None, "If-None-Match", id="E5-inm"),
        pytest.param(
            {"if-modified-since": "fetch-wa-leg-api"}, None, "if-modified-since", id="E5-ims"
        ),
    ],
)
async def test_an_unsendable_credential_name_is_refused(
    handler, credential_headers, headers, names
):
    error = await refused(
        handler,
        command(url=URL, headers=headers, credential_headers=credential_headers),
        FailureReason.INVALID_REQUEST_OPTIONS,
    )

    assert names in str(error)


async def test_the_count_bound_is_over_both_maps(handler):
    headers = {f"x-h{index}": "v" for index in range(MAX_REQUEST_HEADERS)}

    await refused(
        handler,
        command(url=URL, headers=headers, credential_headers={"X-Api-Key": "fetch-wa-leg-api"}),
        FailureReason.INVALID_REQUEST_OPTIONS,
    )


async def test_any_credential_under_browser_is_refused_before_the_table(handler):
    """E1 (D5): a browser sends a header to every host a page contacts; no hop scope exists."""
    error = await refused(
        handler,
        command(url=URL, render="browser", credential_headers={"X-Api-Key": "fetch-wa-leg-api"}),
        FailureReason.INVALID_REQUEST_OPTIONS,
        browser_fetcher=FakeFetcher(),
    )

    assert "browser" in str(error)


async def test_an_unsendable_name_is_refused_even_with_nothing_provisioned(handler):
    """Step 1 is pure and comes first: the issuer hears the shape error, not the table's."""
    await refused(
        handler,
        command(url=URL, credential_headers={"Host": "fetch-wa-leg-api"}),
        FailureReason.INVALID_REQUEST_OPTIONS,
        credentials=table(),
    )


# --- Step 2: resolution against the table -------------------------------------------


async def test_an_unprovisioned_reference_is_credential_unknown(handler):
    error = await refused(
        handler,
        command(url=URL, credential_headers={"X-Api-Key": "fetch-or-leg-api"}),
        FailureReason.CREDENTIAL_UNKNOWN,
    )

    assert "X-Api-Key" in str(error)
    assert "fetch-or-leg-api" in str(error)


async def test_nothing_provisioned_by_default(handler):
    """``build_handler`` without a table provisions nothing: refused, never sent bare."""
    fetcher = FakeFetcher()
    with pytest.raises(PermanentFetchError) as caught:
        await handler(fetcher)(
            command(url=URL, credential_headers={"X-Api-Key": "fetch-wa-leg-api"})
        )

    assert caught.value.reason is FailureReason.CREDENTIAL_UNKNOWN
    assert fetcher.effects == []


@pytest.mark.parametrize("reference", ["gcs-publication", "sk_live_51HpastedSecret", ""])
async def test_a_reference_off_the_rule_is_unknown_and_never_quoted(handler, reference):
    """The likeliest off-rule reference is a pasted secret, and ``detail`` is published."""
    error = await refused(
        handler,
        command(url=URL, credential_headers={"X-Api-Key": reference}),
        FailureReason.CREDENTIAL_UNKNOWN,
    )

    assert "X-Api-Key" in str(error)
    if reference:
        assert reference not in str(error)


async def test_a_header_other_than_the_bindings_is_credential_refused(handler):
    """Operator call 3: a binding pins its header, so the value can't ride an echoed one."""
    error = await refused(
        handler,
        command(url=URL, credential_headers={"Referer": "fetch-wa-leg-api"}),
        FailureReason.CREDENTIAL_REFUSED,
    )

    assert "Referer" in str(error) and "fetch-wa-leg-api" in str(error)
    assert "x-api-key" in str(error)


async def test_the_bindings_header_matches_whatever_the_case(handler):
    fetcher = ScopeRecordingFetcher()

    await handler(fetcher, credentials=WA)(
        command(url=URL, credential_headers={"X-API-KEY": "fetch-wa-leg-api"})
    )

    assert fetcher.scopes[0].credentials == (
        Resolved("fetch-wa-leg-api", WA.resolve("fetch-wa-leg-api")),
    )


async def test_the_byte_bound_counts_the_resolved_values(handler):
    """C4: measured over what goes on the wire, so the reference's length is beside the point."""
    big = table(fetch_wa_leg_api=credential(value="v" * (MAX_REQUEST_HEADER_BYTES - 20)))

    error = await refused(
        handler,
        command(
            url=URL,
            headers={"accept": "a" * 40},
            credential_headers={"X-Api-Key": "fetch-wa-leg-api"},
        ),
        FailureReason.INVALID_REQUEST_OPTIONS,
        credentials=big,
    )

    assert "byte" in str(error)


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("http://api.leg.wa.gov/bills", id="http"),
        pytest.param("https://api.leg.wa.gov:8443/bills", id="another-port"),
        pytest.param("https://www.leg.wa.gov/bills", id="another-host"),
        pytest.param("https://leg.wa.gov/bills", id="the-parent-domain"),
        pytest.param("https://evil.api.leg.wa.gov/bills", id="a-subdomain"),
    ],
)
async def test_a_submitted_url_outside_the_scope_is_credential_refused(handler, url):
    error = await refused(
        handler,
        command(url=url, credential_headers={"X-Api-Key": "fetch-wa-leg-api"}),
        FailureReason.CREDENTIAL_REFUSED,
    )

    assert "fetch-wa-leg-api" in str(error)


@pytest.mark.parametrize(
    "url",
    [
        "https://API.Leg.WA.gov/bills",
        "https://api.leg.wa.gov./bills",
        "https://api.leg.wa.gov:443/bills",
    ],
)
async def test_the_scope_compares_the_canonical_host(handler, url):
    fetcher = ScopeRecordingFetcher()

    await handler(fetcher, credentials=WA)(
        command(url=url, credential_headers={"X-Api-Key": "fetch-wa-leg-api"})
    )

    assert fetcher.effects


async def test_an_idna_host_is_compared_in_punycode(handler):
    bucher = table(fetch_books=credential(hosts=("xn--bcher-kva.example",)))
    fetcher = ScopeRecordingFetcher()

    await handler(fetcher, credentials=bucher)(
        command(url="https://Bücher.example/x", credential_headers={"X-Api-Key": "fetch-books"})
    )

    assert fetcher.effects


async def test_every_binding_must_cover_the_host(handler):
    """Two credentials on one command: the hop must be in scope for both, or neither is sent."""
    two = table(
        fetch_wa_leg_api=credential(),
        fetch_other=credential(header="x-other", hosts=("other.example",)),
    )

    error = await refused(
        handler,
        command(
            url=URL,
            credential_headers={"X-Api-Key": "fetch-wa-leg-api", "X-Other": "fetch-other"},
        ),
        FailureReason.CREDENTIAL_REFUSED,
        credentials=two,
    )

    assert "fetch-other" in str(error)


# --- Ordering: before the ceiling and the pacer -------------------------------------


async def test_a_credential_refusal_comes_before_the_ceiling(handler):
    """A full store parks a command transiently; a refused one must close now, not wait."""
    usage = BlobUsage()
    usage.add(10)

    with pytest.raises(PermanentFetchError) as caught:
        await handler(FakeFetcher(), credentials=WA, usage=usage, ceiling_bytes=1)(
            command(url=URL, credential_headers={"X-Api-Key": "fetch-or-leg-api"})
        )

    assert caught.value.reason is FailureReason.CREDENTIAL_UNKNOWN
    assert not isinstance(caught.value, TransientFetchError)


# --- The fact the issuer hears ------------------------------------------------------


async def test_a_refusal_closes_with_a_terminal_fact_that_names_no_value(
    fake_redis, consumer, settings, handler, caplog
):
    """Terminal, no status, no attempts; the header and reference named; no value anywhere."""
    await fake_redis.xadd(
        TOPIC,
        make_command(
            command_id="cmd-cred",
            url="https://www.leg.wa.gov/x",
            credential_headers={"X-Api-Key": "fetch-wa-leg-api"},
        ),
    )
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]

    with caplog.at_level("DEBUG"):
        outcome = await process_one(
            fake_redis,
            consumer,
            settings,
            message,
            handler(credentials=WA),
            reporter=build_failure_reporter(client=fake_redis),
        )

    assert outcome is Outcome.DEAD_LETTERED
    raw = await fake_redis.xrange("content.blobs")
    assert VALUE not in str(raw)
    [fact] = await decoded_facts(fake_redis, "content.blobs")
    assert isinstance(fact, FetchFailedEvent)
    assert fact.reason == "credential_refused"
    assert fact.terminal is True
    assert fact.status_code is None
    assert fact.attempts is None
    assert "x-api-key" in fact.detail.lower() and "fetch-wa-leg-api" in fact.detail
    assert VALUE not in caplog.text


async def test_a_reference_with_a_trailing_newline_is_off_the_rule(handler):
    """co-core's pattern ends in ``$``; the rule is checked whole, so ``\\n`` can't ride it."""
    error = await refused(
        handler,
        command(url=URL, credential_headers={"X-Api-Key": "fetch-wa-leg-api\n"}),
        FailureReason.CREDENTIAL_UNKNOWN,
    )

    # The off-rule branch, not the unprovisioned one: the whole string was checked.
    assert "not a fetch-<role> reference" in str(error)


async def test_an_unparseable_url_with_credentials_is_not_fetchable(handler):
    """The scope check parses the URL first; one httpx can't parse closes as ``_fetch`` would."""
    await refused(
        handler,
        command(url="https://a.example:abc/", credential_headers={"X-Api-Key": "fetch-wa-leg-api"}),
        FailureReason.NOT_FETCHABLE,
    )


def test_no_credentials_are_never_out_of_scope():
    """An anonymous hop has no scope to leave: any scheme, port and host pass."""
    assert egress.scope_refusal(httpx.URL("http://127.0.0.1:8080/"), ()) is None
