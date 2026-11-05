"""``ContentFetchCommand.render``: which fetcher a command reaches, and its refusals (#63).

``None`` and ``"http"`` are one path, today's. ``"browser"`` goes to the browser
fetcher and **never** to the HTTP driver, in any configuration: a ``browser``
command fetched over plain HTTP stores the empty shell the mode exists to avoid,
and nothing fails. Every refusal here is ``invalid_request_options``, decided from
the command alone, before the ceiling, the pacer or any request.
"""

import pytest

from src.core.errors import FailureReason, PermanentFetchError, TransientFetchError
from src.storage.sweeper import BlobUsage
from src.worker.pacing import HostPacer
from tests.worker.conftest import FakeFetcher, command


async def assert_refused(handler, cmd, *, match: str):
    http, browser = FakeFetcher(), FakeFetcher()
    with pytest.raises(PermanentFetchError, match=match) as caught:
        await handler(http, browser_fetcher=browser)(cmd)
    assert caught.value.reason is FailureReason.INVALID_REQUEST_OPTIONS
    # Pre-request: neither fetcher was asked for anything, and the fact carries
    # neither a status nor an attempt count (co-core's FetchFailedEvent docstring).
    assert http.effects == [] and browser.effects == []
    assert caught.value.status_code is None


# --------------------------------------------------------------------------- routing


@pytest.mark.parametrize("render", [None, "http"])
async def test_an_http_or_omitted_render_stays_on_the_http_driver(handler, render):
    """#177's rule: branch on ``== "browser"``, so ``"http"`` and omitted are one path."""
    http, browser = FakeFetcher(), FakeFetcher()

    await handler(http, browser_fetcher=browser)(command(render=render))

    assert len(http.effects) == 1
    assert browser.effects == []


async def test_a_browser_command_reaches_the_browser_fetcher_and_not_the_http_driver(handler):
    """The sibling #177 asked for: the decode is pinned, and now so is the route."""
    http, browser = FakeFetcher(), FakeFetcher()

    await handler(http, browser_fetcher=browser)(
        command(render="browser", headers={"user-agent": "watcher/0.1.0"}, timeout_seconds=20)
    )

    assert http.effects == []
    assert browser.effect.headers == {"user-agent": "watcher/0.1.0"}
    assert browser.effect.timeout == 20


async def test_a_browser_command_with_no_browser_here_is_parked_never_fetched_over_http(
    handler, caplog
):
    """Operator call 3: transient, with a WARNING, and the HTTP driver is not touched."""
    http = FakeFetcher()

    with pytest.raises(TransientFetchError, match="no browser"):
        await handler(http)(command(render="browser"))

    assert http.effects == []
    assert any(r.levelname == "WARNING" and "no browser" in r.getMessage() for r in caplog.records)


async def test_no_browser_here_parks_before_the_pacer_spends_a_slot(handler, clock):
    """No origin request happens, so none is recorded against the host."""
    pacer = HostPacer(1.0, clock=clock)

    with pytest.raises(TransientFetchError):
        await handler(FakeFetcher(), pacer=pacer)(command(render="browser"))

    assert pacer.wait_seconds(command().url) == 0


# --------------------------------------------------------------------------- refusals


@pytest.mark.parametrize("render", ["pdf", "HTTP", "Browser", "", " browser"])
async def test_an_unknown_render_is_refused(handler, render):
    """An exact match: what ``ContentFetchCommandEmit``'s ``RenderMode`` admits, no more."""
    await assert_refused(handler, command(render=render), match="render")


@pytest.mark.parametrize("name", ["if-none-match", "If-None-Match", "IF-MODIFIED-SINCE"])
async def test_a_validator_beside_browser_is_refused_in_any_case(handler, name):
    """cannobserv#602 decision 3: a rendered page has no upstream validator to replay."""
    await assert_refused(
        handler, command(render="browser", headers={name: '"abc"'}), match="validator"
    )


@pytest.mark.parametrize("name", ["accept-language", "Authorization", "if-match", "cookie"])
async def test_any_header_but_user_agent_beside_browser_is_refused(handler, name):
    """Operator call 1A: the browser sends the issuer's UA and nothing else of theirs."""
    await assert_refused(
        handler,
        command(render="browser", headers={"user-agent": "watcher/0.1.0", name: "x"}),
        match="user-agent",
    )


async def test_a_validator_beside_http_is_still_sent(handler):
    """The refusal is ``browser``'s alone: conditional GET over HTTP is unchanged (#17)."""
    http = FakeFetcher()

    await handler(http)(command(render="http", headers={"if-none-match": '"abc"'}))

    assert http.effect.headers == {"if-none-match": '"abc"'}


async def test_a_render_refusal_comes_before_the_ceiling(handler):
    """A command that can never succeed must not wait on a sweep to hear so."""
    usage = BlobUsage()
    usage.add(10)
    http = FakeFetcher()

    with pytest.raises(PermanentFetchError):
        await handler(http, usage=usage, ceiling_bytes=1)(command(render="pdf"))
