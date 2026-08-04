"""The fetch metadata the success fact carries beyond the bytes themselves.

Split from ``test_handler.py`` by concern (#10): those tests are about what the
byte path *does* — fetch, fingerprint, store, publish, fail — these are about
what the resulting ``blob_available`` *says*. Six optional fields
(cannobserv#271, `final_url` sourced by cannobserv#279) that a broadcast
consumer cannot recover once fetching lives here rather than in Watcher.

The recurring assertion is a **distinction**, not a value: ``None`` means the
origin (or the driver) said nothing, and it must stay distinguishable from a
value that merely looks like a default. A handler that substituted
``command.url`` for a missing ``final_url``, or the normalized
``application/octet-stream`` for a missing ``Content-Type``, would pass a
type check and destroy the only thing these fields are for.
"""

from datetime import UTC, datetime, timedelta

import pytest

from src.worker.handler import MAX_HEADER_VALUE_LENGTH
from tests.worker.conftest import URL, FakeFetcher, command, fetch_result, published_facts


async def test_the_landing_url_is_passed_through(handler, fake_redis):
    """The redirect chain is the whole point: Watcher derives its rate-limiter
    key and an FK from where the fetch *landed*, and after Phase 4 it never sees
    the chain itself (watcher#157, watcher#241)."""
    fetcher = FakeFetcher(fetch_result(final_url="https://example.test/landed"))

    await handler(fetcher)(command(url=URL))

    (fact,) = await published_facts(fake_redis)
    assert fact.final_url == "https://example.test/landed"


async def test_an_unreported_landing_url_stays_none(handler, fake_redis):
    """``None`` means "the driver did not report one", **not** "no redirect".

    Echoing ``command.url`` here would be the tempting fix and the wrong one: an
    issuer could no longer tell "it landed where I asked" from "nobody knows
    where it landed", which is the entire distinction the field carries.
    """
    fetcher = FakeFetcher(fetch_result(final_url=None))

    await handler(fetcher)(command(url=URL))

    (fact,) = await published_facts(fake_redis)
    assert fact.final_url is None


async def test_the_status_code_rides_on_the_fact(handler, fake_redis):
    """Always a 2xx — ``_raise_for_status`` closes every other status as
    ``fetch_failed`` — so its value is telling 200 from 203 or 206."""
    fetcher = FakeFetcher(fetch_result(status_code=203))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.status_code == 203


async def test_fetched_at_is_stamped_when_the_bytes_arrived_not_at_publish(handler, fake_redis):
    """``occurred_at`` is stamped at publish, which under a reclaim is minutes
    after the bytes came off the wire. ``fetched_at`` is the wire instant."""
    before = datetime.now(UTC)

    await handler()(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.fetched_at is not None
    assert before <= fact.fetched_at <= fact.occurred_at


async def test_fetched_at_is_tz_aware_utc(handler, fake_redis):
    """co-core rejects a naive stamp fail-loud (cannobserv#273); this pins that
    the handler never hands it one to reject."""
    await handler()(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.fetched_at is not None
    assert fact.fetched_at.tzinfo is not None
    assert fact.fetched_at.utcoffset() == timedelta(0)


async def test_the_raw_content_type_keeps_what_normalization_drops(handler, fake_redis):
    """``media_type`` and ``content_type_raw`` are two channels, not one.

    Watcher stores the verbatim header as an observed fact (watcher#168); the
    normalized value is what consumers group by. Both ship, unchanged by each
    other.
    """
    fetcher = FakeFetcher(fetch_result(headers={"content-type": "TEXT/HTML; charset=utf-8"}))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.content_type_raw == "TEXT/HTML; charset=utf-8"
    assert fact.media_type == "text/html"


@pytest.mark.parametrize("headers", [{}, {"content-type": ""}, {"content-type": "   "}])
async def test_an_absent_content_type_is_none_not_the_normalized_fallback(
    handler, fake_redis, headers
):
    """``application/octet-stream`` is precisely what Watcher's dispatch reads as
    "unknown, guess from the URL". Substituting it here would silently assert the
    origin said something it did not."""
    await handler(FakeFetcher(fetch_result(headers=headers)))(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.content_type_raw is None
    assert fact.media_type == "application/octet-stream"


async def test_the_validators_are_passed_through_verbatim(handler, fake_redis):
    """Both are replayed unparsed in a conditional GET, so a parse/re-serialize
    round trip could hand the origin something it never sent. The ETag's weak
    prefix and quotes are part of the value."""
    fetcher = FakeFetcher(
        fetch_result(
            headers={
                "content-type": "text/html",
                "etag": 'W/"abc-123"',
                "last-modified": "Wed, 21 Oct 2015 07:28:00 GMT",
            }
        )
    )

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.etag == 'W/"abc-123"'
    assert fact.last_modified == "Wed, 21 Oct 2015 07:28:00 GMT"


async def test_absent_validators_stay_none(handler, fake_redis):
    await handler(FakeFetcher(fetch_result(headers={"content-type": "text/html"})))(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.etag is None
    assert fact.last_modified is None


@pytest.mark.parametrize("name", ["ETag", "Last-Modified", "Content-Type"])
async def test_the_passthroughs_are_found_case_insensitively(handler, fake_redis, name):
    """``FetchResult.headers`` is a plain ``Mapping[str, str]`` with no
    lower-casing guarantee — the same reason ``_media_type`` folds."""
    fetcher = FakeFetcher(fetch_result(headers={name: "value"}))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert "value" in (fact.etag, fact.last_modified, fact.content_type_raw)


async def test_an_absurdly_long_header_value_is_dropped_not_truncated(handler, fake_redis):
    """These are origin-controlled strings on a broadcast stream nothing trims.

    Dropped rather than truncated: a truncated ETag replayed in ``If-None-Match``
    is a validator that will never match, which is strictly worse than no
    validator at all — the origin answers 200 either way, but the issuer believes
    it asked conditionally.
    """
    oversized = "x" * (MAX_HEADER_VALUE_LENGTH + 1)
    fetcher = FakeFetcher(fetch_result(headers={"content-type": "text/html", "etag": oversized}))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.etag is None


async def test_a_header_value_at_the_bound_survives(handler, fake_redis):
    """The bound is a guard against the absurd, not a working limit."""
    at_bound = "x" * MAX_HEADER_VALUE_LENGTH
    fetcher = FakeFetcher(fetch_result(headers={"content-type": "text/html", "etag": at_bound}))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.etag == at_bound


async def test_an_oversized_content_type_still_normalizes_to_a_media_type(handler, fake_redis):
    """The bound applies to the raw channel only. ``media_type`` is required and
    non-optional on the fact, so dropping it is not an available outcome — it
    normalizes to the fallback exactly as an absent header would."""
    fetcher = FakeFetcher(fetch_result(headers={"content-type": "x" * 5000}))

    await handler(fetcher)(command())

    (fact,) = await published_facts(fake_redis)
    assert fact.content_type_raw is None
    assert fact.media_type == "application/octet-stream"
