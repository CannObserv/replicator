"""The bus client's connection policy (CannObserv/broker#1 R7).

Loopback made the absence of one harmless: a local broker either answers in
microseconds or refuses at once. Neither holds across the tailnet hop to the
relocated broker, where the measured path from a *different* region is a ~40 ms
DERP relay. R7 asks every participant for an explicit policy before that
cutover; archiver landed its half in CannObserv/archiver#193 and watcher in
CannObserv/watcher#287, and this is the third.
"""

from __future__ import annotations

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from src.core import bus_client
from src.core.config import Settings


def test_socket_timeout_clears_the_configured_blocking_read() -> None:
    """``socket_timeout`` must exceed the loop's ``BLOCK``, with margin.

    Measured, not assumed: redis-py does **not** extend ``socket_timeout`` for a
    blocking command. A client built with ``socket_timeout=1`` raises
    ``TimeoutError`` after 1.01 s on an ``XREAD ... BLOCK 3000``; at
    ``socket_timeout=10`` the same call returns normally after 3.08 s.

    So a value at or below the block window does not bound a stall, it
    manufactures one on every idle read against a healthy broker.
    """
    for block_ms in (5_000, 15_000, 250):
        assert bus_client.socket_timeout_for(block_ms) > block_ms / 1000
        assert bus_client.socket_timeout_for(block_ms) == (
            block_ms / 1000 + bus_client.BLOCKING_READ_MARGIN_SECONDS
        )


def test_socket_timeout_tracks_the_setting_not_a_copy_of_its_default() -> None:
    """``REPLICATOR_READ_BLOCK_MS`` is a *runtime* knob, so the derivation must
    be too.

    This is where replicator differs from its siblings: archiver and watcher
    each hold ``READ_BLOCK_MS`` as a module constant, so deriving once at import
    is sound there. Here the window is configurable, and a timeout derived from
    the *default* would be silently wrong for anyone who raised the env var -
    the failure being a consumer that times out on every idle read, which reads
    as a broker fault rather than a configuration one.
    """
    raised = Settings(REPLICATOR_READ_BLOCK_MS=20_000)
    client = bus_client.build_bus_client(raised)
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == bus_client.socket_timeout_for(20_000)
    assert kwargs["socket_timeout"] > 20.0, "a 20s block window needs >20s of socket timeout"


def test_build_bus_client_applies_the_whole_policy() -> None:
    client = bus_client.build_bus_client(Settings())
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_timeout"] == bus_client.socket_timeout_for(5_000)
    assert kwargs["socket_connect_timeout"] == bus_client.SOCKET_CONNECT_TIMEOUT_SECONDS
    assert kwargs["health_check_interval"] == bus_client.HEALTH_CHECK_INTERVAL_SECONDS


def test_connect_timeout_is_bounded_and_shorter_than_the_read_timeout() -> None:
    """Connecting carries no ``BLOCK``, so it needs no headroom and should fail
    fast; a *slow* broker must not be cut off mid-read."""
    assert 0 < bus_client.SOCKET_CONNECT_TIMEOUT_SECONDS < bus_client.socket_timeout_for(5_000)


def test_client_takes_no_retry_because_a_retry_re_sends_the_command() -> None:
    """Zero retries, and the zero is the load-bearing part.

    A redis-py retry **re-sends the command**; it does not resume a response.
    ``Redis.execute_command`` wraps ``_send_command_parse_response`` in
    ``Retry.call_with_retry``, so a ``TimeoutError`` raised after the broker
    already applied an ``XADD`` publishes the entry twice.

    Replicator's exposure is ``content.blobs`` and ``content.artifacts``: a
    duplicated artifact fact is absorbed by archiver's writeback (MUST-4 makes a
    repeat expected traffic), but the duplicate is invisible to the producer, and
    nothing here needs it - the worker loop already retries via the PEL and
    ``REPLICATOR_CLAIM_MIN_IDLE_MS``.

    redis-py's own default is also zero - its stock ``Retry`` reads as a policy
    and behaves as none - so this passes the value explicitly to make the zero a
    decision rather than an inherited default.
    """
    client = bus_client.build_bus_client(Settings())
    assert bus_client.BUS_RETRIES == 0
    # Private attribute: redis-py publishes no accessor for the retry count.
    assert client.connection_pool.make_connection().retry._retries == 0


def test_the_retryable_set_is_left_at_the_library_default() -> None:
    """``retry_on_error`` is deliberately not passed: redis-py's ``Retry``
    already carries exactly ``(ConnectionError, TimeoutError)``. Asserted rather
    than merely omitted, so a narrowing upstream stops being silent."""
    client = bus_client.build_bus_client(Settings())
    supported = set(client.connection_pool.make_connection().retry._supported_errors)
    assert RedisConnectionError in supported
    assert RedisTimeoutError in supported


def test_worst_case_connect_stays_within_the_startup_budget() -> None:
    """``socket_connect_timeout`` bounds one *attempt*, not the call.

    Measured against a black-holed address on redis-py 7.4.1: ``connect=5,
    retries=0`` raised at 5.01 s; ``connect=5, retries=1`` at 10.03 s;
    ``connect=2, retries=1`` at 4.02 s. The budget is
    ``(retries + 1) x socket_connect_timeout``, and reading the connect timeout
    alone understates it by the retry factor.

    Asserts the ceiling rather than restating the product, which would be an
    arithmetic identity that holds whatever redis-py does.
    """
    assert bus_client.WORST_CASE_CONNECT_SECONDS <= 10.0


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("redis://localhost:6379/0", "redis://localhost:6379/0"),
        ("redis://:hunter2@broker:6379/0", "redis://:***@broker:6379/0"),
        ("redis://replicator:hunter2@broker:6379/0", "redis://replicator:***@broker:6379/0"),
        ("redis://:hunter2@[::1]:6379/0", "redis://:***@[::1]:6379/0"),
        ("redis://replicator@broker:6379/0", "redis://replicator@broker:6379/0"),
    ],
)
def test_redact_url_removes_the_password(url: str, expected: str) -> None:
    """The broker gains a credential at broker#1 D3, and any line naming the URL
    goes to journald. ``urlsplit().hostname`` strips IPv6 brackets, so they have
    to be restored or the redacted form stops being a URL."""
    assert bus_client.redact_url(url) == expected


def test_redact_url_never_leaks_on_a_url_it_cannot_parse() -> None:
    """Fail closed: the moment redaction is hardest is the moment a malformed
    URL is the thing being reported."""
    assert "hunter2" not in bus_client.redact_url("redis://[not-valid:hunter2@@@")


def test_the_worker_builds_its_client_through_the_policy() -> None:
    """The policy must reach the real client, not merely exist in a module.

    Without this the whole of R7's ask could be reverted to a bare ``from_url``
    with every test above still green - the module is thoroughly covered and its
    *use* would not be. Asserts the seam rather than the behaviour, which is
    what the tests above already own.
    """
    import inspect

    from src.worker import main as worker_main

    source = inspect.getsource(worker_main.run)
    assert "build_bus_client(settings)" in source, "the worker no longer uses the policy"
    assert "Redis.from_url(" not in source, "a bare from_url has come back"
