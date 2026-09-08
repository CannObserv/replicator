"""Connection policy for the worker's Redis client (CannObserv/broker#1 R7).

Until now this was ``Redis.from_url(settings.redis_url)`` with library defaults,
which loopback made harmless: a local broker either answers in microseconds or
refuses immediately. Neither is true once the broker moves to its own node. R7
asks each participant for an explicit policy *before* that cutover, because a
bare client meets a stalled broker with no bound at all - and a wedged consumer
sits inside a worker that still looks alive.

Three things here are not obvious, and each cost a measurement to establish.
They are recorded because the same three caught archiver
(CannObserv/archiver#193) and watcher (CannObserv/watcher#287) in turn.

**``socket_timeout`` has a floor, not a ceiling.** redis-py does not extend it
for a blocking command, so a value at or below the loop's ``BLOCK`` window
raises ``TimeoutError`` on every *idle* read against a perfectly healthy broker.
Measured on redis-py 7.4.1: ``socket_timeout=1`` with ``XREAD ... BLOCK 3000``
raised at 1.01 s; ``socket_timeout=10`` returned normally at 3.08 s.

**Here the window is a runtime setting, not a constant.** This is where
replicator differs from its two siblings, which each hold ``READ_BLOCK_MS`` as a
module constant and can derive once at import. ``REPLICATOR_READ_BLOCK_MS`` is
configurable, so the timeout is derived from the *live* setting at construction.
Deriving from the default instead would be silently wrong for anyone who raised
the knob, and the symptom - a consumer timing out on every idle read - reads as a
broker fault rather than a configuration one.

**A retry re-sends the command.** ``Redis.execute_command`` wraps
``_send_command_parse_response`` in ``Retry.call_with_retry``, so a retry after
the broker already applied an ``XADD`` publishes the entry twice. Hence
``BUS_RETRIES = 0``: the worker loop already retries through the PEL and
``REPLICATOR_CLAIM_MIN_IDLE_MS``, and a second retry policy inside the command
would only blur how long a failure took to surface.
"""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff

from src.core.config import Settings

# Headroom over the blocking read. It absorbs the round trip plus the broker's
# own scheduling slack, and it is the difference between "the read window
# elapsed" and "the socket is stalled". Generous on purpose against a relayed
# path: too tight costs a spinning consumer, too loose costs a few seconds'
# delay in noticing a stall.
BLOCKING_READ_MARGIN_SECONDS = 5.0

# Connecting carries no BLOCK, so it needs no headroom and should fail fast: a
# broker that is down, mis-addressed, or black-holed by an ACL change is the case
# this bounds.
SOCKET_CONNECT_TIMEOUT_SECONDS = 5.0

# PING a connection idle longer than this before reusing it, so a silently
# dropped TCP session surfaces as a retryable error on the next command rather
# than as a first-write failure. Relevant across a relay in a way it never was on
# loopback, where nothing sat between the two ends to time a session out.
HEALTH_CHECK_INTERVAL_SECONDS = 30

# Zero, stated rather than inherited - see the module docstring. redis-py's own
# default is also zero (its stock ``Retry`` object reads as a policy and behaves
# as none), so passing it explicitly makes the zero a decision a future change
# has to argue with rather than a default it can assume away.
BUS_RETRIES = 0

# What an operator actually waits on an unreachable broker. Measured against a
# black-holed address on redis-py 7.4.1: connect=5/retries=0 raised at 5.01 s,
# connect=5/retries=1 at 10.03 s, connect=2/retries=1 at 4.02 s. At
# ``BUS_RETRIES = 0`` the two collapse; keeping it expressed as the product is
# what makes a retry added later visibly cost twice what its own diff says.
WORST_CASE_CONNECT_SECONDS = (BUS_RETRIES + 1) * SOCKET_CONNECT_TIMEOUT_SECONDS

_REDACTED = "***"


def socket_timeout_for(read_block_ms: int) -> float:
    """Socket timeout that clears ``read_block_ms`` with margin."""
    return read_block_ms / 1000 + BLOCKING_READ_MARGIN_SECONDS


def redact_url(redis_url: str) -> str:
    """Return ``redis_url`` with any password replaced.

    The broker gains a credential when it moves to an authenticated node
    (broker#1 D3), and any line naming the URL goes to journald. Fails *closed*:
    a URL that will not parse is replaced wholesale rather than passed through.
    ``urlsplit().hostname`` strips IPv6 brackets, so they are restored - without
    that the redacted form is no longer a URL, and this string's whole job is to
    identify *which* broker is being reported.
    """
    try:
        parts = urlsplit(redis_url)
        if parts.password is None:
            return redis_url
        host = parts.hostname or ""
        if ":" in host:  # IPv6 literal
            host = f"[{host}]"
        if parts.port:
            host = f"{host}:{parts.port}"
        userinfo = f"{parts.username or ''}:{_REDACTED}"
        return urlunsplit(
            (parts.scheme, f"{userinfo}@{host}", parts.path, parts.query, parts.fragment)
        )
    except ValueError:
        return "<unparseable redis url>"


def build_bus_client(settings: Settings) -> Redis:
    """Build the worker's Redis client with an explicit connection policy."""
    return Redis.from_url(
        settings.redis_url,
        socket_timeout=socket_timeout_for(settings.read_block_ms),
        socket_connect_timeout=SOCKET_CONNECT_TIMEOUT_SECONDS,
        health_check_interval=HEALTH_CHECK_INTERVAL_SECONDS,
        # ``retry_on_error`` is deliberately absent: redis-py's Retry already
        # carries exactly (ConnectionError, TimeoutError), so passing the same
        # pair would imply this widens something it does not.
        retry=Retry(ExponentialBackoff(), retries=BUS_RETRIES),
    )
