"""The per-host policy map: what a ``content.fetch-policy`` message does to it (#19).

The map only. Reading the stream — replay, tail, poison recovery — is
``tests/worker/test_policy_reader.py``, and spending the interval it resolves is
``tests/worker/test_handler_pacing.py``.
"""

import logging
from datetime import timedelta

import pytest
from co_core.pure.models.changes import BlobAvailableEvent, FetchPolicyState

from src.worker.policy import FetchPolicyMap
from tests.worker.conftest import now

DEFAULT = 1.0


def policy(
    host: str = "slow.test",
    min_interval_seconds: float | None = 30.0,
    *,
    revoked: bool = False,
    occurred_at=None,
    schema_version: int = 1,
) -> FetchPolicyState:
    return FetchPolicyState(
        schema_version=schema_version,
        occurred_at=occurred_at if occurred_at is not None else now(),
        host=host,
        min_interval_seconds=min_interval_seconds,
        revoked=revoked,
    )


def test_an_unknown_host_has_no_policy():
    """``None``, not the default — resolving the fallback is the pacer's job.

    Answering with the default here would make "no policy" and "a policy that
    happens to equal the default" indistinguishable at the one place the
    difference is worth logging.
    """
    assert FetchPolicyMap(DEFAULT).interval_for("unknown.test") is None


def test_a_policy_is_applied_under_its_host():
    policies = FetchPolicyMap(DEFAULT)

    policies.apply(policy(host="slow.test", min_interval_seconds=30.0))

    assert policies.interval_for("slow.test") == 30.0


def test_a_later_policy_replaces_an_earlier_one():
    """Last write wins — that is the whole semantics of a config/state stream."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()

    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))
    policies.apply(policy(min_interval_seconds=5.0, occurred_at=first + timedelta(seconds=1)))

    assert policies.interval_for("slow.test") == 5.0


def test_an_interval_of_zero_survives_as_zero():
    """A legal value meaning "this host needs no spacing", not an absent one.

    The falsy-zero trap at the storage end: a map that stores only truthy values
    turns an explicit operator decision into a fallback to the default.
    """
    policies = FetchPolicyMap(DEFAULT)

    policies.apply(policy(host="fast.test", min_interval_seconds=0.0))

    assert policies.interval_for("fast.test") == 0.0


def test_a_revoked_host_drops_out_of_the_map():
    """``revoked`` is the tombstone — LWW has no delete.

    It means "no explicit policy for this host", not "no limit": the host
    resolves to ``None`` and the pacer falls back to its conservative default.
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))

    policies.apply(
        policy(
            min_interval_seconds=None,
            revoked=True,
            occurred_at=first + timedelta(seconds=1),
        )
    )

    assert policies.interval_for("slow.test") is None


def test_revocation_is_read_before_the_interval():
    """Branch on ``revoked`` first — the interval is ``None`` on a tombstone.

    A map that reached for ``min_interval_seconds`` first would store a ``None``
    and hand it to the pacer as though it were a number.
    """
    policies = FetchPolicyMap(DEFAULT)

    policies.apply(policy(min_interval_seconds=None, revoked=True))

    assert policies.interval_for("slow.test") is None
    assert policies.tracked_hosts == 0


def test_revoking_a_host_that_was_never_known_is_a_no_op():
    """The producer republishes revoked hosts in its full set, so a booting
    worker sees tombstones for hosts it never held."""
    policies = FetchPolicyMap(DEFAULT)

    policies.apply(policy(host="never.test", min_interval_seconds=None, revoked=True))

    assert policies.interval_for("never.test") is None


def test_a_stale_republish_does_not_revert_a_newer_policy():
    """Arrival order is not publication order once a full set is republished.

    The producer periodically re-emits every host. A republish built from a
    snapshot taken before a change that already shipped would otherwise revert
    it — silently, and in the loosening direction.
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))

    policies.apply(policy(min_interval_seconds=2.0, occurred_at=first - timedelta(seconds=5)))

    assert policies.interval_for("slow.test") == 30.0


def test_a_stale_revocation_does_not_drop_a_newer_policy():
    """The tombstone gets the same ordering guard as a live value — otherwise the
    one message that erases state is the one message that ignores ordering."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))

    policies.apply(
        policy(
            min_interval_seconds=None,
            revoked=True,
            occurred_at=first - timedelta(seconds=5),
        )
    )

    assert policies.interval_for("slow.test") == 30.0


def test_a_republish_of_the_same_stamp_is_applied():
    """``>=``, not ``>``: a producer that stamps a whole full set with one
    ``occurred_at`` must not have every host after the first ignored."""
    policies = FetchPolicyMap(DEFAULT)
    stamp = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=stamp))

    policies.apply(policy(min_interval_seconds=5.0, occurred_at=stamp))

    assert policies.interval_for("slow.test") == 5.0


def test_ordering_is_tracked_per_host():
    """One host's newer stamp must not suppress another host's older one."""
    policies = FetchPolicyMap(DEFAULT)
    stamp = now()
    policies.apply(policy(host="a.test", min_interval_seconds=30.0, occurred_at=stamp))

    policies.apply(
        policy(
            host="b.test",
            min_interval_seconds=5.0,
            occurred_at=stamp - timedelta(seconds=10),
        )
    )

    assert policies.interval_for("b.test") == 5.0


def test_a_payload_from_another_stream_is_ignored(caplog):
    """``from_wire``'s dispatch table is global.

    A ``blob_available`` frame XADDed to ``content.fetch-policy`` decodes
    *cleanly* into the wrong model rather than raising, so there is no anomaly to
    recover from — the guard has to be an ``isinstance`` check here.
    """
    policies = FetchPolicyMap(DEFAULT)
    foreign = BlobAvailableEvent(
        occurred_at=now(),
        command_id="c1",
        url="https://slow.test/a",
        blob_uri="file:///tmp/x.bin",
        content_fingerprint="f" * 64,
        size_bytes=1,
        media_type="text/html",
    )

    with caplog.at_level(logging.WARNING):
        policies.apply(foreign)

    assert policies.tracked_hosts == 0
    assert "not a fetch policy" in caplog.text


def test_an_unsupported_schema_version_is_ignored(caplog):
    """Branch on the version before destructuring, the same rule the command
    path follows. A v2 that moved ``host`` would otherwise be keyed on nothing."""
    policies = FetchPolicyMap(DEFAULT)

    with caplog.at_level(logging.WARNING):
        policies.apply(policy(schema_version=2))

    assert policies.tracked_hosts == 0
    assert "unsupported schema_version" in caplog.text


def test_a_policy_stricter_than_the_default_is_reported(caplog):
    """The enforceable half of "the default must be at least as strict".

    There is no upper bound on a published interval, so nothing can be asserted
    against at startup. What *is* knowable is the moment a real policy turns out
    to be stricter than the fallback that replaces it on revocation or staleness
    — and that is the number the operator has to raise.
    """
    policies = FetchPolicyMap(DEFAULT)

    with caplog.at_level(logging.WARNING):
        policies.apply(policy(host="slow.test", min_interval_seconds=30.0))

    record = next(r for r in caplog.records if "stricter than the fallback default" in r.message)
    assert record.host == "slow.test"
    assert record.min_interval_seconds == 30.0
    assert record.default_interval_seconds == DEFAULT


def test_a_policy_within_the_default_is_not_reported(caplog):
    policies = FetchPolicyMap(DEFAULT)

    with caplog.at_level(logging.WARNING):
        policies.apply(policy(host="fast.test", min_interval_seconds=0.5))

    assert "stricter than the fallback default" not in caplog.text


def test_applying_a_policy_logs_it_against_the_default(caplog):
    """ "Policy never arrived" and "policy says 1.0s" are otherwise identical from
    outside — which is the failure the Watcher cutover would hit and nobody would
    notice."""
    policies = FetchPolicyMap(DEFAULT)

    with caplog.at_level(logging.INFO):
        policies.apply(policy(host="slow.test", min_interval_seconds=30.0))

    record = next(r for r in caplog.records if r.message == "applied a host fetch policy")
    assert record.host == "slow.test"
    assert record.min_interval_seconds == 30.0
    assert record.default_interval_seconds == DEFAULT


def test_the_map_reports_how_many_hosts_it_holds():
    """The gauge that makes an empty replay visible at boot."""
    policies = FetchPolicyMap(DEFAULT)
    policies.apply(policy(host="a.test"))
    policies.apply(policy(host="b.test"))

    assert policies.tracked_hosts == 2


@pytest.mark.parametrize("host", ["SLOW.test", "slow.test"])
def test_the_host_key_is_whatever_the_model_canonicalized(host):
    """co-core lowercases and validates ``host`` on the way in, so the map keys on
    a name the pacer's ``urlsplit().hostname`` produces without a second rule."""
    policies = FetchPolicyMap(DEFAULT)

    policies.apply(policy(host=host))

    assert policies.interval_for("slow.test") == 30.0
