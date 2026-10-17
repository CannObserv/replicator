"""The per-host policy map: what a ``content.fetch-policy`` message does to it (#19).

The map only. Reading the stream — replay, tail, poison recovery — is
``tests/worker/test_policy_reader.py``, and spending the interval it resolves is
``tests/worker/test_handler_pacing.py``.
"""

import json
import logging
from datetime import timedelta

import pytest
from co_core.pure.adapters.bus.envelope import from_wire, to_wire
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
    generation: int | None = None,
) -> FetchPolicyState:
    return FetchPolicyState(
        schema_version=schema_version,
        occurred_at=occurred_at if occurred_at is not None else now(),
        host=host,
        min_interval_seconds=min_interval_seconds,
        revoked=revoked,
        generation=generation,
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
        info_source_id="isrc-1",
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


# --------------------------------------------------------------------------- #
# Logging on change only (#85).
# --------------------------------------------------------------------------- #


def test_a_republish_that_changes_nothing_is_not_logged_again(caplog):
    """The producer re-emits its whole set on a cron, so most applies are no-ops.

    Logging every one of them cost ~31 lines/second on the replicator host
    during a boot replay, and 3 lines every 5 minutes forever after — a journal
    in which the line that means "a host's politeness changed" is buried under
    thousands that mean "nothing changed" (#85).
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))
    caplog.clear()

    with caplog.at_level(logging.INFO):
        policies.apply(policy(min_interval_seconds=30.0, occurred_at=first + timedelta(seconds=1)))

    assert "applied a host fetch policy" not in caplog.text
    assert policies.interval_for("slow.test") == 30.0


def test_a_republish_that_changes_the_interval_is_logged(caplog):
    """The line survives for the case it was added for: a policy that moved."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))
    caplog.clear()

    with caplog.at_level(logging.INFO):
        policies.apply(policy(min_interval_seconds=12.0, occurred_at=first + timedelta(seconds=1)))

    record = next(r for r in caplog.records if r.message == "applied a host fetch policy")
    assert record.min_interval_seconds == 12.0


def test_a_republished_strict_policy_is_warned_about_once(caplog):
    """The "stricter than the fallback" warning rides the same gate.

    It is a standing condition, not an event: repeating it on every republish
    would restore the volume the change removes and say nothing new.
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))
    caplog.clear()

    with caplog.at_level(logging.WARNING):
        policies.apply(policy(min_interval_seconds=30.0, occurred_at=first + timedelta(seconds=1)))

    assert "stricter than the fallback default" not in caplog.text


def test_a_republished_tombstone_is_not_logged_again(caplog):
    """Revocation is an edge too: the producer keeps republishing the tombstone."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))
    policies.apply(
        policy(min_interval_seconds=None, revoked=True, occurred_at=first + timedelta(seconds=1))
    )
    caplog.clear()

    with caplog.at_level(logging.INFO):
        policies.apply(
            policy(
                min_interval_seconds=None, revoked=True, occurred_at=first + timedelta(seconds=2)
            )
        )

    assert "revoked a host fetch policy" not in caplog.text


def test_revoking_a_host_that_was_never_known_logs_nothing(caplog):
    """A booting worker sees tombstones for hosts it never held — one per
    republish, per host, for the whole history of the stream."""
    policies = FetchPolicyMap(DEFAULT)

    with caplog.at_level(logging.INFO):
        policies.apply(policy(host="never.test", min_interval_seconds=None, revoked=True))

    assert "revoked a host fetch policy" not in caplog.text


def test_a_host_revoked_and_then_republished_is_logged_again(caplog):
    """Change-gating must not hide a host coming *back* at the value it had."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))
    policies.apply(
        policy(min_interval_seconds=None, revoked=True, occurred_at=first + timedelta(seconds=1))
    )
    caplog.clear()

    with caplog.at_level(logging.INFO):
        policies.apply(policy(min_interval_seconds=30.0, occurred_at=first + timedelta(seconds=2)))

    record = next(r for r in caplog.records if r.message == "applied a host fetch policy")
    assert record.min_interval_seconds == 30.0


def test_the_map_reports_how_many_policies_outstrip_the_default():
    """A *standing* condition needs a gauge, not only an event (CR 2).

    The per-apply warning is change-gated (#85), so on a stream that has been
    quiet for a day the last one has rotated out of the journal while the
    condition it named still holds. The replay summary carries this number so
    every boot re-asserts it.
    """
    policies = FetchPolicyMap(DEFAULT)
    policies.apply(policy(host="strict.test", min_interval_seconds=30.0))
    policies.apply(policy(host="loose.test", min_interval_seconds=0.5))
    policies.apply(policy(host="equal.test", min_interval_seconds=DEFAULT))

    assert policies.tracked_hosts == 3
    # Strictly greater, the same comparison the warning fires on: a policy
    # equal to the fallback loses nothing when the fallback replaces it.
    assert policies.hosts_stricter_than_default == 1


def test_a_revoked_host_stops_counting_against_the_default():
    """It resolves to the fallback from here on, so it is no longer outstripping it."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(host="strict.test", min_interval_seconds=30.0, occurred_at=first))

    policies.apply(
        policy(
            host="strict.test",
            min_interval_seconds=None,
            revoked=True,
            occurred_at=first + timedelta(seconds=1),
        )
    )

    assert policies.hosts_stricter_than_default == 0


# --------------------------------------------------------------------------- #
# Ordering by `generation` (#136).
#
# Archiver publishes through an outbox that can reorder under retry, so its
# frames carry a per-host `generation`. The rule is co-core's
# `fetch_policy_supersedes`; the map keeps today's `occurred_at` guard only for
# frames that carry none (the operator's decision on #136).
# --------------------------------------------------------------------------- #


def decoded(payload: dict) -> FetchPolicyState:
    """A policy as a producer on any co-core version would put it on the wire."""
    fields = to_wire(policy())
    fields["payload"] = json.dumps(payload)
    message = from_wire(fields, topic="content.fetch-policy")
    assert isinstance(message.payload, FetchPolicyState)
    return message.payload


def test_a_newer_generation_replaces_an_older_one():
    policies = FetchPolicyMap(DEFAULT)
    stamp = now()
    policies.apply(policy(min_interval_seconds=30.0, generation=1, occurred_at=stamp))

    policies.apply(policy(min_interval_seconds=5.0, generation=2, occurred_at=stamp))

    assert policies.interval_for("slow.test") == 5.0


def test_a_generation_decides_alone_over_an_older_stamp():
    """Once both frames are ordered, ``occurred_at`` is not consulted.

    An outbox retry can publish row N+1 with an earlier stamp than row N's
    eventual publish, so the stamp is exactly what the generation replaces.
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, generation=1, occurred_at=first))

    policies.apply(
        policy(min_interval_seconds=5.0, generation=2, occurred_at=first - timedelta(seconds=5))
    )

    assert policies.interval_for("slow.test") == 5.0


def test_a_lower_generation_is_ignored_even_with_a_newer_stamp(caplog):
    """The outbox reorder itself: row N lands after N+1 and must not win."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, generation=5, occurred_at=first))

    with caplog.at_level(logging.INFO):
        policies.apply(
            policy(min_interval_seconds=2.0, generation=4, occurred_at=first + timedelta(seconds=5))
        )

    assert policies.interval_for("slow.test") == 30.0
    record = next(
        r
        for r in caplog.records
        if r.message == "ignoring a fetch policy older than the one already applied"
    )
    # The generation pair, because it is the reason, not the stamps (#136).
    assert record.generation == 4
    assert record.held_generation == 5


def test_an_equal_generation_is_skipped_and_not_logged(caplog):
    """A republish repeats its delta's generation, so equal means "seen".

    And it is the steady state, not an anomaly: every Archiver full-set
    republish repeats every host's generation, and a boot replay walks all of
    them. A line per skip is the #85 volume back again.
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, generation=3, occurred_at=first))
    caplog.clear()

    with caplog.at_level(logging.INFO):
        policies.apply(
            policy(min_interval_seconds=5.0, generation=3, occurred_at=first + timedelta(seconds=1))
        )

    assert policies.interval_for("slow.test") == 30.0
    assert caplog.records == []


def test_a_frame_with_a_generation_claims_a_slot_without_one_despite_an_older_stamp():
    """The ratchet's upward half: Archiver takes over a host Watcher held.

    The two producers' clocks are not comparable, so an ordered frame must not
    lose to the unordered one on ``occurred_at``.
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))

    policies.apply(
        policy(min_interval_seconds=5.0, generation=0, occurred_at=first - timedelta(seconds=60))
    )

    assert policies.interval_for("slow.test") == 5.0


def test_a_frame_without_a_generation_never_overwrites_one_with(caplog):
    """The ratchet's other half: a Watcher that did not stop at F1 cannot fight
    the new owner, however new its stamp. Logged, because the docstring's
    "drops just as silently" is the failure mode worth a line."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, generation=2, occurred_at=first))

    with caplog.at_level(logging.INFO):
        policies.apply(policy(min_interval_seconds=2.0, occurred_at=first + timedelta(minutes=5)))

    assert policies.interval_for("slow.test") == 30.0
    record = next(
        r
        for r in caplog.records
        if r.message == "ignoring a fetch policy without a generation for a host that holds one"
    )
    assert record.host == "slow.test"
    assert record.held_generation == 2


def test_a_tombstone_with_a_generation_is_ordered_like_a_live_frame():
    policies = FetchPolicyMap(DEFAULT)
    stamp = now()
    policies.apply(policy(min_interval_seconds=30.0, generation=1, occurred_at=stamp))

    policies.apply(policy(min_interval_seconds=None, revoked=True, generation=2, occurred_at=stamp))
    # The live frame the tombstone superseded, retried late by the outbox.
    policies.apply(
        policy(min_interval_seconds=30.0, generation=1, occurred_at=stamp + timedelta(seconds=9))
    )

    assert policies.interval_for("slow.test") is None
    assert policies.tracked_hosts == 0


def test_a_stale_tombstone_does_not_drop_a_newer_generation():
    policies = FetchPolicyMap(DEFAULT)
    stamp = now()
    policies.apply(policy(min_interval_seconds=30.0, generation=4, occurred_at=stamp))

    policies.apply(
        policy(
            min_interval_seconds=None,
            revoked=True,
            generation=3,
            occurred_at=stamp + timedelta(seconds=9),
        )
    )

    assert policies.interval_for("slow.test") == 30.0


def test_a_revoked_host_keeps_its_generation():
    """The hazard the held frame exists for (#136 hazard 2).

    A map that forgot the tombstone's slot would treat the host as empty, and
    a stale live frame without a generation would claim it — resurrecting a
    revoked host on the next Watcher republish.
    """
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first))
    policies.apply(
        policy(
            min_interval_seconds=None,
            revoked=True,
            generation=5,
            occurred_at=first - timedelta(seconds=30),
        )
    )

    policies.apply(policy(min_interval_seconds=30.0, occurred_at=first + timedelta(minutes=5)))

    assert policies.interval_for("slow.test") is None


def test_a_tombstone_for_an_unknown_host_still_claims_its_slot():
    """A booting worker meets tombstones for hosts it never held — they are
    still ordered, or a later unordered frame would resurrect them."""
    policies = FetchPolicyMap(DEFAULT)
    first = now()
    policies.apply(
        policy(
            host="never.test",
            min_interval_seconds=None,
            revoked=True,
            generation=1,
            occurred_at=first,
        )
    )

    policies.apply(
        policy(host="never.test", min_interval_seconds=9.0, occurred_at=first + timedelta(hours=1))
    )

    assert policies.interval_for("never.test") is None


@pytest.mark.parametrize(
    "generation_field",
    [{}, {"generation": None}],
    ids=["key-missing", "null"],
)
def test_a_missing_or_null_generation_on_the_wire_is_absent(generation_field):
    """``null`` means absent (cannobserv#565's convention), and so does the key a
    pre-0.37 producer never sends. Built through ``from_wire``: a Python
    ``generation=None`` exercises only the default, not the decode."""
    stamp = now()
    frame = {
        "schema_version": 1,
        "event_type": "fetch_policy",
        "occurred_at": stamp.isoformat(),
        "host": "slow.test",
        "min_interval_seconds": 5.0,
        "revoked": False,
        **generation_field,
    }
    unordered = FetchPolicyMap(DEFAULT)
    unordered.apply(policy(min_interval_seconds=30.0, occurred_at=stamp))
    ordered = FetchPolicyMap(DEFAULT)
    ordered.apply(policy(min_interval_seconds=30.0, generation=0, occurred_at=stamp))

    unordered.apply(decoded(frame))
    ordered.apply(decoded(frame))

    # Stream order (with the >= stamp guard) over an unordered slot...
    assert unordered.interval_for("slow.test") == 5.0
    # ...and never over an ordered one.
    assert ordered.interval_for("slow.test") == 30.0
