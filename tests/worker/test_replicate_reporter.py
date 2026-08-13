"""``replication_failed`` on ``content.artifacts``: the fact an issuer closes on.

The mirror of ``test_reporter.py``. What is pinned here is the shape co-core's
model requires and the shape the *contract* requires, which are not the same
list: the model would accept a fact with a wrong-but-present correlator, and the
contract is what says the three correlators are echoed verbatim and never
recomputed.
"""

import json

import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import from_wire
from co_core.pure.models.changes import ReplicationFailedEvent
from redis.exceptions import ResponseError

from src.core.errors import ReplicateReason
from src.storage.local import LocalBlobStore
from src.worker.aliases import AliasBinding, AliasTable
from src.worker.loop import REPLICATE_SPEC, Outcome, ReplicateFailureReport, poll_once
from src.worker.replicate import build_replicate_handler
from src.worker.replicate_reporter import build_replicate_reporter
from tests.worker.conftest import GROUP, TOPIC, process_one
from tests.worker.test_loop_spec import make_replicate_command

ARTIFACTS = "replicator.test.artifacts"


def a_report(**overrides) -> ReplicateFailureReport:
    fields = {
        "command_id": "rep-1",
        "info_item_rep_spec_id": "iirs-1",
        "source_revision_id": "rev-1",
        "info_source_id": "src-1",
        "reason": ReplicateReason.ALIAS_UNKNOWN,
    }
    return ReplicateFailureReport(**{**fields, **overrides})


async def facts_on(client, topic) -> list[ReplicationFailedEvent]:
    out = []
    for message_id, fields in await client.xrange(topic):
        payload = from_wire(
            {k.decode(): v.decode() for k, v in fields.items()},
            topic=topic,
            message_id=message_id.decode(),
        ).payload
        out.append(payload)
    return out


async def test_a_report_becomes_a_replication_failed_fact(fake_redis):
    report = build_replicate_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await report(a_report(detail="the alias is not provisioned here"))

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert isinstance(fact, ReplicationFailedEvent)
    assert fact.command_id == "rep-1"
    assert fact.reason == "alias_unknown"
    assert fact.detail == "the alias is not provisioned here"


async def test_the_three_correlators_are_echoed_verbatim(fake_redis):
    """The freight test on the emit path (#28, #29).

    Values chosen so a transformation would show: if any of the three is parsed,
    normalized, or swapped for another, the assertion below fails rather than
    passing on a plausible-looking value.
    """
    report = build_replicate_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await report(
        a_report(
            info_item_rep_spec_id="  IIRS/Odd Value  ",
            source_revision_id="rev::2",
            info_source_id="src::3",
        )
    )

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert fact.info_item_rep_spec_id == "  IIRS/Odd Value  "
    assert fact.source_revision_id == "rev::2"
    assert fact.info_source_id == "src::3"


async def test_every_fact_replicator_emits_today_is_terminal(fake_redis):
    """No non-terminal fact yet, and the flag is still on the wire.

    Consumers branch on ``terminal`` first, so it has to be right even while only
    one value is ever sent. The first ``False`` arrives with the first provider
    writer — a 5xx is the retryable case — not before.
    """
    report = build_replicate_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await report(a_report())

    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert fact.terminal is True


async def test_the_envelope_key_is_per_emission_not_per_command(fake_redis):
    """Why the key is ``command_id:occurred_at`` and not the bare id.

    T4's no-op row re-emits a fact for an artifact already written, so one command
    legitimately produces several. A bare key would collapse them — and would
    collide a non-terminal failure with the success that follows it.
    """
    report = build_replicate_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await report(a_report())
    await report(a_report())

    entries = await fake_redis.xrange(ARTIFACTS)
    keys = {json.loads(fields[b"payload"])["command_id"] for _, fields in entries}
    assert len(entries) == 2, "both emissions must survive"
    assert keys == {"rep-1"}


async def test_a_failed_publish_is_swallowed_so_the_dead_letter_still_happens(
    fake_redis, monkeypatch, caplog
):
    """Same asymmetry as the fetch reporter, for the same reason.

    The dead-letter entry is already the durable record. Raising here would turn
    a clean dead-letter into an unclassified handler error that burns the
    delivery ceiling and reaches the same DLQ minutes later, stranding the
    message in the PEL in between.
    """

    async def refuse(*args, **kwargs):
        raise ResponseError("NOGROUP")

    report = build_replicate_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)
    monkeypatch.setattr(fake_redis, "xadd", refuse)

    with caplog.at_level("ERROR", logger="src.worker.replicate_reporter"):
        await report(a_report())  # must not raise

    (record,) = [r for r in caplog.records if r.levelname == "ERROR"]
    assert "failed to publish replication_failed" in record.message
    # The command_id is the whole value of the line: it is what an operator
    # correlates against the DLQ entry that did still get written.
    assert record.command_id == "rep-1"


async def test_the_default_topic_is_content_artifacts(fake_redis):
    """Where a reporter built with no topic actually publishes (CR #21).

    The first version asserted ``streams.CONTENT_ARTIFACTS == "content.artifacts"``
    — a fact about co-core, which would still have passed if this module's default
    were changed to ``content.blobs``. What matters is that an unconfigured
    reporter lands on the replicate stream: a fact on the wrong one reaches a
    consumer group that will never match it, and nothing raises.
    """
    report = build_replicate_reporter(client=fake_redis)  # no artifacts_topic

    await report(a_report())

    assert await fake_redis.xlen(streams.CONTENT_ARTIFACTS) == 1
    assert await fake_redis.xlen(streams.CONTENT_BLOBS) == 0


@pytest.mark.parametrize(
    ("command_kwargs", "expected"),
    [
        pytest.param({"credentials_alias": "nobody"}, "alias_unknown", id="alias-unknown"),
        pytest.param({"provider": "ia"}, "provider_disabled", id="provider-disabled"),
        pytest.param({"destination": "../escape"}, "invalid_destination", id="invalid-destination"),
        pytest.param({"blob_uri": "file:///etc/passwd"}, "invalid_source", id="invalid-source"),
    ],
)
async def test_the_loop_closes_a_replicate_command_with_a_real_fact(
    fake_redis, consumer, settings, tmp_path, command_kwargs, expected
):
    """End to end: a frame on the command stream, a fact on ``content.artifacts``.

    The whole point of shipping the refusing half — an issuer gets a real,
    distinguishable reason for every command it sends, before any provider writer
    exists. Each row is one refusal an issuer will actually see, arriving through
    the loop rather than from a directly-called handler.
    """
    aliases = AliasTable(
        {"primary": AliasBinding(alias="primary", provider="gcs", bucket="b", prefix="reps")}
    )
    handler = build_replicate_handler(store=LocalBlobStore(tmp_path), aliases=aliases)
    reporter = build_replicate_reporter(client=fake_redis, artifacts_topic=ARTIFACTS)

    await fake_redis.xadd(TOPIC, make_replicate_command(command_id="rep-e2e", **command_kwargs))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, handler, reporter=reporter, spec=REPLICATE_SPEC
    )

    assert outcome is Outcome.DEAD_LETTERED
    (fact,) = await facts_on(fake_redis, ARTIFACTS)
    assert fact.command_id == "rep-e2e"
    assert fact.reason == expected
    assert fact.terminal is True
    assert fact.info_item_rep_spec_id == "iirs-1"
