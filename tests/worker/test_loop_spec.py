"""The ``CommandSpec`` seam: the loop over a stream that is not ``content.fetch``.

The generalization in #29 exists so a second command loop is another ``run_loop``
with another spec rather than a second copy of this module. A generic seam with
exactly one instantiation is untested generality, so these drive
``process_message`` over a real ``ContentReplicateCommand`` — co-core 0.9.4's
model, decoded by the same global ``from_wire`` table — against a spec defined
here.

The spec is local to the test on purpose. #29's provider work has not landed, so
there is no replicate report type in ``src/`` yet; what is being pinned is that
the *loop* needs nothing from a stream beyond what ``CommandSpec`` carries. When
the real one lands it replaces this fixture and these assertions still hold.

Note what the local report proves in passing: it names ``info_item_rep_spec_id``,
which the boundaries charter refuses in ``src/`` outside the emit-path allowlist
(#29 granted it there). The scan is over ``src/`` only, so a test may name it
freely — and the fact that this report can carry a field the fetch report has
never heard of is precisely the seam working.
"""

from dataclasses import dataclass

import pytest
from co_core.pure.adapters.bus import streams
from co_core.pure.adapters.bus.envelope import to_wire
from co_core.pure.models.changes import ContentReplicateCommand

from src.core.errors import PermanentError
from src.worker import loop
from src.worker.loop import FETCH_SPEC, CommandSpec, Outcome, poll_once
from tests.worker.conftest import (
    GROUP,
    TOPIC,
    make_command,
    noop_handler,
    process_one,
    unreachable_handler,
)


@dataclass(frozen=True, slots=True)
class ReplicateReport:
    """A second stream's report shape — no ``url``, no ``status_code``.

    The two fields the fetch report requires and this one cannot supply are the
    argument against a single dataclass with everything optional.
    """

    command_id: str
    info_item_rep_spec_id: str
    # ``str``, not ``FailureReason``: the replicate tokens are producer-owned and
    # deliberately absent from fetch's enum (CR #5, #10).
    reason: str
    attempts: int | None = None
    detail: str | None = None
    # No ``status_code`` field at all — ``ReplicationFailedEvent`` models none,
    # so a report carrying one would be inventing a value with nowhere to go.


REPLICATE_SPEC: CommandSpec[ContentReplicateCommand, ReplicateReport] = CommandSpec(
    command_type=ContentReplicateCommand,
    label=streams.CONTENT_REPLICATE,
    dedupe_segment="replicate",
    # ``status_code`` is bound and discarded, which is what a per-stream builder
    # is *for*: the loop passes every cause it has, and each stream decides what
    # its own fact can say. A shared report dataclass would have had to carry the
    # field for fetch's sake and leave it permanently ``None`` here.
    build_report=lambda command, *, status_code=None, **cause: ReplicateReport(
        command_id=command.command_id,
        info_item_rep_spec_id=command.info_item_rep_spec_id,
        **cause,
    ),
    describe=lambda command: {"destination": command.destination},
)


def make_replicate_command(command_id: str = "rep-1") -> dict[str, str]:
    """A well-formed ``content.replicate`` wire frame, through co-core's own encoder."""
    return to_wire(
        ContentReplicateCommand(
            occurred_at="2026-08-13T00:00:00.000000Z",
            command_id=command_id,
            blob_uri="file:///var/lib/replicator/blobs/ab/cd/abcd.bin",
            media_type="application/pdf",
            provider="gcs",
            credentials_alias="primary",
            destination="reports/2026/abcd.pdf",
            info_item_rep_spec_id="iirs-1",
            source_revision_id="rev-1",
            info_source_id="src-1",
        )
    )


async def test_a_replicate_command_is_dispatched_and_acked(fake_redis, consumer, settings):
    """The loop runs a stream whose payload type it was never written for."""
    await fake_redis.xadd(TOPIC, make_replicate_command())
    seen: list[str] = []

    async def handler(command: ContentReplicateCommand) -> None:
        seen.append(command.destination)

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, handler, spec=REPLICATE_SPEC
    )

    assert outcome is Outcome.ACKED
    assert seen == ["reports/2026/abcd.pdf"]
    assert (await fake_redis.xpending(TOPIC, GROUP))["pending"] == 0


async def test_the_two_streams_do_not_share_a_dedupe_namespace(fake_redis, consumer, settings):
    """The safety fix the second stream made necessary (#29).

    Issuer-assigned ids make a cross-stream collision unlikely rather than
    impossible, and its shape is the worst available: the second command acks
    having done nothing. Here both commands deliberately carry the same
    ``command_id``, so a shared namespace would dedupe the replicate one against
    the fetch one and this test would see no handler call at all.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="same-id"))
    await fake_redis.xadd(TOPIC, make_replicate_command(command_id="same-id"))

    first = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    assert await process_one(fake_redis, consumer, settings, first, noop_handler) is Outcome.ACKED

    replicated: list[str] = []

    async def handler(command: ContentReplicateCommand) -> None:
        replicated.append(command.command_id)

    second = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, second, handler, spec=REPLICATE_SPEC
    )

    assert outcome is Outcome.ACKED
    assert replicated == ["same-id"]
    assert await fake_redis.exists(FETCH_SPEC.dedupe_key("same-id"))
    assert await fake_redis.exists(REPLICATE_SPEC.dedupe_key("same-id"))
    assert FETCH_SPEC.dedupe_key("same-id") != REPLICATE_SPEC.dedupe_key("same-id")


async def test_a_replicate_command_still_dedupes_against_itself(fake_redis, consumer, settings):
    """Namespacing the key must not cost the property the key exists for."""
    await fake_redis.xadd(TOPIC, make_replicate_command(command_id="rep-dup"))
    await fake_redis.xadd(TOPIC, make_replicate_command(command_id="rep-dup"))

    first = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    assert (
        await process_one(fake_redis, consumer, settings, first, noop_handler, spec=REPLICATE_SPEC)
        is Outcome.ACKED
    )

    second = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    assert (
        await process_one(
            fake_redis, consumer, settings, second, unreachable_handler, spec=REPLICATE_SPEC
        )
        is Outcome.DEDUPED
    )


async def test_a_fetch_frame_is_foreign_to_the_replicate_spec(fake_redis, consumer, settings):
    """``from_wire``'s dispatch table is global, so each spec polices its own type.

    A ``content.fetch`` frame decodes perfectly well here — it is a valid command,
    just not *this* stream's — and the isinstance gate is what turns that into a
    dead-letter rather than an AttributeError halfway through a handler.
    """
    await fake_redis.xadd(TOPIC, make_command(command_id="cmd-wrong-stream"))

    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, unreachable_handler, spec=REPLICATE_SPEC
    )

    assert outcome is Outcome.DEAD_LETTERED


async def test_the_specs_report_builder_is_what_closes_the_command(fake_redis, consumer, settings):
    """The loop reports a failure on a stream whose fact shape it never learned.

    ``status_code`` is the pointed part: ``PermanentError`` carries one, the loop
    passes it through, and this stream's builder drops it on the floor — because
    ``ReplicationFailedEvent`` models no such field, so ``ReplicateReport`` has
    nowhere to put it. A shared report dataclass would have put it on the wire
    path with one of its two producers unable ever to fill it.
    """
    reports: list[ReplicateReport] = []

    async def collect(report: ReplicateReport) -> None:
        reports.append(report)

    async def handler(command: ContentReplicateCommand) -> None:
        raise PermanentError(
            "the alias is not provisioned here",
            # A replicate token, not one of fetch's — this is the vocabulary
            # split the ``str`` typing exists for, settled in the contract's
            # "What Replicator refuses" (CR #5, #10).
            reason="alias_unknown",
            status_code=412,
        )

    await fake_redis.xadd(TOPIC, make_replicate_command(command_id="rep-refused"))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]
    outcome = await process_one(
        fake_redis, consumer, settings, message, handler, reporter=collect, spec=REPLICATE_SPEC
    )

    assert outcome is Outcome.DEAD_LETTERED
    (report,) = reports
    assert report.command_id == "rep-refused"
    assert report.info_item_rep_spec_id == "iirs-1"
    assert report.detail == "the alias is not provisioned here"
    assert report.reason == "alias_unknown"
    assert not hasattr(report, "status_code")


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        pytest.param(FETCH_SPEC, streams.CONTENT_FETCH, id="fetch"),
        pytest.param(REPLICATE_SPEC, streams.CONTENT_REPLICATE, id="replicate"),
    ],
)
def test_each_spec_labels_its_own_stream(spec, expected):
    """The journal line the operator triages a jam from. One hardcoded name would
    have mislabelled half of them once there were two streams."""
    assert spec.label == expected


def test_every_spec_defined_here_has_its_own_dedupe_segment():
    """CR #8: a shared segment reintroduces exactly what the namespacing prevents.

    ``dedupe_segment`` is free text, and two specs sharing one would silently
    dedupe each other's commands — the failure the per-stream key exists to stop,
    reached from inside rather than from a colliding ``command_id``. Nothing in
    the type system says the segments are distinct, so it is asserted, over every
    spec ``src/worker/loop.py`` exposes plus the local one, so the check grows
    with the module instead of naming today's two.
    """
    specs = [value for value in vars(loop).values() if isinstance(value, CommandSpec)]
    specs.append(REPLICATE_SPEC)
    assert len(specs) >= 2, "the uniqueness check is vacuous with fewer than two specs"

    segments = [spec.dedupe_segment for spec in specs]
    assert len(segments) == len(set(segments)), segments


async def test_a_report_builder_that_raises_still_dead_letters_the_frame(
    fake_redis, consumer, settings, caplog
):
    """CR #2: a broken spec must not become an unrecoverable jam.

    The builder is called from inside ``_handle_unclassified`` too, which is
    where the delivery ceiling lives — so an unguarded raise there means the
    ceiling can never dead-letter the frame. Left alone it strands the message in
    the PEL, ``claim_stale`` returns it, and the loop re-raises forever: a
    programming error in one spec taking down the stream permanently, with no DLQ
    entry to triage from.

    Degrading to no-fact is the right trade. The frame still dead-letters, and
    contract MUST-6 already makes the issuer's reaper the backstop for a command
    that closes without one.
    """
    broken = CommandSpec(
        command_type=ContentReplicateCommand,
        label=streams.CONTENT_REPLICATE,
        dedupe_segment="replicate-broken",
        build_report=_raising_builder,
        describe=lambda command: {"destination": command.destination},
    )
    await_reports: list[object] = []

    async def collect(report: object) -> None:
        await_reports.append(report)

    async def handler(command: ContentReplicateCommand) -> None:
        raise PermanentError("refused", reason="alias_unknown")

    await fake_redis.xadd(TOPIC, make_replicate_command(command_id="rep-broken-spec"))
    message = (await poll_once(fake_redis, consumer, settings, group=GROUP))[0]

    with caplog.at_level("ERROR", logger="src.worker.loop"):
        outcome = await process_one(
            fake_redis, consumer, settings, message, handler, reporter=collect, spec=broken
        )

    assert outcome is Outcome.DEAD_LETTERED
    assert await_reports == []  # no fact — there was no report to publish
    assert (await fake_redis.xpending(TOPIC, GROUP))["pending"] == 0  # not stranded
    assert any(r.message == "could not build a failure report" for r in caplog.records)


def _raising_builder(command, **cause):
    raise TypeError("this spec's builder is broken")
