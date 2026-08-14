"""The replicate handler: which refusal, and in what order (#29).

Every refusal here is terminal and **pre-credential** — the guarantee T1 offers
the issuer, and it holds only because the guards run in a particular order. So
the order is asserted, not just the outcomes: a reordering that put the
destination check after a provider client was constructed would still pass every
per-reason test and would still have broken the contract.

``destination_conflict`` is absent on purpose. It is the one documented refusal
that cannot be pre-credential (learning a destination holds *differing* bytes
takes an authenticated read), and it arrives with the first provider writer.
"""

import pytest

from src.core.errors import PermanentReplicateError, ReplicateReason
from src.storage.local import LocalBlobStore
from src.worker.aliases import AliasBinding, AliasTable
from src.worker.replicate import build_replicate_handler
from tests.worker.test_loop_spec import make_replicate_command_model

FINGERPRINT = "b" * 64
BINDING = AliasBinding(alias="primary", provider="gcs", bucket="co-artifacts", prefix="reps")


@pytest.fixture
def store(tmp_path):
    return LocalBlobStore(tmp_path)


@pytest.fixture
def blob_uri(store):
    return store.store(b"artifact bytes", FINGERPRINT, "application/pdf")


@pytest.fixture
def aliases():
    return AliasTable({"primary": BINDING})


def command(blob_uri, **overrides):
    return make_replicate_command_model(blob_uri=blob_uri, **overrides)


async def refusal(handler, cmd) -> PermanentReplicateError:
    with pytest.raises(PermanentReplicateError) as caught:
        await handler(cmd)
    return caught.value


async def test_an_unprovisioned_alias_is_refused(store, aliases, blob_uri):
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(handler, command(blob_uri, credentials_alias="nobody-stood-this-up"))

    assert exc.reason is ReplicateReason.ALIAS_UNKNOWN


async def test_a_host_with_nothing_provisioned_refuses_everything(store, blob_uri):
    """T5's gate, which is also the default posture of every host today.

    Enabling replication is an explicit operator act on the VM, not a consequence
    of a message arriving — the property that matters most for ``ia``, whose
    items cannot be deleted.
    """
    handler = build_replicate_handler(store=store, aliases=AliasTable({}))

    exc = await refusal(handler, command(blob_uri))

    assert exc.reason is ReplicateReason.ALIAS_UNKNOWN


async def test_an_alias_bound_to_another_provider_is_refused(store, aliases, blob_uri):
    """The message picks the provider and the host picks what the alias means; a
    disagreement is the host's answer, not the message's."""
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(handler, command(blob_uri, provider="ia"))

    assert exc.reason is ReplicateReason.PROVIDER_DISABLED


async def test_an_unknown_provider_is_refused_rather_than_dead_lettered(store, aliases, blob_uri):
    """Why co-core types ``provider`` as ``str`` and not a ``Literal``.

    A ``Literal`` would fail ``from_wire``, dead-letter the frame, and destroy the
    ``command_id`` — so the command could never be closed and its issuer would
    wait forever. It decodes precisely so it can be refused *with* a fact.
    """
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(handler, command(blob_uri, provider="dropbox"))

    assert exc.reason is ReplicateReason.PROVIDER_DISABLED


async def test_a_destination_escaping_the_alias_root_is_refused(store, aliases, blob_uri):
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(handler, command(blob_uri, destination="../../etc/passwd"))

    assert exc.reason is ReplicateReason.INVALID_DESTINATION


async def test_a_blob_uri_this_store_did_not_mint_is_refused(store, aliases):
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(handler, command("file:///etc/replicator/co-pypi-reader.json"))

    assert exc.reason is ReplicateReason.INVALID_SOURCE


async def test_a_blob_that_has_been_swept_is_expired(store, aliases):
    """The distinguishable terminal reason #29 asks for.

    MUST-7 inverts for replicate: the scheduling obligation is the issuer's, so
    the remedy is a fresh fetch under a new ``command_id``, not anything this
    service can do. Distinct from ``invalid_source`` because that one is not
    fixed by fetching again.
    """
    handler = build_replicate_handler(store=store, aliases=aliases)
    never_stored = store.uri_for(FINGERPRINT)

    exc = await refusal(handler, command(never_stored))

    assert exc.reason is ReplicateReason.BLOB_EXPIRED


async def test_a_fully_valid_command_is_refused_because_no_writer_is_enabled(
    store, aliases, blob_uri
):
    """The honest end of the capability's first half.

    Not a stub raising ``NotImplementedError``: ``provider_disabled`` is *true* —
    no provider writer is enabled on any host — and it is the reason whose remedy
    ("act on the host") is the one that will actually resolve it.
    """
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(handler, command(blob_uri))

    assert exc.reason is ReplicateReason.PROVIDER_DISABLED


async def test_every_refusal_happens_before_any_credential_is_touched(store, aliases, blob_uri):
    """T1's second guarantee, asserted structurally.

    The handler is built with no credential source of any kind, so if any path
    needed one it could not run at all. Kept as a test rather than a comment
    because the first provider writer is exactly the change that would quietly
    move a client construction above these guards.
    """
    handler = build_replicate_handler(store=store, aliases=aliases)

    for cmd in (
        command(blob_uri, credentials_alias="unknown"),
        command(blob_uri, provider="ia"),
        command(blob_uri, destination="../escape"),
        command("file:///etc/passwd"),
        command(blob_uri),
    ):
        assert isinstance(await refusal(handler, cmd), PermanentReplicateError)


async def test_the_alias_is_checked_before_the_destination(store, aliases, blob_uri):
    """Order matters, and this pair is where it is observable.

    A command with *both* an unknown alias and a bad destination must report
    ``alias_unknown``: the destination can only be judged against a root, and
    without a resolved binding there is no root to judge it against. Reporting
    ``invalid_destination`` would send the issuer to fix a spec whose real
    problem is on the host.
    """
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(
        handler, command(blob_uri, credentials_alias="unknown", destination="../escape")
    )

    assert exc.reason is ReplicateReason.ALIAS_UNKNOWN


async def test_the_destination_is_checked_before_the_source(store, aliases):
    """Both bad: the destination wins.

    Arbitrary in isolation, so it is pinned rather than left to drift. The
    reasoning is that a bad destination is a spec problem and a bad ``blob_uri``
    is a plumbing problem, and the spec is the one the issuer can fix without
    reading this repo.
    """
    handler = build_replicate_handler(store=store, aliases=aliases)

    exc = await refusal(handler, command("file:///etc/passwd", destination="../escape"))

    assert exc.reason is ReplicateReason.INVALID_DESTINATION


async def test_the_handler_reads_no_domain_field_to_decide_anything(store, aliases, blob_uri):
    """The charter's freight test, at the one place it could be violated.

    ``info_item_rep_spec_id``, ``source_revision_id`` and ``info_source_id`` are
    carried for the issuer's benefit. Changing all three must not change a single
    outcome — if it ever does, Replicator has learned what one of them means.
    """
    handler = build_replicate_handler(store=store, aliases=aliases)

    first = await refusal(handler, command(blob_uri))
    second = await refusal(
        handler,
        command(
            blob_uri,
            info_item_rep_spec_id="completely-different",
            source_revision_id="also-different",
            info_source_id="different-again",
        ),
    )

    assert first.reason is second.reason
    assert str(first) == str(second)
