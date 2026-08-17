"""The failure vocabulary: retry-or-not, and — since #9 — *why*.

``TransientFetchError`` / ``PermanentFetchError`` have always told the loop what
to do with a message. The ``reason`` they now carry is what the loop puts on the
``fetch_failed`` fact, so it is the handler — the only thing that knows which of
three permanent conditions fired — that has to name it.

Since #29 there are two layers: the loop catches ``PermanentError`` /
``TransientError`` so a second command stream needs nothing from it, and the
``*FetchError`` leaves narrow the vocabulary back to ``FailureReason``. Both
layers are tested — the base's contract is what a replicate handler will rely
on, and it has no other coverage (CR #12).

Since #17 there are **three** fates, not two, and the third is a sibling rather
than a leaf: ``CompletedWithoutBlobError`` is a command that finished with no
bytes to announce. Its position in the hierarchy is the whole of its behaviour —
descend it from ``PermanentError`` and the loop's existing arm swallows it, and a
successful conditional GET dead-letters again.
"""

import pytest

from src.core.errors import (
    CompletedWithoutBlobError,
    FailureReason,
    HandlerError,
    PermanentError,
    PermanentFetchError,
    TransientError,
    TransientFetchError,
)


def test_a_permanent_failure_carries_the_reason_the_fact_reports():
    exc = PermanentFetchError("nope", reason=FailureReason.NOT_FETCHABLE)

    assert exc.reason is FailureReason.NOT_FETCHABLE
    assert exc.status_code is None
    assert str(exc) == "nope"


def test_a_permanent_failure_carries_the_status_code_when_there_was_one():
    exc = PermanentFetchError("404", reason=FailureReason.HTTP_STATUS, status_code=404)

    assert exc.status_code == 404


def test_the_reason_is_required():
    """A raise site that does not classify itself would emit an unlabelled fact."""
    with pytest.raises(TypeError):
        PermanentFetchError("unclassified")  # type: ignore[call-arg]


def test_the_reason_is_required_on_the_base_too():
    """CR #12: the leaf narrows the type, so it cannot speak for the base.

    ``PermanentFetchError`` declares its own ``__init__`` purely to require a
    ``FailureReason``, which means the test above exercises *that* signature and
    says nothing about ``PermanentError``'s. A replicate handler raises the base,
    so a change making ``reason`` optional there would reach the wire as an
    unlabelled fact with every existing test still green.
    """
    with pytest.raises(TypeError):
        PermanentError("unclassified")  # type: ignore[call-arg]


def test_the_base_accepts_a_token_that_is_not_one_of_fetchs():
    """The vocabulary is producer-owned per stream (CR #5).

    The point of typing the base's ``reason`` as ``str`` is that replicate's
    refusals — settled in ``docs/contracts/content-replicate-issuer-contract.md``
    — are deliberately not ``FailureReason`` members. If this ever has to import
    an enum to pass, the split has been undone.
    """
    exc = PermanentError("the alias is not provisioned here", reason="alias_unknown")

    assert exc.reason == "alias_unknown"
    assert exc.status_code is None


def test_the_loop_catches_the_bases_not_the_leaves():
    """What makes a second command stream possible without touching the loop.

    ``src.worker.loop`` catches ``PermanentError`` / ``TransientError``; if the
    leaves stopped descending from them, fetch failures would fall through to the
    unclassified path and burn the delivery ceiling instead of dead-lettering.
    """
    assert issubclass(PermanentFetchError, PermanentError)
    assert issubclass(TransientFetchError, TransientError)
    assert issubclass(PermanentError, HandlerError)
    assert issubclass(TransientError, HandlerError)


def test_the_reason_tokens_are_the_wire_tokens():
    """co-core takes ``reason`` as a plain str; these are the values it documents.

    Pinned literally rather than derived: the tokens are a wire contract with
    every ``content.blobs`` consumer, so renaming a member must break a test
    here rather than silently change what Watcher branches on.
    """
    assert {reason.value for reason in FailureReason} == {
        "http_status",
        "invalid_request_options",
        "not_fetchable",
        "not_modified",
        "too_large",
        "unsupported_schema_version",
        "handler_error",
    }


def test_wrong_payload_type_is_not_a_token_this_repo_emits():
    """CR #1: co-core documents it; Replicator deliberately cannot use it.

    A ``command_id`` found inside a foreign payload names somebody else's
    command — usually one that *succeeded*. There is no correct fact to publish
    for that frame, so the token has no raise site here and must not sit in the
    enum looking emittable.
    """
    assert "wrong_payload_type" not in {reason.value for reason in FailureReason}


def test_a_reason_serializes_as_its_token():
    """StrEnum, so ``to_wire``'s JSON dump carries the token, not ``FailureReason.X``."""
    assert f"{FailureReason.TOO_LARGE}" == "too_large"


def test_both_failure_types_still_share_a_base():
    assert issubclass(TransientFetchError, HandlerError)
    assert issubclass(PermanentFetchError, HandlerError)


def test_a_completed_command_with_no_blob_is_a_sibling_of_the_other_two():
    """#17: the third fate is structural, and the hierarchy *is* the mechanism.

    ``process_message`` catches ``PermanentError`` and dead-letters it. A
    ``CompletedWithoutBlobError`` that descended from it would be swallowed by
    that arm, and a successful conditional GET would go back to closing as a
    failure with a DLQ entry — the exact bug #17 exists to fix, reintroduced by a
    one-word change to a base class with every other test still green.
    """
    assert issubclass(CompletedWithoutBlobError, HandlerError)
    assert not issubclass(CompletedWithoutBlobError, PermanentError)
    assert not issubclass(CompletedWithoutBlobError, TransientError)


def test_a_completed_command_with_no_blob_carries_its_reason_and_status():
    """The token and the status still ride the fact; only the fate differs."""
    exc = CompletedWithoutBlobError(
        "not modified", reason=FailureReason.NOT_MODIFIED, status_code=304
    )

    assert exc.reason is FailureReason.NOT_MODIFIED
    assert exc.status_code == 304
    assert str(exc) == "not modified"


def test_the_reason_is_required_on_the_third_fate_too():
    """Same argument as ``PermanentError``'s: a default relabels on the wire."""
    with pytest.raises(TypeError):
        CompletedWithoutBlobError("unclassified")  # type: ignore[call-arg]
