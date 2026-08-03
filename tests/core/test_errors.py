"""The failure vocabulary: retry-or-not, and — since #9 — *why*.

``TransientFetchError`` / ``PermanentFetchError`` have always told the loop what
to do with a message. The ``reason`` they now carry is what the loop puts on the
``fetch_failed`` fact, so it is the handler — the only thing that knows which of
three permanent conditions fired — that has to name it.
"""

import pytest

from src.core.errors import (
    FailureReason,
    HandlerError,
    PermanentFetchError,
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


def test_the_reason_tokens_are_the_wire_tokens():
    """co-core takes ``reason`` as a plain str; these are the values it documents.

    Pinned literally rather than derived: the tokens are a wire contract with
    every ``content.blobs`` consumer, so renaming a member must break a test
    here rather than silently change what Watcher branches on.
    """
    assert {reason.value for reason in FailureReason} == {
        "http_status",
        "not_fetchable",
        "too_large",
        "unsupported_schema_version",
        "wrong_payload_type",
        "handler_error",
    }


def test_a_reason_serializes_as_its_token():
    """StrEnum, so ``to_wire``'s JSON dump carries the token, not ``FailureReason.X``."""
    assert f"{FailureReason.TOO_LARGE}" == "too_large"


def test_both_failure_types_still_share_a_base():
    assert issubclass(TransientFetchError, HandlerError)
    assert issubclass(PermanentFetchError, HandlerError)
