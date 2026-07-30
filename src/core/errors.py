"""Failure vocabulary for the consume path.

The loop — not the handler — decides retry vs dead-letter, but the handler is
the only thing that knows *why* it failed. These two types let it say so
directly, so classification does not rest on the loop recognizing every
exception type the byte path might raise (see the transient tuple in
``src.worker.loop``, which is the fallback for what it did not anticipate).
"""


class HandlerError(RuntimeError):
    """Base for failures a ``content.fetch`` handler reports deliberately."""


class TransientFetchError(HandlerError):
    """The work may succeed later: leave the message unacked for redelivery.

    Exempt from the delivery ceiling — a long-but-genuine outage must never
    silently drop a valid command.
    """


class PermanentFetchError(HandlerError):
    """The work will never succeed for this command: dead-letter it now.

    Retrying a deterministically bad command only burns the ceiling and delays
    the operator seeing it in ``<topic>.dlq``.
    """
