"""The temp-storage seam.

A ``Protocol`` rather than a base class: the loop depends on the shape, not on an
inheritance relationship. **Two implementations satisfy it** (CR #7) —
``src.storage.local.LocalBlobStore``, which announces ``file://``, and
``src.storage.gcs.GcsBlobStore``, which announces ``gs://`` (#7) — and the second
was added without importing anything from here, which was the design claim and is
now a fact rather than a plan. ``REPLICATOR_BLOB_BACKEND`` chooses; nothing
downstream can tell which one it got.

``backend_uri`` is opaque to consumers — it travels as ``blob_available.blob_uri``
and nothing downstream parses it.

**A missing blob is a ``FileNotFoundError``, whatever the backend** (CR #3). That
is the one piece of vocabulary this protocol imposes beyond the signatures: the
filesystem raises it natively and the object store translates the SDK's
``NotFound`` into it, so the consume path keeps a single catch for "these bytes
are gone" instead of one per backend. Every other failure propagates as the
backend raised it, carrying whatever status it has, for the caller to classify
(``src.core.errors.is_terminal_provider_status``).
"""

from typing import IO, Protocol


class BlobStore(Protocol):
    """Content-addressed storage for fetched bytes."""

    def store(self, data: bytes, fingerprint: str, media_type: str) -> str:
        """Store ``data`` under ``fingerprint`` and return its backend URI.

        Content-addressed, so storing bytes that are already stored is a no-op
        that still returns the URI. ``media_type`` is carried for backends that
        record it alongside the object (an object store's content-type metadata);
        a filesystem backend has nowhere to put it and ignores it.
        """
        ...

    def exists(self, fingerprint: str) -> bool:
        """Whether these bytes are already stored."""
        ...

    def uri_for(self, fingerprint: str) -> str:
        """The URI this backend *would* announce for ``fingerprint``.

        Exposed for the replicate path (#29, contract T3a). A ``content.replicate``
        command carries a ``blob_uri`` the issuer got from a ``blob_available``
        fact, and the consumer has to decide whether that string names a blob
        **this** store minted. Comparing against a freshly derived URI answers it
        without ever treating the message's value as a path — which is what keeps
        a read-side traversal off a service that writes to permanent public
        stores. ``store`` already returns this value; this is the same derivation
        without the write.
        """
        ...

    def open(self, fingerprint: str) -> bytes:
        """Read back the bytes stored under ``fingerprint``.

        Raises ``FileNotFoundError`` when they are gone — see the module
        docstring on why that is the protocol's word rather than each backend's.
        """
        ...

    def open_stream(self, fingerprint: str) -> IO[bytes]:
        """A **seekable binary** handle on these bytes; the caller closes it.

        The shape ``GcsCreateIfAbsent.data`` wants (#29). A path would make the
        provider copy something already on disk, and ``bytes`` would pull a whole
        artifact into memory to hand to a driver that streams it anyway — for a
        service whose only reason to hold the bytes is to pass them on.

        Seekable is a hard requirement rather than a preference: the driver
        computes the local md5 **only** on the 412 path, after the failed
        conditional create has already moved the position, so a non-seekable
        stream fails on exactly the redelivery T4 exists to handle.
        """
        ...
