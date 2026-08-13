"""The temp-storage seam.

A ``Protocol`` rather than a base class: the loop depends on the shape, not on an
inheritance relationship, and a GCS or GDrive backend added when durable
replication lands satisfies it without importing anything from here.

``backend_uri`` is opaque to consumers — it travels as ``blob_available.blob_uri``
and nothing downstream parses it.
"""

from typing import Protocol


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
        """Read back the bytes stored under ``fingerprint``."""
        ...
