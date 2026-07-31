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

    def open(self, fingerprint: str) -> bytes:
        """Read back the bytes stored under ``fingerprint``."""
        ...
