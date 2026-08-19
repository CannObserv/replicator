"""Object-store blob backend — the claim-check half of #7.

The bytes leave the shared filesystem and the bus keeps carrying the reference,
so ``blob_available.blob_uri`` becomes ``gs://<bucket>/<key>`` and a consumer no
longer has to live on this host to read it. Same ``BlobStore`` shape as
``LocalBlobStore``; the loop is untouched.

**Every method here blocks.** ``google-cloud-storage`` ships no async client, and
this module deliberately does not grow one: the seam is a synchronous
``Protocol`` and the callers wrap it in ``asyncio.to_thread``, which keeps the
choice at the two places that know whether they are on the event loop.
``co_core_aio.gcs.AsyncGcsDriver`` reached the same arrangement from the other
side, for the same reason.

**Three things the filesystem backend has to work for come free here, and one
gets harder.** Free: no shard directories (a bucket namespace is flat), no
temp-and-rename (a failed conditional create leaves no object rather than a
truncated one), no traversal modes (reachability is an IAM grant). Harder:
retention. There is no ``mtime`` to sweep, so the window is a **bucket lifecycle
rule** and the "since last referenced" clock is ``customTime``, refreshed here on
the re-reference path — see ``_touch``. That moves reaping out of this process
entirely, which is why ``src.worker.retention`` does not run against this
backend, and it costs precision: lifecycle granularity is one day and enforcement
is asynchronous, so ``blob_expires_at`` becomes a floor rather than an exact
horizon (docs/STORAGE.md).
"""

import tempfile
from datetime import UTC, datetime
from typing import IO

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

from src.core.logging import get_logger

logger = get_logger(__name__)

# How long a single object operation may block the thread it runs on. Sized well
# under the fetch timeout ceiling rather than at it: this is the *storage* half
# of a command whose network half has already happened, and a store that hangs
# holds a worker slot the same way a slow origin does.
DEFAULT_TIMEOUT_SECONDS = 120.0


class GcsBlobStore:
    """Content-addressed blob storage in a GCS bucket."""

    # Where ``open_stream`` stops holding a blob in memory and spills to disk.
    # The point of the stream seam is that Replicator's only use for these bytes
    # is to pass them to a driver that streams them; a ``BytesIO`` would satisfy
    # every caller and quietly give that back. 8 MiB keeps the ordinary page in
    # memory and puts the artifact-sized outlier on disk, where the local backend
    # had it all along.
    SPOOL_MAX_BYTES = 8 * 1024 * 1024

    # The key ``preflight`` probes. Under the same prefix as the blobs, so the
    # check exercises the path the blobs actually take, and shaped so nothing
    # fetched can ever address it: a blob key is 64 lowercase hex characters and
    # this is not. It is never written, which is the stronger guarantee — but the
    # name is chosen so that a future change which *did* write it could still not
    # collide with content.
    PREFLIGHT_KEY = "preflight-probe"

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "blobs",
        client: storage.Client | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        # Bucket positional, client by keyword: the same call shape as
        # ``AsyncGcsDriver``, which is what lets ``tests/conftest.py`` guard both
        # constructors with one wrapper rather than two that can disagree.
        #
        # The client is built here rather than lazily so credential resolution
        # fails at boot. A store constructed successfully and unusable on first
        # command would surface as a fetch failure, which is the one class of
        # failure this service reports as though the *origin* were at fault.
        self._client = client if client is not None else storage.Client()
        self._bucket_name = bucket
        self._bucket = self._client.bucket(bucket)
        self._prefix = prefix.strip("/")
        self._timeout = timeout_seconds

    def preflight(self) -> None:
        """Prove at boot that this bucket is there and readable by this identity.

        A read of a key nothing ever writes. That answers the three questions a
        misconfiguration gets wrong — do the credentials resolve, does the bucket
        resolve, may this identity read it — while writing nothing, leaving
        nothing to clean up, and needing no permission the store does not already
        need to do its job.

        **It deliberately does not prove write access**, and a probe object would
        be the wrong way to buy that: creating one needs no permission the first
        real ``store`` will not immediately need, and *deleting* it would need
        ``storage.objects.delete`` — which the worker otherwise never uses,
        because expiry is the lifecycle rule's job. Widening a service account's
        grant so a startup check can tidy up after itself is a poor trade for a
        failure the first fetch reports anyway.

        Nor does it prove the *consumer* can read what we write. That grant is on
        another service account and is verified where it is made
        (docs/DEPLOYMENT.md); it is the honest limit of this check, and the price
        of trading a filesystem coupling for an IAM one.
        """
        self._bucket.blob(self._key(self.PREFLIGHT_KEY)).exists()

    def store(self, data: bytes, fingerprint: str, media_type: str) -> str:
        """Create the object these bytes address, or confirm the one already there."""
        blob = self._blob(fingerprint)
        # Content-addressed, so an object at this key is already the right bytes.
        # Asked before writing for the reason the local backend asks: the caller
        # publishes a fresh fact either way, and the clock has to move for it.
        if blob.exists():
            self._touch(blob)
            return self.uri_for(fingerprint)
        blob.custom_time = datetime.now(UTC)
        try:
            blob.upload_from_string(
                data,
                content_type=media_type,
                # A create, never a put. Two workers can reach this line for the
                # same new fingerprint, and the loser must not overwrite an
                # object a reader may already be streaming.
                if_generation_match=0,
                timeout=self._timeout,
            )
        except PreconditionFailed:
            # Lost the race above. The winner wrote the same bytes — that is what
            # content-addressing means — so this is a success with a different
            # author, not a conflict. Raising would dead-letter a command whose
            # blob is sitting at the key it names.
            logger.debug(
                "another worker created this blob first",
                extra={"content_fingerprint": fingerprint},
            )
            self._touch(self._blob(fingerprint))
        return self.uri_for(fingerprint)

    def exists(self, fingerprint: str) -> bool:
        """Whether these bytes are already stored."""
        return bool(self._blob(fingerprint).exists())

    def uri_for(self, fingerprint: str) -> str:
        """The ``gs://`` URI ``store`` would return for these bytes.

        The same derivation as ``store``'s return value, exposed without the
        write so the replicate guard can compare a message's ``blob_uri`` against
        it rather than parse the message's value into a key (#29, T3a). That
        matters more here than it did on the filesystem, not less: the guard's
        job is to keep a string an issuer chose from naming an object, and the
        objects reachable from this process now include permanent ones.
        """
        return f"gs://{self._bucket_name}/{self.key_for(fingerprint)}"

    def _key(self, name: str) -> str:
        """Apply the prefix to a bare object name.

        Shared by ``key_for`` and ``preflight`` so the probe travels the same
        path a blob does — a check against a differently-rooted key would pass on
        a bucket whose prefix nothing can write to.
        """
        return f"{self._prefix}/{name}" if self._prefix else name

    def key_for(self, fingerprint: str) -> str:
        """The object key for these bytes — flat, unlike the local backend's shards.

        ``LocalBlobStore`` splits two levels deep because a flat ext4 directory
        degrades past roughly ten thousand entries. A bucket namespace has no
        directories to degrade, so the shards would be cargo — and they are
        exactly the kind of cargo that costs later, because every extra rule in
        the derivation is another way ``uri_for`` and a reader's expectation can
        come apart.
        """
        return self._key(f"{fingerprint}.bin")

    def open(self, fingerprint: str) -> bytes:
        """Read back the bytes stored under ``fingerprint``."""
        return self._blob(fingerprint).download_as_bytes(timeout=self._timeout)

    def open_stream(self, fingerprint: str) -> IO[bytes]:
        """A **seekable binary** handle on these bytes; the caller closes it.

        GCS has no seekable remote handle, so the bytes have to land somewhere
        local first. ``SpooledTemporaryFile`` is that somewhere: in memory for
        the ordinary page, on disk past ``SPOOL_MAX_BYTES``, and seekable either
        way — which is the hard requirement, because the replicate driver
        computes its local md5 only on the 412 path, after the failed create has
        already moved the position.
        """
        handle = tempfile.SpooledTemporaryFile(max_size=self.SPOOL_MAX_BYTES)
        try:
            self._blob(fingerprint).download_to_file(handle, timeout=self._timeout)
        except BaseException:
            handle.close()
            raise
        handle.seek(0)
        return handle

    def _blob(self, fingerprint: str) -> storage.Blob:
        """A fresh handle on this fingerprint's object.

        Fresh rather than cached, deliberately. A ``Blob`` carries the generation
        and metadata it was last loaded with, and this store is shared by both
        command loops and — until the lifecycle rule replaces it — anything else
        that reads. A cached handle turns that shared state into reads against a
        generation that no longer exists, which only ever fails under
        concurrency.
        """
        return self._bucket.blob(self.key_for(fingerprint))

    def _touch(self, blob: storage.Blob) -> None:
        """Restart the retention clock on a blob about to be announced again.

        The lifecycle rule is written against ``daysSinceCustomTime`` precisely
        so this is expressible: a creation-age condition would reap a blob that
        was re-referenced moments ago, because re-fetching unchanged bytes never
        rewrites the object. ``customTime`` only ever moves forward, which is
        what this does.

        A vanished object is swallowed, exactly as the local backend swallows
        ``FileNotFoundError``: lifecycle can delete between the existence check
        and this call, and the fallout is a ``blob_uri`` whose reader re-issues.
        Raising would dead-letter a command whose bytes were fine when we looked.
        """
        blob.custom_time = datetime.now(UTC)
        try:
            blob.patch()
        except NotFound:
            logger.debug("blob vanished before its retention clock could be restarted")
