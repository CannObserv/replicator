"""The urllib3 every GCS call rides - transitive, so only a lock bump moves it.

urllib3 arrives through `co-core-sync` and `requests` (google-api-core,
google-cloud-storage); in `src/` only `GcsBlobStore` reaches it. Fetches use
httpx, so a hostile origin's response never does. 2.8.0 fixes three advisories
the 2.7.0 lock carried (#150):

- PYSEC-2026-4175: HTTPS proxy TLS configuration may be ignored or overridden.
- PYSEC-2026-4176: chunked Deflate streaming can loop forever.
- PYSEC-2026-4177: `read_chunked()` buffers an unbounded chunk-size line.

`uv audit --frozen --no-dev` is the real check; this one fails in the suite.
"""

import urllib3

_FIXED_IN = (2, 8, 0)


def _release(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split(".")[:3])


def test_urllib3_carries_the_pysec_2026_4175_to_4177_fixes():
    assert _release(urllib3.__version__) >= _FIXED_IN, (
        f"urllib3 {urllib3.__version__} predates 2.8.0's fixes for "
        "PYSEC-2026-4175/4176/4177; need urllib3 >= 2.8.0 (#150)"
    )
