"""The CA bundle every TLS fetch trusts - certifi's, not the system store.

Nothing in `src/` or the unit sets `verify=`, `truststore` or `SSL_CERT_FILE`, so
httpx verifies public URLs against the venv's certifi. An OS-level CA removal
never reaches it; only a lock bump does (#149).

PYSEC-2026-4320 / GHSA-6q48-6c7w-9gw2: eight revoked eMudhra intermediates with
compromised keys chain to `emSign ECC Root CA - G3`. certifi 2026.10.18 dropped
every emSign root; 2026.10.19 restored the three unaffected ones (G1, C1, C3), so
"no emSign at all" is the system store's posture, not certifi's.
"""

import certifi

_COMPROMISED_ROOT = "# Subject: CN=emSign ECC Root CA - G3 "


def test_certifi_bundle_omits_the_compromised_emsign_root():
    bundle = open(certifi.where(), encoding="ascii").read()
    assert bundle.count(_COMPROMISED_ROOT) == 0, (
        f"certifi {certifi.__version__} still trusts emSign ECC Root CA - G3; "
        "need certifi >= 2026.10.19 (#149)"
    )
