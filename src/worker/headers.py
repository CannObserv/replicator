"""The request-header rules every header Replicator sends on a command's behalf obeys.

Shared by the handler's ``headers`` guards (#11) and the fetch credential table
(#216), whose bindings name a header and whose values are header values: one rule
for both, so a binding can't provision a header the handler would refuse to send.
Moved out of ``handler.py`` unchanged, because the handler imports the table.
"""

import re

# How many request headers a command may carry, and how many bytes they may add
# up to on the wire (#11).
#
# Deliberately not MAX_HEADER_VALUE_LENGTH, which reasons about *response*
# passthroughs riding a broadcast stream nothing trims — a different argument
# that happens to land on a similar number. This bound is about what an origin
# will accept: 8 KiB is the common server-side limit (nginx's
# large_client_header_buffers, Apache's LimitRequestFieldSize), so exceeding it
# earns an opaque 400 from the far end. Refusing locally turns that into a named
# reason on a fact the issuer already consumes.
#
# Constants rather than settings: these bound an unauthenticated capability, and
# a per-deployment knob is an invitation to loosen the one thing standing between
# a bus writer and an arbitrary request.
MAX_REQUEST_HEADERS = 32
MAX_REQUEST_HEADER_BYTES = 8192

# Request headers Replicator will not send on an issuer's behalf.
#
# Two groups, refused for different reasons and listed together because the
# refusal is the same. The hop-by-hop set (RFC 9110 §7.6.1) describes a single
# connection, not the request — httpx and h11 own it, and an issuer's value is
# either ignored or corrupts the framing. `host` and `content-length` are not
# hop-by-hop: httpx derives both, and overriding `host` in particular points the
# request at one origin while addressing another (domain fronting) — precisely
# the widening the contract's trust model says the broker, not this list, is
# holding back.
#
# **Refused, not stripped.** Silently dropping one changes what the origin saw
# with nothing the issuer can observe, which is the same failure the
# reject-not-truncate rule exists to prevent, applied to the request side. It is
# also the conservative direction: relaxing a refusal to a strip later stays
# compatible, tightening a strip into a refusal does not.
REFUSED_HEADERS = frozenset(
    {
        "connection",
        "content-length",
        "host",
        "keep-alive",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)

# Every Proxy-* header, refused as a family: they configure the hop rather than
# the request, and enumerating them would date the list.
REFUSED_HEADER_PREFIX = "proxy-"

# A field name is an RFC 9110 token. Matched against the *folded* name, so the
# alphabetic range is lowercase only.
#
# **`\Z`, never `$`.** Python's `$` also matches immediately before a trailing
# newline, so `^token+$` accepts `"accept\n"` — and httpx passes that straight
# through to the wire as a header name containing a bare LF, which is header
# injection by any other name. `\Z` is the absolute end of the string and the
# only correct anchor for a validator (CR #14). The same trap applies to the
# value pattern below, where `.strip()` happens to hide it; relying on that
# would make one guard's correctness depend on another's ordering.
HEADER_NAME = re.compile(r"\A[!#$%&'*+\-.^_`|~0-9a-z]+\Z")

# A field value here is printable US-ASCII and SP — narrower than RFC 9110's
# VCHAR / obs-text on both edges, and deliberately so on each.
#
# **obs-text (\x80-\xff) is excluded because httpx cannot send it.** It encodes
# header values as ASCII and raises UnicodeEncodeError, which — unlike the
# LocalProtocolError a CRLF earns — is *not* an httpx.HTTPError, so it would slip
# past _fetch's mapping entirely and land in the loop's unclassified branch:
# retried to the delivery ceiling and finally closed as `handler_error` minutes
# later, instead of the immediate `invalid_request_options` this guard exists to
# produce. RFC 9110 deprecates obs-text anyway (CR #1).
#
# **HTAB (\x09) is excluded even though httpx accepts it.** RFC 9110 permits it
# as internal whitespace, but a tab inside a header value is nobody's intent and
# survives no round trip worth having. Refused on purpose, not by oversight
# (CR #3).
#
# What the guard is really for is CR and LF: a CRLF in a value is request
# splitting, and everything else here is the cheap part of drawing that line.
#
# Anchored `\A`/`\Z` for the reason spelled out on `HEADER_NAME` above — `$`
# would admit a trailing newline. Unreachable here (the value is stripped before
# it is matched) and fixed anyway: one guard's correctness must not rest on
# another's ordering.
HEADER_VALUE = re.compile(r"\A[\x20-\x7e]*\Z")
