# The `content.capture` issuer contract

**Status: built, off by default, not yet enabled on any host.** `src/` runs a `content.capture` loop
when `REPLICATOR_CAPTURE_ENABLED` is set. The broker grant is live since 2026-10-20T15:06Z
(broker#114); the pinned browser has to be installed first
([runbook](../plans/2026-10-25-viewport-capture-runbook.md)). **Archiver issues only after this host
reports `capture: enabled`**: Replicator creates the group, then Archiver writes the stream (broker#116).
The loop renders a page in a confined headless browser, stores the PNG in the temporary store and
reports the outcome on `content.artifacts` (#138, cannobserv#571).

**Audience:** Archiver, the sole issuer ([archiver#380](https://github.com/CannObserv/archiver/issues/380)).

**Companions.** [`content-persist-issuer-contract.md`](content-persist-issuer-contract.md) keeps a
capture past its TTL. The wire models are co-core's `ContentCaptureCommand`, `CaptureStoredEvent` and
`CaptureFailedEvent` (co-core 0.39, cannobserv v0.37.1); their docstrings and cannobserv
`docs/change_bus/capture.md` state the shape. This document states the obligations, the limits and the
refusal vocabulary.

---

## What the command means

> **"Show me `url` as a browser at this viewport sees it, once the page has reached `wait`."**

| Field | Meaning |
|---|---|
| `command_id` | The correlator, and the dedupe key (`replicator:cmd:capture:<command_id>`). |
| `url` | `http(s)` only. Echoed on both facts. |
| `info_source_id` | Echoed, never read (#28). |
| `viewport` | `width` × `height` in CSS pixels, each `1..4096`. Device scale is 1. |
| `full_page` | `true` captures the whole document, up to 32768 × 4096 px; `false`, the viewport. |
| `wait` | `None`: after the `load` event (measured, #138). `networkidle`: the network has gone quiet. `selector`: the CSS `selector` is visible. `delay`: `seconds` after `load`. |

**Every wait is after `load`, and each is bounded** by `REPLICATOR_CAPTURE_MAX_WAIT_SECONDS`
(default 30): the first response, `load`, and the condition, each on its own clock. A `delay` over that
bound is refused up front. The whole render, launch to screenshot, sits inside
`REPLICATOR_MAX_FETCH_SECONDS` (120).

## What Replicator does

1. **Validates before launching anything:** the URL, the viewport, the wait.
2. **Paces the navigation** through the same per-host pacer as `content.fetch`
   ([POLITENESS.md](../POLITENESS.md)): one origin, one tolerance, however it is asked. Subresources
   are not paced. A 429 or 503 on the navigation escalates the host's interval as it does for a fetch.
3. **Renders in a confined browser:** a fresh profile per render, renderer sandbox on, a memory cap of
   its own, and **every TCP connection the page opens** checked against the same blocked destinations
   as a fetch (#89, #95): the navigation, each redirect hop, subresources, scripts, WebSockets,
   beacons. UDP (QUIC, WebRTC) is off. The user agent is Chromium's own `HeadlessChrome`.
4. **Stores, then publishes `capture_stored`.** The PNG lands in the temporary store under its sha256,
   with fetched blobs' TTL. A fact never names absent bytes.

**What `capture_stored` says:**

- `content_fingerprint` is the PNG's sha256: **an address, not a change signal.** A render varies with
  fonts, ads and clocks, so two captures of an unchanged page usually differ.
- `media_type` is `image/png`; `image_width` and `image_height` are read from the PNG, not the viewport.
- `final_url` is the main frame's URL when the screenshot was taken.
- `captured_at` is when the screenshot returned; `blob_expires_at` is the earliest the bytes may go.

**A capture is not a fetch occasion.** No `content.blobs` fact, no change fingerprint, no
conditional request.

## What the issuer must do

**C1 — Correlate on `command_id`; expect more than one success.** Delivery is at-least-once, and the
dedupe window is a TTL. A redelivery past it renders again, usually under a new digest. Every fact's
envelope key is `command_id:occurred_at`, so duplicates are distinguishable, never collapsed. Keep the
first, or the latest; either is a true capture.

**C2 — Branch on `terminal`, then treat an unknown `reason` as opaque.** Every fact this service emits
is terminal. A failure still retrying publishes nothing, so silence means "still trying". **Keep a
reaper** that re-issues under a fresh `command_id`, as the fetch contract's MUST-6 requires.

**C3 — Persist what you keep before `blob_expires_at`.** Send `content.persist` with the fact's
`content_fingerprint`, `blob_uri` and `media_type`, verbatim
([persist contract](content-persist-issuer-contract.md), P1, P4). The temp store mints the URI, so
persist's source guard accepts it as it accepts a fetched blob's.

**C4 — Send resolved values.** The command carries no headers and no credential, and Replicator adds
none. A page behind a login captures its login page.

**C5 — Don't issue before `capture: enabled`.** The group is created at `0` when the flag goes on, so
nothing earlier is skipped, but a backlog is rendered however old it is, one at a time, while broker's
undelivered-age probe fires.

## What Replicator refuses

Every refusal is terminal and carries a `reason` on `capture_failed`. The vocabulary is
producer-owned: co-core types `reason` as a plain `str`, and **this table is the registry**. The first
three rows are fixed by co-core's `CaptureFailedEvent` docstring; the rest are Replicator's, spelled
as fetch spells them wherever the remedy is fetch's.

| Condition | `reason` | Issuer's remedy |
|---|---|---|
| A viewport side outside `1..4096`; an unknown `wait.kind`; a `selector` wait with no selector, or one the browser does not parse as CSS; a `delay` that is not a finite number over zero, or is over the wait bound | `invalid_request_options` | Fix the command |
| `load`, or the wait condition, unmet within the wait bound | `wait_timeout` | Re-issue with another condition, under a fresh `command_id`. Terminal because an unmet condition is usually the page's, and each retry is a whole render |
| The URL, a redirect hop, or **any** connection the page opened, is to loopback, RFC 1918, link-local or the tailnet. The render is abandoned at the first one | `destination_refused` | None: the page cannot be captured here. A fetch of it is refused too |
| The URL is not `http(s)`, or names no host; or the browser refuses the navigation outright: a port it blocks (`:6000`, `:25`), a redirect to a scheme it won't open | `not_fetchable` | Fix the URL |
| The navigation's final response is a non-2xx other than those retried below; `status_code` says which | `http_status` | As for a fetch's `http_status` |
| An image taller than 32768 px or wider than 4096 px (a `full_page` grows with the page's overflow), or a PNG over `REPLICATOR_MAX_BLOB_BYTES` | `too_large` | A smaller viewport, or `full_page: false`. **Refused, never clipped:** a clipped image could not be told from a whole one |

The loop's own two apply as on every stream: **`unsupported_schema_version`**, and **`handler_error`**
after the delivery ceiling. `handler_error` is where a killed browser lands (its memory cap, a crash),
a render that overran `REPLICATOR_MAX_FETCH_SECONDS`, and a navigation error that is neither a network
failure nor a refused URL (a certificate error, a redirect loop): a page that fails a render that way
once usually does again, and each retry is a whole render.

**What stays open instead.** A 5xx, 408 or 429 on the navigation, no response to it at all (connect,
TLS-handshake or DNS failure), a host whose politeness window is longer than a poll, and a blob tree over its
disk ceiling, are retried with no fact, exempt from the delivery ceiling, as for a fetch.

## Enabling it on a host

- The runtime, per the [runbook](../plans/2026-10-25-viewport-capture-runbook.md): the pinned
  `chromium-headless-shell` in `REPLICATOR_PLAYWRIGHT_BROWSERS_PATH`, and the unit's delegated
  `browser/` cgroup.
- **`REPLICATOR_CAPTURE_ENABLED=true`** creates `replicator.capture` at boot, at `0`, and starts the
  loop after one test launch. A launch that fails leaves the group in place and the loop off:
  `CRITICAL` names the runbook, and fetch, replicate and persist keep running.
- Settings construction refuses `3 × REPLICATOR_CAPTURE_MAX_WAIT_SECONDS + 30` over
  `REPLICATOR_MAX_FETCH_SECONDS`, so each wait can run to its bound inside the deadline and a slow
  page usually closes as `wait_timeout`. Launch and the screenshot share the 30 s, so a page that
  uses all three waits *and* takes a long screenshot (31 s measured for a 28108 px page) can still
  overrun into the deadline's `handler_error`.
- `worker ready` reports `capture: enabled` with its group and consumer, `capture: unavailable`, or
  `capture: disabled`.

## Charter check

Against the three tests in [replicator-boundaries.md](replicator-boundaries.md#the-three-tests):

1. **No durable per-resource history.** The image is temporary and rebuildable by rendering again;
   keeping it is the issuer's `content.persist`.
2. **Cross-command coordination over resources only the fetcher sees**, twice: the browser (one render
   at a time, its memory cap) and the host's tolerance (the shared pacer). Both are Replicator's.
3. **No domain vocabulary read.** `info_source_id` is echoed, so `capture_reporter.py` is on
   `DOMAIN_ECHO_MODULES`; `capture.py` never reaches the echo and stays off it.
