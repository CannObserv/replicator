---
title: Browser fetch — honour ContentFetchCommand.render, store the rendered document
date: 2026-11-05
status: approved 2026-11-05 (all five operator calls as recommended)
issue: "#63 (epic #154)"
---

# Browser fetch

## Intent

Some sources only produce meaningful content after their scripts run. Fetched over plain HTTP,
they store an empty shell. cannobserv v0.40.1 (cannobserv#602) added
`ContentFetchCommand.render`. `None` or `"http"` means today's fetch. `"browser"` asks Replicator
to load the page in the confined renderer #138 built and store the **rendered document**. Archiver
owns each source's mode (archiver#379), and Watcher puts it on the command (watcher#414). Both
wait on this issue's support announcement, per co-core's MUST in `ContentFetchCommand`'s
docstring.

The scope comes from the cannobserv coordinator's comment of 2026-10-31, steps 2–4:

1. Honour the field.
2. Add two refusal rows.
3. Announce support.

The floor raise (step 1) shipped as #177.

### Goals

- A `render="browser"` command goes through the renderer. `None` and `"http"` stay on
  `AsyncFetchDriver`, byte for byte.
- **A `browser` command is never fetched over HTTP**, in any configuration.
- The browser path returns a `FetchResult` that the rest of the byte path consumes unchanged:
  pacing escalation, the status classifier, the size ceiling, fingerprint, store, `blob_available`.
- Two pre-request refusals as `invalid_request_options`: an unknown `render`, and a validator beside
  `browser`.
- The support statement goes into the fetch issuer contract. Announcement replies for watcher#414
  and archiver#379 are drafted on #63. The coordinator posts them.

### Non-goals

- A wait condition. co-core ships none, and the load event is the ready condition.
- Echoing the mode on the facts (#272's rule).
- WACZ/WARC (watcher#4) and adaptive escalation (watcher#5, absorbed into #63 and settled as
  option 1).
- Per-render child cgroups (see *Concurrency*).

## Evidence (spike, 2026-11-05)

Pinned Playwright 1.64.1 and `chromium-headless-shell` 1248, against a local server (spike
script, not committed). Each run did `goto(commit)`, then `load`, then `content()`, then
`document.contentType`, in a context with `user_agent="watcher/0.1.0"`:

| Served | Status | `document.contentType` | `content()` |
|---|---|---|---|
| `text/html; charset=ISO-8859-1` with a script and an `ETag` | 200 | `text/html` | **The rendered DOM** (the script's text is in it). The origin's `<meta charset="iso-8859-1">` is kept verbatim |
| no `Content-Type`, HTML body | 200 | `text/html` (sniffed) | the DOM |
| `application/xhtml+xml` | 200 | `application/xhtml+xml` | the DOM |
| `application/json`, `text/plain`, `image/png`, `application/xml` | 200 | the served type | **Chromium's viewer wrapper** (`<pre>…`, `<img>`, the XML viewer's stylesheet), not the resource |
| `image/svg+xml` | 200 | `image/svg+xml` | the SVG |
| `application/pdf`, or `Content-Disposition: attachment` | — | — | `goto` raises **`Download is starting`** |
| 429 with `Retry-After: 120` | 429 | — | `response.all_headers()["retry-after"] == "120"` |
| 304, nothing cached | — | — | `goto` raises **`net::ERR_ABORTED`**. No response |
| 302 to the HTML page | 200 | `text/html` | the target's DOM. `all_headers()` gives the **final** response's |

The issuer's `user-agent` reached the origin on every request (the server echoed it back).

## Review of the brief against the code

Posted on #63 before building. The brief's line references all check out at `7c15148`. These are
the refinements:

1. **`headers` under `browser` is unaddressed, and it can't be refused wholesale.** Watcher sends
   `user-agent: watcher/0.1.0` on **every** command (`_command_headers`,
   watcher `src/core/fetch_commands.py`). Operator call 1 below.
2. **The browser `FetchResult` mustn't carry the origin's `etag` / `last_modified` /
   `Content-Type`.** `_passthrough` stays unchanged, but those headers describe bytes that
   aren't stored. The DOM is re-serialized as UTF-8, while the origin's `charset` (and its
   `<meta>`, measured) may say otherwise. Echoing a validator would also earn a refusal on
   Watcher's next command under `browser`.
3. **Serialising renders isn't only about memory.** `_teardown` (`browser.py:406`) writes
   `cgroup.kill` on a failed render, and again if the group is still populated after close. That
   kill is cgroup-wide, so one failing render kills a concurrent render's browser too.
4. **The 304 arm is unreachable under `browser`.** Chromium aborts an uncached 304 navigation with
   `net::ERR_ABORTED` (measured), so `_raise_for_status`'s `not_modified` never sees it. Validators
   are refused under `browser` anyway, so any 304 would be unbidden.
5. **`_navigate` raises on non-2xx and keeps only `Retry-After`.** For the fetch path to use
   `report_rate_limited` / `_raise_for_status` unchanged, the load must *return* the non-2xx status
   and its headers, without waiting for `load`.
6. **The fetch deadline and capture's deadline aren't the same classification.** `_fetch` turns an
   expiry into `TransientFetchError` (retried indefinitely). Capture's `RenderDeadlineError` is
   unclassified (`handler_error` at the delivery ceiling). Operator call 4.
7. **Capture's real-browser pattern *is* a local server.** `test_browser_real.py` serves on
   `127.0.0.1` and narrows the guard's table to the canary's `127.0.0.2/32`. The issue body's "known
   JS-rendered target" becomes a local page whose script writes the DOM, not a public URL.
8. **`timeout_seconds` under `browser` is unaddressed.** Decided below: it bounds each wait stage.
9. **A non-HTML URL:** Chromium stores a viewer wrapper or raises `Download is starting`, so it
   needs a stated outcome. Operator call 2.

## The decided parts

My calls, unless the operator overrides them. The five operator calls are under *For the
operator*.

### Flow

```
content.fetch → _request_options (headers, timeout, render + its refusals)
   → raise_for_ceiling → pace(host)
   → render == "browser" ? BrowserFetcher : AsyncFetchDriver      (inside one MAX_FETCH_SECONDS deadline)
        BrowserFetcher: [render lock] launch (cgroup, sandbox, SOCKS guard) → goto(commit)
          → non-2xx? return status + headers, no document
          → load → document.contentType ∈ {text/html, application/xhtml+xml}? → measure → content()
   → report_rate_limited → _raise_for_status → _raise_for_size → fingerprint → store → blob_available
```

### Selection and the refusals

- **Branch on `render == "browser"`**, never on `render is not None` (#177). `None` and `"http"` are
  one path.
- `_request_options` gains a third field, `render: Literal["http", "browser"]`. It's validated
  beside headers and timeout, before the ceiling and the pacer, so a refusal never waits on
  either:
  - **An unknown `render`** is refused with `invalid_request_options`. It's an exact match, so
    `"HTTP"`, `""` and `"pdf"` are all refused. That matches what `ContentFetchCommandEmit`'s
    `Literal` admits.
  - **`If-None-Match` or `If-Modified-Since` beside `browser`** is refused with
    `invalid_request_options`. It's checked against the **folded** names, the ones
    `_request_headers` already produces, so the case-insensitive match is the existing fold, not a
    second one. The match is re-implemented here: no cannobserv helper, as the coordinator leans.
    Only those two are named: `If-Match` / `If-Unmodified-Since` / `If-Range` aren't, as the
    coordinator leans. Under recommendation 1A they're refused anyway, as "not `user-agent`".
- All of these are terminal, pre-request, with `status_code` and `attempts` unset. That's what
  `_invalid_options` already produces.

### The renderer gains a second operation

`PlaywrightRenderer.load(LoadRequest) -> Loaded` sits beside `render()`. It's the same launch, the
same three confinements and the same teardown, so **a second caller of the seam, not a copy**
(#138's note):

- **Shared:** launch → `_first_of(socks, drive)` → teardown, factored out of `render()`. Capture's
  behaviour doesn't change, and its tests pin that.
- **`_navigate` returns the response.** Capture keeps raising `NavigationStatusError` on non-2xx.
  `load` returns `Loaded(status, headers={"retry-after": …} if present, document=None)` without
  waiting for `load`.
- **Gate on `document.contentType`**, the browser's reading, which sniffs a missing header:
  - `text/html` and `application/xhtml+xml` are rendered and stored;
  - anything else raises `NotADocumentError` (operator call 2);
  - so does `goto`'s `Download is starting`.
- **Measure before the relay.** `new TextEncoder().encode(<doctype + outerHTML>).length` runs in
  the page, inside the cgroup. Over `REPLICATOR_MAX_BLOB_BYTES` it's refused as `too_large` before
  `content()` crosses the unconfined Node driver. `_raise_for_size` re-checks the real bytes,
  because a page that grew after measuring comes out larger (capture's CR 2 shape).
- **Viewport:** a constant 1280×720 (Playwright's default), so a document's fingerprint doesn't
  move with configuration.
- **User agent:** the issuer's `user-agent`, when sent, is the context's `user_agent`. Otherwise
  it's Chromium's own `HeadlessChrome`, as capture uses.
- **Wait bounds:** `timeout_seconds` when sent, else `REPLICATOR_CAPTURE_MAX_WAIT_SECONDS`
  (default 30). It bounds `commit` and `load` each, which is the browser's reading of "per
  operation". One deadline (below) still bounds the whole, so a `timeout_seconds` near 120 lets
  the deadline fire first. That's stated in the reference rather than refused.

### The `FetchResult` the browser returns

| Field | Value |
|---|---|
| `content` | `page.content()` encoded UTF-8: doctype plus the serialized DOM at the load event |
| `status_code` | the navigation's final response status |
| `headers` | **Only** `content-type: <document.contentType>; charset=utf-8`, plus `retry-after` on a non-2xx that sent one. Never `etag` / `last-modified` (cannobserv decision 3) |
| `final_url` | `page.url` when serialized: where the document landed, JS redirects included |
| `fetcher_used` | `"browser"` |
| `duration_ms` | from the load request to serialization, including any wait for the render lock: how long the command held the serial path |

So `blob_available` under `browser` has:

- `media_type` = `text/html` or `application/xhtml+xml`;
- `content_type_raw` describing the **stored** bytes (`…; charset=utf-8`). The charset is the
  one that decodes them, and it takes precedence over the kept `<meta>` for an HTML parser;
- `etag` and `last_modified` = `None`.

### Outcomes under `browser`

| What happened | Fetch outcome |
|---|---|
| unknown `render`; a validator beside `browser`; (1A) any header but `user-agent` | `invalid_request_options`, before any request |
| No renderer on this worker (disabled or failed preflight) | transient, parked, `WARNING`. **Never HTTP** (call 3) |
| The guard refused the URL, a hop, or **any** request the page made | `destination_refused` (the refusal's own `PermanentFetchError`) |
| No response: DNS, connect, TLS, `commit` not reached in the bound | transient (as HTTP's connect / read errors) |
| `NavigationImpossibleError` (unsafe port, scheme) | `not_fetchable` |
| A download, or a document that isn't HTML / XHTML | `not_fetchable` (call 2) |
| Non-2xx final response | **the existing path**: 429/503 escalate the pacer with `Retry-After`, 5xx/408/429 are transient, other non-2xx is `http_status` with `status_code` |
| `load` not reached in the bound; the whole-render deadline | unclassified: `handler_error` at the delivery ceiling (call 4) |
| Browser group-killed at the cap, `ERR_ABORTED` (an unbidden 304), other Playwright errors | unclassified, as capture |
| Measured or real document over `REPLICATOR_MAX_BLOB_BYTES` | `too_large` |
| Success | `blob_available` |

The mapping lives in a new `src/worker/browser_fetch.py` (`BrowserFetcher`, implementing
`Fetcher`). It raises the fetch vocabulary itself, as `capture._render` does for capture.
`_fetch`'s httpx arms never see a browser failure (#173's note).

### Concurrency: one render at a time across capture and fetch (call 5)

There's an `asyncio.Lock` inside `PlaywrightRenderer`, held from launch to teardown, and one
renderer instance is shared by both loops. The wait for it runs **inside** the caller's deadline,
so the stop budget gains no term. The cost: a browser fetch can spend part of its 120 s behind a
capture. HTTP fetches never touch the lock.

### Deadline and stop budget

`_fetch`'s single `REPLICATOR_MAX_FETCH_SECONDS` deadline wraps the browser fetch too.
`_fetch`'s expiry arm becomes mode-aware:

- `http` keeps `TransientFetchError`;
- `browser` raises `RenderDeadlineError`, which moves from `capture.py` to `browser.py` and is
  shared.

Teardown after a cancellation is `cgroup.kill`, which doesn't wait. #138's four terms are
unchanged (5 + 5 + 120 + 30 = 160 against `TimeoutStopSec=180`), so **broker needs no notice**.

### Wiring and the flag

- **`REPLICATOR_BROWSER_FETCH_ENABLED`, off by default** (call 3).
- The renderer is prepared once at boot, when capture **or** browser fetch is on. That's
  `prepare_capture`, renamed `prepare_renderer`: one preflight and one instance, and the lock
  only works with one instance.
- `build_handler(browser_fetcher=None)`: `None` means "no renderer here". It's mirrored in both
  recorders (`test_main.py`, `test_policy_integration.py`), and `-m integration` runs locally.
- **The `_a_whole_render_holds_its_waits` settings check** runs when either flag is on.
- **`worker ready` gains `browser_fetch`:** `enabled`, `disabled` or `unavailable`.
- **Pacing:** the same `pace()` call, against the shared `HostPacer`. One origin request is
  recorded per command, and subresources aren't paced (capture's accepted limit).

### Charter check (`replicator-boundaries.md`)

- Fetching is Replicator's, and a render mode is how it fetches.
- Nothing about the content is interpreted beyond its media type, which the HTTP path already
  reads.
- `render` is a **request option** that is read and branched on, like `headers`. It isn't an
  echoed identity field, so `test_boundaries.py`'s one-field carve-out is untouched.

## Contract changes

The budgets are the brief's, re-measured at build:

| Doc | Change |
|---|---|
| Issuer contract (277 tokens of room) | A `render` row in the command table: **"Honoured since #63"**. That row is the support statement co-core's MUST waits on |
| Issuer reference (plenty) | A new `render` section under request options. It covers what `browser` stores, the refusal rows (2, or 3 under 1A), `user-agent`, `timeout_seconds`, the non-HTML outcome, the charset note and the no-renderer behaviour |
| Outcome reference (131) | `not_fetchable` and `handler_error` rows gain a short `browser` qualifier. The enriched-fields notes gain "under `browser`: describes the stored document; validators `None`". Detail goes to the reference, by link |
| `ENVIRONMENT.md` | the flag |
| `ARCHITECTURE.md` | `browser_fetch.py` in the file map |
| `CONVENTIONS.md` (32), `AGENTS.md` (about 27) | **Untouched** if possible. Otherwise `curating-context` first |

The contract's "Changing this document" rule asks for an announcement on the issuer trackers. That
announcement is the drafted watcher#414 / archiver#379 replies, which the coordinator posts after
verifying live.

## For the operator

Approving the doc approves each recommendation.

1. **`headers` under `browser`.**
   - **A (recommended):** `user-agent` becomes the browser's UA, and every other header is refused
     as `invalid_request_options`. That's a third refusal, so a cannobserv issue keeps
     `ContentFetchCommandEmit` in step (the 2026-10-31 comment's mechanism). The coordinator files
     it. It touches no current issuer: Watcher sends `user-agent` plus the two validators that are
     already refused. It's the conservative direction, since relaxing it later stays compatible.
   - B: every header is sent on **every request the page makes** (`extra_http_headers`). An
     `Authorization` then reaches whatever hosts the page's author chooses.
   - C: the main document only, via `page.route`. Playwright doesn't re-route redirects, so the
     headers silently drop off after the first hop, which is an invisible change to the request.
2. **A non-HTML response under `browser`** (a download, JSON, an image, XML).
   - **A (recommended):** `not_fetchable`, terminal, so the issuer's remedy is `render="http"`.
   - B: store the response body as received. That's HTTP-equivalent bytes, but a download has no
     body to give, so A's refusal is still needed for those.
   - C: store Chromium's viewer wrapper. That's never the resource.
3. **No renderer on this worker** (flag off, or the preflight failed).
   - **Transient (recommended):** parked in the PEL and retried at the claim cadence, with a
     `WARNING`, and never HTTP. It's capture's "no command is skipped" posture, so a restart that
     fixes the browser resumes the backlog, and the issuer's reaper is the backstop.
   - Alternatives: a terminal close (`not_fetchable`), which a host fix can't recover; or
     unclassified, which gives `handler_error` after about 4 min.
   - **The flag goes on in `/etc/replicator/.env` before the announcement.** That's an operator
     step, listed in the host steps.
4. **`load` not reached in the bound, or the deadline, under `browser`:** unclassified, so
   `handler_error` at the delivery ceiling.
   - HTTP's rule (transient indefinitely) would re-spend a whole render, up to 120 s on the serial
     fetch path, every reclaim, forever.
   - The alternative is a new `wait_timeout` token on fetch, mirroring capture. It's additive, but
     it's a new row.
5. **One render at a time across capture and fetch (a lock).** The alternative is per-render child
   cgroups, with teardown scoped to its own child: concurrency, at the cost of the cap becoming
   per-render (N × 1536 MiB) and a cgroup change.

## Tests

The TDD order: each item's test lands red first.

- **Refusals:**
  - unknown `render` (`"pdf"`, `"HTTP"`, `""`);
  - a validator beside `browser`, in any case;
  - (1A) a non-UA header beside `browser`;
  - `None` and `"http"` behave identically;
  - every refusal comes before the ceiling and the pacer.
- **Routing:**
  - a `browser` command reaches the browser fetcher and **not** the HTTP driver (#177's sibling
    test);
  - no renderer means transient and no HTTP request (a recording transport proves zero calls).
- **`BrowserFetcher` against a fake renderer:** every row of the outcome table, including 429 with
  `Retry-After` escalating the shared pacer and 503 escalating without one.
- **The real client (#173's lesson):** `-m browser` runs `PlaywrightRenderer.load` plus
  `BrowserFetcher` plus `build_handler` against a local page whose script writes the DOM. It
  asserts:
  - the stored bytes hold the script's text;
  - `media_type` is `text/html`, and `content_type_raw` ends `charset=utf-8`;
  - `etag` is `None`;
  - the server saw the issuer's UA;
  - a download and a JSON URL are `not_fetchable`;
  - a canary hit is `destination_refused`;
  - the lock holds (two concurrent loads never overlap in the cgroup).
- **Capture unchanged:** its unit and `-m browser` suites pass untouched.
- **Wiring:**
  - both recorders;
  - `worker ready`'s field;
  - the settings check under either flag;
  - `-m integration` locally against a scratch broker.

## Commits (one branch, `63-browser-fetch`)

1. `#63 docs: add design doc for browser fetch` (this)
2. `render` and its refusals in `_request_options`
3. `browser.py`: shared session, `load()`, `NotADocumentError`, the lock, `RenderDeadlineError`
   moved
4. `browser_fetch.py`: `BrowserFetcher` and its outcome map
5. `handler.py`: routing by mode, the mode-aware deadline arm
6. Settings flag, `main.run` wiring, `worker ready`, recorders
7. `-m browser` real-client tests
8. Contracts, `ENVIRONMENT.md`, `ARCHITECTURE.md`

## Accepted limits

- **Inherited from #138:**
  - CR 10 (unauthenticated per-render listener);
  - CR 11 (unconfined Node driver; the document now relays through it, bounded by the in-page
    measure);
  - #156 / #161 guard limits;
  - unpaced subresources.
- **The document is the DOM at the load event.** Content a script adds later is absent until a
  wait-condition field exists in co-core.
- **The serialized DOM keeps the origin's `<meta charset>`.** `content_type_raw` names UTF-8, which
  wins for a conforming parser. A consumer that sniffs only the `<meta>` mis-decodes non-ASCII.
- **An unbidden 304 or a script navigating during serialization** is unclassified (`handler_error`
  at the ceiling).
- **A browser fetch can wait behind a capture** inside its own deadline.
- **A mode flip is a fingerprint discontinuity.** It's the issuer's and Archiver's to handle
  (archiver#379). Nothing to do here.

## Verification debt (expected)

- **No live `browser` command until watcher#414 passes the mode through**, after the announcement.
  Replicator can't `XADD` to `content.fetch`. Instead, these get verified:
  - `-m browser`;
  - `-m integration`;
  - a scratch-stream run with `scripts/seed_fetch.py` (it gains `--render`).
- Post-merge:
  - the operator sets the flag;
  - restart;
  - `worker ready` shows `browser_fetch: enabled`;
  - the coordinator verifies, then posts the announcements.
