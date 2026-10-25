---
title: Viewport capture — consume content.capture, render headless, store, publish the stored-capture fact
date: 2026-10-25
status: proposed 2026-10-25, awaiting approval on #138
issue: "#138 (epic #154)"
---

# Viewport capture

## Intent

Archiver wants a screenshot of a source's page at a stated viewport, kept beside the bytes it
already archives (CannObserv/archiver#380). The operator put the mechanism here (2026-10-14).
Replicator already manages bytes: it renders the page itself, stores the image in its own
temporary store and announces the image after storing it. So #69's ruling holds: the process
that addresses an object is the one that writes it. The contracts are fixed by cannobserv
v0.37.1 (`ContentCaptureCommand`, `CaptureStoredEvent`, `CaptureFailedEvent`; co-core 0.39.1 is
pinned here since #153). Broker granted `content.capture` and `content.capture.dlq` on
2026-10-20T15:06:05Z (broker#114).

This doc also picks the **browser runtime that #63 will reuse**.

### Goals

- Consume `content.capture` in group `replicator.capture`, with the competing-consumer posture of
  the other three command streams.
- Render at the requested viewport. Honour `full_page` and the wait condition, inside
  `REPLICATOR_MAX_FETCH_SECONDS`.
- Store the PNG in the temporary `BlobStore`, content-addressed, under the blob TTL, and announce
  `blob_expires_at`.
- Publish `capture_stored` only after the store succeeds, or `capture_failed` instead. Deduplicate
  on `command_id`.
- Make every request the page makes pass the destination guard (#89, #95), not only the navigation.

### Non-goals

- **#63's render-mode fetch.** It reuses the runtime and the guard, and keeps its own code path.
- **Archiver's issuance** (archiver#380).
- **PDF rasterizing and crop modes** (archiver#411).
- **Authenticated captures.** The command carries no headers by design.
- **Capture-to-capture change detection.** Nothing fingerprints a capture.

### Anti-goals

These would count as failure even if screenshots came out:

- **A browser that can reach loopback, RFC 1918 or the tailnet** through any path: a subresource,
  a script, a WebSocket, WebRTC, QUIC, DNS, or a redirect hop.
- **A runaway render that costs the worker, `tailscaled` or a session** its memory before it costs
  the browser.
- **An unsandboxed renderer** running as the identity that can read `/etc/replicator/.env`.
- **Anything new for broker to re-derive.** No ceiling outside `REPLICATOR_MAX_FETCH_SECONDS`, and
  no read loop other than a blocking `count=1` read with `PollCadence`.
- **A capture enabled on a host with no runtime** that silently skips commands. Creating the group
  at `$` after commands arrived is one way that happens.

## Evidence (spike, 2026-10-25)

A throwaway spike in `/tmp` measured these. Nothing was installed into the repo or the unit. The
brief corrections comment on #138 has the detail.

| What | Measured |
|---|---|
| Install size | `playwright` 1.64.1 package 138M (it bundles Node v24.22.1). `chromium_headless_shell-1248` 273M. `ffmpeg-1013` 5.1M, pulled by the installer. **~416M**, plus a cached wheel. Root has 4.5G free (77%). |
| Launch | 0.05–0.16 s per launch, headless shell |
| Memory (cgroup `memory.peak`) | `example.com`: 199 MiB for the whole tree. en.wikipedia `Cannabis`, full page (1366×28108, 7.9 MB PNG): browser 768 MiB, Python plus Node 230 MiB |
| Time | That full page took 31 s in `screenshot` alone, on this host's 2 vCPUs. A normal page takes under 1 s. |
| Cap failure mode | Browser-only cgroup, `memory.max=400M`, `memory.oom.group=1`: six browser processes group-killed, the worker untouched, Playwright raises `TargetClosedError`. A cap on a scope that also holds Python kills Python instead. |
| Guard | Through a SOCKS5 proxy: `<img>`, `fetch()`, WebSocket, `sendBeacon`, prefetch, an iframe, a 302 hop, `127.0.0.1`, `[::1]` and `10.0.0.1` all reached the proxy. **Zero** canary hits with the flags below. WebRTC STUN reached a loopback canary over UDP four times without `--force-webrtc-ip-handling-policy=disable_non_proxied_udp`. |
| DNS | The proxy received hostnames (SOCKS5 ATYP 3). `strace` saw no port-53 traffic from the browser. Playwright's own `--host-resolver-rules="MAP * ~NOTFOUND …"` fails to parse in this build (`Failed parsing rule`), so it protects nothing. |
| Sandbox | Playwright passes `--no-sandbox` by default. `chromium_sandbox=True` launches here, because unprivileged user namespaces are available. |
| `wait=None` | Playwright's `goto` waits for `load`. A text change 1.5 s after `load` was not captured. The co-core docstring holds, so there's nothing to report on cannobserv. |

## Approaches

### 1. Runtime

| | Pinned Playwright plus its `chromium-headless-shell` (**recommended; brief's operator decision 1**) | `/headless-shell` (Shelley's) | CDP-direct client over `--remote-debugging-pipe` |
|---|---|---|---|
| Pinning | `uv.lock` pins Playwright, and Playwright pins the Chromium revision | The image changes it under us | Our pin, our binary |
| Waits, `full_page`, crash detection | Built in: `wait_until`, `wait_for_selector(state="visible")`, `networkidle`, stitched full page | Same as CDP-direct | About 400–600 lines we write: network-idle tracking, selector polling, full-page stitching, target-crash handling |
| Cost | 138M package with its own Node; a ~50–100 MiB driver process | 0 disk | No Node, but a second browser download anyway |
| Owner | Microsoft, monthly releases | Shelley, disabled here (#97) | Us |

**Recommendation: Playwright**, with the shell installed to `/var/lib/replicator/ms-playwright`
(`StateDirectory=`, which the unit owns). Spending ~140M on Node buys the three behaviours a
CDP-direct client would have to reimplement, and each of them is a wait-condition bug waiting to
happen. CDP-direct stays the fallback if Node's footprint or patch cadence ever becomes a
problem. The seam below keeps it swappable. Node becomes an *outside-apt* runtime in
`patching-hosts`' census, bumped with `playwright` in `uv.lock`.

### 2. The browser's egress guard

| | **A per-render SOCKS5 guard, all traffic through it (recommended)** | Request interception (`page.route`) |
|---|---|---|
| Sees | Every TCP connection the network service opens: subresources, workers, WebSocket, beacons, prefetch, redirects | What Playwright routes. Service workers, some WebSocket paths and browser-initiated traffic go around it |
| Resolution | The proxy resolves, checks **every** address, then **connects to the address it checked**. That closes the DNS-rebinding window `egress.py` leaves open | The browser resolves again after the check: TOCTOU |
| Fails | Closed. A connection the proxy refuses never opens | Open, for whatever the hook misses |

**Recommendation: SOCKS5.** It's simpler than an HTTP proxy, because it needs no HTTP parsing
and handles `http`, `https` and `ws(s)` the same way. Chromium does no SOCKS authentication, and
none is needed: the listener grants only what any local process already has, public internet. It
binds `127.0.0.1:0` per render and closes with it, so a refusal belongs to exactly one capture.
Details:

- **Shared vetting.** `egress.py`'s resolve, classify and range check become one function that
  both `GuardedTransport` and the SOCKS guard call. The deny table stays
  `REPLICATOR_BLOCKED_DESTINATIONS` with its default, so fetch and capture can't drift apart.
- **Commands.** `CONNECT` only. `BIND` and `UDP ASSOCIATE` are refused.
- **Connecting.** Each vetted address is tried in order under a connect timeout. This host has no
  IPv6 route, and the spike's first attempt hung on an AAAA-first answer.
- **Flags.** `--disable-quic` and `--force-webrtc-ip-handling-policy=disable_non_proxied_udp`.
  Service workers are blocked, downloads are off, and each launch gets a fresh profile. Playwright
  already adds `<-loopback>`, and a test asserts the launched argv carries all of them.
- **Residual, stated:** Chromium's IPv6 reachability probe. It's a UDP `connect()` to
  `2001:4860:4860::8888` that sends no datagram.

**A refused request fails the whole capture** with `destination_refused`, as the contract says,
including a subresource. The render is abandoned at the first refusal rather than finished. Cost:
a page that references a private address can never be captured. That's expected to be rare,
and a fetch already refuses the same destination.

### 3. Memory and the OOM posture

| | **Delegated sub-cgroup for the browser (recommended)** | `oom_score_adj` only, plus V8 heap flags | A separate long-lived browser unit, driven over CDP |
|---|---|---|---|
| Hard cap | `memory.max` on the browser only, with `memory.oom.group=1` | None. Compositor and Skia memory sit outside V8 | `MemoryMax=` |
| Runaway kills | The browser, measured | Whatever the kernel picks; +500 makes that likelier the browser | The browser unit |
| Cost | `Delegate=memory pids` plus `DelegateSubgroup=worker` (systemd 255 has it), and the worker re-claims `memory.low` in its leaf | Nothing | A CDP port on loopback any local process can drive. Proxy flags live in a second unit. Lifecycle coupling |

**Recommendation: delegation.** `replicator.service` gains `Delegate=memory pids` and
`DelegateSubgroup=worker`. The worker creates a sibling `browser/` cgroup with:

- `memory.max` = `REPLICATOR_CAPTURE_MEMORY_MAX` (default `1536M`: 2× the measured long-page peak);
- `memory.swap.max=0`;
- `memory.oom.group=1`;
- `pids.max=512`.

`scripts/browser_exec.sh` is Playwright's `executable_path`. It writes `$$` into
`browser/cgroup.procs` and `500` into `/proc/self/oom_score_adj`, which needs no privilege, then
`exec`s the pinned shell, so every Chromium child inherits both. Teardown is `browser.close()`
under a short timeout, then `cgroup.kill`. **#113 knock-on:** without `memory_recursiveprot` the
unit's `MemoryLow=128M` isn't handed down, so the worker writes `128M` into
`worker/memory.low` at boot. `test_deploy.py`'s slice-sum test reads it there.

**Playwright's Node driver stays in `worker/`**, at the worker's -900. If it shared `browser/`,
every cap kill would take the driver too, so a driver restart would be the normal case. It's
bounded by the PNG it relays, base64, under the 64 MiB blob ceiling. The worker leaf keeps the
128M reservation. A reservation isn't a cap, so a heavier worker isn't killed for exceeding it,
only less protected. PR 1 measures the worker leaf's peak across a long-page capture, and raises
the reservation, and with it the `system.slice` grant and its sum test, only if the peak passes
128M (my call).

**Identity and environment.** The browser runs as `exedev`, with the renderer sandbox on, and
Playwright gets `env=` holding only `HOME`, `PATH`, `TMPDIR` and `FONTCONFIG_*`. Nothing from
`/etc/replicator/.env` reaches Chromium. The residual: a renderer exploit *chained with a
sandbox escape* could read what `exedev` can, including the GCS writer keys and the repo `.env`'s
PATs. A dedicated low-privilege browser user would close that, at the cost of a privileged launch
path. That's question 6 for the operator.

**Without delegation the capture loop refuses to run.** That covers a dev run outside the unit.
`REPLICATOR_CAPTURE_CGROUP=off` is an explicit dev escape hatch. It's never the default, and
`worker ready` reports it.

## The decided parts

The brief's three operator decisions are approved by starting the thread. Everything below them
is **my call** unless the operator overrides it (see *For the operator*).

1. **Runtime:** pinned Playwright plus `chromium-headless-shell`, in `/var/lib/replicator/ms-playwright`.
2. **Flag:** `REPLICATOR_CAPTURE_ENABLED`, off by default. Persist is the precedent.
3. **Concurrency:** one render at a time, in its own `run_loop`: a blocking `count=1` read with
   `PollCadence`.

### Flow

```
content.capture → validate → pace(host) → launch (cgroup, sandbox, SOCKS guard) → goto(commit)
   → load → wait condition → measure → screenshot(png) → close → store → capture_stored
   ↘ refused / unmet / closed ────────────────────────────────────────────→ capture_failed
```

- **Validation first, before any browser.** Refusals are `invalid_request_options`:
  - a viewport dimension `< 1` or `> 4096`;
  - an unknown `wait.kind`;
  - a `selector` wait without a non-blank `selector`;
  - a `delay` without finite `seconds > 0`, or over `REPLICATOR_CAPTURE_MAX_WAIT_SECONDS`.

  A URL that isn't `http(s)` is `not_fetchable`.
- **Pacing.** The navigation's host goes through **the same `HostPacer` instance as fetch**. It's
  lifted out of `build_handler` into `main.run` and injected into both handlers. Sleep-or-park and
  429/503 escalation behave as they do for fetch ([POLITENESS.md](../POLITENESS.md)).
  Subresources aren't paced.
- **Waits.** `goto(wait_until="commit")`, then `load`, then the condition (`networkidle`, a
  `selector` that's visible, or `delay`). The `load` wait and the condition wait are each bounded
  by `REPLICATOR_CAPTURE_MAX_WAIT_SECONDS` (default 30). Everything from launch to screenshot sits
  inside one `REPLICATOR_MAX_FETCH_SECONDS` deadline. A settings validator requires
  `2 × max_wait + 30 ≤ max_fetch_seconds`.
- **Image.** PNG only, device scale 1, so `media_type` is `image/png`. `image_width` and
  `image_height` are read from the PNG's IHDR, never from the viewport. Before a `full_page` shot
  the document's scroll height is measured. Over `MAX_CAPTURE_HEIGHT` (32768 px) the capture is
  refused as `too_large`, and so is a PNG over `REPLICATOR_MAX_BLOB_BYTES`. **It's refused, never
  clipped**: a clipped capture couldn't be told from a whole one, which is the argument the
  co-core docstring makes for `wait_timeout`.
- **Facts.**
  - `final_url` is the main frame's URL at capture time: what the image shows.
  - `captured_at` is stamped when the screenshot returns.
  - `blob_uri` is `store.uri_for(digest)`.
  - `blob_expires_at` is `stored_at + REPLICATOR_BLOB_TTL_SECONDS`, read before the store, the way
    `blob_available` computes it.
  - On `local`, the `BlobUsage` ceiling applies the way it does for a fetch.
  - No `content.blobs` fact is published, and no change fingerprint is computed.
- **Group.** `ensure_group` at boot when the flag is on, at **`0`, never `$`**. Replicator holds no
  `XLEN` on command streams, so it can't prove the stream is empty, and `0` makes "never at `$`
  against entries" true by construction. The broker read-back stays `acladmin`'s (broker#116).
  Order makes a backlog unlikely: the flag goes on *before* Archiver issues (brief; broker#116).
  If one exists anyway, it's rendered rather than aged out. There's no age cutoff and no
  extra `reason`. Archiver's reaper re-issues anything it gave up on under a fresh `command_id`.
- **Runtime preflight at boot**, when the flag is on: launch once inside the cgroup and close.
  If it fails, the group is still ensured, so no command is skipped. The capture loop doesn't
  start, a `CRITICAL` line names the runbook, and `worker ready` reports
  `capture: "unavailable"`. Fetch, replicate and persist keep running. Broker's undelivered-age
  probe (320 s) is the alarm.

### Failure vocabulary (draft for the issuer contract's registry)

| `reason` | Fate | When |
|---|---|---|
| `invalid_request_options` | terminal | Validation above. Nothing is launched |
| `not_fetchable` | terminal | A scheme other than `http(s)`, or an unencodable host |
| `destination_refused` | terminal | The URL, a redirect hop, or **any** request the page made |
| `http_status` | terminal, with `status_code` | The navigation's final response is non-2xx, apart from those below |
| *(transient, no fact)* | retry | 5xx, 408 or 429 on the navigation (429/503 escalate the pacer); no response to the navigation (connect, TLS or DNS failure); a parked pacing wait |
| `wait_timeout` | **terminal** | `load`, or the condition, unmet within `REPLICATOR_CAPTURE_MAX_WAIT_SECONDS` |
| `too_large` | terminal | Page height over `MAX_CAPTURE_HEIGHT`, or the PNG over `REPLICATOR_MAX_BLOB_BYTES` |
| `handler_error` | terminal at the delivery ceiling | Unclassified failures: the browser was killed (cap, crash, `TargetClosedError`), or the whole-render deadline expired. These ride `REPLICATOR_MAX_DELIVERY_ATTEMPTS`, not the transient exemption, because a page that kills the browser once usually kills it again |

**Why `wait_timeout` is terminal:** the bound is the operator's ceiling, and an unmet `selector` or
a page that never goes network-idle is usually a property of the page. Retrying would spend a
full render each time, with no delivery ceiling to stop it. The issuer re-issues with a different
condition under a fresh `command_id`.

### Stop budget and broker's threshold

The four terms are unchanged:

| Term | Seconds |
|---|---|
| Poll | 5 |
| Pacing | 5 |
| `REPLICATOR_MAX_FETCH_SECONDS`, which now also bounds launch through screenshot | 120 |
| `REPLICATOR_BLOB_TIMEOUT_SECONDS` | 30 |
| **Total, against `TimeoutStopSec=180`** | **160** |

Teardown after the deadline is `cgroup.kill`, which doesn't wait. So broker's 320 s holds, and
**nothing needs telling on broker#116**. Teardown is bounded but not zero, so the PR states the
measured close time.

### Charter check (`replicator-boundaries.md`)

1. **Durable per-resource history?** No. The image is content-addressed in the temporary tier and
   rebuildable by re-rendering. Keeping it is the issuer's `content.persist`.
2. **Cross-command coordination over a resource only the fetcher can see?** Yes, twice. The
   browser pool (concurrency, memory) and the host's tolerance (pacing) → **Replicator**.
3. **Domain vocabulary?** None is read. `info_source_id` is echoed, so the capture handler and
   reporter join `DOMAIN_ECHO_MODULES`. That's a charter edit, made together with the test.

Result: mechanism. Line 90 moves "archival capture" out of *future*, and "browser" stays future
until #63. The charter is at 9726 of 10000 tokens, so PR 2 pays for its lines with a
`curating-context` split (the worked examples are the candidate), not with overflow.

## PR split

1. **Runtime and guard. No consumer, so nothing runs in production.**
   - The `playwright` dependency, and a `Renderer` seam in `src/worker/browser.py`: launch per
     render, sandbox on, flags, cgroup, teardown.
   - `src/worker/browser_egress.py`, the SOCKS guard, plus the shared vetting refactored out of
     `egress.py`.
   - `scripts/browser_exec.sh`. The unit's `Delegate=` / `DelegateSubgroup=` and the worker-leaf
     `memory.low` claim.
   - `test_deploy.py` and `MEMORY-PROTECTION.md` updates.
   - A runbook, `docs/plans/2026-10-25-viewport-capture-runbook.md`: install, verify, roll back.
   - Tests:
     - unit tests with a fake renderer;
     - `@pytest.mark.browser` tests against the real shell: the canary suite from the spike, the
       sandbox and flags in argv, an OOM group-kill that leaves the worker alive;
     - a CI job that installs the pinned shell and runs `-m browser`. It fails on a missing
       runtime, never skips. GitHub's Ubuntu 24.04 runners block unprivileged user namespaces
       through AppArmor, and they don't delegate cgroups. So the job sets
       `kernel.apparmor_restrict_unprivileged_userns=0` and runs the suite inside a
       `systemd-run --scope -p Delegate=yes` (my call). If a runner won't allow the scope, only
       the OOM group-kill test moves to a host-only marker. It goes on the verification list, and
       the sandbox test stays in CI.
   - **Deploying it does change production:** the worker moves into `worker/`, so the host steps
     include the unit `cp` and a `memory.low` read-back.
2. **The consumer.**
   - Settings: the flag, the group and consumer name, `MAX_WAIT`, `MEMORY_MAX`, `BROWSERS_PATH`.
   - `CAPTURE_SPEC` (segment `capture`), `src/worker/capture.py`,
     `src/worker/capture_reporter.py`, the pacer lift, and `main.run` wiring with the preflight and
     `worker ready`.
   - `docs/contracts/content-capture-issuer-contract.md`: the normative `reason` registry.
   - The charter and `test_boundaries.py`.
   - Docs: AGENTS.md (paid with `curating-context`), `STREAMS.md`, `ARCHITECTURE.md`,
     `COMMANDS.md` (broker's "three" drift), `ENVIRONMENT.md`.
   - CI's import smoke gains the capture types.
   - An integration test against a scratch broker and a local page, and a `scripts/seed_fetch.py`
     equivalent that only accepts a scratch stream.

The pacer lift could go out separately ahead of PR 2. It's small and changes no behaviour.

## For the operator

These are the open calls. Each has a recommendation, and approving the doc approves the
recommendations:

1. **A refused subresource fails the whole capture** (the contract's reading). The alternative,
   blocking the request and capturing the rest, means the image the issuer gets isn't the page.
2. **`wait_timeout` is terminal.**
3. **An over-tall `full_page` is refused (`too_large`), not clipped.**
4. **A missing runtime at boot degrades instead of failing the boot.** Capture doesn't start and
   fetch keeps running. The alternative is a fatal boot, which would trip `OnFailure=`.
5. **PR 1 changes the production unit** (delegation) before any capture exists. The alternative is
   to land delegation with PR 2. That's one fewer deploy with a moved cgroup, but a bigger PR 2.

6. **The browser runs as `exedev`**, sandboxed, with a scrubbed environment. The alternative is a
   dedicated system user with a privileged launcher (a sudo rule or a setuid helper), its own
   cgroup ownership and its own install path. That closes the sandbox-escape residual and adds a
   privileged path to the unit.

## Left to the builder

- Exact SOCKS reply codes, and connect and handshake timeouts.
- How Playwright's driver is restarted after it dies.
- How the PNG is streamed to the store. It's bytes today, and the ceiling is 64 MiB.
- Journal field names. Follow `persist`'s.
- The user agent: Chromium's own `HeadlessChrome`, which is honest and not configurable from the
  wire.

## Verification debt (expected)

- **The first real capture waits on archiver#380's issuance.** Replicator can't `XADD` to
  `content.capture`. What gets verified instead:
  - the `-m browser` suite;
  - a scratch-broker integration run;
  - a local-page capture end to end against `replicator.itest.*` streams.
- Broker's `acladmin` read-back of `replicator.capture` and its `ACL LOG` check (broker#116).

## For #63

- **Reuse** `Renderer`, the SOCKS guard and the cgroup. Don't copy them.
- **Make it a second caller of the seam**, not a branch inside the capture handler.
- **Settle conditional GET:** a rendered DOM has no validators, which is #63's own open question.
- **The memory cap is per render.** If #63 runs renders concurrently with capture, the shared
  `browser/` cgroup needs one render at a time across both, or per-render child cgroups.
