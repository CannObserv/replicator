---
title: Fetch credential references — resolve ContentFetchCommand.credential_headers on the host
date: 2026-11-23
status: proposed 2026-11-23 (awaiting the operator's approval)
issue: "#216 (epic #154)"
---

# Fetch credential references

## Intent

cannobserv v0.51.1 (cannobserv#577) adds `ContentFetchCommand.credential_headers`. It maps a
header **name** to a reference named `fetch-<role>`, for example `{"X-Api-Key": "fetch-wa-leg-api"}`.
The value is never on the frame. Replicator resolves it from host state and sends it only where
the operator said it may go. archiver#376 is the first consumer. Watcher won't set the field until
Replicator announces support, so until then a credentialed frame is fetched without its
credential.

Inputs:
- the issue body, which is upstream's ask;
- the coordinator's brief ([issuecomment-7088910682](https://github.com/CannObserv/replicator/issues/216#issuecomment-7088910682));
- the pickup corrections ([issuecomment-7090721729](https://github.com/CannObserv/replicator/issues/216#issuecomment-7090721729));
- upstream's design, cannobserv `docs/plans/2026-11-21-577-fetch-credential-references-design.md`, approved 2026-11-21. Its D3 (C1–C4) and D4 are what Replicator owes.

### Goals

- A reference resolves against a binding table frozen at boot. The value is held where no `repr`,
  log line, fact, `detail` or traceback can show it (C1).
- The value is sent **only** to hosts its binding names, only over `https`, on every hop
  including the first, and never through `FetchContent.headers` (C2).
- A URL or redirect hop out of scope closes the command `credential_refused` **before that hop
  is sent** (C3).
- The request-option guards cover the union of both maps, with the byte bound measured over the
  resolved values (C4), and they refuse the E1/E3/E4/E5 shapes the canonical class decodes.
- Two new terminal tokens, `credential_unknown` and `credential_refused`.
- The shared client's cookie jar no longer carries one command's cookies into another's fetch.
  Its own commit (§Commits).
- Nothing provisioned is the shipped state, and a safe one: every credentialed frame closes
  `credential_unknown`.

### Non-goals

- Credentials on `content.capture`, on `content.replicate`'s providers, or under
  `render="browser"` (refused, D5).
- Rotation without a restart (D2).
- Any issuer-side merge order. Watcher owns it.

## Evidence [measured 2026-11-23, httpx 0.28.1, `httpx.MockTransport`]

| Probe | Result |
|---|---|
| A transport sets `X-Api-Key` on the request it was handed, and the first hop 302s cross-origin | `https://other.example/land` **receives it**: httpx's `_build_redirect_request` copies the previous request's headers |
| `Set-Cookie` on one `client.get`, then a separate `client.get` to the same host | The later request **sends the cookie**: the shared client's jar lives for the worker's lifetime |
| `Set-Cookie` on a 302, and the next hop of the same fetch | The next hop **sends it**, today. A jar that never stores would break this |
| `httpx.URL("https://Bücher.example./x")` | `.host` = `'bücher.example.'` (IDNA-decoded); `.raw_host` = `b'xn--bcher-kva.example.'` |
| `TypeAdapter(PolicyHost)` on `'Example.COM.'`, `'bücher.example'` | `'example.com'`; non-ASCII refused |
| **Spike:** the D3 transport (below) over a client jar that never stores, under `AsyncFetchDriver` | The bounce cookie reaches `/land`. The next command sends no cookie. The in-scope credential is attached. A cross-origin hop raises before it is sent. The client jar stays empty |

The spike's exception came out of `AsyncFetchDriver.execute` unchanged, which is the path
`destination_refused` already takes (#95).

## Decisions

### D1. The binding table (`src/worker/credentials.py`)

`AliasTable`'s shape (`src/worker/aliases.py`), one file, one owner:

- **`REPLICATOR_FETCH_CREDENTIALS_FILE`** names a JSON table. When it's unset, nothing is
  provisioned. It's read once, at boot, and frozen behind `MappingProxyType`.
- An **unusable entry is dropped and logged** at WARNING, naming the reason and never the value,
  and the load never raises. An unreadable file means nothing is provisioned, logged at ERROR. A
  fetch misconfiguration must not stop a worker whose other commands are fine.

```json
{
  "fetch-wa-leg-api": {
    "header": "X-Api-Key",
    "hosts": ["api.leg.wa.gov"],
    "value_file": "/etc/replicator/fetch-credentials/fetch-wa-leg-api"
  }
}
```

| Field | Rule (a breach drops the entry) |
|---|---|
| key | `validate_fetch_credential` (co-core 0.51.1). The rule, never the value, appears in the log |
| `header` | **(call 3)** one header name: an RFC 9110 token, not in `REFUSED_HEADERS` or `proxy-*`, and not `user-agent`, `if-none-match` or `if-modified-since` |
| `hosts` | A non-empty list. Each entry goes through `TypeAdapter(PolicyHost)` (co-core's canonical form: lower case, no trailing dot, punycode; no port or scheme), so a binding and `content.fetch-policy` spell a host one way |
| `value_file` | An absolute path (`_why_unusable`'s rule), readable at boot. The content, minus one trailing `\n`, must be non-empty, match `_HEADER_VALUE` (printable ASCII and SP), and fit `MAX_REQUEST_HEADER_BYTES` |

Only the declared fields are read. A stray key, including a pasted value, never reaches an
attribute. The boot line `fetch credentials loaded` lists the provisioned references, each one's
`header` and `hosts`, and the `value_file` **paths**. The unset and no-file cases share one INFO
message, as `alias table not provisioned` does today.

### D2. How the value is held, and when it's read (call 2)

- **Held** in a `pydantic.SecretStr` on a frozen, slotted `FetchCredential`. `repr` and `str` give
  `**********`. `get_secret_value()` is called in exactly one place, the transport, when it
  builds the outgoing hop, and the boundary test pins that.
- **Read at boot (recommended).** A value file read at load is checked against D1's rules before
  any command arrives. A bad file drops its binding in the boot log rather than at the first
  fetch, and the hot path does no file I/O. **Rotation is a restart**
  (`sudo systemctl restart replicator`), as it is for the writers' keys.
  - The alternative is reading at use. Rotation then needs no restart, but every credentialed
    fetch reads a file, a file deleted mid-run fails at command time, and the table is no
    longer one frozen snapshot of host state.

### D3. Per-hop scope: one per-command scope transport, outermost (C2, C3)

```
AsyncClient(cookies=<jar that never stores>, follow_redirects=True)
└─ CommandScopeTransport     ← new: reads the command's scope from a ContextVar
   └─ GuardedTransport       (#95, unchanged)
      └─ BodyCeilingTransport (#104, unchanged)
         └─ AsyncHTTPTransport
```

The handler enters `command_scope(...)` around `_fetch`, as a context manager that resets the
`ContextVar` in `finally`. Every command enters one, anonymous ones included. For each hop, the
transport:

1. Reads the scope. **None set means refuse:** a `RuntimeError` names the missing scope. Nothing
   reaches the wire without one, which fails closed.
2. If the scope carries credentials, refuses the hop when its scheme isn't `https`, its port isn't
   443 (**call 4**), or its host isn't in **every** binding's `hosts`. The host is canonicalized
   from `request.url.raw_host`, never `.host`. The refusal is
   `PermanentFetchError(reason=CREDENTIAL_REFUSED)`, raised before anything below runs.
3. Builds a **new** `httpx.Request` with copied headers and never mutates the one it was handed,
   because httpx copies that request's headers into the next hop. It sets each credential header
   on the copy.
4. Adds the command's cookies (`httpx.Cookies.set_cookie_header`) and, after the response,
   extracts that hop's `Set-Cookie` into them (`extract_cookies`). Both are public httpx API. If a
   credential names `Cookie`, the jar's cookies follow it in the same field (`<credential>;
   <jar>`), so a session the origin mints on top of the credential survives the redirect chain.

**Why a `ContextVar`, and not a client per command.** The scope has to reach a transport that
httpx calls once per hop, inside co-core's `AsyncFetchDriver`, which builds the request itself.
httpx follows redirects in the calling task, and `asyncio.timeout` doesn't spawn one, so a
`ContextVar` set around `_fetch` holds on every hop [spike]. A per-command `AsyncClient` over the
shared transport would also work, and httpx's own jar would come free. But it changes the
handler's `Fetcher` seam into a factory, which every handler test injects, and it builds a client
per command. The `ContextVar` keeps the seam.

**Why outermost.** Scope is pure, so a hop out of scope is refused without a DNS lookup. **When a
hop is both out of scope and in a refused range, `credential_refused` wins.** An anonymous command
never sees the difference: the guard runs as today.

**The cookie change is behavioural (call 1).**
- **Today:** one jar for the worker's life. One command's `Set-Cookie` changes another's response,
  and the jar grows without bound.
- **After:** cookies live for **one command's redirect chain**, then go. With credentials, a
  session minted by a credentialed fetch can no longer be replayed on a later command that carries
  none.
- **What it can change:** a source that only served content on a *second* command, because the
  first planted a cookie. Nothing in `src/` relies on cookies [grep], and production's corpus
  can't be measured from here. That's an accepted limit.
- The never-store jar is `http.cookiejar.CookieJar(policy=DefaultCookiePolicy(allowed_domains=[]))`,
  standard library.

### D4. The handler's order, and which token each refusal gets

The existing checks keep their order. New checks are **bold**. Every refusal is terminal, with
`status_code` and `attempts` unset. `detail` names the header and the reference, and for a hop, its
URL. **It never names the value.**

| Step | Check | Refusal |
|---|---|---|
| 1 `_request_options` (pure, before the ceiling) | existing `headers` guards | `invalid_request_options` |
| | **credential names: the same name guards (token, `REFUSED_HEADERS`, `proxy-*`), folded *with* `headers`, so a name in both maps (E3) or twice by case (E4) is a duplicate; the count bound over the union** | `invalid_request_options` |
| | **E5: `user-agent`, `if-none-match`, `if-modified-since` as a credential name** | `invalid_request_options` |
| | **E1: any credential under `render="browser"`** (D5) | `invalid_request_options` |
| 2 **`_resolve_credentials` (pure, against the frozen table)** | a reference off `fetch-<role>`, never provisioned, or dropped at load | `credential_unknown` |
| | a header name that isn't its binding's `header` (call 3) | `credential_refused` |
| | the byte bound over the union, **resolved values counted** | `invalid_request_options` |
| | the first hop: `command.url` not `https`, not on port 443, or not in every binding's `hosts` | `credential_refused` |
| 3 select the fetcher, ceiling, pace | unchanged | |
| 4 `_fetch` under `command_scope` | **every redirect hop** (D3) | `credential_refused` |

The first-hop check in step 2 duplicates the transport's on purpose. It closes the command before
the ceiling and pacing spend anything, and before a pacer record exists for a request that never
went out. The transport's check is the one that holds on a redirect.

`None` and `{}` mean no credential, and the path is today's.

### D5. `render="browser"` with credentials

`invalid_request_options` (the brief's recommendation). It extends the "only `user-agent` under
`browser`" rule (#63, operator call 1A) to the second map. co-core's E1 gives the reason: a header
under `browser` reaches every host the page contacts, and no per-hop scope exists inside a
browser. It's checked in step 1, before resolution, so a browser command never touches the table.

### D6. The tokens (`FailureReason`, `src/core/errors.py`)

- `CREDENTIAL_UNKNOWN = "credential_unknown"`: no usable binding on this host. The remedy is to
  provision it, or fix the name in Archiver.
- `CREDENTIAL_REFUSED = "credential_refused"`: the binding exists, but this use is outside it. That
  covers a host, scheme or port out of scope, or the wrong header. It's `destination_refused`'s
  shape: pre-flight, or at the hop. The remedy is to fix the policy, or widen the binding on the
  host.

An origin's 401 or 403 when a credential *was* sent stays `http_status`.

### Charter check (`replicator-boundaries.md`)

| Test | Credential references |
|---|---|
| 1. Durable per-resource history? | **No.** A binding is host configuration, read at boot, as the alias table is. No state is added in a new place. Cookies now live *shorter*. |
| 2. Coordination over a resource only the fetcher can see? | **Yes:** the secret, which lives on this host by the charter's secrets rule (cannobserv#562), and the hops, which only the transport sees. |
| 3. Without domain vocabulary? | **Yes.** A header name, a reference and hostnames. |

The config taxonomy holds:
- **env** carries a host fact (the table's path);
- the **command** carries this occasion's reference;
- the **value** never travels.

**The boundary tests mirror §2b ("the alias is a key, never a value"):**
- every `credential_headers` occurrence in `src/` is a lookup key or the argument to the resolver,
  in the modules listed;
- `get_secret_value` is called in the transport alone;
- no payload field feeds a credential-shaped parameter.

The charter text gains a line, and its tests change with it.

## For the operator

Approving the doc approves each recommendation.

1. **The cookie jar: per command (recommended).**
   - **A:** cookies live for one command's redirect chain (D3). Its own commit and test, ahead of
     the credential work.
   - B: never store. Simpler, but it breaks a cookie set on a redirect and expected on the next
     hop, which works today [measured].
   - C: leave the jar as it is. Then a credentialed session is replayed on later anonymous
     commands, outside C2's scope.
2. **The value is read at boot, and rotation is a restart (recommended).** The alternative is
   reading at use (D2).
3. **A binding pins its header name (recommended).**
   - **Why:** any bus writer can name any reference. Without the pin, a command could attach
     `fetch-wa-leg-api`'s value as `X-Debug` or `Referer` to an in-scope URL that echoes request
     headers into its body. The body is stored and announced on `content.blobs`. The pin limits
     the value to the header the operator meant.
   - **Cost:** one field per binding. A command naming another header closes `credential_refused`.
   - **Alternative:** no pin, so the command chooses the header.
4. **Port 443 only (recommended).** `PolicyHost` has no port, and a host-only scope would admit
   `https` on any port of that host. Relaxing this later stays compatible, and tightening it
   wouldn't. The alternative is any port.
5. **When a hop is out of scope and in a refused range, `credential_refused` wins (recommended)**,
   because the pure check runs before DNS. The alternative is putting the guard outermost, so
   `destination_refused` wins.

## Contract changes

- **`docs/contracts/content-fetch-issuer-reference.md`** (Request options, `:30`):
  - a `credential_headers` subsection with C1–C4 as Replicator applies them (D1–D5);
  - the E-shapes this worker closes, and with which token;
  - the support line, added only once a deployed worker resolves references.
- **`docs/contracts/content-fetch-outcome-reference.md`**: the two tokens in the reason list and
  the taxonomy table.
- **`docs/contracts/replicator-boundaries.md`**: the §2b line for credential references.
- **`docs/ENVIRONMENT.md`**: `REPLICATOR_FETCH_CREDENTIALS_FILE`, next to
  `REPLICATOR_REPLICATION_ALIASES_FILE`, with the format and the safe unset posture.
- **`docs/ARCHITECTURE.md`, `docs/STREAMS.md`**: the transport stack and the cookie lifetime,
  where the fetch path is described.
- **`tests/conftest.py`**: the new variable joins `PRODUCTION_ENV`. Its name matches
  `test_every_credential_setting_is_scrubbed`'s pattern, which enforces this.

## Tests (TDD, failing first)

- `tests/worker/test_credentials.py`:
  - the table loader: every row of D1's table drops its entry, and the log names the rule and
    never the value;
  - unset and no-file: nothing provisioned, one INFO;
  - unreadable: nothing provisioned, one ERROR;
  - the mapping is immutable;
  - `repr` and `str` of a binding and the table show no value.
- `tests/worker/test_egress.py` (the transport):
  - a cross-origin redirect after an in-scope first hop raises `credential_refused`, and the inner
    transport never sees the second request;
  - `http`→`https` and `https`→`http` on the same host;
  - a non-443 port;
  - an IDNA host and a trailing dot (`raw_host`);
  - the handed request is unmutated;
  - a `Cookie` credential is restored on a same-origin hop;
  - no scope means refuse;
  - an anonymous scope behaves byte for byte as today;
  - out of scope and in a refused range gives `credential_refused`.
- Cookie lifetime (its own commit):
  - the bounce cookie reaches the next hop;
  - a cookie from command 1 doesn't reach command 2;
  - the client jar stays empty.
- `tests/worker/test_handler*.py`:
  - each row of D4, with the token, `terminal`, unset `status_code`/`attempts`, and a `detail`
    naming the header and reference;
  - **a planted test value is absent from the caplog, from every published fact and from a forced
    traceback.**
- `tests/test_boundaries.py`: the §2b mirror, with planted-violation tests for each detector.
- `-m integration`: a live-broker run with a credentialed frame (`seed_fetch.py --credential-header`).

## PRs and commits (each PR says `Refs #216` and merges with a merge commit)

1. **#229**: the co-core 0.51.1 pin. It owes a deploy.
2. This design doc.
3. The build, on branch `216-fetch-credentials`:
   1. `#216 fix: a cookie jar per command, not per worker`: the behaviour change, alone;
   2. `#216 feat: the fetch credential table`: `credentials.py`, the setting, `PRODUCTION_ENV`;
   3. `#216 feat: credential_unknown and credential_refused`;
   4. `#216 feat: guard the credential headers and resolve them`: D4 steps 1–2;
   5. `#216 feat: scope each credential per hop`: D3;
   6. `#216 test: the reference is a key, the value is the transport's`: the charter and its tests;
   7. `#216 docs: ...`: the contract docs, ENVIRONMENT and ARCHITECTURE.

   **It owes a deploy.** With nothing provisioned, the deploy only changes cookie lifetime.

## Host steps, and verification

**Provisioning is the operator's** (`/etc/replicator`, group `replicator` since #202). The exact
commands go on #216 with the build PR. The shape:
- `/etc/replicator/fetch-credentials/` is `root:replicator 0750`, and each value file in it is
  `root:replicator 0640`, the writer keys' posture;
- the table is `/etc/replicator/fetch-credentials.json`, `root:replicator 0640`;
- `REPLICATOR_FETCH_CREDENTIALS_FILE=` goes in `/etc/replicator/.env`;
- then a restart. `fetch credentials loaded` names the references, and never a value.

**End to end.** The brief asks for it on the deployed service, but **this host's `replicator`
identity can't `XADD` to `content.fetch`** (broker#14, `seed_fetch.py`'s docstring). So:
- **Here (recommended):** the **released** code (`/srv/replicator/live`) runs as a scratch worker
  against a scratch `redis-server`, with a scratch table and a **dummy, non-secret** value. The
  target is a public header-echo endpoint:
  - in scope: the header arrives on every hop;
  - a cross-host redirect: `credential_refused`, and the second host receives nothing;
  - an unknown reference: `credential_unknown`;
  - a `grep` of the scratch worker's output and facts for the value: 0 hits, outside the echoed
    body that was fetched on purpose.
  - The endpoint is `httpbin.org` (`/headers`, `/redirect-to`) unless the operator names one.
    The dummy value is the only thing it receives.
- **On the live service (operator, optional):** one frame under Watcher's identity
  (`seed_fetch.py --production`) against a production binding. Then
  `journalctl -u replicator.service | grep -c <value>` should be 0.
- The deployed service's boot line shows the table loaded.

**The announcement** on cannobserv#577 is drafted on #216 after the scratch run, and posted only
with the operator's consent. The issue closes after that.

## Accepted limits

- **Reflection.** An in-scope endpoint that echoes its credential header into the body stores the
  value in a blob. The header pin (call 3) narrows this to the intended header. Only the binding's
  hosts are trusted to keep it.
- **Process memory.** The value lives in the worker's memory from boot. A core dump or `ptrace`
  by the `replicator` user can read it, as it can the writer keys.
- **Cross-command cookies.** A source that relied on them changes behaviour (call 1). That's
  unmeasured.
- **Chromium's parent process** runs as `replicator` and can read the value files, as it can read
  the keys. The renderer sandbox stays on, and no credential is ever sent under `browser`.

## Verification debt (expected)

- No live credentialed frame until Watcher carries the field, which waits on the announcement.
- No measurement of production's dependence on today's cross-command jar.
- `ty` runs advisory only.
