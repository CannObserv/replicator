# The `content.replicate` issuer contract

**Status: shipped for `gcs`.** `src/` runs a `content.replicate` loop that resolves the alias,
guards both paths, writes through `AsyncGcsDriver.create_if_absent`, and emits both facts. All six
refusals are reachable. `gdrive` and `ia` have no conditional create yet and are refused
`provider_disabled` — the same path a host with no binding takes.

Passages marked **⚙** were obligations on the implementation when this document was settled ahead of
the code (#34). Most are now claims about it, and the ones still outstanding are marked **⚙ pending**:
they all sit behind the first provider writer. The original bet stands — the trust model decided
whether the payload grows a credential field, and that was not a contract to re-cut after two
adopters had built.

| ⚙ obligation | State |
|---|---|
| T1 — no credential travels, and none reaches the journal | shipped; `AliasBinding` has nowhere to hold one, asserted structurally |
| T2 — an alias resolves only if provisioned on this host | shipped (`REPLICATOR_REPLICATION_ALIASES_FILE`) |
| T3 — containment, and the path guard | shipped for `gcs`; the `gdrive`/`ia` rows arrive with those providers |
| T3a — resolve by fingerprint, never by path | shipped |
| T4 — the absent/matching/differing table | shipped for `gcs`, and verified against the live bucket |
| T5 — `ia` gated on an operator act | shipped by construction: `ia` cannot be provisioned at all yet |
| T6 — `public_url` never echoed from the command | shipped, **reworded** — see below (#36) |
| Charter — the alias is a key, never a value | shipped, plus a second scan that no payload field feeds a credential parameter |

**Audience:** Archiver, the sole issuer ([archiver#137](https://github.com/CannObserv/archiver/issues/137) step 5).

**Companion.** This states only the **deltas** from
[`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md) and its
[reference](content-fetch-issuer-reference.md). Both remain normative here. The MUST verdict table
below says which of the seven fetch obligations apply verbatim — read the deep document for any row
that does not say *no analogue*, because a thin delta doc whose reader never opens the deep one is
the failure mode this shape trades against.

**Sibling.** [`replicator-boundaries.md`](replicator-boundaries.md) settles what Replicator may
become. Its three tests decide the destination-authority question below, and it is the reason the
RepSpec's *resolution* half never reaches this service.

---

## Why this is not the fetch capability widened

[Provenance and trust](content-fetch-issuer-reference.md#provenance-and-trust) settles `content.fetch`
as an unauthenticated capability whose integrity rests entirely on bus access control, and settles it
well. Its load-bearing sentence is that the damage is bounded by what a **read** can do. Replication
removes that bound, so the conclusion is reached again rather than inherited:

| | `content.fetch` | `content.replicate` |
|---|---|---|
| Direction | **read** an arbitrary origin | **write** our own permanent stores |
| Credentials | none, or issuer-supplied `headers` | **the operator's**, selected by an alias the message names |
| Blast radius | one request from our VM; bytes in temp storage | objects in our GCS bucket / Drive / archive.org item |
| Self-healing | yes — the TTL sweep reclaims it | **no** — a durable artifact is the point |
| Reversible | yes | gcs/gdrive yes; **`ia` items cannot be deleted at all** ([IAS3](https://archive.org/developers/ias3.html): "DELETE bucket is not allowed") |

Every archive.org claim below is read from that [IAS3 API documentation](https://archive.org/developers/ias3.html) — cited because T4 and T5 rest on it, and because it corrects the intuition rather than confirming it.

The conclusion is the same — one localhost broker on one trusted VM, therefore proportionate — but
the **escalation trigger is not**, and that is the whole reason this section exists rather than a
cross-reference.

---

## The trust model

### T1 — No credential ever travels

`credentials_alias` is a **selector, not a secret**. It names a binding that exists on the host or it
names nothing. All three providers resolve locally:

| Provider | Local resolution |
|---|---|
| `gcs` | ADC — a service-account key at `GOOGLE_APPLICATION_CREDENTIALS`, the mechanism this VM already uses for the wheelhouse mirror |
| `gdrive` | service account, plus a Shared Drive membership **or** domain-wide delegation. A bare SA owns no usable My Drive quota, so the alias binding is a provisioning precondition, not just a key file |
| `ia` | an archive.org keypair, sent as `Authorization: LOW <accesskey>:<secret>` (IAS3). Read from host config or `IA_ACCESS_KEY`/`IA_SECRET_KEY` — never from the message |

**Therefore the co-core payload does not grow a credential field**, and cannobserv#303 open question 1
is answered *no secret is forced onto the wire*. ⚙ When a provider lands that cannot resolve locally,
it is refused rather than accommodated — see the escalation triggers.

Two properties the issuer can rely on, mirroring the fetch guarantees: a refused command is refused
**before** any credential is touched, and no credential value reaches the journal.

### T2 — The alias namespace is a capability namespace

There is no issuer identity on the frame, so any bus writer can name any alias, and an alias is
therefore **all-or-nothing**. Under one localhost broker with one writer that is proportionate — the
same argument the fetch document makes, and it survives the change of direction only because the set
of writers is one.

⚙ It is bounded anyway, the cheap way, exactly as the request-options refusal table bounds `headers`:
**an alias resolves only if the operator provisioned it on this host.** The provisioned set is a fact
about *this host* and lives in env-referenced host config, where a command cannot reach it — the
[config taxonomy](replicator-boundaries.md#config-taxonomy)'s first row. An unprovisioned alias is a
terminal refusal before any provider client is constructed. That converts "any writer names any
alias" into "any writer names any alias the operator already stood up," which is a smaller set than
the schema's `{"type": "string", "minLength": 1}`.

This is the cheap part, taken because it is cheap. It is **not** a substitute for the trust model.

### T3 — Destination authority: the issuer renders, the host contains

Two halves, and they are separate decisions.

**The rendered path comes from the issuer.** The RepSpec's `path_template` and `required_fields` are
resolved by Archiver against the InfoItem's `rep_fields` bag, and the **rendered** destination
travels. Replicator receives strings; it never interpolates, never sees a `{ns.field}` placeholder,
and never learns the vocabulary those placeholders are drawn from. Rationale, alternatives, and what
this costs: [**Why the issuer renders**](../plans/2026-08-14-why-the-issuer-renders-settled.md).

**The container comes from the host.** ⚙ The alias binds the destination root, and the message cannot
override it:

| Provider | Host-bound root | Containment check |
|---|---|---|
| `gcs` | bucket (+ optional prefix) | rendered key is under the prefix |
| `gdrive` | base folder id | traversal resolves under it, never above |
| `ia` | identifier prefix + allowed collections | identifier starts with the prefix; archive.org's namespace is global and unrooted, so a **prefix** is the only containment there is |

**This is where the current schema leaks.** In **archiver**,
[`src/core/rep_spec_schema/providers/gdrive/v1.json`](https://github.com/CannObserv/archiver/blob/main/src/core/rep_spec_schema/providers/gdrive/v1.json)
carries `folder_id` and its `ia` sibling carries `collection` — both are *destination containers
named by the message*. The `gcs` sibling correctly carries none: the bucket is injected at runtime by
**cannobserv**'s `StorageConfig.provider(...)`. ⚙ Replicator treats those two as **selectors validated against the
alias's allowed set**, never as authority. Worth raising on cannobserv#303: gcs got this right and
the other two did not, and the asymmetry is invisible until something writes.

⚙ On top of containment, the rendered path is refused for traversal segments (`..`), a leading `/` or
a drive qualifier, backslashes, NUL or control characters, empty segments, and any form that is not
already normalized — checked after percent-decoding, and checked on the rendered string rather than
on a template. For `content.fetch` this surface did not exist: blob paths are content-addressed off
the fingerprint ([`src/storage/local.py`](../../src/storage/local.py)), so no message value ever
reached a path. Replication is the first time one does.

### T3a — the *source* is a path too, and it is the sharper half (#29)

The paragraph above stated the read side has no such surface. That is half the picture: replication
is the first time a message value reaches a path **in both directions**, because
`ContentReplicateCommand.blob_uri` is issuer-supplied and serving the command means resolving it to
local bytes. Nothing in `src/` consumes one today — it is produced only, and
[`BlobStore`](../../src/storage/base.py) has no URI-resolving method (`open()` / `exists()` take a
fingerprint). The resolver is new code, and its obvious implementation — parse the URI, read the path
— is a read-side traversal on a service whose destinations include a **public, undeletable**
archive.org item: `file:///etc/replicator/co-pypi-reader.json` would publish this VM's GCS reader key
permanently. Bus access control answers it as everywhere else, and T5's reasoning applies on top —
one guard against an unretractable failure.

⚙ **The rule: never resolve `blob_uri` as a path.** Extract the fingerprint, validate it as 64
lowercase hex, rebuild through the store's content-addressed mapping. Traversal-proof by
construction rather than by enumerating what to reject — the inversion `tests/test_boundaries.py`'s
echo scan reached after three rounds of an incomplete deny-list. Anything else is a terminal
`invalid_source`. This also keeps [#7](https://github.com/CannObserv/replicator/issues/7) honest: the
scheme is deliberately "the consumer's business", so an object-store backend re-states this guard
rather than inheriting a path-shaped one.

### T4 — Overwrite: a redelivery must never destroy an artifact

Delivery is at-least-once, so the same command *will* arrive twice. This is where the idempotency
question and the trust question are one question.

⚙ **The rule:**

| At the destination | Action |
|---|---|
| absent | write, emit `replication_complete` |
| present, bytes match | **no-op** — re-emit the same fact with the same `public_url` |
| present, bytes differ | terminal `destination_conflict`. Do not overwrite |

Make the rendered path deterministic from the occasion — the RepSpec assignment, the revision, and
the content fingerprint — so a redelivery targets the same key with the same bytes and lands in row
two. This is a property of *path design*, available because the issuer renders (T3): Archiver knows
the revision timestamp; Replicator does not.

**Per-provider mechanics, and one correction worth recording.** The intuition that `ia` is inherently
append-only describes the **Wayback Machine** — captures keyed URL+timestamp, unoverwritable, and
worth wanting. It is not what `ia` means here: the RepSpec's `collection`/`mediatype`/`license` are
archive.org **item** fields, and per IAS3 a PUT to an existing key **overwrites by default**. So the
rule above applies to `ia` as much as to the others.

| Provider | Create-if-absent primitive |
|---|---|
| `gcs` | `ifGenerationMatch=0` — atomic. On 412, compare md5 to choose row two or row three |
| `gdrive` | `get_files_by_name(name, parent_id)` then compare `md5Checksum` — the method already exists on cannobserv's `GoogleDriveAdapter` |
| `ia` | `upload(..., checksum=True)` skips when the remote md5 matches. Unreliable while tasks are pending on the item ([jjjake/internetarchive#289](https://github.com/jjjake/internetarchive/issues/289)), so pair it with `x-archive-keep-old-version:1` as the recoverable-overwrite backstop |

**If Wayback semantics are wanted, that is a fourth provider, not a mode of `ia`.** Save Page Now
takes a URL and fetches the origin itself — it consumes no blob, needs no `blob_uri`, and its
`public_url` is timestamped per capture, so a redelivery yields a *different* citable URL unless
deduped by its own window parameter. Every row of this table would differ. Out of scope here; named
so the `ia` sub-schema is not stretched to cover it later.

### T5 — `ia` is public and permanent

A replication to archive.org publishes under the organization's identity, and an item cannot be
deleted — IAS3 permits file DELETE and forbids bucket DELETE. A defensible "yes, broker access is the
gate" would be acceptable; the answer here is **yes, plus one host-side gate**, because the cost is
one setting and the failure is unretractable.

⚙ The gate is already built by T2: no provisioned `ia` alias on this host means every `ia` command is
a terminal refusal. Enabling IA replication is therefore an explicit operator act on the VM, not a
consequence of a message arriving. First cut writes into a dedicated collection.

### T6 — The completion fact selects a registry value

Archiver writes `public_url` onto `info_item_rep_specs` from `replication_complete`
([archiver#143](https://github.com/CannObserv/archiver/issues/143)). This is the first time an
unauthenticated bus fact chooses a **user-visible, citable URL** in the registry. The answer is bus
access control, same as everything else — a new consequence, not a new mechanism.

**The original wording does not survive contact with `gcs`, and the corrected one is narrower and
true (#36).** It read: *"`public_url` is derived from the provider's response, never echoed from the
command."* The second half holds everywhere. The first half does not: `Blob.public_url` is a
client-side f-string over `api_endpoint + bucket + quoted_name` and never round-trips — it returns a
well-formed URL for an object that was never written. The verdict is not even uniform across the
three providers: `gdrive`'s `webViewLink` *is* response-minted (Drive mints the file id), while
`ia`'s is formatted from the identifier the command named. A promise written at the
provider-response level would be false for two of the three.

⚙ **What is promised instead, and it is the part that was load-bearing all along:**

1. **`public_url` is never echoed from the command.** Nothing on `ContentReplicateCommand` names a
   URL, so there is nothing to echo.
2. **It is present only where the object is known to exist** — a successful write, or a confirming
   read that found it. `GcsCreateResult.public_url` is `None` on every other path, so a fact
   carrying a URL is a fact about an object that is there.
3. **It always names a location inside a container the operator provisioned**, because the root is
   host-bound to the alias and the key is the rendered `destination` the T3 guards already refused
   for traversal, absolute forms and control characters.

The threat T6 named — a bus writer dictating the registry's citable URL — is answered by (1) and (3)
together. Provenance of the string was a proxy for that, and a lossy one.

### Escalation triggers — this capability's own

The fetch document's trigger is "the moment the bus spans hosts or tenants." Replication's fire
**earlier**, and there are three:

1. **A second service gains write access to `content.replicate`.** All-or-nothing aliases stop being
   proportionate the moment more than one writer exists, because the operator loses the ability to
   say *which* writer may use *which* alias. That needs issuer identity on the frame — signed
   envelopes or a per-issuer credential — and it does not exist today.
2. **The broker leaves localhost, or the worker fleet shards across hosts.** Alias resolution becomes
   remote at that point. The answer is workload identity — a per-host service account with its own
   IAM binding — **not** a credential on the wire. Stated explicitly because "just put a token in the
   payload" is the shape this failure mode reliably takes, and T1 is the line it crosses.
3. **A provider is proposed that cannot resolve its credential locally.** Refuse the provider; do not
   widen the payload.

None of the three is met today. Message signing and per-alias grants are the conversation when one
is, and not before.

---

## Why the issuer renders

The decision in T3, its alternatives, and what it costs — the two rejected shapes, what the chosen
one gives up, and the consequences for cannobserv#303 and the shared renderer — are recorded in
[`docs/plans/2026-08-14-why-the-issuer-renders-settled.md`](../plans/2026-08-14-why-the-issuer-renders-settled.md).
Settled; not re-litigated here.

---

## What the issuer MUST do

Verdicts on the seven fetch obligations. A row that is not *no analogue* is read in the deep
document, not summarized here.

| Fetch MUST | Verdict for replicate |
|---|---|
| **1** — fresh `command_id` per *occasion*, never per resource | **verbatim.** Same dedupe key, same TTL, same TTL-bounded intermittency. A `command_id` derived from `(rep_spec_id, info_item_id)` breaks re-replication of a changed item exactly as a URL-derived id breaks re-fetch |
| **2** — persist `command_id → domain` before publishing | **verbatim** |
| **3** — correlate on `command_id` only | **adapts.** `url` has no analogue; the trap it warns against does: the **destination path is not a key** either. Two RepSpecs can render one path, and one RepSpec renders many over time |
| **4** — correlation is idempotent, one command can yield many facts | **verbatim**, and load-bearing here: T4's no-op row deliberately re-emits a fact for an artifact already written |
| **5** — do not dedupe facts on `content_fingerprint` | **no analogue** |
| **6** — handle the failure fact, keep a reaper anyway | **verbatim.** Non-terminal failures are still silent; a command can still close without a fact |
| **7** — copy the bytes before the blob expires | **inverts.** For fetch this is the consumer's obligation; for replicate it is the issuer's *scheduling* obligation. Issue while the blob lives — the clock runs from last **fetch** reference, not last read — and handle `blob_expired` as terminal. `blob_expires_at` on the `blob_available` fact is the value to schedule against. [#7](https://github.com/CannObserv/replicator/issues/7) changes this calculus |

⚙ **R1 — render, do not delegate.** Resolve `path_template` against `rep_fields` before publishing,
and publish the result. A command carrying an unrendered template is refused.

⚙ **R2 — make the destination deterministic per occasion.** T4's no-op row only works if a redelivery
renders the same key. Include the discriminator that makes each replication occasion distinct — the
revision, its timestamp, or the fingerprint — rather than relying on a provider to be append-only.

⚙ **R3 — do not treat `public_url` as stable across occasions.** Each replication occasion yields its
own artifact and its own URL; the registry row records which.

---

## What Replicator refuses

⚙ All refusals are terminal, all happen before any credential is touched, and each carries a `reason`
token on the failure fact. Following the precedent in
[`src/core/errors.py`](../../src/core/errors.py), the vocabulary is producer-owned — co-core types
`reason` as a plain `str` — so **none of these needs a contract change**.

| Condition | `reason` |
|---|---|
| the alias is not provisioned on this host | `alias_unknown` |
| the provider is not enabled on this host (T5), or the host's credential cannot write there | `provider_disabled` |
| the rendered path escapes the alias root, or `object_options` names a container the alias does not allow | `invalid_destination` |
| the destination exists with different bytes | `destination_conflict` |
| the blob is gone | `blob_expired` |
| `blob_uri` is not a reference this store minted (T3a) | `invalid_source` |

`invalid_destination` covers both the path guard and the container guard for the reason
`invalid_request_options` covers two fields on the fetch side: the issuer's remedy is identical
either way — fix the spec and re-issue under a fresh `command_id` — and `detail` carries which guard
refused it. `alias_unknown` and `provider_disabled` stay separate because their remedies are not the
same one (fix the spec; act on the host).

**A provider 4xx closes the command, and it mostly lands on `provider_disabled`.** The write itself
can fail in ways no pre-flight guard can see — the host's credential lacks create permission on the
bound bucket (403), the bucket named by the binding no longer exists (404), the provider rejects an
`object_options` value outright (400). The first two are refused `provider_disabled`, which widens
that row past its original T5 reading of "nobody turned it on": the observable state is now "this
host cannot write there", and the remedy is the same operator act either way. A 400 is
`invalid_destination`, because what the provider rejected came off the command. **Everything else —
5xx, 429, 408, and any failure carrying no status at all — leaves the command open**, because T4
makes retrying the write safe. Those publish no fact at all while they retry, and the retry is
**unbounded**: a transient failure is exempt from the delivery ceiling, so a permanently-broken
provider retries until an operator intervenes rather than eventually dead-lettering into a fact.
That is precisely the silence MUST-6's reaper exists for — do not wait on a fact that may never
come.

`invalid_source` stays separate by the same test: not `blob_expired` (the bytes were never named, so
re-fetching fixes nothing) and not `invalid_destination` (the remedy is a bug in the issuer's
plumbing, not a bad RepSpec). It is a sixth token where co-core 0.9.4's `ReplicationFailedEvent`
docstring registers five, so that docstring needs a matching entry —
[cannobserv#330](https://github.com/CannObserv/cannobserv/issues/330). `reason` is a plain `str`, so nothing on the wire breaks meanwhile; the cost is a
consumer-facing registry one row short, the drift that docstring asks to be kept in step.

---

## Charter check

⚙ **No new vocabulary invariant from `required_fields`.** #34's Q7 asks about a collision between
`required_fields`' dotted domain keys (`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`, e.g. `info_item.slug`)
and the no-domain-vocabulary scan, which bans those words in `src/` as identifiers *and* as string
literals. Under T3 that collision does not arise: the dotted keys never reach this service. Recorded
as a **consequence of the render decision** — adopting the rejected alternative reopens it, and would
require the render path to treat every key as opaque with no prefix ever special-cased.

⚙ **A second exemption is needed anyway, and this document predicted otherwise (#29).** The sentence
above originally read "no new vocabulary invariant, **and no exemption**". The first half holds; the
second does not. co-core 0.9.4 makes `info_item_rep_spec_id` required on `ContentReplicateCommand`
and on **both** replicate facts, and it carries the `info_item` token — so the vocabulary scan fails
the moment the emit path names it, which no implementation style avoids because the model requires
the field. The prediction was not wrong about what it examined; the field entered the payload after
#34 was settled, which is the standing hazard of settling a contract ahead of the models it
describes.

Granted on exactly `info_source_id`'s terms and no wider, with the arithmetic and the cross-wiring
rule pinned by their own tests. The charter is the authoritative record:
[**replicator-boundaries.md**](replicator-boundaries.md#reviewing-a-proposed-payload-field). Note
what stays refused — `info_item_id`, the *real* domain key, is one underscore-separated step away and
holding a table of them is precisely the domain model the charter exists to prevent.

⚙ **One new invariant when the code lands: the alias is a key, never a value.** An AST scan asserting
every `credentials_alias` occurrence is a lookup key or a resolver argument — the mirror of the
existing scan that keeps `info_source_id` echoed and never interpreted — plus the assertion that no
payload field feeds a provider client's credential.

**Unaffected:** *no locally-defined wire models* (Replicator declares none; under T3 the RepSpec
resolution half does not travel, so there is nothing here tempted to model it), *no database* (alias
bindings are host config read into memory — no per-resource history, rebuildable from the file), and
*ingress is read-only* (no new surface).

---

## Deliberately open

Settled in a cannobserv `docs/plans/` design doc alongside #303, not here:

- **Fan-out** — one command per (revision, RepSpec) with independent `command_id`s, or one command
  carrying a list. Per-spec is probably right, for MUST-1's reason.
- **Blob lifetime** — whether an expired blob terminates or triggers a re-issued fetch, and how that
  sequences against [#7](https://github.com/CannObserv/replicator/issues/7).

---

## Where the rest of the contract lives

- [`content-fetch-issuer-contract.md`](content-fetch-issuer-contract.md) and its
  [reference](content-fetch-issuer-reference.md) — **both normative here.** The frame, the MUSTs in
  full, the failure taxonomy, the silent conditions, the DLQ, and the fetch trust model this document
  departs from.
- [`replicator-boundaries.md`](replicator-boundaries.md) — the three tests, the config taxonomy, and
  the payload-field review question (#12).
- [`2026-08-14-why-the-issuer-renders-settled.md`](../plans/2026-08-14-why-the-issuer-renders-settled.md)
  — historical: T3's rejected alternatives, what the chosen shape costs, and the consequences for
  cannobserv#303.
- [cannobserv#303](https://github.com/CannObserv/cannobserv/issues/303) — the models. Open question 1
  is answered by **T1**.
