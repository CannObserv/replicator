# The `content.replicate` issuer reference

**Status:** normative, and a companion to
[`content-replicate-issuer-contract.md`](content-replicate-issuer-contract.md). Everything here binds
Replicator's behaviour exactly as the contract does — this file is not commentary.

**What is here rather than there.** The contract carries the clauses an issuer builds against —
T1–T6, R1–R3, the MUST verdicts and the refusals. This file carries the reasoning several of them
rest on and the corrections recorded against them, each under the clause it expands — plus the
escalation triggers, the Charter check and what is deliberately open, whole — so the
contract stays short enough to read start to finish — the shape the `content.fetch` contract and
its two references already have.

---

## Why this is not the fetch capability widened

Expands the contract's [section of the same name](content-replicate-issuer-contract.md#why-this-is-not-the-fetch-capability-widened).

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

Every archive.org claim in the contract is read from that [IAS3 API documentation](https://archive.org/developers/ias3.html) — cited because T4 and T5 rest on it, and because it corrects the intuition rather than confirming it.

The conclusion is the same — bus access control is proportionate to this capability — but
the **escalation trigger is not**, which is why the contract reaches the conclusion again rather than
cross-referencing the fetch reference.

**The premise it originally rested on is gone, and the conclusion survives it anyway (#89).** That
premise was "one localhost broker on one trusted VM". The broker has run on its own node since
CannObserv/broker#1 and Replicator on another since #88, so what bounds the writers is now
per-service Redis ACL users (CannObserv/broker#2) and the Tailscale ACL admitting only bus
participants. Restated rather than quietly left standing, because a trust argument whose stated
premise is false is worse than no argument: the next reader cannot tell which half to re-derive.

## Escalation triggers — this capability's own

The fetch document's trigger is "the moment the bus spans hosts or tenants." Replication's fire
**earlier**, and there are three:

1. **A second service gains write access to `content.replicate`.** All-or-nothing aliases stop being
   proportionate the moment more than one writer exists, because the operator loses the ability to
   say *which* writer may use *which* alias. **Met, in capability, as of CannObserv/broker#14's
   measurement** — `replicator`'s own grant reaches the stream it consumes. Answered by that
   issue's per-service selectors rather than by issuer identity on the frame (T2), because a grant
   that is wider than declared is a broker fact, not a wire fact.
2. **The broker leaves localhost, or the worker fleet shards across hosts.** Alias resolution becomes
   remote at that point. The answer is workload identity — a per-host service account with its own
   IAM binding — **not** a credential on the wire. Stated explicitly because "just put a token in the
   payload" is the shape this failure mode reliably takes, and T1 is the line it crosses.
   **Met since CannObserv/broker#1 and #88**, and the wire is unchanged by it: every alias still
   resolves locally, so T1 holds as written. What it leaves is a provisioning question rather than a
   protocol one — whether this node's binding is its own service account or one it shares — and that
   is the operator's to confirm per host.
3. **A provider is proposed that cannot resolve its credential locally.** Refuse the provider; do not
   widen the payload. Not met.

**Two of the three are now met (#89), and neither answer is on the wire.** Trigger 1 is a broker
grant to narrow; trigger 2 is a provisioning fact to confirm. No payload field, no signature, no
credential travels — which is the outcome T1 exists to protect and the reason these triggers were
written down before they fired. Message signing becomes the conversation if a writer that *holds* a
legitimate grant is compromised, and not before.

**The fetch document's destination guard (#95) does not extend here.** A replicate destination is
host-bound by the alias (T3) rather than named by the issuer, so there is no address for a guard to
refuse; the containment check is the alias's root, and it already runs.

## Why T3a exists: the source is a path too

Expands T3a in [the contract](content-replicate-issuer-contract.md).

T3's last paragraph states the read side has no such surface. That is half the picture: replication
is the first time a message value reaches a path **in both directions**, because
`ContentReplicateCommand.blob_uri` is issuer-supplied and serving the command means resolving it to
local bytes. [`locate_blob`](../../src/worker/replicate.py) is the one consumer, and
`BlobStore` (`co_core.pure.util.blobstore`, since #114) has no URI-resolving method (`open()` / `exists()` take a
fingerprint), by design. The obvious implementation — parse the URI, read the path
— is a read-side traversal on a service whose destinations include a **public, undeletable**
archive.org item: `file:///etc/replicator/co-pypi-reader.json` would publish this VM's GCS reader key
permanently. Bus access control answers it as everywhere else, and T5's reasoning applies on top —
one guard against an unretractable failure.

**Two stores, the same comparison (#114).** With a permanent store configured, `locate_blob` asks
each store to derive the URI for the extracted fingerprint and accepts only an exact match. The
permanent store therefore widens T3a by one bucket and prefix, not to every `gs://` URI, and the bytes
are read from whichever store matched.

## T4: `ia` overwrites, and Wayback would be a fourth provider

Expands T4 in [the contract](content-replicate-issuer-contract.md).

**Per-provider mechanics, and one correction worth recording.** The intuition that `ia` is inherently
append-only describes the **Wayback Machine** — captures keyed URL+timestamp, unoverwritable, and
worth wanting. It is not what `ia` means here: the RepSpec's `collection`/`mediatype`/`license` are
archive.org **item** fields, and per IAS3 a PUT to an existing key **overwrites by default**. So the
T4 rule applies to `ia` as much as to the others.

**If Wayback semantics are wanted, that is a fourth provider, not a mode of `ia`.** Save Page Now
takes a URL and fetches the origin itself — it consumes no blob, needs no `blob_uri`, and its
`public_url` is timestamped per capture, so a redelivery yields a *different* citable URL unless
deduped by its own window parameter. Every row of T4's provider table would differ. Out of scope here; named
so the `ia` sub-schema is not stretched to cover it later.

## T6: why the first wording was narrowed

Expands T6 in [the contract](content-replicate-issuer-contract.md).

**The original wording does not survive contact with `gcs`, and the corrected one is narrower and
true (#36).** It read: *"`public_url` is derived from the provider's response, never echoed from the
command."* The second half holds everywhere. The first half does not: `Blob.public_url` is a
client-side f-string over `api_endpoint + bucket + quoted_name` and never round-trips — it returns a
well-formed URL for an object that was never written. The verdict is not even uniform across the
three providers: `gdrive`'s `webViewLink` *is* response-minted (Drive mints the file id), while
`ia`'s is formatted from the identifier the command named. A promise written at the
provider-response level would be false for two of the three.

## Charter check

⚙ **No new vocabulary invariant from `required_fields`.** #34's Q7 asks about a collision between
`required_fields`' dotted domain keys (`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`, e.g. `info_item.slug`)
and the no-domain-vocabulary scan, which bans those words in `src/` as identifiers *and* as string
literals. Under T3 that collision does not arise: the dotted keys never reach this service. Recorded
as a **consequence of the render decision** — adopting the rejected alternative reopens it, and would
require the render path to treat every key as opaque with no prefix ever special-cased.

⚙ **A second exemption is needed anyway (#29).** co-core 0.9.4 requires `info_item_rep_spec_id`, and
it carries the `info_item` token; why the contract once predicted otherwise: [the reference](content-replicate-issuer-reference.md#the-exemption-the-charter-check-did-not-foresee).

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

## The exemption the Charter check did not foresee

Expands the contract's [Charter check](content-replicate-issuer-contract.md#charter-check).

**A second exemption is needed anyway, and the contract predicted otherwise (#29).** The sentence
in the Charter check originally read "no new vocabulary invariant, **and no exemption**". The first half holds; the
second does not. co-core 0.9.4 makes `info_item_rep_spec_id` required on `ContentReplicateCommand`
and on **both** replicate facts, and it carries the `info_item` token — so the vocabulary scan fails
the moment the emit path names it, which no implementation style avoids because the model requires
the field. The prediction was not wrong about what it examined; the field entered the payload after
#34 was settled, which is the standing hazard of settling a contract ahead of the models it
describes.

## Deliberately open

Settled in a cannobserv `docs/plans/` design doc alongside #303, not here:

- **Fan-out** — one command per (revision, RepSpec) with independent `command_id`s, or one command
  carrying a list. Per-spec is probably right, for MUST-1's reason.
- **Blob lifetime** — whether an expired blob terminates or triggers a re-issued fetch. #7 settled
  the half that was Replicator's: the window is now a stated commitment with an auditable
  mechanism behind it (the contract's MUST-7 window). What remains open is cluster-level and no single service can decide
  it — whether *anything* turns "a replication observed an expired blob" into a re-fetch, given
  that the replicate issuer is not a fetch issuer and the bus edge between them carries
  announcements, not requests.
