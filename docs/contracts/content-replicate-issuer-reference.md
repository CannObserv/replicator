# The `content.replicate` issuer reference

**Status:** normative, and a companion to
[`content-replicate-issuer-contract.md`](content-replicate-issuer-contract.md). Everything here binds
Replicator's behaviour exactly as the contract does — this file is not commentary.

**What is here rather than there.** The contract carries the clauses an issuer builds against —
T1–T6, R1–R3, the MUST verdicts and the refusals. This file carries the reasoning several of them
rest on and the corrections recorded against them, each under the clause it expands, so the
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
the **escalation trigger is not**, and that is the whole reason this section exists rather than a
cross-reference.

**The premise it originally rested on is gone, and the conclusion survives it anyway (#89).** That
premise was "one localhost broker on one trusted VM". The broker has run on its own node since
CannObserv/broker#1 and Replicator on another since #88, so what bounds the writers is now
per-service Redis ACL users (CannObserv/broker#2) and the Tailscale ACL admitting only bus
participants. Restated rather than quietly left standing, because a trust argument whose stated
premise is false is worse than no argument: the next reader cannot tell which half to re-derive.

## Why T3a exists: the source is a path too

Expands T3a in [the contract](content-replicate-issuer-contract.md).

T3's last paragraph states the read side has no such surface. That is half the picture: replication
is the first time a message value reaches a path **in both directions**, because
`ContentReplicateCommand.blob_uri` is issuer-supplied and serving the command means resolving it to
local bytes. [`locate_blob`](../../src/worker/replicate.py) is the one consumer, and
[`BlobStore`](../../src/storage/base.py) has no URI-resolving method (`open()` / `exists()` take a
fingerprint), by design. The obvious implementation — parse the URI, read the path
— is a read-side traversal on a service whose destinations include a **public, undeletable**
archive.org item: `file:///etc/replicator/co-pypi-reader.json` would publish this VM's GCS reader key
permanently. Bus access control answers it as everywhere else, and T5's reasoning applies on top —
one guard against an unretractable failure.

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

## The exemption the Charter check did not foresee

Expands the contract's [Charter check](content-replicate-issuer-contract.md#charter-check).

⚙ **A second exemption is needed anyway, and the contract predicted otherwise (#29).** The sentence
in the Charter check originally read "no new vocabulary invariant, **and no exemption**". The first half holds; the
second does not. co-core 0.9.4 makes `info_item_rep_spec_id` required on `ContentReplicateCommand`
and on **both** replicate facts, and it carries the `info_item` token — so the vocabulary scan fails
the moment the emit path names it, which no implementation style avoids because the model requires
the field. The prediction was not wrong about what it examined; the field entered the payload after
#34 was settled, which is the standing hazard of settling a contract ahead of the models it
describes.
