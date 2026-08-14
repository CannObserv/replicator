# Why the issuer renders: how T3's first half was decided

**Status:** settled with T3 (#34), relocated 2026-08-14 in #35 so the contract states the shipped
rule and this states the alternatives it was chosen over.
Relocated out of [`docs/contracts/content-replicate-issuer-contract.md`](../contracts/content-replicate-issuer-contract.md)
— the same split #24 made for the `fetch_failed` open question.

The current contract is
[T3](../contracts/content-replicate-issuer-contract.md#t3--destination-authority-the-issuer-renders-the-host-contains).

---

## Why the issuer renders

The decision in T3, its alternatives, and what it costs.

**Rejected — the wire carries `path_template` + the `rep_fields` bag, and Replicator renders.** This
is what cannobserv#303 currently implies. It survives the letter of
[charter test 3](../contracts/replicator-boundaries.md#the-three-tests) by the `info_source_id` precedent — blind
`format_map` substitution is mechanical, not interpretation — but it spends that precedent, and it
loses on three concrete grounds:

- **Failure locality.** A `required_fields` entry with no value in the bag is an Archiver *data*
  problem. Archiver already detects it synchronously at assignment
  (archiver's `src/core/tools/assign_rep_spec.py` validates `rep_fields` against `required_fields`,
  returning errors to the caller). Moving the render to Replicator converts that into a failure fact
  arriving asynchronously on a different service, where the remedy is not.
- **Provenance.** `InfoItemRepSpec` is effective-dated and asserts "item X replicated under spec R,
  producing the artifact at `public_url`" — which is why an assigned RepSpec's document is *frozen*
  (archiver's `src/core/tools/update_rep_spec.py`, tier 3: "the artefact in the provider bucket was
  written under the old `path_template` and nothing records what it was"). A rendered path on the
  wire records exactly what was used, at the moment it was used. A template re-rendered downstream
  does not.
- **One normalizer, not two.** The `cannobserv.storage` package already produces this value with zero
  I/O, and slug derivation lives inside it (`util.normalize_string`, the `<raw>` + `<raw>_slug` rule
  codified in **cannobserv**'s `docs/STORAGE_VARS.md` — not this repo's
  [docs/STORAGE.md](../STORAGE.md), which is the blob tree). A second implementation here would not
  fail loudly when it drifted —
  it would write to a *different path*, silently, which is the worst available failure.

**Also rejected — the template lives in host config per alias, values come from the message.** The
strongest containment posture: the operator fixes the layout and the message only fills variables.
Rejected because it duplicates layout knowledge on every host and desynchronizes the moment a RepSpec
adds a location shape. T3's containment check retains the part of this that matters.

**What the chosen shape costs, stated plainly.** Replicator stops being able to enforce layout
*policy* — its only lever is the alias root, and within that root the issuer decides everything.
Accepted: the root is the boundary that bounds blast radius; the layout inside it does not.

**Consequences for cannobserv#303.** The command carries `provider`, `credentials_alias`, the
rendered destination, and `object_options`. It does **not** carry `path_template` or
`required_fields` — the resolution half of the RepSpec document stays issuer-side. That is a
simplification of the payload as currently sketched, not an addition.

**Consequence for the shared library.** If the render is hoisted out of Archiver, hoist the
**renderer**, not the schema — `cannobserv.storage`'s `format_map` + required-namespace pre-check (in
its `storage/provider.py`) is
the piece with years of use behind it. The RepSpec schema can be published as a standard on its own
timeline; it has no reason to become a Replicator dependency, and under this decision it never does.
