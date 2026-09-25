---
title: Persist by digest — a content-addressed permanent store, hash as the address, and one naming scheme for buckets and identities
date: 2026-09-25
status: approved 2026-09-25
issue: "#114 (items 2 and 3)"
---

# Persist by digest

## Problem

#114 item 2 proposed persisting blobs through a `content.replicate` alias bound to a private,
content-addressed bucket. That overloads two wire fields. `destination` becomes a value the host
could derive, the digest plus `.bin`. `public_url` would name a private object that returns 403 to
anyone without credentials, and Archiver writes it to `info_item_rep_specs.public_url` as a citable
URL and shows it behind an Open button. A `private_url` column would stop the overload, but it keeps
the old habit of storing a location the digest already determines. Observo is dropping that habit
(observo#626). Three naming problems sit alongside:

- **The two permanent buckets differ by two letters.** A new private `co-gcs-replicator` would sit
  beside the public `co-gcs-replication`, so one typo in the alias table swaps them.
- **The bucket name is taken by the worker's identity.** The worker's only identity is also named
  `co-gcs-replicator`.
- **One identity holds every grant.** The same account writes public and private buckets alike.

## Approach

Persisting becomes its own command, and the digest becomes the address. A new `content.persist`
command carries the raw-bytes digest and a source reference. Replicator copies the bytes into its
permanent store, `gs://co-gcs-replicator/blobs/<sha256>.bin`, with the shared `GcsBlobStore`: create
if absent, touch off, crc32c on the create. It then reports a `blob_persisted` fact carrying the
digest, not a URL. Readers derive the location with co-core's `gcs_uri(bucket, digest)` from a bucket
they are configured with. Archiver records the digest and when it was persisted, never a URL.
`public_url` stays for publication copies only, where a citation needs a real URL.

The replicate path gains item 3. A `blob_uri` minted by any store this host reads is accepted, temp
or permanent, and resolved by digest. That lets Archiver publish weeks after a fetch, once the bytes
are persisted, instead of racing the temp tier's 7-day TTL (MUST-7).

Buckets are named per service or role, and each has its own writer identity:

| Bucket | Role | Writer | Grants |
|---|---|---|---|
| `co-gcs-blobs` | temp tier | `co-gcs-replicator-writer` | create, get, list, update |
| `co-gcs-replicator` | permanent content-addressed store (new) | `co-gcs-replicator-writer` | create, get, list |
| `co-gcs-publication` | public citable copies (new; public via `allUsers`) | `co-gcs-publication-writer` | create, get, list |
| `co-gcs-replication` | frozen legacy publication bucket | none | stays public forever; nothing moves |

The split identity follows the one boundary where a bug does real damage. Private bytes written into
a public bucket are a disclosure. A stray write into the temp or permanent store is only clutter. The
alias binding gains an optional `credentials_file`, a host path read at boot. That amends T1 without
weakening it: credentials still resolve locally and never travel. The old `co-gcs-replicator`
account is retired once the new identities are live.

**Alias naming becomes a rule with enforcement.** After this change, aliases exist only for
publication destinations. Persist names no alias: the permanent store is host configuration, like
the temp store. The rule:

- **Name shape.** An alias is `<provider>-<role>`, matching `^(gcs|gdrive|ia)-[a-z][a-z0-9-]*$`,
  for example `gcs-publication`.
- **Replicator refuses a mismatched binding at load.** It refuses a provider prefix that differs
  from the binding's provider. For `gcs` it also refuses any bucket other than `co-gcs-<role>` or
  its test twin `co-gcs-test-<role>`. The name then determines the bucket, so a typo can no longer
  bind the public bucket under a private name or the reverse.
- **One shared pattern.** co-core exports the pattern, so Archiver's RepSpec schema validates
  `credentials_alias` against the same definition and a bad name fails when the RepSpec is saved.
- **The replicate contract lists the cluster's aliases** and what each binds. Names are selectors,
  not secrets (T1), so listing them costs nothing.
- **Adding an alias takes three acts in one change:** the host binding (operator), the contract
  row, and the RepSpec that uses it.

`primary` is the one legacy name. It stays accepted, on an allowlist with an expiry, until
Archiver's RepSpecs move to `gcs-publication` in the publication cutover.

## Tradeoffs / alternatives

- **Persist as a replicate alias (#114 item 2 as filed).** Rejected. It needs a destination the
  host could derive, and a `public_url` that is either false (403) or a locator in a column the
  registry treats as a citation.
- **A `private_url` field or column.** Rejected. It is the same URL habit moved to a second column,
  and Observo is abandoning it (#626). The digest already is the address, so storing a derivation
  of it invites the drift #621 and #625 cost Observo.
- **Replicator persists every fetched blob.** Rejected. Deciding what to keep is policy, and charter
  test 3 gives policy to the issuer. It would also make every fetched page permanent.
- **One writer identity for everything.** Simpler, but a guard bug or a mistyped alias could put
  private bytes into the public bucket, and nothing at IAM would stop it. This repo already puts
  T4's never-overwrite rule at IAM; the public/private line deserves the same.
- **Renaming `co-gcs-replication` in place.** Not possible: GCS cannot rename buckets. Moving its
  objects would break every citation to them. So the old bucket is frozen, not migrated.
- **Dropping `blob_uri` from `blob_available` now.** Deferred. It is a required field Watcher and
  Archiver both carry, and T3a already ignores its path. Once digests reach Archiver it becomes
  redundant, and removing it can be a later wire change.

## Steps

1. **Settle this plan**, then file the cross-repo issues it needs. Each names the step it unblocks.
   - **cannobserv:** add `ContentPersistCommand`, `BlobPersistedEvent`, `PersistFailedEvent` and the
     stream constants. Recommend putting the outcomes on `content.artifacts`; the placement is
     co-core's decision. Also export the shared alias-name pattern.
   - **Watcher:** forward the raw-bytes digest on `SourceRevisionObservedEvent`. Watcher does not
     read the permanent store: #222's diffs run over Observo's canonical text, stored by hash in
     Observo's own store.
   - **Archiver:** store the digest, and issue a persist for every observed revision. Which
     revisions to persist is Archiver's decision; every one is the working assumption. Persist
     before publishing, send the permanent URI for publication, validate `credentials_alias`
     against the shared pattern, and migrate RepSpecs from `primary` to `gcs-publication`.

   **Done 2026-09-25:** CannObserv/cannobserv#493, CannObserv/watcher#329, CannObserv/archiver#276.
2. **Identities (operator).** Create `co-gcs-replicator-writer`, `co-gcs-publication-writer` and
   `co-gcs-test-replicator-writer`. Grant the first the temp role on `co-gcs-blobs`, **plus an
   interim `objectCreator` on `co-gcs-replication`**: until step 4 hands publication to its own
   identity, the worker's one identity still writes it. Give the worker the new key as its default
   credentials, restart, and verify a live store. A repo change switches CI's `gcs` job to the new
   test identity once it exists. Done when a day of the journal shows no permission errors and the
   old accounts are disabled. Commands: [the operator runbook](2026-09-25-persist-by-digest-operator-runbook.md).
3. **Buckets (operator).**
   - Create `co-gcs-replicator`: private, uniform bucket-level access, no lifecycle rule, default
     soft delete.
   - Create `co-gcs-publication`: `allUsers` read, matching the old bucket.
   - Create both test twins, with `test` infixed.
   - Add `objectViewer` on `co-gcs-replicator` only for identities that open bytes. Archiver needs
     none: it never opens a blob and only passes the reference through
     (`replication_issuance.py:15-16`). Observo gets one only if its extraction ever re-reads persisted
     bytes.

   Done when `testIamPermissions` shows the grant table above, identity by identity.
4. **Publication cutover**, which needs step 5's per-alias `credentials_file` merged first. Bind `gcs-publication`, and `primary` for the transition, to
   `co-gcs-publication` with the publication writer's `credentials_file`. Revoke every write on
   `co-gcs-replication`, including step 2's interim grant. Done when the next `replication_complete` names the new bucket, and a
   write to the old bucket is refused. `primary` is removed once Archiver's RepSpecs no longer name
   it.
5. **Replicator code, test-first** (no wire change):
   - The alias `credentials_file`, with the T1 edit to the contract.
   - The alias naming rule enforced at load, with the `primary` allowlist and its expiry, plus the
     contract's alias list.
   - A content-addressed store per permanent alias, built with touch off and preflighted at boot.
   - `locate_blob` resolves temp first, then each permanent store, behind the T3a gate.
   - The additive MUST-7 relaxation in the replicate contract.
   - Test-name scan updates.

   Done when CI's `gcs` job exercises a permanent-store source.
6. **Persist handler.** Once co-core ships the models, add the `content.persist` loop through the
   existing `run_loop` / `CommandSpec`, plus a new issuer contract under `docs/contracts/`. Done when
   the `gcs` job persists into `co-gcs-test-replicator` twice and the second is a no-op success.
7. **Archiver and Watcher changes land in their repos.** Done when one real revision is persisted,
   then published from the permanent URI after its temp blob has expired.

## Decisions (2026-09-25)

- **Persist outcomes go on `content.artifacts`.** That is the recommendation; the final placement is
  co-core's call.
- **Archiver persists every observed revision** as the working assumption. The policy is Archiver's.
- **Alias naming becomes a rule with enforcement now**, as described under Approach.
- **Watcher gets no permanent-store grant.** Watcher#222's redesign diffs Observo-derived canonical
  text read from Observo's store by hash, not raw blobs. The only Replicator dependency on that
  path is Observo's extraction reading the raw blob from the temp store, which the 7-day window
  already covers.
- **`co-gcs-replicator` keeps GCS's default 7-day soft delete** as the operator's undo.
- **The test identity is renamed** from `co-gcs-test-replicator` to `co-gcs-test-replicator-writer`
  in step 2, so the bucket's test twin can take the name. CI's workload-identity binding moves with
  it.

## Open questions / risks

- **The allowlist expiry for `primary`.** It depends on when Archiver migrates its RepSpecs. The
  expiry should fail loudly at boot, not silently drop the alias.
- **Existing RepSpec data may carry other alias names** that production never bound. Archiver's
  docs mention `gcs-cannobserv-prod`, `ia-cannobserv` and others. The shared pattern would reject
  any stored RepSpec that doesn't match, so Archiver needs a data audit before it enforces the
  pattern.
