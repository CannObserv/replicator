---
title: Persist by digest — a content-addressed permanent store, hash as the address, and one naming scheme for buckets and identities
date: 2026-09-25
status: draft
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
     stream constants. The outcomes go on `content.artifacts`; see the open questions.
   - **Watcher:** forward the raw-bytes digest on `SourceRevisionObservedEvent`.
   - **Archiver:** store the digest, issue persists, persist before publishing, and send the
     permanent URI for publication.

   Done when each issue exists and links here.
2. **Identities (operator).** Create `co-gcs-replicator-writer` and `co-gcs-publication-writer`.
   Grant the first the temp role on `co-gcs-blobs`. Give the worker the new key as its default
   credentials, restart, and verify a live store. Done when the old account is disabled with no
   errors in the journal.
3. **Buckets (operator).**
   - Create `co-gcs-replicator`: private, uniform bucket-level access, no lifecycle rule, default
     soft delete.
   - Create `co-gcs-publication`: `allUsers` read, matching the old bucket.
   - Create both test twins, with `test` infixed.
   - Add `objectViewer` for Archiver's and Observo's service accounts on `co-gcs-replicator`.

   Done when `testIamPermissions` shows the grant table above, identity by identity.
4. **Publication cutover.** Point the production alias at `co-gcs-publication` with the publication
   writer's `credentials_file`, and revoke every write on `co-gcs-replication`. Done when the next
   `replication_complete` names the new bucket, and a write to the old bucket is refused.
5. **Replicator code, test-first** (no wire change):
   - The alias `credentials_file`, with the T1 edit to the contract.
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

## Open questions / risks

- **Stream placement.** The recommendation is to put persist outcomes on `content.artifacts` beside
  replicate's, since Archiver already consumes both there. The alternative is `content.blobs`,
  which is about blobs but is Watcher's stream. This is co-core's call to make.
- **Which revisions Archiver persists.** Every observed revision, or only those with a RepSpec
  assignment? This is policy and belongs to Archiver, but it sizes the bucket.
- **Alias names are data in Archiver's RepSpec documents.** Archiver's docs use names like
  `gcs-cannobserv-prod`, but production's alias table binds `primary`. Rebinding keeps the name;
  renaming it is a RepSpec data change coordinated with Archiver.
- **Watcher needs no permanent-store grant today.** It never re-reads old bytes, because its diff
  pipeline is gone. Add the grant if diffs return.
- **Soft delete on `co-gcs-replicator`.** Keep the default 7 days as the operator's undo for an
  accidental delete, or clear it, since writers hold no delete anyway? The recommendation is to keep
  it.
- **`co-gcs-test-replicator` is today's test identity's name.** Rename that identity to
  `co-gcs-test-replicator-writer` in step 2, so the bucket twin can take the name. CI's
  workload-identity binding moves with it.
