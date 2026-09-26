---
title: Stamp replicated objects with their digest, and copy GCS to GCS server-side (#114 items 4 and 5)
date: 2026-09-26
status: approved
---

# Replicate: the content stamp and the server-side copy

## Problem

Two gaps in the replicate write path remain from #114.

- **Item 4.** A public artifact in `co-gcs-publication` has no link back to the blob it came from. Its key is the issuer's rendered destination, not the digest.
- **Item 5.** Every GCS replicate downloads the whole blob onto this VM, through `open_stream` on the temp or permanent store, and uploads it again. Bytes that already sit in GCS transit an 8 GiB host for nothing.

co-core 0.19.5 shipped both primitives:

- `metadata` on `GcsCreateIfAbsent` (cannobserv#490), with `METADATA_CONTENT_SHA256 = "co-content-sha256"`;
- `GcsCopyIfAbsent` with `AsyncGcsDriver.copy_if_absent` (cannobserv#485). It is `Blob.rewrite` under `ifGenerationMatch=0`, and it resolves a 412 by comparing the two objects' reported md5s.

The floor is already 0.19.6, so no dependency change is needed.

## Approach

**Item 4.** Every replicate write passes `metadata={METADATA_CONTENT_SHA256: fingerprint}`. The stamp rides the create's own request, so it needs no `update` grant. co-core does not back-fill it on the 412 path, so an object written before the release stays unstamped. The contract will say the stamp is on "every object written after", not on every object.

**Item 5.** `ConditionalWriter` grows `copy_if_absent`. `_write` uses it when the located source is a GCS store, and keeps today's `open_stream` upload otherwise; that covers a `local` temp tier and every dev host.

- **Source.** The bucket and key are taken from `source.store.uri_for(fingerprint)`. That is a URI the store itself minted, so parsing it is not the T3a hazard; the message's `blob_uri` is still never parsed.
- **Same stamp.** The copy carries the same `metadata` stamp as the create.
- **Mapping outcomes.**
  - A missing source raises `FileNotFoundError`, which maps to `blob_expired` exactly as a missing read does today.
  - Provider errors keep `_classify_provider_failure`.
  - T4's three rows are unchanged; only the evidence for "identical" moves from a local md5 to the source's reported md5.

**Identity.** The rewrite runs as the alias's writer (`co-gcs-publication-writer`, step 5's per-alias credentials), and a rewrite needs `storage.objects.get` on the source. That identity has `objectCreator` on `co-gcs-publication` only. So item 5 needs an **operator grant before the deploy**: `roles/storage.objectViewer` for `co-gcs-publication-writer` on `co-gcs-blobs` and `co-gcs-replicator`.

- **Verifying the grant.** Checked with `testIamPermissions` using the writer's key. That check is read-only; a probe write into the public bucket would be permanent, because nothing there holds `delete`.
- **Without the grant,** every GCS publication would close `provider_disabled` (a terminal 403). That is why the grant comes first.

## Tradeoffs / alternatives

- **Run the copy as the worker's identity** (`co-gcs-replicator-writer`): rejected. Step 5 gave each alias its own credentials precisely so the worker identity holds no write on the publication bucket.
- **Opt in per alias in the alias table** (e.g. `"server_side_copy": true`): rejected for now. It adds a host-config field and a second operator write for a property the grant already decides. Worth revisiting if a host ever binds an alias whose identity must not read the temp tier.
- **Probe at boot and fall back to upload.** `testIamPermissions` per alias per source bucket at startup; copy where granted, upload plus a WARNING where not. This never closes a command on a missing grant. It is rejected as the default because it hides a missing grant behind a slower path that works, and adds a boot round trip per binding. It is the answer to open question 1 if you prefer robustness to strictness.
- **Fall back to upload on a 403 from the copy:** rejected. The 403 cannot say whether the source read or the destination create was refused, so the fallback would also mask a real `provider_disabled`.
- **Copy for `content.persist` too (temp → permanent, same identity):** out of scope. Persist's local re-hash is what refuses corrupt bytes (`source_corrupt`, cannobserv#492), and a server-side copy would skip that check.

## Steps

1. **Item 4, test-first.** `_write` passes the stamp on the upload. A handler test asserts the `metadata` on the effect, and a `-m gcs` test asserts `co-content-sha256` on the written object in `co-gcs-test-replication`.
2. **Operator runbook** `docs/plans/2026-09-26-server-side-copy-runbook.md`:
   - phase A: grant `objectViewer` to `co-gcs-publication-writer` on `co-gcs-blobs` and `co-gcs-replicator`;
   - phase B: `testIamPermissions` check with the writer's key, output recorded;
   - phase C: after deploy, confirm the first real replicate logs `method: copy`.

   The test twins need nothing: the test writer already holds `objectAdmin` on all four.
3. **Item 5, test-first.** `ConditionalWriter.copy_if_absent`; `_write` chooses the copy for a GCS source and the upload for a local one.
   - Unit tests cover: which method is used; the source bucket and key from `uri_for`; `FileNotFoundError` mapping to `blob_expired`; the provider classification unchanged; and the stamp on the copy.
   - The journal line gains `method: copy | upload`.
4. **Real-bucket tests.** A copy from `co-gcs-test-blobs`, and one from `co-gcs-test-replicator`, into `co-gcs-test-replication`, covering:
   - T4's absent and identical rows via the copy;
   - the temp tier's `customTime` is not carried onto the destination;
   - the stamp is present.
5. **Docs:**
   - the replicate contract: the status table, T4's evidence sentence, and an informative note on the stamp;
   - the reference;
   - STREAMS.md's replicate bullet;
   - INFRASTRUCTURE.md's grant table;
   - ARCHITECTURE.md;
   - the #114 plan's item notes.
6. **Ship in order:** runbook phases A–B done, then merge and deploy, then phase C.

## Open questions / risks

**Decided at review (2026-09-26):** question 1 — grant first, no boot probe; question 2 — the wider read is accepted.

1. **A missing grant closes publications as `provider_disabled`.** Grant-first (recommended) makes that an ordering rule; the boot probe (alternatives, third bullet) makes it impossible at the cost of a silent slow path. Which do you want?
2. **The grant widens the publication writer's reach.** It could then read the private permanent store and the temp tier. Its key sits on this VM beside the worker's, which reads both already, so the change in exposure on this host is small. It is still a new read path for a key whose job is a public bucket.
3. **Location.** A cross-location or cross-storage-class rewrite copies bytes server-side, and may take several calls on the rewrite token. The driver loops on the token, so this is time rather than correctness; `REPLICATOR_REPLICATE_WRITE_TIMEOUT_SECONDS` bounds each call.
