---
title: Operator runbook for #114 item 5 — the server-side copy's grant
date: 2026-09-26
status: phase A pending
plan: 2026-09-26-replicate-content-stamp-and-server-side-copy.md
---

# Operator runbook — the server-side copy's grant

Item 5 changes how a GCS replicate reaches the publication bucket. Today it downloads the blob onto
this VM and uploads it again. Afterwards it runs a `rewrite`: GCS copies the object itself, as the
alias's writer, `co-gcs-publication-writer`. A rewrite needs `storage.objects.get` on the source, and
that identity holds only `objectCreator` on `co-gcs-publication`.

**The grant must land before the deploy.** A replicate without it is refused with a 403, which
`_classify_provider_failure` closes as `provider_disabled`, a terminal fact to Archiver for a valid
publication. The order was decided at plan review: grant first, no boot-time fallback.

**What the grant adds.** `roles/storage.objectViewer` (`get`, `list`) on the two buckets a
`blob_uri` may name: the temp tier `co-gcs-blobs` and the permanent store `co-gcs-replicator`. The
writer's key sits on this VM beside the worker's, which reads both already. Nothing gains `update` or
`delete`. The test twins need nothing, because `co-gcs-test-replicator-writer` holds `objectAdmin`
on all four.

Run phase A from a workstation with `roles/storage.admin` on `co-gcs`. Phases B and C run on
`co-replicator`.

## Phase A — the grant (workstation)

```bash
P=co-gcs-publication-writer@co-gcs.iam.gserviceaccount.com

for B in co-gcs-blobs co-gcs-replicator; do
  gcloud storage buckets add-iam-policy-binding gs://$B \
    --member=serviceAccount:$P --role=roles/storage.objectViewer
done

for B in co-gcs-blobs co-gcs-replicator; do
  echo "== $B"; gcloud storage buckets get-iam-policy gs://$B --format="yaml(bindings)"
done
```

**Send back:** both policies.

## Phase B — check the grant with the writer's key (on `co-replicator`)

This check is read-only, and deliberately so: a probe write into the public bucket would be permanent,
because nothing there holds `delete`.

```bash
cd /home/exedev/replicator && env -u GOOGLE_APPLICATION_CREDENTIALS \
  GOOGLE_APPLICATION_CREDENTIALS=/etc/replicator/co-gcs-publication-writer.json \
  uv run --no-sync python -c "
from google.cloud import storage
c = storage.Client(project='co-gcs')
perms = ['storage.objects.create', 'storage.objects.get', 'storage.objects.list',
         'storage.objects.update', 'storage.objects.delete']
for b in ['co-gcs-publication', 'co-gcs-blobs', 'co-gcs-replicator', 'co-gcs-replication']:
    print(b.ljust(22), sorted(p.split('.')[-1] for p in c.bucket(b).test_iam_permissions(perms)))
"
```

| Bucket | Before (2026-09-26) | Expected after |
|---|---|---|
| `co-gcs-publication` | `create`, `get`, `list` | unchanged |
| `co-gcs-blobs` | none | `get`, `list` |
| `co-gcs-replicator` | none | `get`, `list` |
| `co-gcs-replication` | `get`, `list` (public) | unchanged |

`update` and `delete` must appear nowhere.

## Then: merge and deploy

The code ships only after phase B matches the table: merge the branch, push, then
`uv sync --frozen && sudo systemctl restart replicator`.

## Phase C — the first real copy (on `co-replicator`)

Real replications are rare, so this waits for one. The journal line names the method:

```bash
journalctl -u replicator --since "2026-09-26" -o cat | grep '"replicated a blob"' | grep -o '"method": "[a-z]*"'
```

Expect `"method": "copy"`. Then check that the object carries the stamp and not the temp tier's
retention clock:

```bash
gcloud storage objects describe gs://co-gcs-publication/<key> --format="yaml(metadata,custom_time)"
```

Expect `co-content-sha256: <digest>` and no `custom_time`.

**Rollback.** Revert the merge and redeploy. The upload path needs no grant, and the extra read can
stay or be removed with `remove-iam-policy-binding`.
