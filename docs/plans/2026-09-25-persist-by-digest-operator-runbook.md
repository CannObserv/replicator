---
title: Operator runbook for Persist by digest, steps 2 and 3 — identities and buckets
date: 2026-09-25
status: phases A and B done 2026-09-25; C next
plan: 2026-09-25-content-addressed-persist-and-storage-naming.md
---

# Operator runbook — identities and buckets

Run phases A and C from a workstation that holds `roles/iam.serviceAccountAdmin` and
`roles/storage.admin` on project `co-gcs`. The VM's identities hold neither, by design. Every
command in A and C only adds; nothing is disabled until phase E. Each phase says what to send back
before the next one starts.

## Phase A — identities (workstation)

```bash
PROJECT=co-gcs
PROJECT_NUMBER=912903030445
W=co-gcs-replicator-writer@co-gcs.iam.gserviceaccount.com
P=co-gcs-publication-writer@co-gcs.iam.gserviceaccount.com
T=co-gcs-test-replicator-writer@co-gcs.iam.gserviceaccount.com

gcloud iam service-accounts create co-gcs-replicator-writer --project=$PROJECT \
  --display-name="Replicator writer: temp tier and permanent store"
gcloud iam service-accounts create co-gcs-publication-writer --project=$PROJECT \
  --display-name="Replicator writer: public citable copies"
gcloud iam service-accounts create co-gcs-test-replicator-writer --project=$PROJECT \
  --display-name="Replicator test writer (CI and -m gcs)"

# The temp tier: the custom role (create, get, list, update; never delete).
gcloud storage buckets add-iam-policy-binding gs://co-gcs-blobs \
  --member=serviceAccount:$W --role=projects/co-gcs/roles/replicatorTempBlobWriter

# INTERIM: publication writes keep working under the new identity until the
# cutover (plan step 4) hands them to co-gcs-publication-writer, which removes it.
gcloud storage buckets add-iam-policy-binding gs://co-gcs-replication \
  --member=serviceAccount:$W --role=roles/storage.objectCreator

# The test identity mirrors today's co-gcs-test-replicator grants.
gcloud storage buckets add-iam-policy-binding gs://co-gcs-test-replication \
  --member=serviceAccount:$T --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding gs://co-gcs-test-blobs \
  --member=serviceAccount:$T --role=roles/storage.objectAdmin
gcloud iam service-accounts add-iam-policy-binding $T --project=$PROJECT \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/github/attribute.repository/CannObserv/replicator"

# Keys for the VM. Only these two: the publication writer's key is minted in the
# step 4 cutover, when something reads it.
gcloud iam service-accounts keys create co-gcs-replicator-writer.json --iam-account=$W
gcloud iam service-accounts keys create co-gcs-test-replicator-writer.json --iam-account=$T
```

**Send back:** that the three accounts exist. Copy both key files to the VM's `/tmp`. Phase B
installs them and phase D removes the copies.

## Phase B — the VM switches identity (on `co-replicator`)

```bash
sudo install -o root -g exedev -m 640 /tmp/co-gcs-replicator-writer.json /etc/replicator/
sudo install -o root -g exedev -m 640 /tmp/co-gcs-test-replicator-writer.json /etc/replicator/
sudo sed -i 's#^GOOGLE_APPLICATION_CREDENTIALS=.*#GOOGLE_APPLICATION_CREDENTIALS=/etc/replicator/co-gcs-replicator-writer.json#' /etc/replicator/.env
sudo systemctl restart replicator
sudo journalctl -u replicator --since "-2min" -o cat | grep -E 'storing blobs in an object store|worker ready|WARNING|ERROR'
```

Then check what the new identity can do, bucket by bucket. The expected results are in the plan's
grant table, plus the interim `create` on `co-gcs-replication`:

```bash
cd /home/exedev/replicator && export $(sudo grep -E '^GOOGLE_APPLICATION_CREDENTIALS=' /etc/replicator/.env | xargs) && uv run --no-sync python -c "
from google.cloud import storage
c = storage.Client()
perms = ['storage.objects.create', 'storage.objects.get', 'storage.objects.list',
         'storage.objects.update', 'storage.objects.delete']
for b in ['co-gcs-blobs', 'co-gcs-replication']:
    print(b, sorted(c.bucket(b).test_iam_permissions(perms)))
"
```

A repo change lands alongside phase B, merged only once phase A exists. It switches CI's `gcs` job
to `co-gcs-test-replicator-writer` and renames the key path in the docs. The
`REPLICATOR_TEST_GCS_CREDENTIALS` path in the test commands changes the same way.

**Done 2026-09-25.** Before the switch, `testIamPermissions` from each key file matched the plan:
the worker identity had `create, get, list, update` on `co-gcs-blobs` and `create, get, list` on
`co-gcs-replication`. The `get` and `list` there come from `allUsers`. The test identity had all five
on both test buckets and no write on production. The worker restarted at 15:50 UTC as
`co-gcs-replicator-writer` (checked in `/proc/<pid>/environ`), and its boot preflight listed
`co-gcs-blobs`. The prior `.env` is at `/etc/replicator/.env.bak-pre-writer`. CI's `gcs` job passed on
the new identity (run 36157674876). It now also runs the 7 temp-store rows that had skipped since #7.

## Phase C — buckets (workstation)

Run this in the same shell as phase A, or re-run phase A's five variable lines first, since the
commands below use `$PROJECT`, `$W`, `$P` and `$T`. Match the new buckets' location to the
existing publication bucket before creating anything:

```bash
gcloud storage buckets describe gs://co-gcs-replication \
  --format="yaml(location, default_storage_class, soft_delete_policy, uniform_bucket_level_access)"
LOCATION=us-west1   # replace with the location printed above

# The permanent content-addressed store: private, no lifecycle, default 7-day soft delete.
gcloud storage buckets create gs://co-gcs-replicator --project=$PROJECT --location=$LOCATION \
  --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets add-iam-policy-binding gs://co-gcs-replicator \
  --member=serviceAccount:$W --role=roles/storage.objectCreator
gcloud storage buckets add-iam-policy-binding gs://co-gcs-replicator \
  --member=serviceAccount:$W --role=roles/storage.objectViewer

# The new publication bucket: public like the old one; its writer creates and reads, nothing more.
gcloud storage buckets create gs://co-gcs-publication --project=$PROJECT --location=$LOCATION \
  --default-storage-class=STANDARD --uniform-bucket-level-access
gcloud storage buckets add-iam-policy-binding gs://co-gcs-publication \
  --member=allUsers --role=roles/storage.objectViewer
gcloud storage buckets add-iam-policy-binding gs://co-gcs-publication \
  --member=serviceAccount:$P --role=roles/storage.objectCreator

# Test twins: the opposite grants on purpose, like co-gcs-test-replication (#38, #50).
cat > /tmp/age-1-day.json <<'EOF'
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 1}}]}
EOF
for B in co-gcs-test-replicator co-gcs-test-publication; do
  gcloud storage buckets create gs://$B --project=$PROJECT --location=$LOCATION \
    --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention \
    --soft-delete-duration=0
  gcloud storage buckets update gs://$B --lifecycle-file=/tmp/age-1-day.json
  gcloud storage buckets add-iam-policy-binding gs://$B \
    --member=serviceAccount:$T --role=roles/storage.objectAdmin
done
```

**Send back:** the printed location, and the output of:

```bash
for B in co-gcs-replicator co-gcs-publication co-gcs-test-replicator co-gcs-test-publication; do
  echo "== $B"; gcloud storage buckets get-iam-policy gs://$B --format="yaml(bindings)"
done
```

## Phase D — verify, then tidy (on `co-replicator`)

Re-run phase B's permission check with `co-gcs-replicator` added to the bucket list. Expect
`create`, `get` and `list` there, never `update` or `delete`. Then remove the key copies:
`rm /tmp/co-gcs-*-writer.json` on the VM, and remove the local copies on the workstation.

## Phase E — retire the old identities (workstation, after a clean day)

Run this only once a full day of the journal shows no permission errors under the new identity,
and CI has passed on the new test identity:

```bash
gcloud iam service-accounts disable co-gcs-replicator@co-gcs.iam.gserviceaccount.com --project=$PROJECT
gcloud iam service-accounts disable co-gcs-test-replicator@co-gcs.iam.gserviceaccount.com --project=$PROJECT
```

Disabling is reversible, and deleting is not. Delete a week later, then remove
`/etc/replicator/co-gcs-replicator.json` and `/etc/replicator/co-gcs-test-replicator.json` from the
VM. The interim `objectCreator` on `co-gcs-replication` is removed in the step 4 cutover, not here.
