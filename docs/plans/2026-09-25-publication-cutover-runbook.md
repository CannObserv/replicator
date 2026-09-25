---
title: Operator runbook for Persist by digest, step 4 — the publication cutover
date: 2026-09-25
status: phases A and B done 2026-09-25; C waits on a decision about co-gcs-cli-writer
plan: 2026-09-25-content-addressed-persist-and-storage-naming.md
---

# Operator runbook — the publication cutover

Plan step 4. Publication moves from `co-gcs-replication`, written by the worker's own identity through
an interim grant, to `co-gcs-publication`, written by `co-gcs-publication-writer` through the alias
binding's `credentials_file` (step 5, shipped in `897ceef`). Afterwards the old bucket stays public and
takes no writes. Nothing moves out of it, because every citation to it must keep resolving.

**What does not change.** Archiver's RepSpecs name `primary`, and `primary` now binds the new bucket
too, so no RepSpec edit is needed on cutover day. `gcs-publication` is bound beside it for Archiver's
migration (archiver#276), which must land before `primary` expires on 2026-12-31. The key layout is
unchanged: the prefix is still empty, and a rendered destination lands at the same key, in the new
bucket. No cluster code depends on the bucket name. Archiver records the `public_url` each
`replication_complete` returns, and cannobserv names the old bucket only in test fixtures.

**How it is verified.** Real replications are rare: there was one in the 14 days before 2026-09-25,
on 2026-09-16. So phase B writes a probe object through the worker's own driver and key, and phase C
deletes it. The plan's done condition, the next real `replication_complete` naming the new bucket, is
checked in phase D whenever that command arrives.

Run phases A and C from a workstation with `roles/iam.serviceAccountKeyAdmin` and
`roles/storage.admin` on `co-gcs`. Phases B and D run on `co-replicator`.

## Phase A — the key, and who can write today (workstation)

```bash
PROJECT=co-gcs
W=co-gcs-replicator-writer@co-gcs.iam.gserviceaccount.com
P=co-gcs-publication-writer@co-gcs.iam.gserviceaccount.com

gcloud iam service-accounts keys create co-gcs-publication-writer.json --iam-account=$P

# Every binding on the old bucket, so phase C revokes the whole write set rather than
# the ones we remember.
gcloud storage buckets get-iam-policy gs://co-gcs-replication --format="yaml(bindings)"
```

**Send back:** the policy output, and copy the key to the VM's `/tmp`.

**Done 2026-09-25.** The policy listed three service accounts with `objectCreator` on the old bucket.
Two were expected: `co-gcs-replicator-writer` (the interim grant) and `co-gcs-replicator` (retired).
The third, **`co-gcs-cli-writer`**, also holds `objectViewer`, and no CannObserv repo names it. It
most likely serves a tool outside these repos, perhaps whatever writes `console_workspace/`. Phase C
leaves it alone until the operator decides.

## Phase B — bind the new bucket (on `co-replicator`)

**B1. Check the key's permissions before the worker uses it.** Expect `create`, `get` and `list`
on `co-gcs-publication`. The `get` and `list` come from `allUsers`, and T4's confirming read on a
412 needs that `get`. Expect only the public `get` and `list` on `co-gcs-replication`, and nothing on
the private buckets.

```bash
cd /home/exedev/replicator && GOOGLE_APPLICATION_CREDENTIALS=/tmp/co-gcs-publication-writer.json uv run --no-sync python -c "
from google.cloud import storage
c = storage.Client(project='co-gcs')
perms = ['storage.objects.create', 'storage.objects.get', 'storage.objects.list',
         'storage.objects.update', 'storage.objects.delete']
for b in ['co-gcs-publication', 'co-gcs-replication', 'co-gcs-replicator', 'co-gcs-blobs']:
    print(b.ljust(22), sorted(p.split('.')[-1] for p in c.bucket(b).test_iam_permissions(perms)))
"
```

**B2. Install the key and the new alias table.** The previous table is kept for rollback.

```bash
sudo install -o root -g exedev -m 640 /tmp/co-gcs-publication-writer.json /etc/replicator/
sudo cp -p /etc/replicator/replication-aliases.json /etc/replicator/replication-aliases.json.bak-pre-cutover
sudo tee /etc/replicator/replication-aliases.json >/dev/null <<'EOF'
{
  "gcs-publication": {
    "provider": "gcs",
    "bucket": "co-gcs-publication",
    "credentials_file": "/etc/replicator/co-gcs-publication-writer.json"
  },
  "primary": {
    "provider": "gcs",
    "bucket": "co-gcs-publication",
    "credentials_file": "/etc/replicator/co-gcs-publication-writer.json"
  }
}
EOF
sudo chown root:exedev /etc/replicator/replication-aliases.json && sudo chmod 640 /etc/replicator/replication-aliases.json
sudo systemctl restart replicator
sudo journalctl -u replicator --since "-2min" -o cat \
  | grep -E 'alias table loaded|ignoring an unusable alias|could not build a provider writer|worker ready|ERROR'
```

The expected result is `alias table loaded` with both aliases in `provisioned` and both in
`credentials_files`, then `worker ready`. Any line saying `ignoring an unusable alias binding` or
`could not build a provider writer` means roll back (below).

**B3. Probe the write path.** The probe uses the same driver, key and effect the replicate handler
builds. It runs twice: the first run expects `wrote`, and the second expects `already_identical`,
which is T4's no-op row and the one that needs the public `get`.

```bash
cd /home/exedev/replicator && for run in 1 2; do uv run --no-sync python -c "
import asyncio, io
from co_core.effects.gcs import GcsCreateIfAbsent
from co_core_aio.gcs import AsyncGcsDriver
from src.worker.main import load_credentials

async def main():
    creds = load_credentials('/etc/replicator/co-gcs-publication-writer.json')
    async with AsyncGcsDriver('co-gcs-publication', credentials=creds) as driver:
        result = await driver.create_if_absent(GcsCreateIfAbsent(
            blob_name='_cutover-probe/2026-09-25.txt',
            data=io.BytesIO(b'replicator#114 publication cutover probe\n'),
            content_type='text/plain'))
        print(result.outcome.value, result.public_url)

asyncio.run(main())
"; done
curl -s -o /dev/null -w '%{http_code}\n' https://storage.googleapis.com/co-gcs-publication/_cutover-probe/2026-09-25.txt
```

**Send back:** nothing. B1–B3 run on the VM, so the agent can run them and report.

**Done 2026-09-25, 22:53 UTC.** B1 matched the expectation exactly: `create`, `get` and `list` on
`co-gcs-publication`, the public `get` and `list` on `co-gcs-replication`, and nothing on
`co-gcs-replicator` or `co-gcs-blobs`. After B2's restart the worker loaded both aliases with the
publication key in `credentials_files`, built both writers, and logged `worker ready` (build
`5621367`). B3's probe printed `wrote`, then `already_identical`, and the object is publicly served
(HTTP 200).

**Rollback, before phase C only:** restore `replication-aliases.json.bak-pre-cutover` and restart.
`primary` then writes to the old bucket as the worker's own identity again, which works only while
phase C's revocations have not been made.

## Phase C — freeze the old bucket (workstation)

Revoke every service-account write on `co-gcs-replication` that phase A's policy listed. The interim
grant and the retired account's are the two expected. Leave `allUsers` read and GCS's
project-convenience bindings alone. Then delete the probe object, which the publication writer cannot
do.

```bash
gcloud storage buckets remove-iam-policy-binding gs://co-gcs-replication \
  --member=serviceAccount:$W --role=roles/storage.objectCreator
gcloud storage buckets remove-iam-policy-binding gs://co-gcs-replication \
  --member=serviceAccount:co-gcs-replicator@co-gcs.iam.gserviceaccount.com --role=roles/storage.objectCreator
# co-gcs-cli-writer also holds objectCreator (and objectViewer) here, and no repo names it.
# Revoke it only once whatever uses it is known to be gone or moved:
#   gcloud storage buckets remove-iam-policy-binding gs://co-gcs-replication \
#     --member=serviceAccount:co-gcs-cli-writer@co-gcs.iam.gserviceaccount.com --role=roles/storage.objectCreator

gcloud storage rm gs://co-gcs-publication/_cutover-probe/2026-09-25.txt

gcloud storage buckets get-iam-policy gs://co-gcs-replication --format="yaml(bindings)"
```

**Send back:** the final policy output.

## Phase D — verify (on `co-replicator`)

- **The old bucket refuses writes.** Re-run B1 with the worker's key
  (`/etc/replicator/co-gcs-replicator-writer.json`). `co-gcs-replication` should show only `get` and
  `list`.
- **The probe is gone.** The `curl` at the end of B3 should now print `404`.
- **Tidy.** `rm /tmp/co-gcs-publication-writer.json` on the VM, and remove the workstation copy.
- **The plan's done condition, whenever it comes.** The next `replicated a blob` line in the journal
  names its `key`. Check that the key is served at `https://storage.googleapis.com/co-gcs-publication/<key>`
  and is absent from the old bucket.

A repo change lands with phase B. It updates the alias table's description in `docs/ENVIRONMENT.md`
and `README.md`, the alias list's state column in the replicate contract, and the old bucket's writer
row in `docs/INFRASTRUCTURE.md`.

**One edge, accepted.** Suppose a command wrote to the old bucket before the cutover but crashed before
its fact was published. Its redelivery writes a second copy in the new bucket and reports that URL.
`XPENDING` showed no pending entries in `replicator.replicate` on 2026-09-25, and replications are rare, so the window is small.
