# Tailscale — the `co-replicator` node

What is specific to **this** node. The general primer — what a tailnet is, tags,
ACL shape, key hygiene — lives in CannObserv/observo's `docs/reference/tailscale.md`,
and archiver's and notifier's copies describe their own nodes. Provisioned for #88
on 2026-09-11; the move's design is
[`docs/plans/2026-09-11-co-replicator-vm-migration-design.md`](../plans/2026-09-11-co-replicator-vm-migration-design.md).

## The node

| | |
|---|---|
| exe.dev VM | `co-replicator` / `co-replicator.exe.xyz`, region `pdx`, 2 vCPU / 4 GiB / 20 GiB, proxy `private` (port 8000) |
| Tailnet | `cannobserv.org.github`, hostname **`replicator`** |
| Addresses | `100.114.136.20`, `fd7a:115c:a1e0::d430:8815` |
| Node ID | `ndzdyptvG421CNTRL` |
| Tags | `tag:replicator` — single tag, `KeyExpiry: null` (tagged nodes do not expire) |
| Prefs | `CorpDNS: true`, `RunSSH: true` |

The exe.dev name has the cohort's `co-` prefix (`co-broker`, `co-registrar`); the
tailnet name stays bare, so every broker URL and ACL rule reads naturally.

## What the ACL allows

Replicator is a pure broker client plus internet egress. **No rule lists
`tag:replicator` as a `dst` for any service port** — the worker binds nothing.

| Rule | Why |
|---|---|
| `tag:replicator → tag:broker:6379` | The bus. The only `src` rule this node has |
| The tailnet's `autogroup:member → *:*`, plus an `ssh` block admitting `autogroup:member` as `exedev` or `root` | Admin reach, from your own devices only (watcher#296 D3). No tagged node reaches this one |

Tailscale SSH needs **both** halves. Peer visibility follows `acls` rules, not
`ssh` rules: without the network rule the node is absent from the peer's netmap,
MagicDNS will not resolve it, and the `ssh` rule is never consulted (archiver#193).

The build phase's `tag:watcher → tag:replicator:22` edge, and `tag:watcher` in the
`ssh` block, came out at #88's decommission (2026-09-12). What this node enforces
is in its netmap, which needs root here: `sudo tailscale debug netmap`. Its
`PacketFilter` admits only `autogroup:member` devices, and `SSHPolicy.rules` holds
one rule, those devices as `exedev`/`root`. **Read `SSHPolicy.rules` after any
policy edit that touches `ssh`.** The first edit here left it empty, so nobody had
Tailscale SSH, while `tailscale status` looked no different.

Verified 2026-09-11 in both directions: from here, `broker:6379` answers
`-NOAUTH`, while `watcher:22/8000/8001/5432` and `broker:22/9000` are filtered;
from `broker`, `replicator:22/8000/8001` are filtered.

## The path to the broker, measured

`tailscale status` names the path — `direct` is the word to look for; `relay`
means DERP:

```
100.97.91.19  broker  tagged-devices  linux  active; direct 16.145.19.221:13218
```

Measured from this node with the service live, 2026-09-11, as the `replicator`
ACL user:

| | n | min | p50 | max |
|---|---|---|---|---|
| Cold — fresh TCP + `AUTH` + `PING` | 6 | 3.96 | **4.05** | 10.48 ms |
| Warm `PING` | 30 | 0.44 | **0.47** | 0.56 ms |

A first `tailscale ping` after boot went **DERP(sea) at 17–18 ms** before the
direct path formed at 1 ms. From the watcher VM (`lax`) the same hop never left
DERP(sea), at 36–40 ms. The difference is visible in the worker's own boot line:
the `content.fetch-policy` replay took **333 ms** there and **39 ms** here.

## Identity survives a reboot — and what the boot taught the floor check

Rebooted four times on 2026-09-11, once before any traffic and three times with
the service enabled. Every time: the same node ID, addresses and tag;
`replicator` back and active with `NRestarts=0` within ~4 s; the path to
`broker` direct again.

The three service boots measured what `After=tailscaled.service` does and does
not buy the floor check. Offsets are from tailscaled starting:

| Boot | tailscaled Running | `check_redis_floor.sh` | Result |
|---|---|---|---|
| `After=` only | +1.65 s | +0.73 s | `UNVERIFIED` — `No address associated with hostname` |
| + a wait for the tailnet address | +0.61 s | +0.80 s, address already local | `UNVERIFIED` — same error |
| + the floor check retrying (#88) | +1.65 s | retried, +1.76 s | `broker reachable after 1s`, 7.0.15 meets the floor |

`No address associated with hostname` is `EAI_NODATA` — the cohort's `Error -5`
— and it is an *answer*: MagicDNS is up but does not yet return `broker`.
`After=` orders only against tailscaled starting, and the second boot proved an
address wait guards the wrong thing, since the address was local while the name
still failed. So the check retries the dependency itself, for
`REPLICATOR_REDIS_FLOOR_WAIT` ([ENVIRONMENT.md](../ENVIRONMENT.md)). It is
notifier#43's R1 on the name side: that reboot measured a 2.4 s margin for a
bind and warned it could invert; this one was ~0.2 s for a lookup, and did. The
worker itself was never affected — its backoff absorbs a slow tailnet.

## DNS: two names, two resolvers

exeuntu ships no system resolver here — `systemd-resolved` is `not-found`, and
masked on `co-broker` and `co-registrar` — so tailscaled runs in direct mode:
`tailscale up` rewrote `/etc/resolv.conf` to MagicDNS (the original,
`nameserver 1.1.1.1`, is at `/etc/resolv.pre-tailscale-backup.conf`), and that
survives reboots. **Every** lookup on this host goes through tailscaled, public
names included — the cost notifier#43 D8 accepted. exe.dev writes the VM's own
names into `/etc/hosts`:

```
replicator     -> 100.114.136.20   (MagicDNS)
co-replicator  -> 10.42.0.42       (/etc/hosts)
broker         -> 100.97.91.19     (MagicDNS)
```

A check that reaches this host by one name says nothing about the other. For
the tailnet, the honest check is `tailscale ping` or a name lookup from the peer.

**`CorpDNS` must be true.** It can be false while `tailscale status` looks
healthy — every name lookup then fails with `Error -5`, which is how archiver#193's
cutover took down three services at once:

```bash
tailscale debug prefs | grep CorpDNS     # must be true
sudo tailscale set --accept-dns=true     # if it is not
```

## Joining — the setup script as it ran

`new --setup-script` runs once at first boot and cannot be re-run, and it runs
before any admin path exists. This is what created this node (key redacted),
passed inline over `POST https://exe.dev/exec` with `\n` escapes:

```bash
#!/bin/bash
set -euo pipefail
trap 'sudo shred -u /run/ts.key 2>/dev/null; sudo shred -u /exe.dev/setup 2>/dev/null; true' EXIT
exec >>/home/exedev/setup.log 2>&1 || exec >>/tmp/setup.log 2>&1
date -u
sudo install -m 600 /dev/null /run/ts.key
echo -n <tskey-auth-…> | sudo tee /run/ts.key >/dev/null
sudo systemctl enable --now tailscaled
sudo tailscale up --auth-key=file:/run/ts.key --hostname=replicator --ssh --accept-dns=true
echo setup-done
```

- **The trap comes first.** Under `set -e` a failing `tailscale up` aborts the
  script; archiver#193's did, and its key survived in `/exe.dev/setup` and the
  journal because the shred came after. Here the shred is unconditional, and it
  covers `/exe.dev/setup`, which holds the script — key included.
- **The key never touches an argv.** `echo` is a builtin; `sudo tee` sees only
  the path. The request body was built in a `0600` file and shredded after the
  POST.
- **A single-tag, pre-approved, non-ephemeral key, minted before the first join.**
  A tagged key applies its whole tag set, and tags bind at device registration:
  re-tagging later takes `tailscale logout` and a fresh `up`.

## Tailscale SSH journals your command line

Every command run over `tailscale ssh` is logged into **this node's** journal,
argv and all (`tailscaled: ssh-session(…): starting non-pty command: … --cmd=…`).
**Never put a secret in a `tailscale ssh` command line** — send it on stdin:

```bash
tar -cf - <files> | tailscale ssh exedev@replicator 'tar -C <dest> -xf -'
```

#88's residue audit found its own `grep` pattern in the journal that way; the
build that followed moved every key and env file over stdin for this reason.

## What this host does not run

- **A broker.** `redis-server` is installed for the integration tests that spawn
  their own, with its service **masked**; the bus is `co-broker`.
- **SocratiCode.** No Docker, Node or index here until CannObserv/notifier#57's
  shared Qdrant; `grep`/`rg` in the meantime.
- **Anything listening on the tailnet.** Loopback carries exe.dev's own
  `shelley.socket` (`127.0.0.1:9999`) — relevant to the fetch trust question in #89.

## Token vocabulary

| Token | Direction | Where |
|---|---|---|
| Tailscale **auth key** (`tskey-auth-…`) | this VM → joins the tailnet | spent at join, never copied here; its copy in the old host's repo `.env` (`TAILSCALE_KEY_REPLICATOR`) went with that checkout at #88's decommission |
| exe.dev **API token** (`exe1.…`) | agent → `POST https://exe.dev/exec` | repo `.env` as `EXE_API_TOKEN`; scope `new`/`ls`/`whoami`, no `rm` |
| Redis password (inside `REPLICATOR_REDIS_URL`) | worker → broker, as the `replicator` ACL user | `/etc/replicator/.env`, unit-scoped |
| GCS service-account keys | worker, wheelhouse step, `gcs`-marked tests | `/etc/replicator/*.json`, `root:exedev 0640` |

Independent secrets for independent hops: a tailnet key never authenticates a
Redis connection, and the Redis password is not what gets a host onto the tailnet.
