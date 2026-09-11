#!/bin/bash
# Wait until THIS host's tailnet address is assigned (#88).
#
# Run as a non-fatal `ExecStartPre` on replicator.service, ahead of
# check_redis_floor.sh. The unit's `After=tailscaled.service` orders against
# tailscaled *starting*, not *running*: on the with-service reboot test
# (2026-09-11) the floor check ran ~1 s before MagicDNS could resolve `broker`
# and reported the >=7.0 floor UNVERIFIED on a cold boot. The address appears
# at tailscaled's Running transition, so waiting for it lets the check see.
#
# Carried from CannObserv/broker's deploy/wait-for-tailnet-addr.sh (broker#1
# R1), keeping its two rules:
#   - probe /proc/net/fib_trie, NEVER `ip addr`: systemd's sandbox SIGSYS-kills
#     `ip` and blocks AF_NETLINK *silently*, so an `ip`-based probe exits 0 and
#     detects nothing (CannObserv/observo#479);
#   - match the address as "/32 host LOCAL", never as bare digits a peer's
#     route could share.
# And changing two things:
#   - the address is matched WHOLE, so waiting for 100.114.136.2 is not
#     satisfied by a local 100.114.136.20 (broker's `grep -F` is a prefix match);
#   - a timeout is reported, not "refusing to start": whether it blocks is the
#     unit's decision, and here it does not - the broker's absence never
#     hard-fails this worker's start.
#
# Usage: wait_for_tailnet_addr.sh <addr> [timeout_s] [fib_trie]
# The third argument exists for the tests, which drive fixture tables.
set -uo pipefail
ADDR="${1:?usage: wait_for_tailnet_addr.sh <addr> [timeout_s] [fib_trie]}"
TIMEOUT="${2:-30}"
FIB="${3:-/proc/net/fib_trie}"

# The kernel prints each address on its own `|-- <addr>` line with the entry's
# type on the next; anchor the end so a longer address cannot match.
PATTERN="\|-- ${ADDR//./\\.}[[:space:]]*\$"

for ((i = 0; i < TIMEOUT; i++)); do
    if grep -A1 -E -- "${PATTERN}" "${FIB}" 2>/dev/null | grep -q "host LOCAL"; then
        [ "$i" -gt 0 ] && echo "tailnet address ${ADDR} present after ${i}s"
        exit 0
    fi
    sleep 1
done
echo "wait_for_tailnet_addr: tailnet address ${ADDR} not assigned after ${TIMEOUT}s" >&2
exit 1
