#!/usr/bin/env bash
# Assert the Redis change-bus broker meets the >=7.0 server floor.
#
# Run as an `ExecStartPre` on replicator.service. Mirrored from archiver's
# scripts/check_redis_floor.sh (archiver#109), where the floor is asserted as a
# courtesy by the bus *operator*. Here it is a direct requirement: Replicator is
# the cluster's first user of `AsyncBusConsumer.claim_stale` (crash recovery),
# which reads `XAUTOCLAIM`'s three-element reply — the deleted-ids element added
# in Redis *server* 7.0. Against a < 7.0 server the recovery path raises.
#
# Redis itself is Archiver-operated cluster infrastructure; Replicator is a
# client and never ships or manages a broker.
#
# Soft on absence, hard on age — matching replicator.service's Wants=/After=
# (not Requires=):
#   - broker unreachable  -> may still be starting; don't block  -> exit 0 (warn)
#   - version read, < 7.0 -> a real downgrade, block the worker  -> exit 1
#   - version read, >=7.0 -> ok                                  -> exit 0
#
# Unlike archiver's copy there is no "URL unset -> skip" branch: an unset
# REPLICATOR_REDIS_URL means the worker falls back to the same localhost default
# used below (see src/core/config.py), so skipping would leave the real
# connection unchecked.
set -uo pipefail

URL="${REPLICATOR_REDIS_URL:-redis://localhost:6379/0}"

if ! command -v redis-cli >/dev/null 2>&1; then
  echo "check_redis_floor: redis-cli not found — cannot verify floor, not blocking start" >&2
  exit 0
fi

# A rediss:// URL needs a TLS-capable redis-cli; a build without `--tls` cannot
# connect, so INFO returns nothing and the check silently no-ops (soft-skips
# below). Warn so that gap is visible — relevant at a managed-provider migration,
# where the URL becomes rediss:// but the floor still matters.
case "${URL}" in
  rediss://*)
    if ! redis-cli --help 2>&1 | grep -q -- '--tls'; then
      echo "check_redis_floor: redis-cli lacks TLS support (no --tls) for a rediss:// URL —" >&2
      echo "check_redis_floor: the floor check will no-op; install a TLS-capable redis-cli" >&2
    fi
    ;;
esac

# `-u` accepts redis:// and rediss:// URLs (TLS + auth). INFO server carries the
# `redis_version:MAJOR.MINOR.PATCH` line. Wrap in `timeout` so this ExecStartPre
# can never hang worker startup: redis-cli has no connect-timeout flag, and a
# rediss:// URL against a plaintext/unreachable endpoint blocks on the TLS
# handshake indefinitely. A timeout kill yields an empty version -> soft-skip.
# REPLICATOR_REDIS_FLOOR_TIMEOUT (seconds, default 5) bounds the call.
TIMEOUT_SECS="${REPLICATOR_REDIS_FLOOR_TIMEOUT:-5}"
TIMEOUT_BIN="$(command -v timeout || true)"
if [ -n "${TIMEOUT_BIN}" ]; then
  version="$("${TIMEOUT_BIN}" "${TIMEOUT_SECS}" redis-cli -u "${URL}" INFO server 2>/dev/null | tr -d '\r' | sed -n 's/^redis_version:\(.*\)$/\1/p')"
else
  version="$(redis-cli -u "${URL}" INFO server 2>/dev/null | tr -d '\r' | sed -n 's/^redis_version:\(.*\)$/\1/p')"
fi

if [ -z "${version}" ]; then
  echo "check_redis_floor: could not read redis_version (broker unreachable?) — not blocking start" >&2
  exit 0
fi

major="${version%%.*}"
if ! [ "${major}" -ge 7 ] 2>/dev/null; then
  echo "check_redis_floor: Redis ${version} is below the >=7.0 change-bus floor" >&2
  echo "check_redis_floor: claim_stale (XAUTOCLAIM three-element reply) requires server >= 7.0" >&2
  exit 1
fi

echo "check_redis_floor: Redis ${version} meets the >=7.0 floor"
exit 0
