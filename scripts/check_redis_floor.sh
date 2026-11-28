#!/usr/bin/env bash
# Assert the Redis change-bus broker meets the >=7.2 server floor.
#
# Run as an `ExecStartPre` on replicator.service. Mirrored from archiver's
# scripts/check_redis_floor.sh (archiver#109), where the floor is asserted as a
# courtesy by the bus *operator*. Here it is a direct requirement: Replicator is
# the cluster's first user of `AsyncBusConsumer.claim_stale_page` (crash recovery),
# which reads `XAUTOCLAIM`'s three-element reply — the deleted-ids element added
# in Redis *server* 7.0. Against a < 7.0 server the recovery path raises.
#
# The floor is 7.2, not 7.0, since #256: one engine, the broker's (Valkey 7.2
# since broker#85, `redis_version:7.2.4`). Every test, CI leg and rehearsal runs
# it alone, and 7.0 differs where this repo reads the bus (a consumer registers
# at its first read on 7.2, its first delivery on 7.0: #251). A 7.0 or 7.1 broker
# would run a worker nobody measured against it. Valkey 8 still reports 7.2.4.
#
# Redis runs on its own node, operated from CannObserv/broker (archiver#193 D6
# ended the arrangement where Archiver operated it); Replicator is a client and
# never ships or manages a broker.
#
# Soft on absence, hard on age — the unit orders After=tailscaled.service only,
# never Requires=, so the broker's absence must not block the start:
#   - unreachable / auth  -> the floor is UNVERIFIED, but don't block -> exit 0
#                            (unreachable only after REPLICATOR_REDIS_FLOOR_WAIT
#                            seconds of retrying - the boot wait below, #88)
#                            (the two are reported distinctly - archiver#195)
#   - version read, < 7.2 -> a real downgrade, block the worker  -> exit 1
#                            (so is one it cannot compare: a version was read)
#   - version read, >=7.2 -> ok                                  -> exit 0
#
# UNVERIFIED now outlives the start it was reported in (#133 CR 5). Boot waits a
# down broker out (`retry_at_boot`) instead of exiting at its first call, so a
# start that soft-passed here can consume once the broker returns without this
# check ever reading its version. The same exposure the consume loop's in-place
# reconnect already had; it bites only a broker downgraded during an outage.
#
# Unlike archiver's copy there is no "URL unset -> skip" branch: an unset
# REPLICATOR_REDIS_URL means the worker falls back to the same localhost default
# used below (see src/core/config.py), so skipping would leave the real
# connection unchecked.
#
# Unlike archiver's copy, the client is resolved, not named (#256): valkey-cli,
# the broker's engine's since broker#85, else redis-cli. rcli.sh resolves it,
# and this script reads the result from RCLI_CMD[0]. A missing client stays a
# soft skip, as in archiver's copy: the worker needs no CLI, so blocking would stop
# a healthy worker over a lost package. `apt-mark manual valkey-tools` is the
# guard against losing it.
set -uo pipefail

URL="${REPLICATOR_REDIS_URL:-redis://localhost:6379/0}"

# The credential goes to the client through REDISCLI_AUTH, never argv (#127):
# this runs on every service start, and /proc/<pid>/cmdline is world-readable.
# Located by expansion, not `dirname`: this runs with whatever PATH the unit has.
HERE="${BASH_SOURCE[0]%/*}"
[ "${HERE}" = "${BASH_SOURCE[0]}" ] && HERE=.
# Soft like every other cause of an unread version, but named: without it the
# probe cannot run, and the fall-through would guess "timed out?" instead.
# Bash's own message, kept on stderr, says whether it was absent or broken.
# shellcheck source=scripts/rcli.sh
if ! . "${HERE}/rcli.sh"; then
  echo "check_redis_floor: ${HERE}/rcli.sh could not be loaded — cannot probe the broker" >&2
  echo "check_redis_floor: — >=7.2 floor UNVERIFIED, not blocking start" >&2
  exit 0
fi
if ! rcli_command "${URL}"; then
  echo "check_redis_floor: — >=7.2 floor UNVERIFIED, not blocking start" >&2
  exit 0
fi

CLIENT="${RCLI_CMD[0]}"
if ! command -v "${CLIENT}" >/dev/null 2>&1; then
  echo "check_redis_floor: neither valkey-cli nor redis-cli is on PATH — cannot probe the broker" >&2
  echo "check_redis_floor: — >=7.2 floor UNVERIFIED, not blocking start" >&2
  exit 0
fi

# A rediss:// URL needs a TLS-capable client; a build without `--tls` cannot
# connect, so INFO returns nothing and the check silently no-ops (soft-skips
# below). Warn so that gap is visible — relevant at a managed-provider migration,
# where the URL becomes rediss:// but the floor still matters.
case "${URL}" in
  rediss://*)
    if ! "${CLIENT}" --help 2>&1 | grep -q -- '--tls'; then
      echo "check_redis_floor: ${CLIENT} lacks TLS support (no --tls) for a rediss:// URL —" >&2
      echo "check_redis_floor: the floor check will no-op; install a TLS-capable ${CLIENT}" >&2
    fi
    ;;
esac

# RCLI_CMD is `<client> [--user U] -u <URL without userinfo>`; `-u` still
# carries the scheme, so rediss:// keeps TLS. INFO server carries the
# `redis_version:MAJOR.MINOR.PATCH` line. Wrap in `timeout` so this ExecStartPre
# can never hang worker startup: the client has no connect-timeout flag, and a
# rediss:// URL against a plaintext/unreachable endpoint blocks on the TLS
# handshake indefinitely. A timeout kill yields an empty version -> soft-skip.
# REPLICATOR_REDIS_FLOOR_TIMEOUT (seconds, default 5) bounds the call.
TIMEOUT_SECS="${REPLICATOR_REDIS_FLOOR_TIMEOUT:-5}"
TIMEOUT_BIN="$(command -v timeout || true)"

# The boot wait (#88). At a cold boot the unit's After=tailscaled.service is not
# enough: tailscaled has started, but MagicDNS answers `broker` with no address
# (EAI_NODATA) for a moment longer, and this check landed inside that window on
# both of co-replicator's measured boots. A wait for the tailnet *address* was
# tried and disproved - the address was already local while the name still did
# not resolve - so this retries the dependency itself: an 'unreachable' probe is
# repeated once a second until REPLICATOR_REDIS_FLOOR_WAIT seconds have passed.
# Only 'unreachable': an auth refusal is an answer no wait can change, and a
# silent failure is a timeout kill that has already spent TIMEOUT_SECS.
# tests/test_deploy.py bounds WAIT + TIMEOUT against the unit's start budget.
WAIT_SECS="${REPLICATOR_REDIS_FLOOR_WAIT:-30}"
case "${WAIT_SECS}" in
  ''|*[!0-9]*)
    echo "check_redis_floor: REPLICATOR_REDIS_FLOOR_WAIT='${WAIT_SECS}' is not a whole number of seconds — using the default, 30" >&2
    WAIT_SECS=30
    ;;
esac

# Stderr is CAPTURED rather than discarded (CannObserv/archiver#195): an
# authentication rejection and an unreachable host both produce an empty reply,
# and stderr is the only thing that tells them apart. The results land in
# globals, NOT on stdout - a `$(redis_probe ...)` call would run this in a
# subshell and discard exactly the assignment the change exists to make.
ERR_FILE="$(mktemp)"
trap 'rm -f "${ERR_FILE}"' EXIT

PROBE_OUT=""
PROBE_ERR=""
redis_probe() {
  if [ -n "${TIMEOUT_BIN}" ]; then
    PROBE_OUT="$(rcli_exec "${TIMEOUT_BIN}" "${TIMEOUT_SECS}" "${RCLI_CMD[@]}" "$@" 2>"${ERR_FILE}" | tr -d '\r')"
  else
    PROBE_OUT="$(rcli_exec "${RCLI_CMD[@]}" "$@" 2>"${ERR_FILE}" | tr -d '\r')"
  fi
  PROBE_ERR="$(tr -d '\r' < "${ERR_FILE}")"
}

# Three answers, because they want three different operator responses - and
# because the message that conflated them ran on every start of this service
# for days while describing the wrong system.
#
#   auth        reached the broker, it refused the credential
#   unreachable never got that far
#   unknown     no stderr to go on (a timeout kill leaves none) - say so rather
#               than guess; guessing is what misled last time
probe_failure_kind() {
  case "${PROBE_ERR}" in
    *WRONGPASS*|*NOAUTH*|*NOPERM*|*"invalid username-password"*|*"AUTH failed"*|*"Authentication required"*)
      echo auth ;;
    *"Could not connect"*|*"Connection refused"*|*"onnection timed out"*|\
    *"Name or service not known"*|*"No route to host"*|*"Temporary failure in name resolution"*|\
    *"onnection reset"*|*"Network is unreachable"*)
      echo unreachable ;;
    *)
      echo unknown ;;
  esac
}

started=${SECONDS}
while :; do
  redis_probe INFO server
  version="$(printf '%s\n' "${PROBE_OUT}" | sed -n 's/^redis_version:\(.*\)$/\1/p')"
  [ -n "${version}" ] && break
  [ "$(probe_failure_kind)" = unreachable ] || break
  [ $(( SECONDS - started )) -lt "${WAIT_SECS}" ] || break
  sleep 1
done
waited=$(( SECONDS - started ))
if [ -n "${version}" ] && [ "${waited}" -gt 0 ]; then
  echo "check_redis_floor: broker reachable after ${waited}s"
fi

if [ -z "${version}" ]; then
  # Whatever the cause, the >=7.2 floor was NOT checked. Say "unverified",
  # never nothing: a guard known to be off is a different situation from one
  # assumed to be on, and only the first gets looked at.
  case "$(probe_failure_kind)" in
    auth)
      echo "check_redis_floor: reached the broker but could not authenticate — >=7.2 floor UNVERIFIED" >&2
      echo "check_redis_floor: broker said: ${PROBE_ERR}" >&2
      echo "check_redis_floor: this probe authenticates as the worker does (#127), so expect the" >&2
      echo "check_redis_floor: worker to be refused too — check REPLICATOR_REDIS_URL's user and" >&2
      echo "check_redis_floor: password against broker's ACL. Not blocking start: the worker" >&2
      echo "check_redis_floor: reports its own refusal, and this guard asserts the version only" >&2
      ;;
    unreachable)
      echo "check_redis_floor: broker unreachable after ${waited}s of retrying — >=7.2 floor UNVERIFIED, not blocking start" >&2
      echo "check_redis_floor: broker said: ${PROBE_ERR}" >&2
      ;;
    *)
      echo "check_redis_floor: could not reach or authenticate against the broker (probe timed out?)" >&2
      echo "check_redis_floor: — >=7.2 floor UNVERIFIED, not blocking start" >&2
      ;;
  esac
  exit 0
fi

# MAJOR.MINOR[.PATCH], both numeric, or it fails: a floor that waved through
# what it could not parse would be no floor.
major="" minor=""
if [[ "${version}" =~ ^([0-9]+)\.([0-9]+)(\.|$) ]]; then
  major="${BASH_REMATCH[1]}" minor="${BASH_REMATCH[2]}"
fi
if [ -z "${major}" ] || (( 10#${major} < 7 || (10#${major} == 7 && 10#${minor} < 2) )); then
  echo "check_redis_floor: Redis ${version} is below the >=7.2 floor" >&2
  echo "check_redis_floor: the broker's engine is Valkey 7.2 (redis_version 7.2.4), the only one tested (#256);" >&2
  echo "check_redis_floor: claim_stale_page (XAUTOCLAIM three-element reply) needs server >= 7.0 regardless" >&2
  exit 1
fi

echo "check_redis_floor: Redis ${version} meets the >=7.2 floor (via ${CLIENT})"
exit 0
