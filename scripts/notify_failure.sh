#!/usr/bin/env bash
# Report that a replicator unit entered a failed state (#94).
#
# Run as the `ExecStart` of `replicator-failure-notify@.service`, which
# `replicator.service` names in `OnFailure=` and instantiates with `%n`.
#
# WHY THIS EXISTS. The unit bounds its restart loops at both timescales and then
# stays `failed` deliberately — a wedged worker should read as `failed`, not as
# `active (running)` consuming nothing. That design is sound and stays. What was
# missing is the other half: the unit's own comment claimed a failure was visible
# "in systemctl status + OnFailure=" while the ini file carried no OnFailure= at
# all. On 2026-09-16 the unit sat failed for 56 minutes and the thing that
# noticed was a *sibling repo*, reading the broker from another VM.
#
# THE CONTRACT IS "NEVER MAKE THE OUTAGE WORSE". This runs only when something
# has already failed, so:
#
#   - every path exits 0. A failed notification must not add a second failed
#     unit to an incident that already has one.
#   - the journal record is the guarantee; the POST is best-effort. The record
#     is written BEFORE any dispatch is attempted, so a hung or killed dispatch
#     still leaves a trace naming the unit that failed.
#   - the dispatch is time-bounded. The handler gets its own start timeout from
#     systemd, and blowing through it is how a notifier outage would become a
#     second failed unit.
#
# NOTIFIER OUTAGES CORRELATE WITH BROKER OUTAGES — both are other VMs on the same
# tailnet, and the 2026-09-16 incident degraded the network path itself. An
# unreachable notifier is the expected case here, not the edge case, which is why
# it is soft and why the floor is a local journal record rather than a delivery
# receipt.
#
# CONFIGURATION (all optional, all from the environment systemd provides):
#   REPLICATOR_NOTIFY_URL              endpoint to POST the incident to. Unset
#                                      (the compiled-in default) means record
#                                      only — no outbound call at all.
#   REPLICATOR_NOTIFY_TOKEN            the credential. `Authorization: Bearer` in
#                                      webhook mode, `X-API-Key` in notifier mode.
#                                      Omitted entirely when unset: an empty one
#                                      reads as a configured credential and is
#                                      worse than no header.
#   REPLICATOR_NOTIFY_TIMEOUT_SECONDS  dispatch ceiling (default below).
#   REPLICATOR_NOTIFY_MODE             `webhook` (default) or `notifier` (#108).
#   REPLICATOR_NOTIFY_TEMPLATE_ID      notifier mode: the stored template's ULID.
#   REPLICATOR_NOTIFY_CHANNEL_IDS      notifier mode: comma-separated channel ULIDs.
#
# Webhook mode POSTs the self-describing incident object as-is, which is what
# lets the handler serve a plain webhook. Notifier mode wraps the same seven
# fields in the cohort notifier's /dispatch request (agreed on
# CannObserv/notifier#70): they become `variables`, rendered by the template in
# deploy/notifier-template.json, and delivery is scored on the 202 body's
# `status` rather than on the 202.
#
# Exit codes:
#   0  always — recorded, whether or not anything was dispatched.
set -uo pipefail

UNIT="${1:-<unknown>}"
URL="${REPLICATOR_NOTIFY_URL:-}"
TOKEN="${REPLICATOR_NOTIFY_TOKEN:-}"
# A newline is refused rather than escaped (CR 14). curl's config format is
# line-oriented, and a value containing one ends at the newline — measured: the
# remainder is NOT taken as a directive (an injected `user = "…"` line is ignored
# and no Basic auth is sent), so the risk is not injection. It is that curl would
# dispatch a silently shortened credential, whose 401 reads as the notifier's
# fault rather than the token's — the same masquerade CR 4 removed for the
# timeout. Dropping the header is the honest failure: the record still says the
# dispatch went out, and the line below says why it was unauthenticated.
case "${TOKEN}" in
  *[$'\n\r']*)
    echo "notify_failure: REPLICATOR_NOTIFY_TOKEN contains a newline, which curl's config format cannot carry — dispatching without it" >&2
    TOKEN=""
    ;;
esac

# Named and defaulted rather than handed to curl as-is (CR 4). An unusable value
# would otherwise reach the failure record as a bare `curl_exit: 2`, which reads
# as "the notifier is down" — so an operator mid-incident chases the wrong VM
# instead of their own typo. Same rule REPLICATOR_REDIS_FLOOR_WAIT follows.
#
# The cap is the second half and not optional (CR 12). Validating only the shape
# left 99999 acceptable, and the handler unit kills this process at
# TimeoutStartSec — so an over-large value is enforced by SIGKILL rather than by
# curl, which is exactly the "notification becomes a second failed unit" outcome
# the ceiling exists to prevent. TIMEOUT_MAX must stay below that directive;
# tests/test_notify_failure.py reads both and pins the relation.
TIMEOUT_DEFAULT=10
TIMEOUT_MAX=30
TIMEOUT="${REPLICATOR_NOTIFY_TIMEOUT_SECONDS:-${TIMEOUT_DEFAULT}}"
# No '' arm: ${VAR:-default} substitutes on null as well as unset, so an empty
# value is already the default by the time it reaches here (CR 17).
case "${TIMEOUT}" in
  *[!0-9]* | 0)
    echo "notify_failure: REPLICATOR_NOTIFY_TIMEOUT_SECONDS=${TIMEOUT} is not a positive integer — using ${TIMEOUT_DEFAULT}" >&2
    TIMEOUT="${TIMEOUT_DEFAULT}"
    ;;
esac
if [ "${TIMEOUT}" -gt "${TIMEOUT_MAX}" ]; then
  echo "notify_failure: REPLICATOR_NOTIFY_TIMEOUT_SECONDS=${TIMEOUT} is above the ${TIMEOUT_MAX}s ceiling the unit's own TimeoutStartSec allows — using ${TIMEOUT_MAX}" >&2
  TIMEOUT="${TIMEOUT_MAX}"
fi

# Read by the unit from /run/replicator/build-id, which outlives a failed start —
# that persistence is the point here, since the build that failed is exactly what
# an incident record needs to name.
BUILD="${BUILD_ID:-<unknown>}"
# `||` alone catches a non-zero exit but not empty output (CR 8), and an empty
# `host` in an incident record is ambiguous where `<unknown>` is merely unknown.
HOST="$(hostname 2>/dev/null)"
HOST="${HOST:-<unknown>}"
NOW="$(date -u +%Y-%m-%dT%H:%M:%S.%6NZ)"

# Minimal JSON string escaping: backslash first (or it would re-escape the quotes
# this adds), then quote, then the control characters a unit name or hostname
# could carry. Enough for the four interpolations below, all of which are systemd
# identifiers or values this script produced.
_json() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  printf '%s' "$s"
}

MESSAGE="$(_json "${UNIT}") entered a failed state on $(_json "${HOST}") (build $(_json "${BUILD}"))"

# The floor. Written first and unconditionally: if everything below this line
# fails, is killed, or never runs, the incident is still in the journal.
printf '{"level":"CRITICAL","event":"unit_failed","unit":"%s","host":"%s","build":"%s","message":"%s","timestamp":"%s"}\n' \
  "$(_json "${UNIT}")" "$(_json "${HOST}")" "$(_json "${BUILD}")" "${MESSAGE}" "${NOW}" >&2

if [ -z "${URL}" ]; then
  echo "notify_failure: no REPLICATOR_NOTIFY_URL configured — recorded locally only" >&2
  exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "notify_failure: curl not found — cannot dispatch, recorded locally only" >&2
  exit 0
fi

INCIDENT="$(
  printf '{"level":"CRITICAL","event":"unit_failed","unit":"%s","host":"%s","build":"%s","message":"%s","timestamp":"%s"}' \
    "$(_json "${UNIT}")" "$(_json "${HOST}")" "$(_json "${BUILD}")" "${MESSAGE}" "${NOW}"
)"

# An unknown mode dispatches nothing rather than falling back to webhook: the
# likeliest unknown value is a misspelt `notifier`, and the flat payload sent to a
# /dispatch URL is a 422 that reads as notifier's fault.
MODE="${REPLICATOR_NOTIFY_MODE:-webhook}"
case "${MODE}" in
  webhook)
    PAYLOAD="${INCIDENT}"
    ;;
  notifier)
    TEMPLATE_ID="${REPLICATOR_NOTIFY_TEMPLATE_ID:-}"
    CHANNELS=""
    IFS=',' read -ra _RAW_CHANNELS <<< "${REPLICATOR_NOTIFY_CHANNEL_IDS:-}"
    for _c in "${_RAW_CHANNELS[@]}"; do
      _c="${_c//[[:space:]]/}"
      [ -n "${_c}" ] && CHANNELS="${CHANNELS:+${CHANNELS},}\"$(_json "${_c}")\""
    done
    # Both are required by the request notifier accepts (channel_ids has
    # minItems 1 even with a template), so a request missing either is a 422
    # this handler already knows it would get.
    if [ -z "${TEMPLATE_ID}" ]; then
      echo "notify_failure: notifier mode needs REPLICATOR_NOTIFY_TEMPLATE_ID — recorded locally only" >&2
      exit 0
    fi
    if [ -z "${CHANNELS}" ]; then
      echo "notify_failure: notifier mode needs REPLICATOR_NOTIFY_CHANNEL_IDS — recorded locally only" >&2
      exit 0
    fi
    # The failed run's InvocationID, which systemd ≥251 hands an OnFailure=
    # handler. One key per failure, so a POST that timed out after landing
    # replays instead of paging twice. Null without one: notifier answers a
    # replayed key with the prior record and makes NO new delivery attempt, so a
    # key that could repeat across failures would silently swallow an alert.
    IDEMPOTENCY="null"
    if [ -n "${MONITOR_INVOCATION_ID:-}" ]; then
      IDEMPOTENCY="\"$(_json "${UNIT}:${MONITOR_INVOCATION_ID}")\""
    fi
    PAYLOAD="$(
      printf '{"template_id":"%s","channel_ids":[%s],"variables":%s,"idempotency_key":%s,"metadata":{"event":"unit_failed"}}' \
        "$(_json "${TEMPLATE_ID}")" "${CHANNELS}" "${INCIDENT}" "${IDEMPOTENCY}"
    )"
    ;;
  *)
    echo "notify_failure: REPLICATOR_NOTIFY_MODE=${MODE} is neither webhook nor notifier — recorded locally only" >&2
    exit 0
    ;;
esac
AUTH_HEADER="Authorization: Bearer"
[ "${MODE}" = notifier ] && AUTH_HEADER="X-API-Key:"

# No --show-error: curl's stderr is discarded below, so the flag was dead config
# and the sentence it prints was being dropped (CR 2). What replaces it is the
# exit-code mapping further down — curl's codes are stable and documented, and
# reading them costs no tempfile on a path that runs only during an incident.
#
# Built as an array so an unset token contributes no argument at all, rather than
# an empty -H that curl would send as a bare header.
CURL_ARGS=(
  --silent
  --request POST
  --header 'Content-Type: application/json'
  --max-time "${TIMEOUT}"
  --write-out '\n%{http_code}'
  --data "${PAYLOAD}"
)
# The token goes in on STDIN, never in argv (CR 5). A curl invocation carrying
# `--header "Authorization: Bearer ..."` publishes the credential to every user
# on the box for the life of the process, via ps and /proc/<pid>/cmdline —
# AGENTS.md treats this env boundary as a security boundary, and argv is the
# usual way one leaks. curl's config format is `key = "value"` with backslash
# escapes, so the two characters that can break out of it are escaped first.
_dispatch() {
  if [ -n "${TOKEN}" ]; then
    local escaped="${TOKEN//\\/\\\\}"
    escaped="${escaped//\"/\\\"}"
    printf 'header = "%s %s"\n' "${AUTH_HEADER}" "${escaped}" \
      | curl "${CURL_ARGS[@]}" --config - "${URL}"
  else
    curl "${CURL_ARGS[@]}" "${URL}"
  fi
}

# The body comes back on stdout with the status code on its own last line.
RESPONSE="$(_dispatch 2>/dev/null)"
RC=$?
STATUS="${RESPONSE##*$'\n'}"
BODY="${RESPONSE%$'\n'*}"
[ "${BODY}" = "${RESPONSE}" ] && BODY=""

# A 2xx is delivery; everything else — transport failure, malformed URL, 4xx, 5xx
# — is a failed dispatch, reported and dropped. There is no retry: systemd is not
# holding a queue for us, and the record above already survived.
if [ "${RC}" -eq 0 ] && [ "${STATUS#2}" != "${STATUS}" ] && [ ${#STATUS} -eq 3 ]; then
  # http_status is quoted in BOTH branches (CR 3). It was a JSON number here and
  # a string below, and one field name with two types breaks the consumer this
  # payload exists to feed.
  if [ "${MODE}" = webhook ]; then
    printf '{"level":"INFO","event":"unit_failed_notified","unit":"%s","host":"%s","build":"%s","notify_dispatched":true,"http_status":"%s"}\n' \
      "$(_json "${UNIT}")" "$(_json "${HOST}")" "$(_json "${BUILD}")" "$(_json "${STATUS}")" >&2
    exit 0
  fi
  # Notifier answers 202 whether or not anyone was told; the body's top-level
  # `status` is the delivery verdict. jq rather than a pattern, because every
  # entry in `attempts` carries a `status` of its own. No jq, or a body that is
  # not a DispatchOut, is `unknown` — never scored as delivered.
  DELIVERY="$(printf '%s' "${BODY}" | jq -r '.status | select(type == "string")' 2>/dev/null)"
  case "${DELIVERY}" in
    succeeded) LEVEL=INFO    EVENT=unit_failed_notified ;;
    partial)   LEVEL=WARNING EVENT=unit_failed_notify_degraded ;;
    failed)    LEVEL=ERROR   EVENT=unit_failed_notify_undelivered ;;
    *)         LEVEL=WARNING EVENT=unit_failed_notify_unconfirmed DELIVERY=unknown ;;
  esac
  printf '{"level":"%s","event":"%s","unit":"%s","host":"%s","build":"%s","notify_dispatched":true,"http_status":"%s","delivery_status":"%s"}\n' \
    "${LEVEL}" "${EVENT}" "$(_json "${UNIT}")" "$(_json "${HOST}")" "$(_json "${BUILD}")" \
    "$(_json "${STATUS}")" "$(_json "${DELIVERY}")" >&2
  exit 0
fi

# What the exit code meant, since curl's own sentence was never going to survive
# the redirect above. Only the codes this path can realistically produce are
# named; anything else reports the number, which is still greppable.
#
# No 22 arm: curl returns it only under --fail, which is deliberately not passed
# (CR 15). Without it a 4xx/5xx comes back as RC 0 with the code in STATUS, which
# is what lets the last arm tell a 500 apart from a refusal — an arm claiming to
# handle error statuses would have misdirected a reader looking for exactly that.
case "${RC}" in
  2)  REASON="bad curl configuration — check REPLICATOR_NOTIFY_TIMEOUT_SECONDS" ;;
  3)  REASON="malformed REPLICATOR_NOTIFY_URL" ;;
  6)  REASON="could not resolve the notifier host" ;;
  7)  REASON="connection refused by the notifier" ;;
  28) REASON="timed out after ${TIMEOUT}s" ;;
  35 | 60) REASON="TLS handshake or certificate failure" ;;
  0)  REASON="notifier answered ${STATUS:-<none>}, which is not a 2xx" ;;
  *)  REASON="curl exit ${RC}" ;;
esac

# unit, host and build are repeated rather than left to the record above (CR 9):
# a consumer that ingests only the dispatch-outcome line can attribute it without
# having to correlate two records.
printf '{"level":"ERROR","event":"unit_failed_notify_failed","unit":"%s","host":"%s","build":"%s","notify_dispatched":false,"curl_exit":%s,"http_status":"%s","reason":"%s"}\n' \
  "$(_json "${UNIT}")" "$(_json "${HOST}")" "$(_json "${BUILD}")" "${RC}" \
  "$(_json "${STATUS:-<none>}")" "$(_json "${REASON}")" >&2
echo "notify_failure: dispatch failed (${REASON}) — the incident is recorded above, not delivered" >&2
exit 0
