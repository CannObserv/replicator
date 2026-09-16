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
#   REPLICATOR_NOTIFY_TOKEN            sent as `Authorization: Bearer`. Omitted
#                                      entirely when unset: an empty bearer reads
#                                      as a configured credential and is worse
#                                      than no header.
#   REPLICATOR_NOTIFY_TIMEOUT_SECONDS  dispatch ceiling (default below).
#
# The payload is a self-describing incident object, deliberately NOT the
# cohort notifier's request schema. Pointing this at `http://notifier:9000`
# means giving the endpoint something that maps the payload onto that service's
# {template_id, variables, channel_ids} shape — that mapping is the operator's,
# and keeping it out of here is what lets the same handler serve a plain webhook.
#
# Exit codes:
#   0  always — recorded, whether or not anything was dispatched.
set -uo pipefail

UNIT="${1:-<unknown>}"
URL="${REPLICATOR_NOTIFY_URL:-}"
TOKEN="${REPLICATOR_NOTIFY_TOKEN:-}"
TIMEOUT="${REPLICATOR_NOTIFY_TIMEOUT_SECONDS:-10}"

# Read by the unit from /run/replicator/build-id, which outlives a failed start —
# that persistence is the point here, since the build that failed is exactly what
# an incident record needs to name.
BUILD="${BUILD_ID:-<unknown>}"
HOST="$(hostname 2>/dev/null || echo '<unknown>')"
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

PAYLOAD="$(
  printf '{"level":"CRITICAL","event":"unit_failed","unit":"%s","host":"%s","build":"%s","message":"%s","timestamp":"%s"}' \
    "$(_json "${UNIT}")" "$(_json "${HOST}")" "$(_json "${BUILD}")" "${MESSAGE}" "${NOW}"
)"

# Built as an array so an unset token contributes no argument at all, rather than
# an empty -H that curl would send as a bare header.
CURL_ARGS=(
  --silent --show-error
  --request POST
  --header 'Content-Type: application/json'
  --max-time "${TIMEOUT}"
  --output /dev/null
  --write-out '%{http_code}'
  --data "${PAYLOAD}"
)
if [ -n "${TOKEN}" ]; then
  CURL_ARGS+=(--header "Authorization: Bearer ${TOKEN}")
fi

STATUS="$(curl "${CURL_ARGS[@]}" "${URL}" 2>/dev/null)"
RC=$?

# A 2xx is delivery; everything else — transport failure, malformed URL, 4xx, 5xx
# — is a failed dispatch, reported and dropped. There is no retry: systemd is not
# holding a queue for us, and the record above already survived.
if [ "${RC}" -eq 0 ] && [ "${STATUS#2}" != "${STATUS}" ] && [ ${#STATUS} -eq 3 ]; then
  printf '{"level":"INFO","event":"unit_failed_notified","unit":"%s","notify_dispatched":true,"http_status":%s}\n' \
    "$(_json "${UNIT}")" "${STATUS}" >&2
  exit 0
fi

printf '{"level":"ERROR","event":"unit_failed_notify_failed","unit":"%s","notify_dispatched":false,"curl_exit":%s,"http_status":"%s"}\n' \
  "$(_json "${UNIT}")" "${RC}" "$(_json "${STATUS:-<none>}")" >&2
echo "notify_failure: dispatch failed — the incident is recorded above, not delivered" >&2
exit 0
