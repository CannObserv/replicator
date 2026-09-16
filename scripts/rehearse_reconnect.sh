#!/usr/bin/env bash
# Rehearse the half of reconnection that systemd owns (#94).
#
# WHAT THIS IS FOR. `tests/worker/test_reconnect_integration.py` proves the
# in-process property against a real socket: the loop rides out a short outage,
# gives up on a long one, and a freshly started worker picks up what was
# stranded. What it cannot prove is the handoff — that systemd actually restarts
# the worker it just watched exit, that the ExecStartPre chain passes on the way
# back in, and that the start limit is not spent before the broker returns.
#
# That handoff is exactly where 2026-09-16 was lost. The worker exited as
# designed; three ExecStartPre refusals then burned StartLimitBurst in sixteen
# seconds, and the unit was `failed` sixteen minutes before redis came back. No
# pytest models that, so this script does.
#
# WHAT IT REFUSES TO TOUCH. It never stops `co-broker` — that would take down
# three services — and it never touches `replicator.service`. It spawns its own
# `redis-server` on a loopback port it owns, and drives a scratch unit under
# /run/systemd/system (tmpfs; gone on reboot) with its own consumer names, so it
# cannot steal the live worker's pending entries.
#
# THE TIMINGS ARE COMPRESSED, DELIBERATELY. The scratch unit overrides the cycle
# ceiling and the backoff so an exit takes seconds rather than the production ten
# minutes. What is under test is systemd's semantics — Restart=, the start limit,
# the ExecStartPre chain — not the wall-clock of the backoff, which the pytest
# half covers. The one production value it keeps is StartLimitBurst, because
# whether the budget survives the outage is the question.
#
# Usage:  sudo bash scripts/rehearse_reconnect.sh [--keep]
#         --keep   leave the scratch unit and broker up for inspection
#
# Exit codes:
#   0  the worker reconnected with no human step
#   1  it did not — the message says which assertion failed
#   2  the rehearsal could not run (missing tool, not root, unsafe state)
set -uo pipefail

UNIT="rehearse-replicator"
RUNDIR="/run/systemd/system"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

say() { echo "rehearse: $*" >&2; }
die() { say "ABORT — $*"; exit 2; }
fail() { say "FAIL — $*"; exit 1; }

[ "$(id -u)" -eq 0 ] || die "must run as root (systemd units and daemon-reload)"
command -v redis-server >/dev/null || die "redis-server is not on PATH"
command -v redis-cli >/dev/null || die "redis-cli is not on PATH"
[ -f "${REPO}/src/worker/main.py" ] || die "cannot find the worker at ${REPO}"

# A port nothing holds. Racy by nature, which is why the identity check below
# exists rather than trusting the bind.
PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
WORKDIR="$(mktemp -d)"
URL="redis://127.0.0.1:${PORT}/0"
BROKER_PID=""

cleanup() {
  if [ "${KEEP}" = "1" ]; then
    say "--keep: leaving ${UNIT}.service and the broker on ${PORT} up"
    return
  fi
  systemctl stop "${UNIT}.service" 2>/dev/null
  systemctl reset-failed "${UNIT}.service" 2>/dev/null
  rm -f "${RUNDIR}/${UNIT}.service"
  systemctl daemon-reload 2>/dev/null
  [ -n "${BROKER_PID}" ] && kill "${BROKER_PID}" 2>/dev/null
  rm -rf "${WORKDIR}"
}
trap cleanup EXIT

start_broker() {
  # --appendonly yes: the stream and the consumer group must survive the restart,
  # which is what the real broker's AOF gave us on 2026-09-16. Without it the
  # worker would come back to NOGROUP, a different incident entirely.
  redis-server --port "${PORT}" --bind 127.0.0.1 --appendonly yes \
    --dir "${WORKDIR}" --daemonize no >/dev/null 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 100); do
    if redis-cli -u "${URL}" PING 2>/dev/null | grep -q PONG; then
      local reported
      reported="$(redis-cli -u "${URL}" INFO server 2>/dev/null | sed -n 's/^process_id:\([0-9]*\).*/\1/p')"
      [ "${reported}" = "${BROKER_PID}" ] || die "port ${PORT} is answered by pid ${reported}, not the ${BROKER_PID} we started — refusing to drive a broker we do not own"
      return 0
    fi
    sleep 0.1
  done
  die "the scratch broker never answered on ${PORT}"
}

stop_broker() {
  [ -n "${BROKER_PID}" ] || return 0
  kill "${BROKER_PID}" 2>/dev/null
  wait "${BROKER_PID}" 2>/dev/null
  BROKER_PID=""
  for _ in $(seq 1 50); do
    redis-cli -u "${URL}" PING 2>/dev/null | grep -q PONG || return 0
    sleep 0.1
  done
  die "the scratch broker on ${PORT} would not stop"
}

nrestarts() { systemctl show "${UNIT}.service" -p NRestarts --value 2>/dev/null; }
active()    { systemctl is-active "${UNIT}.service" 2>/dev/null; }

say "scratch broker on ${PORT}, scratch unit ${UNIT}.service, repo ${REPO}"
start_broker

# The scratch unit. Restart= and StartLimitBurst match the real unit, because
# they are what is under test; the cycle ceiling and backoff are compressed.
BURST="$(sed -n 's/^StartLimitBurst=\([0-9]*\).*/\1/p' "${REPO}/deploy/replicator.service" | tail -1)"
[ -n "${BURST}" ] || die "could not read StartLimitBurst from deploy/replicator.service"
say "using the real unit's StartLimitBurst=${BURST}"

cat > "${RUNDIR}/${UNIT}.service" <<EOF
[Unit]
Description=REHEARSAL (#94) — replicator against a broker that goes away and returns
StartLimitIntervalSec=600
StartLimitBurst=${BURST}

[Service]
Type=simple
User=exedev
WorkingDirectory=${REPO}
Environment=REPLICATOR_REDIS_URL=${URL}
Environment=REPLICATOR_CONSUMER_NAME=rehearsal-fetch-1
Environment=REPLICATOR_REPLICATE_CONSUMER_NAME=rehearsal-replicate-1
Environment=REPLICATOR_MAX_CONSECUTIVE_CYCLE_FAILURES=4
Environment=REPLICATOR_ERROR_BACKOFF_BASE_SECONDS=0.2
Environment=REPLICATOR_ERROR_BACKOFF_MAX_SECONDS=1
Environment=REPLICATOR_ALLOW_ANY_CHECKOUT=1
Environment=BUILD_ID=rehearsal
ExecStart=/usr/local/bin/uv run --frozen --no-sync python -m src.worker.main
Restart=on-failure
RestartSec=1
TimeoutStopSec=30
EOF

systemctl daemon-reload
systemctl start "${UNIT}.service" || die "the scratch unit would not start"

# 1. Healthy first — otherwise a later success proves only that it ever worked.
for _ in $(seq 1 100); do
  [ "$(active)" = "active" ] && break
  sleep 0.2
done
[ "$(active)" = "active" ] || fail "the worker never reached active against a healthy broker"
say "PASS 1/4 — worker active against a healthy broker"

BEFORE="$(nrestarts)"

# 2. The outage. Longer than the compressed ceiling, so the worker exits and
#    systemd is asked to do its half.
stop_broker
say "broker down; waiting for the worker to exhaust its cycle ceiling"
EXITED=0
for _ in $(seq 1 200); do
  [ "$(nrestarts)" != "${BEFORE}" ] && { EXITED=1; break; }
  [ "$(active)" = "failed" ] && break
  sleep 0.2
done
[ "${EXITED}" = "1" ] || fail "the worker never exited during the outage (NRestarts stuck at ${BEFORE}) — it would look alive to systemd while consuming nothing"
say "PASS 2/4 — worker exited and systemd restarted it (NRestarts ${BEFORE} -> $(nrestarts))"

# 3. The budget must survive the outage. This is the assertion #94 turns on: on
#    2026-09-16 the unit was `failed` sixteen minutes before the broker returned.
[ "$(active)" != "failed" ] || fail "the unit hit its start limit while the broker was still down — an operator would be required, which is precisely the #94 outage"
say "PASS 3/4 — start budget survived the outage (unit is $(active))"

# 4. The broker returns, and consumption resumes with nobody typing anything.
start_broker
say "broker back; waiting for the worker to reconnect"
RECOVERED=0
for _ in $(seq 1 300); do
  if [ "$(active)" = "active" ] && redis-cli -u "${URL}" PING 2>/dev/null | grep -q PONG; then
    # `worker ready` after the broker returned is the reconnection: the unit's
    # whole ExecStartPre chain passed on the way back in.
    if journalctl -u "${UNIT}.service" --since "-2min" 2>/dev/null | grep -q '"message": "worker ready"'; then
      RECOVERED=1
      break
    fi
  fi
  sleep 0.2
done
[ "${RECOVERED}" = "1" ] || fail "the worker did not come back after the broker returned (unit is $(active))"

say "PASS 4/4 — worker reconnected with no human step"
say "OK — the handoff systemd owns works: exit, restart, guards pass, consumption resumes"
exit 0
