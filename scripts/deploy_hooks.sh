# scripts/deploy_hooks.sh — this repo's half of its release deploy.
#
# Sourced by scripts/deploy.sh, the deploying-releases core
# (gregoryfoster/skills#372), which is installed, never edited here. This file is
# the repo's own: its configuration, and at most six hooks. Every variable has a
# default in the core and every hook a default that does nothing (verify's:
# "no check for this unit"). The contract, each hook's arguments and exit codes,
# and every pilot's hooks filled in: deploying-releases'
# references/hook-contract.md. Why each value here is what it is:
# docs/plans/2026-11-14-releases-design.md (#151).
#
# A hook runs in a subshell with errexit on, from the directory the core names.
# Return non-zero to refuse (before the switch) or fail (after it).
#
# Every variable below is read by the core, which sources this file.
# shellcheck shell=bash disable=SC2034

# --- configuration -------------------------------------------------------------

PROJECT="replicator"

SERVICE_DIR=.
UNITS_DIR=deploy

# A public repo, so CI is read anonymously. CI_JOBS are the jobs' display names,
# as GitHub's API lists them; tests/deploy/test_deploy_hooks.py holds them to
# .github/workflows/ci.yml. The weekly measurement commit (github-actions[bot])
# has no CI run, and the core refuses it: deploy the commit under it.
CI=github
CI_AUTH=anonymous
CI_WORKFLOW=ci.yml
GITHUB_REPO="CannObserv/replicator"
CI_JOBS=(
  "lint"
  "test"
  "browser (the renderer's confinements, pinned headless shell)"
  "gcs (T4 against the test bucket)"
)

PYTHON=/usr/bin/python3.12
# co-core resolves from the private co-pypi index (#183), through this repo's own
# wrapper until #201 swaps in the shared one (install.sh --co-pypi --replace).
UV_WRAPPER=scripts/co-pypi.sh
# This wrapper has no default key path (scripts/co_pypi_token.py refuses an unset
# variable), and a deploy's shell loads no env file: shipping-work runs the deploy
# command bare. The shared wrapper defaults to this same path, so this line goes
# with #201.
export REPLICATOR_CO_PYPI_CREDENTIALS="${REPLICATOR_CO_PYPI_CREDENTIALS:-/etc/replicator/co-pypi-reader.json}"
# scripts/ too: patching-hosts runs scripts.inflight from the live release.
SOURCE_DIRS=(src scripts)
IMPORT_MODULES=(src.worker.main scripts.inflight)

# Installed by hand, by the operator (docs/MEMORY-PROTECTION.md, DEPLOYMENT.md):
# compared after every deploy, never installed.
HOST_CONFIGS=(
  "99-co-replicator-memory.conf=/etc/sysctl.d/99-co-replicator-memory.conf"
  "apt/preferences.d/tailscale.pref=/etc/apt/preferences.d/tailscale.pref"
  "needrestart.conf.d/replicator.conf=/etc/needrestart/conf.d/replicator.conf"
  "system.slice.d/replicator-memory.conf=/etc/systemd/system/system.slice.d/replicator-memory.conf"
  "tailscaled.service.d/memory.conf=/etc/systemd/system/tailscaled.service.d/memory.conf"
)
ENABLE_NEW=()
# Zero spares. The core keeps the live release and the one it replaced whatever
# KEEP says, so a one-step rollback needs none; each release is ~259 MB, and two
# put root at ~82.7% against the patching run's 84.0% ceiling (the design doc's
# Disk section, measured 2026-11-14).
KEEP=0

# --- hooks -------------------------------------------------------------------

# The release passes its own writer guard (#52): from a release, checkout_refusal()
# must answer None, or every replicate and persist command would close
# provider_disabled. Asked here, before the switch, rather than learned from the
# first refused command. Runs after REVISION is written, which the guard reads.
post_build_check() {
  .venv/bin/python -I -B -c 'import sys
from src.worker.checkout import checkout_refusal
sys.exit(checkout_refusal())'
}

# The worker: a new main PID running from the release (the core's own check), then
# its own start record naming the build. "worker ready" is logged after the floor
# check, the broker connection, the groups and the writers, and its "build" is
# BUILD_ID as the unit stamped it from REVISION, so one line proves the whole start.
# The failure handler is a template: the core neither restarts nor verifies it.
verify() { # <unit> <want>
  case "$1" in
    replicator.service)
      verify_started_on "$1" "$2" || return 1
      local pid deadline=$((SECONDS + 120))
      pid="$(systemctl show -p MainPID --value "$1")"
      while :; do
        if journalctl _PID="$pid" -o cat --no-pager 2>/dev/null |
          jq -Rne --arg want "$2" \
            '[inputs | fromjson? | select(.message? == "worker ready" and .build? == $want)] | length > 0' \
            >/dev/null; then
          say "$1 logged \"worker ready\" on $2 (pid $pid)"
          return 0
        fi
        ((SECONDS < deadline)) || break
        sleep 5
      done
      note "$1 (pid $pid) logged no \"worker ready\" naming $2 within 120s: journalctl _PID=$pid"
      return 1
      ;;
    *) return 2 ;;
  esac
}
