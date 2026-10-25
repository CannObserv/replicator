#!/bin/bash
# Playwright's executable_path for a capture render (#138). Not run by hand.
#
# Two things must hold for *every* Chromium process, and both are inherited
# across fork/exec, so the launcher sets them on itself and then becomes the
# browser:
#
# - the delegated <unit>/browser cgroup (src/worker/browser_cgroup.py), whose
#   memory.max, memory.oom.group and pids.max make a runaway render cost the
#   browser instead of the worker. Written first: a launch that cannot join it
#   fails here (set -e), rather than running uncapped at the worker's -900;
# - oom_score_adj 500, so if the host as a whole runs short the kernel picks the
#   browser well before the worker (-900) or tailscaled (-950). Raising one's own
#   adjust needs no privilege.
#
# The two variables are the renderer's, read here and unset before exec, so the
# browser's environment is exactly what src/worker/browser.py passed it.
set -euo pipefail

if [ -n "${REPLICATOR_BROWSER_CGROUP_PROCS:-}" ]; then
    echo "$$" > "$REPLICATOR_BROWSER_CGROUP_PROCS"
fi
echo 500 > /proc/self/oom_score_adj

exe="${REPLICATOR_BROWSER_EXECUTABLE:?the renderer names the pinned shell}"
unset REPLICATOR_BROWSER_CGROUP_PROCS REPLICATOR_BROWSER_EXECUTABLE
exec "$exe" "$@"
