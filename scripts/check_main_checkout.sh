#!/usr/bin/env bash
# Assert the deployed checkout is on `main`, so the service can only ever start
# code that is on the shared branch.
#
# Run as an `ExecStartPre` on replicator.service, *before* the BUILD_ID stamp.
# AGENTS.md states the invariant plainly — "Code committed to main is the deployed
# code" — and until #37 nothing enforced it: a `systemctl restart` while the main
# checkout sat on a feature branch deployed unmerged code and stamped BUILD_ID
# with the branch commit, which makes the journal *look* correct while describing
# code that is on no shared branch. It happened on 2026-08-14 during #29 (branch
# `29-replicate-refusals` ran as build `7d6f195` while main was at `b69771a`).
#
# Two tiers, because "not deployed code" is not one condition:
#   - HEAD is not `main`           -> unshared code, refuse            -> exit 1
#   - detached HEAD                -> no branch to verify, refuse      -> exit 1
#   - not a git work tree          -> cannot verify anything, refuse   -> exit 1
#   - main behind origin/main      -> stale but shared, warn           -> exit 0
#   - main ahead of origin/main    -> unpushed but on main, warn       -> exit 0
#   - dirty working tree           -> not deployed code either, warn   -> exit 0
#   - clean checkout on main       -> ok                               -> exit 0
#
# The warn tier is warn rather than refuse for one reason each: `origin/main` is a
# cached ref, only as fresh as the last fetch, so refusing on it would make the
# service unstartable during a network outage; and a dirty tree refusal would
# block an operator mid-incident. Ahead-of-origin is the arguable one — unpushed
# commits on main are precisely the "on no shared branch" case #37 objects to —
# but it shares origin/main's staleness problem, so it warns here and the
# refuse/warn call is left to a follow-up rather than decided by this script.
#
# Unlike scripts/check_redis_floor.sh, absence is *hard* rather than soft. There
# the missing thing is a broker that may still be starting; here it is the
# evidence that the code is main's code, and soft-passing would make "remove git
# from the box" a silent bypass of the guard.
#
# ESCAPE HATCH: REPLICATOR_ALLOW_ANY_CHECKOUT=1 in /etc/replicator/.env bypasses
# the refusal, because "I am deliberately testing a branch on this VM" is a real
# thing to want and a guard with no override gets removed rather than overridden.
# Note that the *documented* branch-testing path does not need it at all: run the
# worker directly under a distinct REPLICATOR_CONSUMER_NAME
# (`uv run python -m src.worker.main`, see AGENTS.md §Server Lifecycle), which
# never touches this unit. Reach for the override only when the thing under test
# is the unit itself.
#
# The checkout under test is the *current directory*, which under systemd is the
# unit's WorkingDirectory. Deliberately not a hardcoded path: WorkingDirectory is
# already what the BUILD_ID stamp's own `git rev-parse` reads and what ExecStart
# runs from, so deriving all three from one place means a WorkingDirectory change
# cannot leave the guard verifying a different tree than the one that starts.
set -uo pipefail

DEPLOY_BRANCH="main"
REMOTE_REF="origin/${DEPLOY_BRANCH}"

# Only `1`. A `=true` would be a plausible typo, and an override that silently
# fails to override is worse than no override at all — so say so rather than
# refusing with a message about the branch.
case "${REPLICATOR_ALLOW_ANY_CHECKOUT:-}" in
  "")
    ;;
  1)
    echo "check_main_checkout: REPLICATOR_ALLOW_ANY_CHECKOUT=1 — checkout guard bypassed" >&2
    exit 0
    ;;
  *)
    echo "check_main_checkout: REPLICATOR_ALLOW_ANY_CHECKOUT is '${REPLICATOR_ALLOW_ANY_CHECKOUT}', not '1' — not bypassing" >&2
    ;;
esac

if ! command -v git >/dev/null 2>&1; then
  echo "check_main_checkout: git not found — cannot verify the checkout is ${DEPLOY_BRANCH}" >&2
  echo "check_main_checkout: set REPLICATOR_ALLOW_ANY_CHECKOUT=1 to start anyway" >&2
  exit 1
fi

# One refusal, three ways to reach it: the directory is not a work tree, git
# declines to trust it (dubious ownership on a shared VM), or it is a work tree
# whose HEAD has no commit yet. All mean the same thing — there is nothing to
# check — and all three would otherwise fall through to the detached-HEAD branch
# below and print an empty SHA, which is the confusing message this splits out to
# avoid.
unverifiable=""
head_sha=""
if [ "$(git rev-parse --is-inside-work-tree 2>/dev/null)" != "true" ]; then
  unverifiable="$(pwd) is not a git work tree (or git declines to trust it)"
elif ! head_sha="$(git rev-parse --verify --quiet --short HEAD 2>/dev/null)"; then
  unverifiable="$(pwd) has an unborn HEAD — no commit to verify"
fi

if [ -n "${unverifiable}" ]; then
  echo "check_main_checkout: ${unverifiable}" >&2
  echo "check_main_checkout: cannot verify the deployed code is ${DEPLOY_BRANCH}; refusing to start" >&2
  echo "check_main_checkout: set REPLICATOR_ALLOW_ANY_CHECKOUT=1 to start anyway" >&2
  exit 1
fi

# `--abbrev-ref` reports the literal string HEAD when HEAD is detached, so a
# detached checkout would be caught by the branch comparison below anyway; it is
# split out only so the journal says "detached" instead of "on 'HEAD', not 'main'".
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)"
if [ "${branch}" = "HEAD" ] || [ -z "${branch}" ]; then
  echo "check_main_checkout: HEAD is detached at ${head_sha}" >&2
  echo "check_main_checkout: no branch to verify; refusing to start" >&2
  echo "check_main_checkout: 'git checkout ${DEPLOY_BRANCH}', or set REPLICATOR_ALLOW_ANY_CHECKOUT=1" >&2
  exit 1
fi

if [ "${branch}" != "${DEPLOY_BRANCH}" ]; then
  echo "check_main_checkout: HEAD is on '${branch}', not '${DEPLOY_BRANCH}'" >&2
  echo "check_main_checkout: refusing to start — the service would run unmerged code" >&2
  echo "check_main_checkout: 'git checkout ${DEPLOY_BRANCH}', or set REPLICATOR_ALLOW_ANY_CHECKOUT=1" >&2
  exit 1
fi

# --- On main. Everything below is advisory; the exit code is already 0. -------

# No fetch: an ExecStartPre must not make a network call the start can wait on,
# and a warning computed from the last fetch is worth more than a hang.
if git rev-parse --verify --quiet "${REMOTE_REF}" >/dev/null 2>&1; then
  behind="$(git rev-list --count "HEAD..${REMOTE_REF}" 2>/dev/null)"
  ahead="$(git rev-list --count "${REMOTE_REF}..HEAD" 2>/dev/null)"

  if [ -n "${behind}" ] && [ "${behind}" -gt 0 ] 2>/dev/null; then
    echo "check_main_checkout: ${DEPLOY_BRANCH} is ${behind} commit(s) behind ${REMOTE_REF} as of the last fetch — starting anyway" >&2
  fi
  if [ -n "${ahead}" ] && [ "${ahead}" -gt 0 ] 2>/dev/null; then
    echo "check_main_checkout: ${DEPLOY_BRANCH} is ${ahead} commit(s) ahead of ${REMOTE_REF} (unpushed) — starting anyway" >&2
  fi
fi

# Tracked, non-submodule files only, on the same reasoning both times: a warning
# that fires routinely for a reason unrelated to the worker's code trains
# operators to ignore the line, which is the failure a warning tier exists to
# avoid. An untracked file is not a modification to deployed code; and
# `skills-vendor/` holds two submodules that a once-a-day refresh hook moves, so
# counting submodule state would keep this warning permanently lit over vendored
# agent tooling the worker never loads.
if [ -n "$(git status --porcelain --untracked-files=no --ignore-submodules=all 2>/dev/null)" ]; then
  echo "check_main_checkout: working tree has uncommitted changes to tracked files — starting anyway" >&2
fi

echo "check_main_checkout: on ${DEPLOY_BRANCH} at ${head_sha}"
exit 0
