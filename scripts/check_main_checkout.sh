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
#   - dubiously-owned repository   -> cannot verify anything, refuse   -> exit 1
#   - unborn HEAD                  -> nothing to verify, refuse        -> exit 1
#   - main ahead of origin/main    -> unpushed, so unshared, refuse    -> exit 1
#   - main behind origin/main      -> stale but shared, warn           -> exit 0
#   - no origin/main ref at all    -> nothing to compare against, warn -> exit 0
#   - dirty working tree           -> not deployed code either, warn   -> exit 0
#   - clean checkout on main       -> ok                               -> exit 0
#
# Ahead-of-origin refuses and behind-of-origin warns, which looks inconsistent for
# two readings of one cached ref and is not (#48). `origin/main` is updated by
# `git fetch` *and* by a successful `git push` from this repository. So a
# never-fetched ref can hide behind-ness — remote commits this checkout cannot see,
# which is why refusing there would fail an operator whose only sin is a network
# outage — but it cannot manufacture ahead-ness for commits this checkout pushed,
# because the push would have moved the ref. Ahead is local evidence: these commits
# are here, and nothing here ever published them. That is exactly the "code that is
# on no shared branch" case #37 objects to, and it is assertable without the network
# call an ExecStartPre must not make. The only false positive needs someone to
# publish the identical SHAs from another clone; through a PR merge the SHAs differ,
# so the tree reads as diverged rather than ahead — still unshared, still refused.
#
# The remaining warns are warns for one reason each: a dirty-tree refusal would block
# an operator mid-incident, and a missing `origin/main` is absence of evidence rather
# than evidence of an unshared commit — HEAD has already been proven to be `main` by
# then. It is said out loud rather than passed over in silence only because ahead now
# refuses, which would otherwise make `git remote remove origin` a quiet bypass.
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

# One refusal, several ways to reach it: git will not trust the directory, it is
# not a work tree at all, or it is a work tree whose HEAD has no commit yet. All
# mean the same thing — there is nothing to check — and all of them would
# otherwise fall through to the detached-HEAD branch below and print an empty
# SHA, which is the confusing message this splits out to avoid.
#
# Dubious ownership gets its own message rather than sharing "not a git work
# tree". The directory plainly *is* a repository in that case, so the generic
# line sends an operator hunting the wrong hypothesis during a failed start —
# the same misleading-verdict failure this whole guard exists to close, one
# level down. It is reachable without anyone doing anything strange: a single
# root-run git command, a restore, or a UID change on this shared VM is enough.
unverifiable=""
remedy=""
head_sha=""
# One probe, stderr folded in, matched with `case` rather than grep: this runs on
# every start, so it costs one git invocation and no external process.
probe="$(git rev-parse --is-inside-work-tree 2>&1)"
case "${probe}" in
  true)
    if ! head_sha="$(git rev-parse --verify --quiet --short HEAD 2>/dev/null)"; then
      unverifiable="$(pwd) has an unborn HEAD — no commit to verify"
    fi
    ;;
  *"dubious ownership"*)
    unverifiable="git refuses $(pwd) as dubiously owned — the directory is a repository, but not this user's"
    remedy="git config --global --add safe.directory $(pwd), or restore the directory's owner"
    ;;
  *)
    unverifiable="$(pwd) is not a git work tree"
    ;;
esac

if [ -n "${unverifiable}" ]; then
  echo "check_main_checkout: ${unverifiable}" >&2
  echo "check_main_checkout: cannot verify the deployed code is ${DEPLOY_BRANCH}; refusing to start" >&2
  [ -n "${remedy}" ] && echo "check_main_checkout: fix with: ${remedy}" >&2
  echo "check_main_checkout: set REPLICATOR_ALLOW_ANY_CHECKOUT=1 to start anyway" >&2
  exit 1
fi

# `--abbrev-ref` reports the literal string HEAD when HEAD is detached, so a
# detached checkout would be caught by the branch comparison below anyway; it is
# split out only so the journal says "detached" instead of "on 'HEAD', not 'main'".
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)"

# An empty name should be unreachable — `rev-parse --verify HEAD` succeeded
# above, so there is a commit to name. Refused on its own line anyway rather than
# folded into the detached case below: calling a git failure "detached at <sha>"
# is the same misleading-verdict shape the unborn-HEAD case was split out to
# avoid, and an unreachable branch costs three lines to keep honest.
if [ -z "${branch}" ]; then
  echo "check_main_checkout: git could not name HEAD's branch in $(pwd)" >&2
  echo "check_main_checkout: cannot verify the deployed code is ${DEPLOY_BRANCH}; refusing to start" >&2
  echo "check_main_checkout: set REPLICATOR_ALLOW_ANY_CHECKOUT=1 to start anyway" >&2
  exit 1
fi

if [ "${branch}" = "HEAD" ]; then
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

# --- On main. The warns come first, then the one refusal that can still fire. --
#
# Ordered that way so a single failed start names every condition: an operator who
# fixes the ahead refusal should not then discover a dirty tree on the next start.
# Same reasoning as printing `behind` before the `ahead` refusal below.

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

# No fetch: an ExecStartPre must not make a network call the start can wait on,
# and a warning computed from the last fetch is worth more than a hang.
if git rev-parse --verify --quiet "${REMOTE_REF}" >/dev/null 2>&1; then
  behind="$(git rev-list --count "HEAD..${REMOTE_REF}" 2>/dev/null)"
  ahead="$(git rev-list --count "${REMOTE_REF}..HEAD" 2>/dev/null)"

  # Behind is printed before the ahead refusal rather than after it, so a diverged
  # checkout names both sides: an operator told only "ahead" pushes and is rejected.
  if [ -n "${behind}" ] && [ "${behind}" -gt 0 ] 2>/dev/null; then
    echo "check_main_checkout: ${DEPLOY_BRANCH} is ${behind} commit(s) behind ${REMOTE_REF} as of the last fetch — starting anyway" >&2
  fi
  if [ -n "${ahead}" ] && [ "${ahead}" -gt 0 ] 2>/dev/null; then
    echo "check_main_checkout: ${DEPLOY_BRANCH} is ${ahead} commit(s) ahead of ${REMOTE_REF} — unpushed, so on no shared branch" >&2
    echo "check_main_checkout: refusing to start — the service would run code nobody else has" >&2
    echo "check_main_checkout: 'git push' to share them, or 'git reset --hard ${REMOTE_REF}' to drop them" >&2
    # Named because it is the one reading of this refusal an operator can get wrong,
    # and the fix is not the same: a ref stale in this direction means the commits
    # were published from somewhere other than this repository.
    echo "check_main_checkout: if they are already pushed, ${REMOTE_REF} is stale — 'git fetch'" >&2
    echo "check_main_checkout: during a network partition, set REPLICATOR_ALLOW_ANY_CHECKOUT=1" >&2
    exit 1
  fi
else
  # Not fatal: HEAD is already proven to be ${DEPLOY_BRANCH}, so this is absence of
  # evidence, not evidence of an unshared commit. Said out loud all the same — with
  # ahead refusing above, silence here would make removing the remote a way to bypass
  # that refusal without touching the guard.
  echo "check_main_checkout: no ${REMOTE_REF} ref in $(pwd) — cannot tell whether ${DEPLOY_BRANCH} is shared — starting anyway" >&2
fi

echo "check_main_checkout: on ${DEPLOY_BRANCH} at ${head_sha}"
exit 0
