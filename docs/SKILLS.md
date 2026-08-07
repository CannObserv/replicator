# Agent Skills

Skills are packaged workflows an agent loads on demand. Two discovery systems read the same set:

- **agentskills.io** scans `skills/` at the repo root.
- **Claude Code** scans `.claude/skills/`.

Both resolve through a two-level symlink chain:

```
.claude/skills/<name>  →  ../../skills/<name>  →  ../skills-vendor/<owner>-<repo>/skills/<name>
```

Pointing the second link through `skills/` rather than straight at vendor means a **local override**
(a real directory committed in `skills/`) automatically shadows the vendor copy in *both* systems —
no `.claude/skills/` change needed.

Managed by the [`managing-skills`](../skills/managing-skills/) skill.

## Vendor submodules

| Submodule | Upstream |
|---|---|
| `skills-vendor/gregoryfoster-skills` | https://github.com/gregoryfoster/skills |
| `skills-vendor/obra-superpowers` | https://github.com/obra/superpowers |

`skills-vendor/` is **read-only** — make changes upstream, then bump the pointer.

## Available skills

### Local overrides

| Skill | Overrides | Why |
|---|---|---|
| `brainstorming` | `obra-superpowers/brainstorming` | Project conventions: `docs/plans/` path, `#<n> [type]: desc` commit convention, `writing-plans` optional not mandatory, invokes `using-git-worktrees` after design approval, FastAPI stack context |

Every override's `SKILL.md` must declare `overrides:` and `override-reason:` in its frontmatter
`metadata` block. A local directory is a **complete replacement**, not a partial merge.

### From `gregoryfoster-skills`

| Skill | Purpose |
|---|---|
| `curating-context` | Curates `AGENTS.md` and its live reference docs against a token budget |
| `enforcing-architecture` | Turns an accepted architecture finding into an executable fitness function |
| `init-project-fastapi` | Bootstraps a CannObserv FastAPI service (this repo's own foundation) |
| `init-socraticode` | Installs and indexes SocratiCode semantic code search |
| `managing-skills` | Adds/updates/removes skill submodules and symlinks |
| `orchestrating-issue-backlog` | Prioritizes an open issue backlog; analyzes conflicts and dependencies |
| `reviewing-architecture` | High-level structural and design-principle review |
| `reviewing-code-python-fastapi` | Structured code review for uv + ruff + pytest + Pydantic v2 projects |
| `shipping-work-python-fastapi` | Finalizes work: gates, commits, PR |
| `using-git-worktrees` | Parallel branch checkouts via `git worktree` |
| `writing-plans` | Short reviewed plan before non-trivial implementation |

Only the `-python-fastapi` variants of the cross-cutting review/ship workflows are linked; the
stack-neutral and other-stack variants are deliberately skipped.

`enforcing-architecture` is linked because `reviewing-architecture` **delegates** to it when a finding
is accepted with a `N: fix + fitness` or bare `N: fitness` directive — without the symlink that
directive fails to resolve. The daily auto-refresh hook bumps the submodule pointer but never creates
per-skill symlinks, so linking a newly published skill stays a manual step (#13).

**`curating-context` is pinned at v1.2 (`3fc7b71`) until the wave-B comparison resolves (#22).** The
twelve cohort repos are the held-out validation split for the skill itself: a proposed change is
tried on one arm and scored against the other, and Replicator's first curation is this arm's data
point. Bumping the vendored pointer past v1.2 before that resolves puts two skill versions inside one
arm and `score-cohort.sh` returns INCONCLUSIVE rather than a verdict.

**So the daily auto-refresh is suspended for the duration of the hold.** The hook's `SessionStart`
entry is removed from `.claude/settings.json`; `.claude/hooks/skills-submodule-update.sh` itself is
left in place, so restoring the refresh is a one-entry edit and not a re-install. Suspension is
blunter than the problem — it also stops the `obra-superpowers` refresh and the `.skills/doctor.sh`
self-heal commit, neither of which has anything to do with the hold — but it is the only remedy a
consumer repo currently has: `git submodule update --remote --merge -- skills-vendor/` takes no
per-submodule exclusion, and one submodule carries every `gregoryfoster` skill, so pinning
`curating-context` alone is not expressible. Tracked upstream as
[gregoryfoster/skills#100](https://github.com/gregoryfoster/skills/issues/100), which proposes a
committed pin file the hook consults; restore the `SessionStart` entry when that lands or when the
hold ends, whichever comes first.

While the refresh is suspended, updating vendor skills is manual:

```bash
git -C skills-vendor/obra-superpowers fetch origin && git -C skills-vendor/obra-superpowers checkout origin/HEAD
bash .skills/doctor.sh
git add skills-vendor/obra-superpowers && git commit -m "chore: update skills submodules"
```

The commit is the step the suspended hook used to perform; without it the new pointer is
discarded by the next checkout.

Leave `skills-vendor/gregoryfoster-skills` at `3fc7b71` until the hold ends.

## The write-guard hook dangles on a submodule-less checkout

`curating-context` installs `.claude/hooks/context-budget-guard.sh` as a symlink into the
vendored skill, but `doctor.sh` scans `skills/*` only — `.claude/hooks/*` is outside its heal
scope. On a checkout where the submodule is not initialized (fresh clone, `git worktree add`,
shallow CI clone) the hook path dangles and the wired `PostToolUse` command fails with
`No such file or directory` on **every** `Edit`/`Write`/`MultiEdit`, naming a path that `ls`
shows as present. Run `bash .skills/doctor.sh` — it initializes the submodule and so heals the
hook as a side effect, even though it never inspects it. Tracked upstream as
[gregoryfoster/skills#99](https://github.com/gregoryfoster/skills/issues/99); the guard itself
is correctly non-blocking once it resolves.

CI is **no longer** one of those submodule-less checkouts — see [Submodules in CI](#submodules-in-ci)
below. A `git worktree add` still is.

### From `obra-superpowers`

| Skill | Purpose |
|---|---|
| `dispatching-parallel-agents` | 2+ independent tasks with no shared state |
| `executing-plans` | Execute a written plan in a separate session with review checkpoints |
| `finishing-a-development-branch` | Decide how to integrate completed work |
| `receiving-code-review` | Process review feedback before implementing suggestions |
| `requesting-code-review` | Verify work before merging |
| `subagent-driven-development` | Execute plan tasks in the current session |
| `systematic-debugging` | Any bug or test failure, before proposing fixes |
| `test-driven-development` | Any feature or bugfix, before writing implementation code |
| `using-superpowers` | Conversation start — how to find and use skills |
| `verification-before-completion` | Before claiming work complete, fixed, or passing |
| `writing-skills` | Creating, editing, or verifying skills |

## Plans directory

`docs/plans/` is the default governed by `writing-plans`. Override with a single-line
`.skills/plans_dir` file at the repo root if a different path is ever wanted.

## Maintenance

```bash
# Fresh clone — submodules are not populated by a plain clone
git submodule update --init --recursive

# Repair dangling symlinks (auto-runs submodule init, then reports if it can't self-heal).
# The reviewing-* / shipping-* skills invoke this as a Phase 1 preflight.
bash .skills/doctor.sh

# Pull upstream skill changes and bump the pointers
git submodule update --remote --merge
git add skills-vendor/ && git commit -m "chore: update skill submodules"
```

A `SessionStart` hook (`.claude/hooks/skills-submodule-update.sh`) refreshes `skills-vendor/` at most
once per UTC day, on `main` only, auto-commits the pointer bump (and `.skills/doctor.sh` when it
changed — never `.skills/` wholesale, which holds operator config), and never blocks a session. It
also re-installs `.skills/doctor.sh` each session, ahead of both gates, so the doctor self-heals on
any branch if deleted. Logs to `.git/skills-update.log`.

**The hook is a symlink into the submodule, not a copy** (`managing-skills` Step 1). That is what
makes upstream fixes to the script arrive on the normal submodule refresh; a copy freezes at whatever
version was current the day it was installed and drifts silently thereafter — this repo's had, for
the whole `.skills/doctor.sh` commit path (#16). `readlink .claude/hooks/skills-submodule-update.sh`
is the check; an empty result means someone re-copied it.

## Submodules in CI

`tests/test_skills_hook.py` pins that symlink, and its second assertion **dereferences** it — a
correctly-shaped link into an unpopulated submodule is a dangling no-op, which is the failure #16
existed to catch. `actions/checkout` does not fetch submodules by default, so the `test` job carries
the key that makes the test runnable at all:

```yaml
- uses: actions/checkout@v5
  with:
    submodules: true
```

Without it the link dangles on the runner and the test **cannot pass in CI** — it fails identically
on every commit, which is indistinguishable from a check that just started failing and trains
everyone to merge past red. That is what happened: `main` was red from `7bf5988` until #27.

Three things to know before touching it:

- **`lint` deliberately omits the key.** Ruff `extend-exclude`s `skills-vendor/` (`pyproject.toml`),
  so the linter never reads what the job never fetches. Both halves are load-bearing; either alone
  would do.
- **It costs ~2.2s** of a ~1m10s job, for both submodules. `submodules: true` fetches
  `obra-superpowers` too, which no test dereferences — `actions/checkout` takes no per-submodule
  selector, and trading the declarative key for an imperative `git submodule update --init <path>`
  step to save about a second is not worth it.
- **CI resolves the SHA pinned here, never upstream tip.** So the key neither lifts nor weakens the
  v1.2 hold above. A pin far behind upstream is fine: GitHub serves arbitrary SHAs, so the shallow
  submodule fetch resolves it.

The coupling accepted in exchange: an upstream force-push that garbage-collects a pinned SHA fails
the job **at checkout**, an error that looks nothing like a test failure. If CI dies before the
`Install uv` step, read the checkout log before the test output.
