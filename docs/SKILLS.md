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

**`curating-context` is pinned at v1.2 until the wave-B comparison resolves (#22).** The twelve
cohort repos are the held-out validation split for the skill itself: a proposed change is tried on
one arm and scored against the other, and Replicator's first curation is this arm's data point.
Bumping the vendored pointer past v1.2 before that resolves puts two skill versions inside one arm
and `score-cohort.sh` returns INCONCLUSIVE rather than a verdict. The daily auto-refresh hook bumps
the submodule pointer, so this pin is a review obligation, not something the tooling enforces.

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
