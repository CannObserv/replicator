---
name: brainstorming
description: Explores user intent, requirements, and design before any implementation. Use when the user says "brainstorm", "design this", "let's design", or proposes a new feature without a prior design discussion.
compatibility: Designed for Claude. Requires git and gh CLI. Python project using FastAPI, Pydantic, uv, ruff, pytest.
metadata:
  author: gregoryfoster
  triggers: brainstorm, design this, let's design
  overrides: obra-superpowers/brainstorming
  synced-from: "obra-superpowers v6.3.0 (b36e082)"
  override-reason: "Project-specific conventions: docs/plans/ path, #<n> [type]: desc commit convention, writing-plans is optional not mandatory; invokes using-git-worktrees after design approval for any multi-step implementation; opens a GitHub issue at design approval; FastAPI stack context. Upstream's visual companion, dot-graph process flow, spec self-review and user review gate are deliberately omitted — this project reviews the design in chat and the doc in the commit."
---

# Brainstorming Ideas Into Designs — replicator

Help turn ideas into fully formed designs through natural collaborative dialogue.

Start by classifying how much process the request needs, then work through your
path: understand the context, refine the idea, present a design, and get the
user's approval.

<HARD-GATE>
Do NOT invoke any implementation skill, write any code, scaffold any project, or
take any implementation action until you have told the user what you intend and
they have approved it. This applies to EVERY task on EVERY path below — the
ceremony scales with the task; the approval gate never does. The design doc
itself is the one file you may write before implementation begins.
</HARD-GATE>

## Three Paths

Before your first question, classify the request and say the classification out
loud — "this looks bounded, so I'll present a short design here rather than write
a doc" — so the user can override it:

- **Spike** — a feasibility question ("can we...", "is it possible...", "quick
  and dirty is fine") whose output is an answer, not code you keep. Present the
  question and what you'll try in 2-3 sentences, get a nod, then find out as
  cheaply as correctness allows. No design doc, no issue. Report findings as a
  recommendation; anything you built stays labeled throwaway.
- **Bounded** — a well-scoped change to code that already exists in this repo: a
  new setting, one handler, a one-file fix. Understanding the kind of service is
  not enough — bounded means the flow you are changing is already here to read.
  If there is no existing flow to change, the task is not bounded. Ask the
  clarifying questions that matter, present a short design IN CHAT (a few
  sentences to a few short paragraphs), and STOP. Implementation starts only
  after the user says yes to that design — a bounded task's approval is as hard a
  gate as an architectural one. No design doc, no plan document.
- **Architectural** — new subsystems, a new stream or consumer group, changes
  that restructure how modules fit together, and anything touching a wire
  contract or the boundaries charter. Follow the full process: questions,
  approaches, sectioned design, a written design doc, a GitHub issue.

When in doubt between two paths, take the heavier one. The ratchet is one-way:
hidden complexity discovered mid-task upgrades the path — stop, say so, and step
up. Nothing downgrades mid-task.

**A contract change is always architectural.** Anything altering `docs/contracts/`,
a payload other repos consume, or a stream's semantics affects consumers who
cannot see this conversation.

## When brainstorming applies at all

The paths above decide *how much* process. This decides *whether* to start:

1. **Explicit trigger** — the user says "brainstorm", "design this", "let's design"
2. **New feature request** — functionality not yet discussed or designed in this conversation
3. **Ambiguous scope** — a request could be read multiple ways, and design discussion prevents wasted work

Brainstorming is **not** required for:
- Bug fixes with a clear, agreed-upon cause and solution
- Explicit directed tasks ("add this field", "fix this test") with no design ambiguity
- Continuation of a previously approved design in the same conversation

## Anti-Pattern: "Too Simple To Need Approval"

Every path ends with the user approving your intent before implementation. A
config change, a single-function utility, one new setting — the design may be two
sentences in chat, but you MUST present it and get approval. "Simple" tasks are
where unexamined assumptions cause the most wasted work. What scales with
simplicity is the artifact, never the approval.

## Red Flags

| Thought | Reality |
|---------|---------|
| "This is too simple to need a design" | Simple means a short design, not no design. Two sentences in chat, then approval. |
| "I'll call it bounded and skip the doc" | Reaching for a label to skip work IS the doubt — take the heavier path. |
| "It's bounded and the design is obvious — I'll start while they read it" | The gate is the approval, not the design's length. Present, then stop until you hear yes. |
| "I understand this kind of service, so it's bounded" | Bounded measures the repo, not your familiarity. A new subsystem has no existing flow — it is architectural. |
| "The spike works, so I'll keep the code" | A spike's output is an answer. Keeping the code is a new request — classify it. |
| "It grew, but I'm almost done — no need to re-classify" | Hidden complexity upgrades the path mid-task. Stop and say so. |
| "They approved the spike, so the follow-up change is approved too" | Each task gets its own classification and its own approval. |
| "It only changes our own payload field" | A field on a wire contract is another repo's input. Architectural. |

## Checklist

Classify first, announce the path, then create a task for each item on your path
and complete them in order.

**Spike:**
1. **Explore project context** — enough to frame the probe
2. **Present question + probe plan** — 2-3 sentences
3. **Get approval** — a nod is enough
4. **Investigate** — as cheaply as correctness allows
5. **Report findings** — a recommendation; label anything built as throwaway

**Bounded:**
1. **Explore project context** — read AGENTS.md, check recent commits, review the files you will touch
2. **Ask clarifying questions** — one at a time, the ones that matter
3. **Present short design in chat** — approach, files touched, testing
4. **Get approval** — STOP and wait for an explicit yes; presenting the design and starting in the same breath is skipping the gate
5. **Implement** — the normal workflow, TDD included; no design doc, no issue

**Architectural:**
1. **Explore project context** — read AGENTS.md and the relevant detail docs, check recent commits
2. **Ask clarifying questions** — one at a time; purpose, constraints, success criteria
3. **Propose 2–3 approaches** — with trade-offs and a recommendation
4. **Present design** — in sections scaled to complexity; get approval after each section
5. **Write design doc** — save to `docs/plans/YYYY-MM-DD-<topic>-design.md` and commit
6. **Open a GitHub issue** — the tracking record; report its number
7. **Set up worktree** — invoke `using-git-worktrees` for any multi-step implementation
8. **Hand off** — proceed to implementation, or invoke `writing-plans` if a formal plan is wanted

## The Process

The subsections below serve the bounded and architectural paths (a spike stops at
"present the probe, get a nod"). **Exploring approaches** onward is
architectural-path depth — for bounded work, context plus a few questions plus a
short in-chat design is the whole process.

**Understanding the idea:**

- Read AGENTS.md and the relevant source files before asking anything
- Before asking detailed questions, assess scope: if the request describes several independent subsystems, flag it immediately rather than spending questions refining a project that needs decomposing first
- If it is too large for one design, help decompose it: what are the independent pieces, how do they relate, what order should they be built? Each sub-project gets its own design → issue → implementation cycle
- Ask **one question at a time** — stacked questions get partial answers
- Prefer multiple choice when the options are bounded; open-ended is fine otherwise
- Focus on purpose, constraints, success criteria, and what failure looks like

**Exploring approaches:**

- Propose 2–3 approaches with explicit trade-offs
- Lead with your recommendation and explain why
- YAGNI ruthlessly — remove unnecessary features from every approach
- For a payload or stream change, flag breaking vs. non-breaking, and name the consumers

**Presenting the design:**

- Scale each section to its complexity: a few sentences if straightforward, up to ~250 words if nuanced
- Ask after each section whether it looks right before continuing
- Cover the dimensions that apply: architecture, wire contract, failure classification, retention, testing strategy
- Be ready to go back and clarify if something doesn't land

## After the Design (architectural path)

**Write the design doc:**
- Path: `docs/plans/YYYY-MM-DD-<topic>-design.md`
- Include: goal, approved approach, key decisions and their rationale, out-of-scope items

**Open a GitHub issue:**

```bash
gh issue create \
  --title "<topic — concise imperative phrase>" \
  --body "$(cat <<'EOF'
## Summary
<1–3 sentence summary of what was designed>

## Design doc
`docs/plans/YYYY-MM-DD-<topic>-design.md`

## Scope
<bullet list of the key decisions / in-scope items from the design>
EOF
)"
```

Report the issue number to the user (e.g. "Opened #42").

**Commit the design doc:**
```
#<n> docs: add design doc for <topic>
```

**Set up a worktree (multi-step implementation):**
- Invoke `using-git-worktrees` to create an isolated workspace on a feature branch
- Use `.worktrees/` as the local directory (verify it is gitignored first)
- The service deploys whatever HEAD the main checkout is on — never leave it on a branch
- Skip for single-commit or directed fixes where isolation adds no value

**Hand off:**
- For small changes: proceed directly to implementation
- For multi-step work: invoke `writing-plans` to create a task-by-task plan (optional — not required)
- Do NOT invoke any other skill without asking

## Key Principles

- **One question at a time** — never stack multiple questions in one message
- **Multiple choice preferred** — easier to answer than open-ended when options are clear
- **YAGNI** — remove scope creep from every proposed approach
- **Project conventions first** — check AGENTS.md before proposing any architectural change
- **Incremental approval** — present the design in sections, get buy-in as you go
- **Flexibility** — go back and clarify whenever something doesn't make sense

## Proactive suggestion

When a user makes a feature request without explicit design context, suggest
brainstorming before diving in:

> "Before I start, this looks like a good candidate for a quick design discussion
> to make sure we're aligned on approach. Want me to run brainstorming, or do you
> have a specific implementation in mind?"

This is a suggestion, not a HARD-GATE — if the user confirms they have a clear
intent, proceed.
