# Worker-hardening backlog — prioritized batch plan (#17, #20, #25, #37, #44)

Tracking issue: **#47**.

## Goal

Clear five open issues that harden the consume path and the deploy/CI surface, as three
merge-safe batches run by an agent team in git worktrees. Two are latent runtime defects on
the live byte path (#17 reports a successful conditional GET as a terminal failure; #20 burns
the delivery ceiling on someone else's Redis incident), two are guardrails on the path that
deploys and verifies everything else (#37 the main-checkout guard, #44 the CI concurrency
group), and one restores an adaptive-politeness capability the cluster lost at Watcher's
Phase 4 cutover (#25). The plan fixes what each agent owns down to line windows, because
three of the five converge on `src/worker/handler.py` and `src/worker/loop.py`.

## Approved approach

Hybrid parallelism: agents run in parallel within a batch, with a human review-and-merge gate
between batches. **Per-batch ceiling: 2 agents.**

The ceiling is the **host**, not the tooling and not a shared service — both of the usual
suspects came back negative and were checked rather than assumed:

- **Worktree provisioning:** no custom script. `skills/using-git-worktrees/scripts/worktree-create.sh`
  is plain `git worktree add` plus the Iron Law check — no port pool, no vhosts, no DB clone,
  no overlays. Root resolves to `<repo>/.worktrees/`.
- **Shared backing services:** none that serialize. `pyproject.toml` sets
  `addopts = -m 'not integration'`, so the default suite is hermetic on fakeredis. The
  integration suite is *already* concurrency-safe by construction:
  `tests/worker/conftest.py:242` mints `replicator.itest.<uuid4>` per test, and its docstring
  says why — *"the uuid keeps concurrent runs … from colliding on a group whose PEL would
  otherwise leak."* Blob tests use `tmp_path`. No Postgres (`.github/workflows/ci.yml:151`
  states this explicitly), no destructive session fixture.
- **The real constraint:** 2 CPU cores; 7 GB RAM with ~1 GB available; 2.7 GB free disk (85%
  full). Each worktree needs its own 181 MB `.venv`, and `[tool.uv] find-links = ["./.wheelhouse"]`
  is repo-relative, so each worktree also needs `scripts/sync_wheelhouse.py` run (ADC) or a
  symlink to the main checkout's 25 MB `.wheelhouse`. Two concurrent `uv run pytest --cov`
  runs over ~11,800 test lines is what 2 cores and 1 GB will carry; a third risks an OOM
  kill mid-run, which presents as a mysterious red rather than a clean failure.

**Branch strategy.** Batch branches are the merge *target*; workers branch from `origin/main`
and merge the batch branch in themselves. Intra-batch worker→batch integration is
fast-forward or regular merge (never squash/rebase), so the orchestrator's
`worktree-destroy.sh --base batch/<X>` ancestor check holds. **Batch→main is a regular merge
commit** (`--no-ff`), matching this repo's precedent (`Merge #29: …`, `Merge CR round-2
findings (#46)`).

**The main checkout's branch must never move.** `replicator.service` is `enabled` and
`active` with `WorkingDirectory=/home/exedev/replicator` — the main checkout *is* the
deployment. Create batch branches with `git branch batch/<X> main` **without checking out**,
and integrate in a worktree. Fittingly, #37 is the issue that makes this invariant
enforceable rather than merely documented.

## Prioritization rubrics

**Score = (Foundation × 2) + (Correctness × 3) + Scope**, max **18**.

Correctness carries ×3 by agreement: three of the five are silent-failure issues, where the
system reports an answer that is wrong rather than failing visibly.

| Dimension | 1 | 2 | 3 |
|---|---|---|---|
| **Foundation Leverage** | Standalone improvement | 1–2 other issues benefit | Multiple issues depend on or are simplified by this |
| **Correctness Risk** | Cosmetic / organizational | Edge-case incorrect behavior, runtime failure risk | Data loss, race conditions, silent failures |
| **Scope Clarity** | Requires design discovery | Clear direction, minor decisions needed | Mechanical — implementation is obvious from the issue |

Blast radius drives *sequencing*, not score.

## Scored backlog

| # | Issue | Found. | Corr. | Scope | **Score** | Blast |
|---|---|:--:|:--:|:--:|:--:|---|
| **17** | Body-less 304 dead-letters — make "not modified" first-class | 3 | 3 | 2 | **17** | **High** |
| **20** | Classify redis `OutOfMemoryError` as transient | 1 | 3 | 3 | **14** | Low |
| **37** | `replicator.service` should refuse any checkout but main | 1 | 3 | 3 | **14** | Low |
| **44** | CI: a main commit can land with no CI signal | 1 | 3 | 3 | **14** | Low |
| **25** | Adaptive per-host backoff on 429 | 1 | 2 | 2 | **10** | Med |

Scoring notes:

- **#17** — Foundation 3: it introduces the loop's *third fate* (fact + ack, no DLQ), which
  the next body-less-but-fine outcome reuses, and it unblocks a filed cross-repo chain
  (cannobserv#298 vocabulary, watcher#249 consumer). Correctness 3: with #11 shipped the
  false-negative is live, not prospective. Scope 2 rather than 3 only because it is the
  largest diff of the five.
- **#25** — re-scored 9 → 10 at the approval gate after two decisions were taken (below).
  Foundation 1 despite #17's thread citing it: #17 is *strengthened by* #25's existence as an
  argument, but neither depends on the other.

**No issue is closed-in-fact.** Each was greped against the tree: `errors.py:52` still calls
a body-less 304 permanent; `loop.py:74` `_TRANSIENT_ERRORS` has no `OutOfMemoryError`;
`pacing.py` has no `report_rate_limited`; `scripts/check_main_checkout.sh` does not exist;
`ci.yml:16` is still `github.head_ref || github.ref`. Nothing needed rescoping to a residual.

### Decisions taken at the approval gate

Both belong to #25, whose Scope Clarity 1 was caused by the issue naming its own open
questions. Written back to the issue as a comment, not only recorded here.

1. **`Retry-After` is in scope, on 429 *and* 503, capped** to `BACKOFF_MAX_INTERVAL`. The
   issue only offered "plausibly … worth honouring when present". Requires both wire forms
   (delta-seconds and HTTP-date) and a malformed-header path that falls back to the
   multiplier rather than raising.
2. **Escalation constants are module constants in `src/worker/pacing.py`**, beside
   `MAX_TRACKED_HOSTS` — not `REPLICATOR_*` env vars. Escalation is mechanism; the numbers
   the issuer owns travel on `content.fetch-policy` and these are not those. Consequence:
   #25 touches neither `src/core/config.py` nor `pyproject.toml`.

And one scope boundary fixed on #17, taking its own 2026-08-06 recommendation as the
decision: **the "do not attempt conditional GET yet" warning does not come out on merge.**
It downgrades to "the outcome exists; your consumer does not handle it yet". Full removal
waits on watcher#249 — `apply_fetch_blob` still has no branch for "no bytes, your last
fingerprint stands".

## Conflict zones

| File | Issues | Required resolution |
|---|---|---|
| `src/worker/handler.py` `_raise_for_status` (638–657) | #17, #25 | **Sequence #17 → #25.** Not splittable by line window: both edit the same 20-line function. #25 needs the pacer and `result.headers`, but the function takes `(result, command)` and `TransientFetchError(detail)` at `:653` carries no status — so #25 must change the signature or that construction, which is restructuring inside #17's window. |
| `src/worker/loop.py` | #17, #20 | **Line-window ownership.** #20 owns 30–82 (imports + `_TRANSIENT_ERRORS`); #17 owns 380+ (`Outcome` 381–387, `process_message`'s except arms 510–542, new `_close_without_dlq` ~640). ~300 lines apart, additions only, **no restructuring** — they add to *different* import statements. |
| `AGENTS.md` | #17, #37 | Separated windows: #17 the Bus Conventions DLQ-vs-retry bullet, #37 Server Lifecycle / Environment Variables. Additions only. Both terse — the file is curated to a 6,000-token budget (soft; no CI gate). Batch A merges before B starts, so in practice uncontested. |
| `.socraticodecontextartifacts.json` | #17, #37, #44 | **Read-only for every worker** — see Key decisions. |
| `tests/worker/test_loop_*.py` | #17, #20 | #20's test goes in `test_loop_failures.py`, explicitly. `test_loop_dlq.py` and `test_loop_facts.py` are #17's. |

### The test-surface *modify* half

Invisible from source-file overlap, and no issue body mentions a test. All four are #17's, so
there is no cross-agent hazard — but a worker that only *adds* tests leaves the suite red:

- `tests/worker/test_handler.py:140` — `test_a_body_less_redirect_is_permanent` asserts
  `PermanentFetchError` + `FailureReason.HTTP_STATUS` + `status_code == 304`. The test's
  **name** becomes false, not only its body.
- `tests/worker/test_loop_dlq.py:3` — module docstring, *"Five routes reach `<topic>.dlq`"*.
  #17 is the first terminal close that reaches none.
- `src/core/errors.py:52` — `"""A non-2xx the origin meant: 4xx, or a body-less 304."""`
- `docs/ARCHITECTURE.md:71` — names a body-less 304 as `PermanentFetchError`.

### Verified *not* contested

- **`tests/test_boundaries.py`** — `DOMAIN_TOKENS` is
  `{info_source, info_item, watched_item, tenant, aspect}`; none of the five introduces one.
  The eight charter invariants must stay green, not change. #17 should record in its PR that
  the three boundaries tests were run, per its own thread.
- **`pyproject.toml`** and **`src/core/config.py`** — nobody, once #25's constants are module
  constants.
- **`deploy/replicator.service`** — #37 alone. **`.github/workflows/ci.yml`** — #44 alone.

### Coverage gate

`[tool.coverage.report] fail_under = 80` runs inside every worker's `uv run pytest`, over all
of `src/`. #25 adds the most new uncovered surface (`Retry-After` parsing, two wire forms plus
a malformed path); brief it to cover them rather than discovering the gate at signal time.

## Dependency graph

```
#44  .github/workflows/ci.yml                    ─── independent of all
#37  deploy unit + scripts/check_main_checkout.sh ─── independent of all
#20  loop.py:74 _TRANSIENT_ERRORS                ─── independent of #37/#44
                                                      separated window vs #17
#17  handler.py + loop.py + reporter.py          ──►  #25  pacing.py + handler.py 429/503
     + errors.py + ARCHITECTURE.md + 2 contracts        (edge is file-region, not logic)
```

Exactly one edge, **#17 → #25**, and it exists because of a 20-line function rather than
because either needs the other's behaviour. At a ceiling of 2, five issues require three
batches (2+2+1) under any grouping.

## Batch execution plan

| Batch | Issues | Agents | Gate |
|---|---|:--:|---|
| **A** | #37, #44 | 2 (parallel) | start immediately |
| **B** | #17, #20 | 2 (parallel) | after A merged to main |
| **C** | #25 | 1 | after B merged to main |

**Batch A — `batch/a`.** Zero contested files between the two. Chosen first deliberately: both
are guardrails on the path that deploys and verifies every later batch, so B and C land under
fixed CI (#44) and behind the checkout guard (#37).

- **A1 · #44** — `.github/workflows/ci.yml` only: `github.ref` → `github.sha` in the
  concurrency group, plus correcting the comment that claims an intent
  `cancel-in-progress` cannot deliver alone. A `docs/SKILLS.md` note is optional and its own
  window.
- **A2 · #37** — `scripts/check_main_checkout.sh` (new, modelled on `check_redis_floor.sh`),
  a fatal `ExecStartPre` in `deploy/replicator.service` placed **before** the `BUILD_ID`
  stamp, assertions in `tests/test_deploy.py`, the `REPLICATOR_ALLOW_ANY_CHECKOUT=1` escape
  hatch documented in `docs/DEPLOYMENT.md`, and a clause in AGENTS.md §Server Lifecycle.
  Verdicts: refuse on non-`main` and on detached HEAD; warn on behind-`origin/main` and on a
  dirty tree.

**Batch B — `batch/b`.** One contested file, resolved by line window.

- **B1 · #17** — owns `handler.py` 638–657, `loop.py` 380+, `reporter.py`, `errors.py`,
  `docs/ARCHITECTURE.md`, both contract docs, AGENTS.md §Bus Conventions, and
  `test_handler.py` / `test_loop_dlq.py` / `test_loop_facts.py` / `test_reporter.py` /
  `tests/core/test_errors.py`. Shape **A** from the issue: `reason="not_modified"`,
  `terminal=True`, on the existing `fetch_failed` fact — verified to need no co-core bump
  (`FetchFailedEvent.model_fields["reason"].annotation` is plain `str` on the installed
  0.10.0). Structural fate, not a token branch: `CompletedWithoutBlobError` as a sibling
  under `HandlerError`, a new `Outcome` member, `_close_without_dlq` beside `_close` with the
  inverted publish-then-ack ordering in its docstring. Write the dedupe key on the 304 path.
  412 stays `http_status` + DLQ, stated as a decision in the taxonomy table. Log WARNING for
  an *unbidden* 304 (no `if-none-match` / `if-modified-since` in `command.headers`), INFO
  otherwise. Give the not-modified case its own reporter message — it becomes the most
  common line in the journal.
- **B2 · #20** — owns `loop.py` 30–82 and `tests/worker/test_loop_failures.py`. One import,
  one tuple member, one test asserting `Outcome.RETRY` with the delivery counter and the DLQ
  untouched. Read-only on `Outcome`. Also close the loop with CannObserv/archiver#128 so its
  `deploy/README.md` durability cells move off *unasserted*.

**Batch C — `batch/c` not required** (single agent; the agent's own branch serves).

- **C1 · #25** — owns `_raise_for_status` outright, plus `pacing.py`,
  `tests/worker/test_pacing.py`, `tests/worker/test_handler_pacing.py`. `HostPacer` grows
  `report_rate_limited(host)` and an in-memory quiet-window reset. Constants recovered from
  watcher history: `BACKOFF_MULTIPLIER = 2.0`, `BACKOFF_MAX_INTERVAL = 60.0`, a hard floor of
  `2.0` on the first escalation, `DECAY_WINDOW = 1800.0`. The policy lookup keeps supplying
  the floor; escalation never goes below it.

## Key decisions

**Guards before substance (A before B).** #17 outscores everything at 17/18, and it still
waits. Two reasons, both sequencing rather than priority: it is the only High-blast issue, so
it wants a batch where its file windows are not shared; and #37/#44 are the most *schedulable*
things in the backlog — zero contested files with anything — so they fill a slot that would
otherwise idle. The stronger argument is that a trivial issue can be a hard gate: #44 is what
makes each batch's merge commit actually get a CI run, and #37 is the guard on the very
checkout this orchestration operates from.

**`.socraticodecontextartifacts.json` is read-only for all five agents.** All of AGENTS.md,
`docs/ARCHITECTURE.md`, both contract docs, `docs/DEPLOYMENT.md`, `deploy/replicator.service`
and `.github/workflows/ci.yml` are manifest-described artifacts, so #17, #37 and #44 all
have reason to touch it. Three concurrent edits to one JSON file is the failure mode; worse,
AGENTS.md warns that nothing re-embeds the manifest, so the *fix* is a
`codebase_context_index` re-run — and the local Qdrant store is shared across worktrees, so
two workers re-indexing concurrently would race on a genuinely shared backing service. **The
orchestrator re-runs `codebase_context_index` once, post-merge, per batch.** This is the one
place where the "no shared backing service" finding above has an exception, and it is
avoided rather than provisioned around.

**No chain-appending artifact exists here.** No Alembic (Replicator is DB-free by charter),
no sequence-numbered ADRs, and `docs/plans/` filenames are date-based. Nothing needs a single
designated chain-owning agent.

**#37 needs a post-merge operator step that no worker can take.** Per AGENTS.md the installed
unit is a `cp`, not a symlink: `sudo cp deploy/replicator.service /etc/systemd/system/ &&
sudo systemctl daemon-reload`. A guard living only in the repo's copy of the unit is not a
guard — the same failure mode one level up, which is what #37 says about itself. Before any
restart, run `systemctl is-enabled` and `systemctl cat` first: an inactive-but-preset-enabled
unit is a deliberate hold, not a fault.

**Verification-mode asymmetry on #44.** Its agent verifies in a worktree under the *old*
concurrency config; the first run under the new group is the orchestrator's post-merge push.
The change governs which runs get *evicted from the queue*, not what any job does, so the
asymmetry is mild — but if a later batch shows an odd CI pattern, check this before
attributing it to that batch's diff.

**`origin/main` is behind local `main` by one commit** (`04ba17a chore: update skills
submodules`) as of writing. Worker worktrees are cut from `origin/main`, not from the
orchestrator's checkout, so **local main must be pushed before Batch A launches** or every
worker gets a tree missing that commit. Push once, not alongside another merge — a burst of
main pushes is exactly the #44 race.

**Every worker prompt carries the expected test count on its batch branch**, with "stop and
report if it does not match". This is the only detector that reliably catches a worktree cut
from the wrong base.

## Runtime note on issue-body decay

Issue bodies are a snapshot of filing time, and this backlog is three sequential mutations of
what they describe. Every worker prompt must say: **treat the issue body as a proposal, not a
specification** — verify every `file:line`, every claimed call site, and every prescribed
implementation against the current tree, and **report what turned out to be wrong** rather
than implementing around it silently. Phrase it as *"I want the corrections, not a report
that matches the prediction."*

This is not hypothetical for these five. Orchestration alone found, before any agent ran:

- **#17** — its stated blocker (#27) is closed; its "in flight" prerequisite (#11) has
  shipped, which converts the whole issue from prospective to live; and its co-core floor
  argument cites `>=0.7.7,<0.8` against an actual pin of `>=0.10,<0.11`.
- **#25** — its Refs name a **deleted file** (`watcher/src/core/rate_limiter.py`, removed by
  `2b98989`), and the method it says to mirror (`_maybe_decay_backoff`) was never in that
  file, is **DB-backed**, and fired after a successful fetch rather than on a timer. Its
  "decay" was a one-step reset. Two of the four constants it depends on are unnamed in it.
- Also worth a worker's scepticism: **AGENTS.md documents the co-core floor as
  `>=0.9.4,<0.10` while `pyproject.toml:32` pins `>=0.10,<0.11`.** Out of scope for all five
  issues; do not fix it in passing, but do not trust the AGENTS.md number either.

A caution learned the same way: an orchestrator's own quick grep is exactly as falsifiable as
the issue body it audits, and it carries more authority because it arrives as a correction.
The first pass here reported that `_maybe_decay_backoff` never existed anywhere in Watcher —
wrong, because the grep was scoped to one file at one commit. Depth of batch raises staleness;
depth of confidence does not raise accuracy.

## Deferred items

- **Removing the "do not attempt conditional GET yet" warning** from both contract docs.
  Gated on CannObserv/watcher#249 teaching `apply_fetch_blob` a no-bytes branch, and on
  cannobserv#298 documenting the token in `FetchFailedEvent`'s docstring. #17 downgrades the
  warning; a follow-on removes it.
- **`content_unchanged` as a dedicated fact (shape B in #17).** Not minted. Costs a co-core
  model, a `ChangeEventPayload` union member, an `idempotency_key` rule, a `from_wire` entry
  and a version floor, for consumer-side handling that is byte-for-byte what shape A already
  produces. Stays available and additive if `fetch_failed` carrying a non-failure later
  becomes untenable.
- **Non-terminal `fetch_failed` facts (#9 §3).** Still deferred on its own merits —
  `content.blobs` is broadcast and nothing trims it, so a fact per reclaim during an origin
  outage is unbounded growth. #25 deliberately does *not* reopen it: escalation is mechanism
  state, and publishing it would route a mechanism decision through a policy channel.
- **Bucket depth / burst allowance on `HostPacer`.** Fixed spacing, deliberately — a burst is
  precisely what an origin notices (#12).
- **Fixing AGENTS.md's stale co-core floor** (`>=0.9.4,<0.10` vs the pinned `>=0.10,<0.11`).
  Real, found during this orchestration, and not any of these five issues' business. Worth
  its own issue.

## Out of scope

- **#7** (object-store blob backend / claim-check) and **#38** (separate GCS test bucket) —
  open, and excluded by the five named for this pass.
- **`REPLICATOR_*` env vars for #25's escalation constants** — decided against at the
  approval gate; see Decisions taken.
- **Honouring `Retry-After` on statuses other than 429/503**, and any gradual (rather than
  one-step) decay curve. Watcher's behaviour is the target; improving on it is a follow-on.
- **Adding a Postgres outbox to make the 304 fact durable.** Forbidden by
  `docs/contracts/replicator-boundaries.md`; the consumer group's PEL is the durable record
  of intent.
