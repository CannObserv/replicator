# SocratiCode — code exploration reference

Detail doc for the `## Code Exploration Policy` block in `AGENTS.md`. That block
carries the rule; this file carries the table.

**Owned by `init-socraticode` — do not hand-edit.** A re-run overwrites this file
wholesale. Repo-specific exploration notes belong in `AGENTS.md` under
`## Code Exploration Notes (repo-specific)`, outside the marker pair.

**Requires `init-socraticode` at or after the #115 split** (upstream
`gregoryfoster/skills@9125148`). `skills-vendor/gregoryfoster-skills` is pinned
older than that, so a re-run *today* does not write this file at all — it restores
the pre-split eight-row block into `AGENTS.md` and leaves this file orphaned,
duplicating what came back. Bump the pin before re-running (#41). The
`## Code Exploration Notes` section survives either way: the old sweep targets only
`## Code Exploration Policy`.

## When to use each tool

| Goal | Tool |
|------|------|
| Where is X defined / how does Y work / what files touch Z | `codebase_search` |
| Exact string/regex match (errors, log lines, known symbols) | `grep` / `rg` |
| Blast radius of changing/deleting a file or function | `codebase_impact` |
| What does an entry point actually do? | `codebase_flow` |
| Callers and callees of a function | `codebase_symbol` |
| Every symbol declared in a file | `codebase_symbols` |
| Imports/dependents of a file | `codebase_graph_query` |
| Import cycles | `codebase_graph_circular` |
| Bus contracts, deploy topology, MVP design rationale, env vars | `codebase_context` / `codebase_context_search` |
| Path-pattern walks ("all `*.py` under `src/worker/`") | the Explore subagent |

## Prefetch

The `codebase_*` MCP tools are **deferred**: their schemas are not in the session
until a `ToolSearch` prefetch loads them, and calling one before that fails
validation. The `SessionStart` hook in `.claude/settings.json` prints the exact
`select:` argument every session; run it verbatim if it did not fire.

## Per-tool notes

- **`codebase_search`** takes a natural-language query, not a regex. It ranks by
  embedding similarity, so an empty result means "nothing scored above the
  threshold", not "no such code" — retry with `minScore: 0` before concluding
  absence.
- **`codebase_impact` / `codebase_graph_query`** read the AST dependency graph,
  which is built separately from the embeddings. If the graph is stale or
  low-yield they answer *empty* rather than erroring.
- **`codebase_flow`** traces from an entry point; give it a real file path
  (`src/worker/main.py`), not a symbol name.
- **`codebase_context_search`** only sees files listed in
  `.socraticodecontextartifacts.json` — this repo registers the design plans, the
  systemd unit, the command reference, and every doc in the Detail Docs index. A
  path that does not resolve is skipped silently, so a missing answer is often a
  manifest problem. The manifest is a source, not the artifact: see
  `AGENTS.md` → `## Code Exploration Notes (repo-specific)`.
- **The file watcher is ephemeral.** It lives only while an MCP server process is
  running. After a long gap, or after a reboot, re-run `codebase_index` rather
  than trusting the index to be current.

## Cross-repo search

**Cross-repo search.** `SOCRATICODE_LINKED_PROJECTS` (in `.claude/settings.local.json`, gitignored) links the archiver, watcher, and notifier checkouts, so `codebase_search` spans the cluster. Use it for the co-core contracts, the parent integration strategy, and the producer-side outbox precedent — all of which live in archiver, not here. Linked projects contribute results only once they are themselves indexed.

## Graph health

`codebase_graph_status` reporting `READY` does **not** mean the graph resolved
anything — `READY` is reachable with a handful of edges across hundreds of files.
Check yield, not status:

```bash
node skills/init-socraticode/scripts/mcp-driver.mjs health-check .
```

`verdict: "low"` means the graph resolved almost no edges, so `codebase_graph_query`,
`codebase_impact` and `codebase_flow` answer with an ordinary "no dependency
information found" rather than an error — which reads as *no dependents*, the
opposite of the truth. Send dependency questions to `grep` until it is rebuilt.

## Index scope

`.socraticodeignore` (repo root, gitignore syntax, layered on the built-in
defaults and `.gitignore`) controls what gets embedded. Editing it affects
**subsequent** scans only — re-index to apply it. Vendored trees dominate the
index if left in, and vendored prose outranks first-party code in
`codebase_search` results.
