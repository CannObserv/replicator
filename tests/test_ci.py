"""Every commit that lands on `main` must keep its own CI run.

A concurrency group holds **at most one pending run**, and that queue depth is
not what `cancel-in-progress` controls: with the flag false GitHub still evicts
the *pending* member of a group when a third run queues behind an in-progress
one. So a group keyed on the branch — `github.ref` is `refs/heads/main` for
every push — silently drops the middle commit of any burst of three, leaving a
commit on the protected branch with no signal at all.

Keying the group on `github.sha` for pushes puts each `main` run alone in its
group, so nothing can evict it. The pull-request half must not change: on a
`pull_request` event `github.head_ref` is non-empty, so PR runs still share one
group per branch and still collapse under `cancel-in-progress: true`.

Enforced here because the failure is invisible in the workflow file — it reads
like a correct expression, and the comment above it originally claimed an intent
`cancel-in-progress` cannot deliver on its own (#44).
"""

import re
from pathlib import Path

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"


def _concurrency() -> dict[str, str]:
    """The top-level ``concurrency:`` mapping, values left uninterpolated.

    Parsed as text rather than YAML: pyyaml is not a declared dependency, and
    ``tests/test_deploy.py`` sets the precedent for asserting against a config
    file the same way the tool that reads it would see the raw string.
    """
    block = re.search(
        r"^concurrency:\n((?:[ \t]+\S.*\n)+)",
        WORKFLOW.read_text(),
        flags=re.MULTILINE,
    )
    assert block, f"no top-level concurrency block in {WORKFLOW.name}"

    entries = {}
    for line in block.group(1).splitlines():
        key, _, value = line.strip().partition(":")
        entries[key] = value.strip()
    return entries


def test_the_concurrency_group_is_unique_per_commit_on_main():
    """`github.sha`, never `github.ref` — a branch-keyed group has one pending slot."""
    group = _concurrency()["group"]

    assert "github.sha" in group, (
        f"concurrency group must key pushes on github.sha, got {group!r} — a group "
        "holds one pending run, so a burst of main pushes evicts the middle one (#44)"
    )
    assert not re.search(r"github\.ref\b", group), (
        f"concurrency group still keys on github.ref: {group!r} — every push to main "
        "shares refs/heads/main, which is the eviction this fix removes (#44)"
    )


def test_pull_request_runs_still_share_a_group_per_branch():
    """The collapsing half is deliberate and must survive the push-side fix."""
    group = _concurrency()["group"]

    assert "github.head_ref" in group, (
        f"concurrency group must fall back through github.head_ref: {group!r} — it is "
        "the only term that makes two runs on one PR branch collide and cancel (#44)"
    )
    assert group.index("github.head_ref") < group.index("github.sha"), (
        f"github.head_ref must be tried before github.sha: {group!r} — the sha is "
        "unique per commit, so leading with it would never collapse a PR's runs"
    )


def test_cancellation_stays_gated_on_pull_request_events():
    """Push runs must never be cancelled; only superseded PR runs are."""
    cancel = _concurrency()["cancel-in-progress"]

    assert "github.event_name == 'pull_request'" in cancel, (
        f"cancel-in-progress must stay gated on the event name, got {cancel!r} — an "
        "unconditional true cancels an in-progress main run mid-verification (#44)"
    )
