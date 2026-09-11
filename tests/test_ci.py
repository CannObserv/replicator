"""Every commit that lands on `main` must keep its own CI run, and its one runnable
integration file must keep running.

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


def _test_job_runs() -> list[str]:
    """Every ``run:`` line in the workflow, as raw text.

    Text rather than YAML for ``_concurrency``'s reason: pyyaml is not a declared
    dependency of this project, and asserting against the file as the tool that
    reads it would see is the precedent ``tests/test_deploy.py`` set.
    """
    return [line.strip() for line in WORKFLOW.read_text().splitlines()]


def test_the_broker_oom_suite_runs_in_ci():
    """#83: the one `integration` file that needs no external broker must actually run.

    Every other marked test needs a scratch broker started beside it, which no runner
    has — but ``test_oom_integration.py`` spawns its own ``redis-server``, so
    nothing but the binary stands between CI and the suite that is the evidence
    behind the capped-broker record in ``docs/CONVENTIONS.md`` and behind
    CannObserv/broker#6 and CannObserv/broker#2.

    Pinned because the failure is silent in both directions: deleting the step
    leaves a green workflow, and the suite's own absence of a signal is the thing
    it would remove.
    """
    lines = _test_job_runs()

    assert any(
        "-m integration" in line and "tests/worker/test_oom_integration.py" in line
        for line in lines
    ), "no CI step runs tests/worker/test_oom_integration.py under -m integration"


def test_a_missing_scratch_broker_fails_ci_rather_than_skipping():
    """A skip here is silent by construction, so the precondition is asserted.

    ``capped_server`` skips when ``redis-server`` is absent — correct on a
    developer's machine, and in CI it would mean an apt failure produced a green
    job with the whole suite unrun. That is the failure ``docs/TESTING.md``
    already names for the ``gcs`` job, whose answer is this same shape: assert the
    precondition rather than discover it.
    """
    guard = re.search(
        r"if ! command -v redis-server.*?exit 1",
        WORKFLOW.read_text(),
        flags=re.DOTALL,
    )

    # The `exit 1` has to be *this* guard's. The workflow holds two other
    # `exit 1`s — the WIF-provider assertions — so a bare "does the file contain
    # exit 1" would pass a guard that only warned.
    assert guard, "nothing in CI fails the job when redis-server did not resolve"
