"""No test names a production destination.

Production `co-gcs-replication` can never be a test target, and the reason is the
grant rather than a policy anyone can relax: the writer SA holds
`storage.objects.{create,get,list}` and **no `delete`** — which is what enforces
the replicate contract's T4 "never overwrite, never delete" at IAM rather than
only in our code. A conflict fixture cannot reset itself against it, so the same
test cannot run twice, and every run that writes leaves permanent litter (#38).

**The property that makes the production grant correct is the property that makes
production untestable.** So the destination is refused here rather than merely
unused, in the same shape `tests/conftest.py` refuses Redis db 0: the live
`content.fetch` stream lives there and a test frame written to it would be
fetched for real. Same argument one service down.

**Why this is not in `tests/test_boundaries.py`.** That file is the executable
half of the boundaries charter and says so in its first line — every assertion in
it is a way the charter's rule stops being true one defensible commit at a time,
and its docstring sends a reader to `docs/contracts/replicator-boundaries.md`
before deleting anything. This scan enforces nothing the charter claims; it is a
property of the test tree, and its reasoning lives in `docs/TESTING.md`. Filing
it there would have cost the boundaries file the one navigational promise it
makes.

**This scan is the fast signal, not the boundary** (CR #7). It matches whole
string literals, so a name assembled from parts — a concatenation, an f-string
around a variable — passes it. That is not a hole to plug: the same evasion would
defeat any static check, and what actually stops a write is the runtime half in
`tests/conftest.py`, which sees the bucket a driver was *constructed with* rather
than the source it came from, and behind that the IAM grant, which refuses the
write whatever both of them believe. Read the three as layers with different
failure modes, and do not spend effort making this one exhaustive.

**Two conventions inherited from that file, because both earn their keep.** The
detector is itself tested — a structural scan that quietly walks zero files
passes forever while enforcing nothing, which is worse than no test because the
issue then cites it. And the scan is AST-based rather than grep: prose names the
production bucket legitimately, here and in `docs/DEPLOYMENT.md`, and a test
whose first tripper is an English sentence gets deleted rather than heeded.
"""

import ast
import os
from pathlib import Path

import pytest
from co_core_aio.gcs import AsyncGcsDriver

from src.storage.gcs import GcsBlobStore
from tests.conftest import (
    PRODUCTION_ENV,
    TEST_BLOB_BUCKET_ENV,
    TEST_BUCKET_ENV,
    TEST_CREDENTIALS_ENV,
    guarded_init,
    resolve_test_blob_bucket,
    resolve_test_bucket,
)

REPO = Path(__file__).resolve().parents[1]
SCANNED = (REPO / "src", REPO / "tests")

# The literals no module under `src/` or `tests/` may contain. Both halves of the
# production identity, because either one alone is enough to write:
#
# `co-gcs-replication` — the bucket. Named in `docs/DEPLOYMENT.md` and in
# `scripts/sync_wheelhouse.py`'s docstring, neither of which is scanned; nowhere
# a runtime value can come from.
#
# `co-gcs-replicator` — the writer SA, which is also its key file's basename
# (`/etc/replicator/co-gcs-replicator.json`) and the local part of its email. The
# bucket name is the fast check; the identity is the one that matters, because a
# test pointed at the right bucket with the wrong ADC is a state reachable today
# — `AGENTS.md` tells us to `set -a; . /etc/replicator/.env` before shell work,
# which puts that key into the environment `uv run pytest` inherits.
#
# Deliberately *not* forbidden: `co-pypi-reader.json`, the wheelhouse reader,
# which `tests/worker/test_replicate_guards.py` names on purpose to prove the
# path guard refuses a real secret. Refusing every `/etc/replicator/` path would
# have taken that test with it.
FORBIDDEN_DESTINATIONS = ("co-gcs-replication", "co-gcs-replicator")

# The test bucket is `co-gcs-test-replication` and not `co-gcs-replication-test`
# precisely so the check above can stay a substring match. A suffixed name
# *contains* the production name, which would force this scan to carry a negative
# lookahead — and a scan whose correctness depends on one is a scan that breaks
# the first time someone adds a second test bucket (#50).
TEST_BUCKET = "co-gcs-test-replication"


def _python_files() -> list[Path]:
    """Every module under the scanned roots except this one, sorted.

    This file is excluded because it is the one place the forbidden strings have
    to appear — the same self-exclusion the vocabulary scan's token list needs,
    and for the same reason.
    """
    here = Path(__file__).resolve()
    return sorted(path for root in SCANNED for path in root.rglob("*.py") if path.resolve() != here)


def _string_literals(tree: ast.AST) -> set[str]:
    """Every string constant in ``tree``, docstrings included.

    Docstrings are *not* skipped here, unlike the vocabulary scan. That scan is
    about words which have an ordinary English meaning, so prose has to be
    exempt. A bucket name has no ordinary meaning: a module under `src/` or
    `tests/` explaining what `co-gcs-replication` is has the production name in
    reach of anyone copying the paragraph, and the two files that legitimately
    explain it are outside the scanned roots.
    """
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def _hits(tree: ast.AST) -> set[str]:
    """Which ``FORBIDDEN_DESTINATIONS`` appear inside any string literal."""
    literals = _string_literals(tree)
    return {name for name in FORBIDDEN_DESTINATIONS if any(name in text for text in literals)}


def test_no_module_names_a_production_destination():
    files = _python_files()
    assert files, "the scan found no modules — it is a no-op"

    # Parsed once per file, not once per clause (CR #5): the same call in the
    # value and the condition read and parsed all of `src/` and `tests/` twice.
    hits = {path.relative_to(REPO).as_posix(): _hits(ast.parse(path.read_text())) for path in files}
    offenders = {module: sorted(found) for module, found in hits.items() if found}

    assert not offenders


@pytest.mark.parametrize(
    "source",
    [
        pytest.param('BUCKET = "co-gcs-replication"', id="a-bucket-constant"),
        pytest.param('AliasBinding(bucket="co-gcs-replication")', id="a-keyword-argument"),
        pytest.param('{"public": {"bucket": "co-gcs-replication"}}', id="a-dict-value"),
        pytest.param('URL = "https://storage.googleapis.com/co-gcs-replication/x"', id="a-url"),
        pytest.param('KEY = "/etc/replicator/co-gcs-replicator.json"', id="the-writer-key-path"),
        pytest.param('SA = "co-gcs-replicator@co-gcs.iam.gserviceaccount.com"', id="the-sa-email"),
    ],
)
def test_the_destination_detector_sees_a_planted_literal(source):
    assert _hits(ast.parse(source))


@pytest.mark.parametrize(
    "source",
    [
        pytest.param(f'BUCKET = "{TEST_BUCKET}"', id="the-test-bucket"),
        pytest.param('BUCKET = "example-replication-bucket"', id="the-neutral-fake"),
        pytest.param('KEY = "file:///etc/replicator/co-pypi-reader.json"', id="the-wheelhouse-key"),
    ],
)
def test_the_destination_detector_passes_a_safe_name(source):
    assert not _hits(ast.parse(source))


def test_the_test_bucket_is_not_a_superstring_of_production():
    """The naming decision, pinned so a rename cannot quietly undo the scan.

    `co-gcs-replication-test` would contain the production name, and this scan
    would then refuse the very bucket it exists to steer traffic towards — or,
    worse, be "fixed" with a lookahead that admits `co-gcs-replication-prod` too.
    """
    assert not any(name in TEST_BUCKET for name in FORBIDDEN_DESTINATIONS)


# --------------------------------------------------------------------------
# The runtime half: production must be unreachable, not merely unnamed
# --------------------------------------------------------------------------

# The scan above is static, and static is not enough. A bucket name can be built
# at runtime from a variable the environment supplied, and the environment on
# this VM supplies exactly that: `AGENTS.md` instructs loading
# `/etc/replicator/.env` for shell work, so `uv run pytest` inherits the
# production ADC and — once #50 provisions it — the production alias table too.
# Nothing in the tree read either one today; the reason is that
# `tests/worker/test_main_writers.py` stubs the driver, which is an accident of
# how those tests are written rather than a property anyone asserted.
#
# So the fixtures in `tests/conftest.py` assert the negative directly, and these
# are their tests.


def test_the_production_environment_is_not_visible_to_a_test():
    """The autouse scrub, asserted from inside an ordinary test.

    Both variables are present in the shell an agent is told to work from. A
    test that reads either is reading production configuration.
    """
    assert [name for name in PRODUCTION_ENV if name in os.environ] == []


def test_the_test_bucket_variable_has_no_fallback():
    """Absent means skip, never "use the default".

    A default here is the whole bug: it is what turns "no test bucket is
    configured" into "write to whichever bucket the code would have picked",
    which on a worker configured for production is the production one. Compare
    `REPLICATOR_TEST_REDIS_URL`, which *does* default — to db 15 on localhost,
    a destination that cannot be the live one because `real_redis` refuses db 0
    outright. There is no equivalent safe default for a bucket.
    """
    assert resolve_test_bucket({}) is None
    assert resolve_test_bucket({TEST_BUCKET_ENV: "some-bucket"}) == "some-bucket"


def test_an_unmarked_test_cannot_construct_a_real_driver():
    """The negative nothing asserted before (#38).

    `AsyncGcsDriver.__init__` resolves ADC in its own body — `storage.Client()`
    reads the key file, and on a GCE-style host reaches the metadata server — so
    the refusal has to happen *before* the call through, not after. That is what
    makes this cheap enough to leave on for the whole suite.
    """
    with pytest.raises(AssertionError, match="not marked"):
        AsyncGcsDriver("any-bucket-at-all")


def test_the_bucket_guard_refuses_before_the_original_runs():
    calls = []

    class Driver:
        def __init__(self, bucket, *, client=None):
            calls.append(bucket)

    guarded = guarded_init(Driver.__init__, TEST_BUCKET)

    with pytest.raises(AssertionError):
        guarded(object(), "co-gcs-replication")

    assert calls == []


def test_the_bucket_guard_reads_the_keyword_form_too():
    """`AsyncGcsDriver(bucket=...)` is as legal as the positional call.

    A guard that only inspected `args[0]` would pass every keyword call through
    to ADC — refusing nothing, while reading as though it refused everything.
    """
    guarded = guarded_init(lambda self, bucket, **kw: None, TEST_BUCKET)

    with pytest.raises(AssertionError):
        guarded(object(), bucket="co-gcs-replication")


def test_the_bucket_guard_passes_the_test_bucket():
    seen = []
    guarded = guarded_init(lambda self, bucket, **kw: seen.append(bucket), TEST_BUCKET)

    guarded(object(), TEST_BUCKET)

    assert seen == [TEST_BUCKET]


@pytest.mark.gcs
def test_a_marked_test_reaches_the_test_bucket_and_nothing_else(gcs_bucket):
    """The opt-in path, and the only `gcs`-marked test until #53 lands.

    It touches no network: the guard refuses a wrong bucket *before* the real
    ``__init__`` builds its client, so the refusal is observable without
    authenticating. What it proves is the wiring — that a marked test is handed
    the provisioned bucket, is pointed at the test identity rather than the
    production one, and is still refused everything else.

    Without it the skip logic would itself be untested, and `-m gcs` would report
    "no tests ran" on an unprovisioned host — indistinguishable from a suite that
    silently stopped collecting.
    """
    assert gcs_bucket == os.environ[TEST_BUCKET_ENV]
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == os.environ[TEST_CREDENTIALS_ENV]

    with pytest.raises(AssertionError, match="may only reach"):
        AsyncGcsDriver("some-other-bucket")


def test_an_unmarked_test_cannot_construct_a_real_blob_store():
    """The same refusal, one store over (#7).

    `GcsBlobStore.__init__` builds `storage.Client()` in its own body exactly as
    `AsyncGcsDriver.__init__` does, so it is reachable by the same accident and
    has to be refused by the same wrapper. The guard is here rather than in
    `tests/storage/test_gcs.py` because the property is about the *suite* — no
    test anywhere constructs one — which no module about the store could assert.
    """
    with pytest.raises(AssertionError, match="not marked"):
        GcsBlobStore("any-bucket-at-all")


def test_the_two_stores_are_guarded_against_different_buckets():
    """One variable per destination, because the two grants are opposites.

    The replicate bucket's SA deliberately holds **no delete** — that is what
    enforces T4's "never overwrite, never delete" at IAM rather than only in our
    code. A temp store whose whole job is to expire its objects needs the
    opposite posture. Pointing both at one bucket would mean either the temp
    store cannot expire or the permanent one can be erased, and the second
    failure is unrecoverable.
    """
    assert TEST_BUCKET_ENV != TEST_BLOB_BUCKET_ENV


def test_the_blob_bucket_variable_has_no_fallback():
    """Absent means skip, never "use the default" — #50's rule, second bucket."""
    assert resolve_test_blob_bucket({}) is None
    assert resolve_test_blob_bucket({TEST_BLOB_BUCKET_ENV: "some-bucket"}) == "some-bucket"


def test_the_blob_store_guard_names_its_own_variable():
    """A refusal that named the wrong variable would send an operator to the wrong line."""
    guarded = guarded_init(
        lambda self, bucket, **kw: None,
        "co-gcs-test-blobs",
        label="GcsBlobStore",
        env_name=TEST_BLOB_BUCKET_ENV,
    )

    with pytest.raises(AssertionError, match=TEST_BLOB_BUCKET_ENV):
        guarded(object(), "some-other-bucket")


def test_the_guard_admits_an_injected_client():
    """A supplied client resolves no credential and reaches no bucket by name.

    The refusal is aimed at a constructor that builds `storage.Client()` itself.
    Refusing an injected one too would force every unit test of store *decisions*
    — which key, which precondition, what a lost race means — to be marked `gcs`
    and skipped on an unprovisioned host, which is how a mark stops meaning
    "touches the network" and starts meaning "constructs this class".
    """
    seen = []
    guarded = guarded_init(lambda self, bucket, **kw: seen.append(bucket), None)

    guarded(object(), "any-bucket-at-all", client=object())

    assert seen == ["any-bucket-at-all"]


def test_the_guard_still_refuses_a_bare_construction_beside_it():
    """The admission above is about the client, not about relaxing the rule."""
    guarded = guarded_init(lambda self, bucket, **kw: None, None)

    with pytest.raises(AssertionError, match="not marked"):
        guarded(object(), "any-bucket-at-all", client=None)


def test_the_injected_client_carve_out_does_not_reach_a_marked_test():
    """CR #4: the admission is for tests that reach no network, not for any client.

    Ahead of the bucket comparison it also admitted a `@pytest.mark.gcs` test —
    the one state with a credential actually resolved — so a driver could be
    constructed against any bucket by handing it a client. The carve-out belongs
    to the unmarked state, where nothing is expected because nothing is allowed.
    """
    guarded = guarded_init(lambda self, bucket, **kw: None, TEST_BUCKET, marked=True)

    with pytest.raises(AssertionError, match="may only reach"):
        guarded(object(), "some-other-bucket", client=object())
