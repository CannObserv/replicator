"""The alias namespace: what an operator provisioned on *this host* (#29, T1/T2).

``credentials_alias`` on a ``content.replicate`` command is a **selector, not a
secret** — it names a binding that exists here or it names nothing. These tests
pin the half of the trust model that is cheap and mechanical: the provisioned set
is a fact about this host, a command cannot reach it, and an alias nobody stood
up is a terminal refusal before any provider client is constructed.

What they deliberately do *not* cover is credential material. A binding names a
bucket, a folder, an identifier prefix — never a key. The credential is resolved
by the provider SDK from host state (ADC, a keypair in host config); since #114 a
binding may name *which* key file, by absolute path, and nothing else about it.

The alias naming rule (#114) is here too: a name determines its provider and,
for ``gcs``, its bucket.
"""

import json
from datetime import UTC, datetime, timedelta

import pytest

from src.worker.aliases import (
    ALIAS_NAME_PATTERN,
    LEGACY_ALIASES,
    AliasBinding,
    AliasTable,
    load_alias_table,
)

# Named by the #114 rule: `gcs-<role>` binds `co-gcs-<role>`.
GCS = {
    "provider": "gcs",
    "bucket": "co-gcs-artifacts",
    "prefix": "replications",
}
OTHER = {"provider": "gcs", "bucket": "co-gcs-other"}


def write_aliases(tmp_path, mapping):
    path = tmp_path / "aliases.json"
    path.write_text(json.dumps(mapping))
    return path


def test_an_unset_path_provisions_nothing():
    """The default posture, and it is the safe one (T5).

    No file means no alias resolves, so every replicate command is refused. That
    is what makes enabling replication an explicit operator act on the VM rather
    than a consequence of a message arriving — which matters most for ``ia``,
    whose items cannot be deleted.
    """
    table = load_alias_table(None)

    assert table.resolve("gcs-artifacts") is None
    assert table.provisioned == ()


UNPROVISIONED = "no alias table on this host — replication is not provisioned"


def test_an_unset_path_says_so_in_the_journal(caplog):
    """Unset leaves the same line a missing file does, and says *why* (#86).

    Before this, ``None`` returned silently and only the missing-file branch
    logged — so the one state every host has actually been in was the one state
    the journal could not show. An operator asked "is this host provisioned?"
    had to infer it from the absence of a line, and the ``worker ready`` line's
    ``replication_aliases: []`` was the only positive evidence. One message for
    both unprovisioned states keeps a single grep honest; ``detail`` is what
    tells them apart.
    """
    with caplog.at_level("INFO", logger="src.worker.aliases"):
        load_alias_table(None)

    [record] = [r for r in caplog.records if r.message == UNPROVISIONED]
    assert "REPLICATOR_REPLICATION_ALIASES_FILE" in record.detail
    assert "alias_unknown" in record.detail
    assert not hasattr(record, "path")


def test_a_missing_file_provisions_nothing_rather_than_raising(tmp_path, caplog):
    """A path that does not exist is "nothing provisioned", not a boot failure.

    Deliberate: the worker's job is ``content.fetch``, and a replicate config
    typo must not take the fetch loop down with it. The refusals say so per
    command, which reaches the operator through the same channel every other
    replicate problem does — and the journal names the path it looked for, so
    a typo in the variable is one grep from its cause (#86).
    """
    path = tmp_path / "absent.json"
    with caplog.at_level("INFO", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ()
    [record] = [r for r in caplog.records if r.message == UNPROVISIONED]
    assert record.path == str(path)
    assert "alias_unknown" in record.detail


def test_a_binding_is_resolved_by_name(tmp_path):
    table = load_alias_table(write_aliases(tmp_path, {"gcs-artifacts": GCS}))

    binding = table.resolve("gcs-artifacts")

    assert binding == AliasBinding(provider="gcs", bucket="co-gcs-artifacts", prefix="replications")
    assert table.provisioned == ("gcs-artifacts",)


def test_an_unprovisioned_alias_resolves_to_nothing(tmp_path):
    """T2: any bus writer can name any alias, so the set has to be bounded here.

    This is what converts "any writer names any alias" into "any writer names any
    alias the operator already stood up".
    """
    table = load_alias_table(write_aliases(tmp_path, {"gcs-artifacts": GCS}))

    assert table.resolve("not-provisioned") is None
    assert table.resolve("") is None


def test_a_malformed_file_provisions_nothing_and_says_so(tmp_path, caplog):
    """Fail closed, loudly. A half-parsed alias table is worse than none.

    Refusing everything is recoverable — the operator fixes the file and the next
    command works. Silently provisioning the entries that happened to parse would
    make *which* aliases exist depend on where the JSON broke.
    """
    path = tmp_path / "aliases.json"
    path.write_text("{not json")

    with caplog.at_level("ERROR", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ()
    assert any(r.message == "alias table is unreadable" for r in caplog.records)


@pytest.mark.parametrize(
    ("entry", "why"),
    [
        pytest.param({"bucket": "b"}, "no provider", id="missing-provider"),
        pytest.param({"provider": "gcs"}, "gcs needs a bucket", id="gcs-without-a-bucket"),
        pytest.param(
            {"provider": "nope", "bucket": "b"}, "unknown provider", id="unknown-provider"
        ),
        pytest.param("not-a-mapping", "not a mapping", id="scalar-entry"),
    ],
)
def test_an_unusable_entry_is_dropped_rather_than_half_provisioned(tmp_path, caplog, entry, why):
    """One bad entry drops itself, not the table.

    The opposite of the malformed-file case, and the difference is whether the
    *structure* parsed: a readable table with one bad row is unambiguous about
    which rows are good, so the good ones stand and the bad one refuses like an
    alias nobody wrote.
    """
    path = write_aliases(tmp_path, {"gcs-artifacts": GCS, "gcs-other": entry})

    with caplog.at_level("WARNING", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ("gcs-artifacts",)
    assert table.resolve("gcs-other") is None
    assert any(r.message == "ignoring an unusable alias binding" for r in caplog.records)


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("[]", id="a-list"),
        pytest.param('"just a string"', id="a-scalar"),
        pytest.param("null", id="null"),
    ],
)
def test_valid_json_that_is_not_a_table_provisions_nothing(tmp_path, caplog, raw):
    """CR #17: parsing is not the same as being a table.

    ``json.loads`` succeeds on a list or a bare scalar, so the type check is a
    separate branch from the parse failure above — and it was the untested one.
    """
    path = tmp_path / "aliases.json"
    path.write_text(raw)

    with caplog.at_level("ERROR", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ()
    assert any(r.message == "alias table is unreadable" for r in caplog.records)


def test_an_empty_alias_name_is_dropped(tmp_path, caplog):
    """CR #17: an empty key would resolve for a command carrying an empty alias.

    ``AliasTable.resolve("")`` already returns None for an unprovisioned table,
    but a file with `"": {...}` would have made the empty alias *resolvable* —
    every command that omitted the field landing on one operator's binding.
    """
    path = write_aliases(tmp_path, {"": GCS, "gcs-artifacts": GCS})

    with caplog.at_level("WARNING", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ("gcs-artifacts",)
    assert table.resolve("") is None


def test_the_bindings_mapping_cannot_be_mutated_through(tmp_path):
    """CR #19: frozen stops reassignment, not mutation.

    The class docstring claims there is no path that adds an alias, and the test
    below only pinned attribute reassignment — so the property it is named for
    was not the property it checked.
    """
    table = load_alias_table(write_aliases(tmp_path, {"gcs-artifacts": GCS}))

    with pytest.raises(TypeError):
        table.bindings["smuggled"] = AliasBinding(provider="gcs", bucket="b")

    assert table.resolve("smuggled") is None


def test_the_table_is_a_snapshot_a_command_cannot_reach(tmp_path):
    """The provisioned set is host state, read once, never influenced by a message.

    ``AliasTable`` is frozen and its bindings are frozen: there is no path from
    the consume loop that could add an alias, which is the property T2 leans on.
    """
    table = load_alias_table(write_aliases(tmp_path, {"gcs-artifacts": GCS}))
    binding = table.resolve("gcs-artifacts")

    with pytest.raises(AttributeError):
        binding.bucket = "somewhere-else"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        table.bindings = {}  # type: ignore[misc]


def test_a_gcs_binding_may_omit_the_prefix(tmp_path):
    """The bucket alone is a root. A prefix narrows it; its absence does not widen
    anything beyond the bucket the operator named."""
    table = load_alias_table(
        write_aliases(tmp_path, {"gcs-flat": {"provider": "gcs", "bucket": "co-gcs-flat"}})
    )

    assert table.resolve("gcs-flat").prefix == ""


def test_no_binding_field_can_carry_a_credential(tmp_path):
    """T1, asserted structurally rather than by convention.

    A binding names *where*, never *how to authenticate*. Extra keys in the file
    are dropped rather than carried, so an operator who pastes a key into the
    alias table does not get it loaded into the worker's memory — and a later
    field named for a secret has to get past this test first.
    """
    path = write_aliases(
        tmp_path, {"gcs-artifacts": {**GCS, "secret": "s3cr3t", "credentials": "/etc/key.json"}}
    )

    binding = load_alias_table(path).resolve("gcs-artifacts")

    assert not hasattr(binding, "secret")
    assert not hasattr(binding, "credentials")
    assert "s3cr3t" not in repr(binding)


def test_the_table_reports_what_it_provisioned_in_a_stable_order(tmp_path):
    """Logged at boot, so an operator can see what this host will accept."""
    zed, alpha = {**GCS, "bucket": "co-gcs-zed"}, {**GCS, "bucket": "co-gcs-alpha"}
    table = load_alias_table(write_aliases(tmp_path, {"gcs-zed": zed, "gcs-alpha": alpha}))

    assert table.provisioned == ("gcs-alpha", "gcs-zed")


def test_an_empty_table_is_not_an_error(tmp_path):
    table = load_alias_table(write_aliases(tmp_path, {}))

    assert isinstance(table, AliasTable)
    assert table.provisioned == ()


@pytest.mark.parametrize(
    "make",
    [
        pytest.param(lambda tmp: None, id="unset-path"),
        pytest.param(lambda tmp: tmp / "absent.json", id="missing-file"),
        pytest.param(lambda tmp: _write(tmp, "{not json"), id="unreadable"),
        pytest.param(lambda tmp: _write(tmp, "[]"), id="not-a-table"),
        pytest.param(lambda tmp: _write(tmp, json.dumps({})), id="empty-table"),
        pytest.param(lambda tmp: _write(tmp, json.dumps({"gcs-artifacts": GCS})), id="populated"),
    ],
)
def test_every_construction_path_returns_an_immutable_table(tmp_path, make):
    """CR #23: the immutability test pinned one path, and there are six.

    ``load_alias_table`` returns early four separate times before the populated
    case, each building its own table. A fifth early return with a bare ``{}``
    would leave one path mutable with every existing test still green — which is
    exactly how the original ``frozen=True`` claim came to be untrue.
    """
    table = load_alias_table(make(tmp_path))

    with pytest.raises(TypeError):
        table.bindings["smuggled"] = AliasBinding(provider="gcs", bucket="b")


def _write(tmp_path, text: str):
    path = tmp_path / "aliases.json"
    path.write_text(text)
    return path


def test_a_binding_may_name_a_credentials_file(tmp_path):
    """The one per-alias identity knob (#114): a host *path*, never key material.

    Splitting the writer identities puts public and private buckets behind
    different service accounts, so a binding has to say which key its writer
    loads. It says so by path, read at boot like the default ADC key, so T1 holds
    as written: nothing about the credential comes off the message, and the key
    itself never enters this object.
    """
    entry = {**GCS, "credentials_file": "/etc/replicator/co-gcs-publication-writer.json"}

    binding = load_alias_table(write_aliases(tmp_path, {"gcs-artifacts": entry})).resolve(
        "gcs-artifacts"
    )

    assert binding.credentials_file == "/etc/replicator/co-gcs-publication-writer.json"


def test_a_binding_without_a_credentials_file_writes_as_the_host_default(tmp_path):
    """Absent means ADC: `GOOGLE_APPLICATION_CREDENTIALS`, as every binding did before #114."""
    binding = load_alias_table(write_aliases(tmp_path, {"gcs-artifacts": GCS})).resolve(
        "gcs-artifacts"
    )

    assert binding.credentials_file == ""


PASTED_KEY = '{"type": "service_account", "private_key": "-----BEGIN PRIVATE KEY-----"}'


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("keys/publication.json", id="relative"),
        pytest.param("", id="empty"),
        pytest.param(42, id="not-a-string"),
        pytest.param(PASTED_KEY, id="a-pasted-key"),
    ],
)
def test_a_credentials_file_that_is_not_an_absolute_path_drops_the_binding(tmp_path, caplog, value):
    """Refused rather than resolved against the worker's cwd, which is the repo.

    A relative path would load whatever key happened to sit beside the checkout,
    and a pasted key is the mistake the field's name invites. Either way the
    binding refuses like an alias nobody wrote — a writer built on the default
    identity instead would put the wrong account behind a public bucket, which
    is the boundary the split exists for.
    """
    path = write_aliases(
        tmp_path, {"gcs-artifacts": GCS, "gcs-other": {**OTHER, "credentials_file": value}}
    )

    with caplog.at_level("WARNING", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ("gcs-artifacts",)
    (record,) = [r for r in caplog.records if r.message == "ignoring an unusable alias binding"]
    assert "credentials_file" in record.detail


def test_a_pasted_key_never_reaches_the_journal(tmp_path, caplog):
    """The refusal names the rule, not the value: quoting it back would log the key."""
    path = write_aliases(tmp_path, {"gcs-other": {**OTHER, "credentials_file": PASTED_KEY}})

    with caplog.at_level("DEBUG", logger="src.worker.aliases"):
        load_alias_table(path)

    assert caplog.records
    assert not any("PRIVATE KEY" in str(vars(r)) for r in caplog.records)


def test_the_boot_line_says_which_bindings_load_their_own_key(tmp_path, caplog):
    """What an operator checks at the publication cutover: which key each alias writes as."""
    entry = {**OTHER, "credentials_file": "/etc/replicator/co-gcs-publication-writer.json"}
    path = write_aliases(tmp_path, {"gcs-other": entry, "gcs-artifacts": GCS})

    with caplog.at_level("INFO", logger="src.worker.aliases"):
        load_alias_table(path)

    (record,) = [r for r in caplog.records if r.message == "alias table loaded"]
    assert record.credentials_files == {
        "gcs-other": "/etc/replicator/co-gcs-publication-writer.json"
    }


# The alias naming rule (#114). A name determines its provider and, for `gcs`,
# its bucket, so one typo can no longer bind the public bucket under a private
# name or the reverse.


def gcs_entry(bucket, **extra):
    return {"provider": "gcs", "bucket": bucket, **extra}


def test_the_name_pattern_is_the_plans():
    """Pinned as a string because co-core will export it (cannobserv#493) for
    Archiver's RepSpec schema, and the two definitions must be the same text."""
    assert ALIAS_NAME_PATTERN == r"^(gcs|gdrive|ia)-[a-z][a-z0-9-]*$"


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("public", id="no-provider-prefix"),
        pytest.param("GCS-publication", id="upper-case"),
        pytest.param("gcs_publication", id="underscore"),
        pytest.param("gcs-", id="no-role"),
        pytest.param("gcs-9lives", id="role-starts-with-a-digit"),
        pytest.param("s3-publication", id="unknown-provider-prefix"),
    ],
)
def test_a_name_outside_the_rule_drops_the_binding(tmp_path, caplog, name):
    path = write_aliases(tmp_path, {name: gcs_entry("co-gcs-publication")})

    with caplog.at_level("WARNING", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ()
    assert any(r.message == "ignoring an unusable alias binding" for r in caplog.records)


def test_the_name_must_carry_the_bindings_provider(tmp_path):
    """`ia-publication` bound to `gcs` reads as archive.org to anyone writing a RepSpec."""
    path = write_aliases(tmp_path, {"ia-publication": gcs_entry("co-gcs-publication")})

    assert load_alias_table(path).provisioned == ()


@pytest.mark.parametrize(
    "bucket",
    [
        pytest.param("co-gcs-publication", id="production"),
        pytest.param("co-gcs-test-publication", id="test-twin"),
    ],
)
def test_a_gcs_name_binds_the_bucket_it_names(tmp_path, bucket):
    table = load_alias_table(write_aliases(tmp_path, {"gcs-publication": gcs_entry(bucket)}))

    assert table.resolve("gcs-publication").bucket == bucket


@pytest.mark.parametrize(
    "bucket",
    [
        pytest.param("co-gcs-blobs", id="another-role"),
        pytest.param("co-gcs-publication-archive", id="a-superstring"),
        pytest.param("co-gcs-publicatio", id="a-typo"),
        pytest.param("example-bucket", id="outside-the-scheme"),
    ],
)
def test_a_gcs_name_refuses_any_other_bucket(tmp_path, caplog, bucket):
    """The typo this rule exists for: a name and a bucket that disagree."""
    path = write_aliases(tmp_path, {"gcs-publication": gcs_entry(bucket)})

    with caplog.at_level("WARNING", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ()
    assert any(r.message == "ignoring an unusable alias binding" for r in caplog.records)


def test_a_legacy_name_is_accepted_outside_the_rule(tmp_path):
    """`primary` predates the rule and Archiver's RepSpecs name it; it stands until
    they move to `gcs-publication` (archiver#276), bucket unchecked."""
    table = load_alias_table(write_aliases(tmp_path, {"primary": gcs_entry("co-artifacts")}))

    assert table.provisioned == ("primary",)


def test_a_legacy_name_past_its_expiry_is_kept_and_reported(tmp_path, caplog):
    """Loud, not lossy. Dropping it would refuse Archiver's publications
    `alias_unknown`, a terminal answer to a date nobody acted on."""
    (name, expiry), *_ = LEGACY_ALIASES.items()
    path = write_aliases(tmp_path, {name: gcs_entry("co-artifacts")})

    with caplog.at_level("ERROR", logger="src.worker.aliases"):
        table = load_alias_table(path, today=expiry + timedelta(days=1))

    assert table.provisioned == (name,)
    (record,) = [r for r in caplog.records if r.message == "a legacy alias is past its expiry"]
    assert record.alias == name
    assert record.expiry == expiry.isoformat()


def test_a_legacy_name_within_its_expiry_is_not_reported(tmp_path, caplog):
    (name, expiry), *_ = LEGACY_ALIASES.items()
    path = write_aliases(tmp_path, {name: gcs_entry("co-artifacts")})

    with caplog.at_level("ERROR", logger="src.worker.aliases"):
        load_alias_table(path, today=expiry)

    assert not [r for r in caplog.records if r.levelname == "ERROR"]


def test_no_legacy_name_has_outlived_its_expiry():
    """The tripwire. On the expiry this fails every CI run until someone removes the
    name (Archiver migrated) or moves the date (it has not): the decision the
    date stands for, forced rather than forgotten."""
    today = datetime.now(UTC).date()
    overdue = {name: str(expiry) for name, expiry in LEGACY_ALIASES.items() if today > expiry}

    assert not overdue
