"""The alias namespace: what an operator provisioned on *this host* (#29, T1/T2).

``credentials_alias`` on a ``content.replicate`` command is a **selector, not a
secret** — it names a binding that exists here or it names nothing. These tests
pin the half of the trust model that is cheap and mechanical: the provisioned set
is a fact about this host, a command cannot reach it, and an alias nobody stood
up is a terminal refusal before any provider client is constructed.

What they deliberately do *not* cover is credential material. A binding names a
bucket, a folder, an identifier prefix — never a key. The credential is resolved
by the provider SDK from host state (ADC, a keypair in host config), which is why
``AliasBinding`` has nowhere to put one.
"""

import json

import pytest

from src.worker.aliases import AliasBinding, AliasTable, load_alias_table

GCS = {
    "provider": "gcs",
    "bucket": "co-artifacts",
    "prefix": "replications",
}


def write_aliases(tmp_path, mapping):
    path = tmp_path / "aliases.json"
    path.write_text(json.dumps(mapping))
    return path


def test_an_unset_path_provisions_nothing(monkeypatch):
    """The default posture, and it is the safe one (T5).

    No file means no alias resolves, so every replicate command is refused. That
    is what makes enabling replication an explicit operator act on the VM rather
    than a consequence of a message arriving — which matters most for ``ia``,
    whose items cannot be deleted.
    """
    table = load_alias_table(None)

    assert table.resolve("primary") is None
    assert table.provisioned == ()


def test_a_missing_file_provisions_nothing_rather_than_raising(tmp_path):
    """A path that does not exist is "nothing provisioned", not a boot failure.

    Deliberate: the worker's job is ``content.fetch``, and a replicate config
    typo must not take the fetch loop down with it. The refusals say so per
    command, which reaches the operator through the same channel every other
    replicate problem does.
    """
    table = load_alias_table(tmp_path / "absent.json")

    assert table.provisioned == ()


def test_a_binding_is_resolved_by_name(tmp_path):
    table = load_alias_table(write_aliases(tmp_path, {"primary": GCS}))

    binding = table.resolve("primary")

    assert binding == AliasBinding(
        alias="primary", provider="gcs", bucket="co-artifacts", prefix="replications"
    )
    assert table.provisioned == ("primary",)


def test_an_unprovisioned_alias_resolves_to_nothing(tmp_path):
    """T2: any bus writer can name any alias, so the set has to be bounded here.

    This is what converts "any writer names any alias" into "any writer names any
    alias the operator already stood up".
    """
    table = load_alias_table(write_aliases(tmp_path, {"primary": GCS}))

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
    path = write_aliases(tmp_path, {"good": GCS, "bad": entry})

    with caplog.at_level("WARNING", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ("good",)
    assert table.resolve("bad") is None
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
    path = write_aliases(tmp_path, {"": GCS, "good": GCS})

    with caplog.at_level("WARNING", logger="src.worker.aliases"):
        table = load_alias_table(path)

    assert table.provisioned == ("good",)
    assert table.resolve("") is None


def test_the_bindings_mapping_cannot_be_mutated_through(tmp_path):
    """CR #19: frozen stops reassignment, not mutation.

    The class docstring claims there is no path that adds an alias, and the test
    below only pinned attribute reassignment — so the property it is named for
    was not the property it checked.
    """
    table = load_alias_table(write_aliases(tmp_path, {"primary": GCS}))

    with pytest.raises(TypeError):
        table.bindings["smuggled"] = AliasBinding(alias="smuggled", provider="gcs", bucket="b")

    assert table.resolve("smuggled") is None


def test_the_table_is_a_snapshot_a_command_cannot_reach(tmp_path):
    """The provisioned set is host state, read once, never influenced by a message.

    ``AliasTable`` is frozen and its bindings are frozen: there is no path from
    the consume loop that could add an alias, which is the property T2 leans on.
    """
    table = load_alias_table(write_aliases(tmp_path, {"primary": GCS}))
    binding = table.resolve("primary")

    with pytest.raises(AttributeError):
        binding.bucket = "somewhere-else"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        table.bindings = {}  # type: ignore[misc]


def test_a_gcs_binding_may_omit_the_prefix(tmp_path):
    """The bucket alone is a root. A prefix narrows it; its absence does not widen
    anything beyond the bucket the operator named."""
    table = load_alias_table(write_aliases(tmp_path, {"flat": {"provider": "gcs", "bucket": "b"}}))

    assert table.resolve("flat").prefix == ""


def test_no_binding_field_can_carry_a_credential(tmp_path):
    """T1, asserted structurally rather than by convention.

    A binding names *where*, never *how to authenticate*. Extra keys in the file
    are dropped rather than carried, so an operator who pastes a key into the
    alias table does not get it loaded into the worker's memory — and a later
    field named for a secret has to get past this test first.
    """
    path = write_aliases(
        tmp_path, {"primary": {**GCS, "secret": "s3cr3t", "credentials": "/etc/key.json"}}
    )

    binding = load_alias_table(path).resolve("primary")

    assert not hasattr(binding, "secret")
    assert not hasattr(binding, "credentials")
    assert "s3cr3t" not in repr(binding)


def test_the_table_reports_what_it_provisioned_in_a_stable_order(tmp_path):
    """Logged at boot, so an operator can see what this host will accept."""
    table = load_alias_table(write_aliases(tmp_path, {"zed": GCS, "alpha": GCS}))

    assert table.provisioned == ("alpha", "zed")


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
        pytest.param(lambda tmp: _write(tmp, json.dumps({"primary": GCS})), id="populated"),
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
        table.bindings["smuggled"] = AliasBinding(alias="s", provider="gcs", bucket="b")


def _write(tmp_path, text: str):
    path = tmp_path / "aliases.json"
    path.write_text(text)
    return path
