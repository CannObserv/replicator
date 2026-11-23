"""The fetch credential table: which references this host resolves, and to what (#216).

``ContentFetchCommand.credential_headers`` maps a header name to a ``fetch-<role>``
reference. The reference is a **key, never a value** (cannobserv#577, C1): it names
a binding an operator provisioned here, and the binding says which header the value
may ride, to which hosts, and where on this host the value lives.

These tests pin the table's half of that, the half that is host state:

- the table is read once, at boot, and nothing a command carries reaches it;
- a bad entry is dropped with the rule it broke and **never the value**;
- the value sits in a ``SecretStr`` and no ``repr`` or log line shows it.

The planted value below is the test's tripwire: it must appear in no log record, no
``repr`` and no ``str`` of anything the loader returns.
"""

import json
import logging

import pytest
from pydantic import SecretStr

from src.worker.credentials import (
    UNCREDENTIALED_HEADERS,
    FetchCredential,
    FetchCredentialTable,
    load_fetch_credentials,
)
from src.worker.headers import MAX_REQUEST_HEADER_BYTES

VALUE = "planted-test-value-3f9c2b"  # never a real credential; a tripwire for leaks
UNPROVISIONED = "no fetch credential table on this host — fetch credentials are not provisioned"


def write_value(tmp_path, content: str = VALUE + "\n", name: str = "fetch-wa-leg-api"):
    directory = tmp_path / "fetch-credentials"
    directory.mkdir(exist_ok=True)
    path = directory / name
    path.write_text(content)
    return path


def binding(path, **overrides):
    return {
        "header": "X-Api-Key",
        "hosts": ["api.leg.wa.gov"],
        "value_file": str(path),
        **overrides,
    }


def write_table(tmp_path, mapping):
    path = tmp_path / "fetch-credentials.json"
    path.write_text(json.dumps(mapping))
    return path


def load(tmp_path, mapping, caplog=None):
    path = write_table(tmp_path, mapping)
    if caplog is None:
        return load_fetch_credentials(path)
    with caplog.at_level(logging.INFO, logger="src.worker.credentials"):
        return load_fetch_credentials(path)


def assert_no_value_in(caplog):
    for record in caplog.records:
        assert VALUE not in record.getMessage()
        assert VALUE not in json.dumps(record.__dict__, default=str)


# --- Unprovisioned: the safe default ----------------------------------------------


def test_an_unset_path_provisions_nothing(caplog):
    with caplog.at_level(logging.INFO, logger="src.worker.credentials"):
        table = load_fetch_credentials(None)

    assert table.provisioned == ()
    assert table.resolve("fetch-wa-leg-api") is None
    assert [r.message for r in caplog.records] == [UNPROVISIONED]
    assert "REPLICATOR_FETCH_CREDENTIALS_FILE is unset" in caplog.records[0].detail


def test_a_missing_file_provisions_nothing_under_the_same_message(tmp_path, caplog):
    with caplog.at_level(logging.INFO, logger="src.worker.credentials"):
        table = load_fetch_credentials(tmp_path / "absent.json")

    assert table.provisioned == ()
    assert [(r.levelname, r.message) for r in caplog.records] == [("INFO", UNPROVISIONED)]


@pytest.mark.parametrize("raw", ["{not json", "[]", '"a string"', "null"])
def test_an_unreadable_table_provisions_nothing_and_says_so(tmp_path, caplog, raw):
    """Refusing everything is recoverable; provisioning whatever parsed first is not."""
    path = tmp_path / "fetch-credentials.json"
    path.write_text(raw)

    with caplog.at_level(logging.INFO, logger="src.worker.credentials"):
        table = load_fetch_credentials(path)

    assert table.provisioned == ()
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("ERROR", "fetch credential table is unreadable")
    ]


# --- A good binding -----------------------------------------------------------------


def test_a_binding_resolves_to_its_header_hosts_and_value(tmp_path):
    value_file = write_value(tmp_path)

    table = load(tmp_path, {"fetch-wa-leg-api": binding(value_file)})
    credential = table.resolve("fetch-wa-leg-api")

    assert table.provisioned == ("fetch-wa-leg-api",)
    assert credential.header == "x-api-key"
    assert credential.hosts == frozenset({"api.leg.wa.gov"})
    assert credential.value_file == str(value_file)
    assert isinstance(credential.value, SecretStr)
    assert credential.value.get_secret_value() == VALUE


def test_one_trailing_newline_is_the_files_not_the_values(tmp_path):
    """``echo`` and every editor end a file with one; it is never part of the value."""
    value_file = write_value(tmp_path, content=VALUE)

    credential = load(tmp_path, {"fetch-wa-leg-api": binding(value_file)}).resolve(
        "fetch-wa-leg-api"
    )

    assert credential.value.get_secret_value() == VALUE


def test_hosts_are_held_in_policy_hosts_canonical_form(tmp_path):
    """One spelling per host, the one ``content.fetch-policy`` uses (co-core's ``PolicyHost``)."""
    value_file = write_value(tmp_path)

    credential = load(
        tmp_path,
        {
            "fetch-wa-leg-api": binding(
                value_file, hosts=["API.Leg.WA.gov.", "xn--bcher-kva.example"]
            )
        },
    ).resolve("fetch-wa-leg-api")

    assert credential.hosts == frozenset({"api.leg.wa.gov", "xn--bcher-kva.example"})


def test_the_boot_line_names_references_headers_hosts_and_paths_never_values(tmp_path, caplog):
    value_file = write_value(tmp_path)

    load(tmp_path, {"fetch-wa-leg-api": binding(value_file)}, caplog)

    [loaded] = [r for r in caplog.records if r.message == "fetch credentials loaded"]
    assert loaded.provisioned == {
        "fetch-wa-leg-api": {
            "header": "x-api-key",
            "hosts": ["api.leg.wa.gov"],
            "value_file": str(value_file),
        }
    }
    assert_no_value_in(caplog)


# --- The value never shows ----------------------------------------------------------


def test_neither_a_binding_nor_the_table_shows_the_value(tmp_path):
    value_file = write_value(tmp_path)
    table = load(tmp_path, {"fetch-wa-leg-api": binding(value_file)})
    credential = table.resolve("fetch-wa-leg-api")

    for shown in (repr(credential), str(credential), repr(table), str(table)):
        assert VALUE not in shown


def test_a_stray_field_never_reaches_an_attribute(tmp_path):
    """Only the declared fields are read: a value pasted into the table is dropped."""
    value_file = write_value(tmp_path)

    credential = load(
        tmp_path, {"fetch-wa-leg-api": binding(value_file, value=VALUE, secret=VALUE)}
    ).resolve("fetch-wa-leg-api")

    assert not hasattr(credential, "secret")
    assert VALUE not in repr(credential)


# --- Immutable host state -----------------------------------------------------------


def test_the_mapping_cannot_be_mutated_through(tmp_path):
    table = load(tmp_path, {"fetch-wa-leg-api": binding(write_value(tmp_path))})

    with pytest.raises(TypeError):
        table.bindings["fetch-smuggled"] = table.resolve("fetch-wa-leg-api")


def test_a_binding_and_the_table_are_frozen(tmp_path):
    table = load(tmp_path, {"fetch-wa-leg-api": binding(write_value(tmp_path))})
    credential = table.resolve("fetch-wa-leg-api")

    with pytest.raises(AttributeError):
        credential.hosts = frozenset({"elsewhere.example"})  # type: ignore[misc]
    with pytest.raises(AttributeError):
        table.bindings = {}  # type: ignore[misc]


def test_every_construction_path_returns_an_immutable_table(tmp_path):
    for table in (
        load_fetch_credentials(None),
        load_fetch_credentials(tmp_path / "absent.json"),
        load(tmp_path, {}),
    ):
        assert isinstance(table, FetchCredentialTable)
        with pytest.raises(TypeError):
            table.bindings["fetch-x"] = None  # type: ignore[index]


# --- A bad entry drops itself, never the table, and never quotes the value ------------


def _dropped(tmp_path, caplog, reference, entry, why):
    good = write_value(tmp_path, name="fetch-good")
    table = load(tmp_path, {"fetch-good": binding(good), reference: entry}, caplog)

    assert table.provisioned == ("fetch-good",)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert [r.message for r in warnings] == ["ignoring an unusable fetch credential binding"]
    assert why in warnings[0].detail, warnings[0].detail
    assert_no_value_in(caplog)
    return warnings[0]


@pytest.mark.parametrize(
    "reference",
    ["gcs-publication", "fetch-", "FETCH-x", "fetch_x", VALUE],
)
def test_a_reference_off_the_rule_is_dropped_and_not_quoted(tmp_path, caplog, reference):
    """The likeliest wrong key is a pasted secret, so the log carries the rule, not the key."""
    warning = _dropped(tmp_path, caplog, reference, binding(write_value(tmp_path)), "fetch-<role>")

    assert getattr(warning, "reference", None) is None


@pytest.mark.parametrize(
    ("overrides", "why"),
    [
        pytest.param({"header": None}, "header", id="no-header"),
        pytest.param({"header": "X Api Key"}, "token", id="not-a-token"),
        pytest.param({"header": "X-Api-Key\n"}, "token", id="trailing-newline"),
        pytest.param({"header": "Host"}, "not a header Replicator will send", id="refused"),
        pytest.param({"header": "Proxy-Authorization"}, "not a header", id="proxy-family"),
        pytest.param({"header": "User-Agent"}, "never a credential", id="E5-user-agent"),
        pytest.param({"header": "If-None-Match"}, "never a credential", id="E5-validator"),
        pytest.param({"header": "if-modified-since"}, "never a credential", id="E5-validator-2"),
        pytest.param({"hosts": []}, "hosts", id="no-hosts"),
        pytest.param({"hosts": "api.leg.wa.gov"}, "hosts", id="hosts-a-string"),
        pytest.param({"hosts": [7]}, "hostname", id="host-not-a-string"),
        pytest.param({"hosts": ["https://api.leg.wa.gov"]}, "host", id="host-with-scheme"),
        pytest.param({"hosts": ["api.leg.wa.gov:443"]}, "host", id="host-with-port"),
        pytest.param({"hosts": ["bücher.example"]}, "host", id="host-not-punycode"),
        pytest.param({"hosts": ["*.leg.wa.gov"]}, "wildcard", id="wildcard"),
        pytest.param({"hosts": [""]}, "host", id="empty-host"),
        pytest.param({"value_file": "relative/path"}, "absolute", id="relative-path"),
        pytest.param({"value_file": 7}, "absolute", id="not-a-string"),
    ],
)
def test_an_unusable_binding_is_dropped_with_the_rule_it_broke(tmp_path, caplog, overrides, why):
    entry = binding(write_value(tmp_path), **overrides)
    if overrides.get("header", "") is None:
        del entry["header"]

    warning = _dropped(tmp_path, caplog, "fetch-wa-leg-api", entry, why)

    assert warning.reference == "fetch-wa-leg-api"


def test_a_binding_that_is_not_an_object_is_dropped(tmp_path, caplog):
    _dropped(tmp_path, caplog, "fetch-wa-leg-api", "x-api-key", "not an object")


@pytest.mark.parametrize(
    ("content", "why"),
    [
        pytest.param("", "empty", id="empty"),
        pytest.param("\n", "empty", id="only-a-newline"),
        pytest.param(f"{VALUE}\r\n", "printable", id="crlf"),
        pytest.param(f"{VALUE}\n\n", "printable", id="two-newlines"),
        pytest.param(f"{VALUE}\nsplit", "printable", id="embedded-newline"),
        pytest.param(f"{VALUE}\tx", "printable", id="tab"),
        pytest.param(f"{VALUE}é", "printable", id="non-ascii"),
        pytest.param(f" {VALUE}", "whitespace", id="leading-space"),
        pytest.param(f"{VALUE} \n", "whitespace", id="trailing-space"),
        pytest.param("v" * MAX_REQUEST_HEADER_BYTES, "byte", id="over-the-bound"),
    ],
)
def test_an_unsendable_value_drops_the_binding_and_is_never_quoted(tmp_path, caplog, content, why):
    """A value httpx would refuse, or send altered, is refused at boot rather than at a fetch."""
    value_file = write_value(tmp_path, content=content)

    _dropped(tmp_path, caplog, "fetch-wa-leg-api", binding(value_file), why)


def test_a_value_exactly_at_the_byte_bound_is_kept(tmp_path):
    """The bound counts the field line: name, ``": "``, value, CRLF — the handler's measure."""
    content = "v" * (MAX_REQUEST_HEADER_BYTES - len("x-api-key") - 4)

    table = load(tmp_path, {"fetch-wa-leg-api": binding(write_value(tmp_path, content=content))})

    assert table.provisioned == ("fetch-wa-leg-api",)


def test_an_unreadable_value_file_drops_the_binding(tmp_path, caplog):
    warning = _dropped(
        tmp_path,
        caplog,
        "fetch-wa-leg-api",
        binding(tmp_path / "fetch-credentials" / "absent"),
        "unreadable",
    )

    assert warning.value_file.endswith("absent")


def test_a_value_file_that_is_not_utf8_drops_the_binding(tmp_path, caplog):
    value_file = write_value(tmp_path)
    value_file.write_bytes(b"\xff\xfe" + VALUE.encode())

    _dropped(tmp_path, caplog, "fetch-wa-leg-api", binding(value_file), "printable")


def test_the_names_that_are_never_credentials_are_e5s():
    """co-core's E5: the driver's default and the two validators the handler replays."""
    assert UNCREDENTIALED_HEADERS == frozenset({"user-agent", "if-none-match", "if-modified-since"})


def test_a_credential_is_constructed_only_with_a_secret():
    """``value`` is typed ``SecretStr``: a plain string on the field is a type error."""
    with pytest.raises(TypeError):
        FetchCredential(
            header="x-api-key", hosts=frozenset({"a.example"}), value=VALUE, value_file="/v"
        )


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"value_file": VALUE}, id="value-pasted-as-value_file"),
        pytest.param({"hosts": [VALUE + ":x"]}, id="value-pasted-as-a-host"),
        pytest.param({"hosts": ["api.leg.wa.gov", f"https://{VALUE}"]}, id="value-in-a-host-url"),
    ],
)
def test_a_value_pasted_into_another_field_is_never_quoted(tmp_path, caplog, overrides):
    """CR 1: a value pasted where a path or host belongs is dropped, not logged.

    The alias table refuses to quote a non-path ``credentials_file`` for this reason;
    the same holds for ``value_file`` and for ``hosts``, whose validator's own message
    would quote the host back.
    """
    entry = binding(write_value(tmp_path), **overrides)

    warning = _dropped(tmp_path, caplog, "fetch-wa-leg-api", entry, "")

    assert warning.reference == "fetch-wa-leg-api"


def test_a_value_file_that_never_ends_drops_the_binding_at_once(tmp_path, caplog):
    """CR 2: the read is bounded, so ``/dev/zero`` can't hold the boot forever."""
    warning = _dropped(
        tmp_path, caplog, "fetch-wa-leg-api", binding("/dev/zero"), "byte header bound"
    )

    assert warning.value_file == "/dev/zero"
