"""The fetch credential table: which references this host resolves, and to what (#216).

``ContentFetchCommand.credential_headers`` (cannobserv#577) maps a header name to a
``fetch-<role>`` reference. The reference is a **key, never a value** (C1): it names
a binding an operator provisioned on this host, or it names nothing. The binding says
which header the value may ride (operator call 3), to which hosts (C2), and where on
this host the value lives. The value never travels on the bus.

**``aliases.py``'s shape, for its reasons.** The provisioned set is host state, so env
carries the table's path (``REPLICATOR_FETCH_CREDENTIALS_FILE``) and the file carries
the table. It's read once, at boot, and frozen behind ``MappingProxyType``: no path
from the consume loop can add a binding. Unset means nothing is provisioned, which is
the safe default: every credentialed command closes ``credential_unknown``.

**Values are read at boot, too** (operator call 2). A bad value file drops its binding
in the boot log rather than at the first fetch, the hot path does no file I/O, and
rotation is a restart, as it is for the writers' keys. Each value is held in a
``SecretStr`` on a frozen, slotted :class:`FetchCredential`, so ``repr`` and ``str``
show ``**********``; ``get_secret_value()`` is called in one place, the transport
that builds the outgoing hop (``egress.py``), and ``tests/test_boundaries.py`` pins
that.

**Never raises, and never quotes a value.** An unreadable table provisions nothing
(ERROR); an unusable binding drops itself (WARNING) with the rule it broke. Neither
names a value, and a reference off the rule isn't named either: the likeliest wrong
key, like the likeliest wrong ``credentials_file``, is a pasted secret.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, NamedTuple

from co_core.pure.models.changes import PolicyHost
from co_core.pure.util.aliases import FETCH_CREDENTIAL_PATTERN, validate_fetch_credential
from pydantic import SecretStr, TypeAdapter, ValidationError

from src.core.logging import get_logger
from src.worker.headers import (
    HEADER_NAME,
    HEADER_VALUE,
    MAX_REQUEST_HEADER_BYTES,
    REFUSED_HEADER_PREFIX,
    REFUSED_HEADERS,
)

logger = get_logger(__name__)

# One message for unset and for a path with no file behind it, as the alias table
# has it, so one grep answers "is this host provisioned?".
_UNPROVISIONED = "no fetch credential table on this host — fetch credentials are not provisioned"
_DROPPED = "ignoring an unusable fetch credential binding"

# co-core's E5 (cannobserv#577): headers a credential may never name. ``user-agent``
# is the driver's default and the fingerprint-continuity lever; the two validators are
# what a revalidating issuer replays from ``blob_available`` (#17). None is a secret.
UNCREDENTIALED_HEADERS = frozenset({"user-agent", "if-none-match", "if-modified-since"})

_POLICY_HOST = TypeAdapter(PolicyHost)


@dataclass(frozen=True, slots=True)
class FetchCredential:
    """One binding: the header it rides, the hosts it may reach, and its value.

    ``header`` is folded to lower case, as the handler folds every header it sends.
    ``hosts`` are in ``PolicyHost``'s canonical form (lower case, punycode, no
    trailing dot), the spelling ``content.fetch-policy`` uses. ``value_file`` is the
    path the value was read from, for the boot line; never the value.

    ``slots=True`` drops a stray field in the table rather than carrying it, as
    ``AliasBinding``'s does.
    """

    header: str
    hosts: frozenset[str]
    value: SecretStr
    value_file: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, SecretStr):
            raise TypeError("a FetchCredential's value is a SecretStr, never a plain string")


class Resolved(NamedTuple):
    """A reference the handler resolved, beside its binding: what a command scope holds.

    The reference rides along so a hop refusal can name it; the table's key stays the
    one owner of the name, as the alias table's does (``AliasBinding``'s CR #39).
    """

    reference: str
    credential: FetchCredential


@dataclass(frozen=True, slots=True)
class FetchCredentialTable:
    """The provisioned set, read once at boot. A command can resolve it and never change it."""

    bindings: Mapping[str, FetchCredential]

    def resolve(self, reference: str) -> FetchCredential | None:
        """The binding for ``reference``, or ``None`` if nobody provisioned it here."""
        return self.bindings.get(reference)

    @property
    def provisioned(self) -> tuple[str, ...]:
        """Every reference this host resolves, sorted: logged once at boot."""
        return tuple(sorted(self.bindings))


def unprovisioned() -> FetchCredentialTable:
    """The nothing-provisioned table: every reference resolves to ``None``."""
    return FetchCredentialTable(MappingProxyType({}))


def load_fetch_credentials(path: Path | None) -> FetchCredentialTable:
    """Read the table and every value it names, failing **closed** at every step.

    - **no path, or no file**: nothing is provisioned, INFO, one message for both;
    - **an unreadable table**: nothing is provisioned, ERROR;
    - **one unusable binding**: that binding alone is dropped, WARNING, with the rule.

    Never raises: a fetch misconfiguration must not stop a worker whose other
    commands are fine. Each refusal reports per command, as ``credential_unknown``.
    """
    if path is None:
        logger.info(
            _UNPROVISIONED,
            extra={
                "detail": "REPLICATOR_FETCH_CREDENTIALS_FILE is unset; "
                "every credentialed content.fetch command is refused credential_unknown"
            },
        )
        return unprovisioned()
    try:
        raw = json.loads(Path(path).read_text())
    except FileNotFoundError:
        logger.info(
            _UNPROVISIONED,
            extra={
                "path": str(path),
                "detail": "no file at that path; "
                "every credentialed content.fetch command is refused credential_unknown",
            },
        )
        return unprovisioned()
    except Exception as exc:
        # The exception's type, never its message: a JSON error's message quotes no
        # content today, and a refusal that never quotes must not rest on that.
        logger.error(
            "fetch credential table is unreadable",
            extra={
                "path": str(path),
                "error": type(exc).__name__,
                "detail": "nothing is provisioned; every credentialed command will be refused",
            },
        )
        return unprovisioned()
    if not isinstance(raw, dict):
        logger.error(
            "fetch credential table is unreadable",
            extra={"path": str(path), "detail": "expected an object of reference -> binding"},
        )
        return unprovisioned()

    bindings: dict[str, FetchCredential] = {}
    for position, (reference, entry) in enumerate(raw.items()):
        credential = _credential_or_none(str(reference), entry, position)
        if credential is not None:
            bindings[str(reference)] = credential
    table = FetchCredentialTable(MappingProxyType(bindings))
    logger.info(
        "fetch credentials loaded",
        extra={
            "path": str(path),
            "provisioned": {
                reference: {
                    "header": credential.header,
                    "hosts": sorted(credential.hosts),
                    "value_file": credential.value_file,
                }
                for reference, credential in sorted(bindings.items())
            },
        },
    )
    return table


def _credential_or_none(reference: str, entry: Any, position: int) -> FetchCredential | None:
    """One binding, or ``None`` with the rule it broke in the journal."""
    try:
        validate_fetch_credential(reference)
    except ValueError:
        # Not named: a key off the rule is likeliest a pasted secret. Its position
        # in the table is what the operator finds it by.
        logger.warning(
            _DROPPED,
            extra={
                "position": position,
                "detail": f"the reference is not fetch-<role> ({FETCH_CREDENTIAL_PATTERN}); "
                "not quoted, in case it is a pasted secret",
            },
        )
        return None
    why, credential = _why_unusable(entry)
    if why is not None:
        extra: dict[str, Any] = {"reference": reference, "detail": why}
        if isinstance(entry, dict) and isinstance(entry.get("value_file"), str):
            extra["value_file"] = entry["value_file"]
        logger.warning(_DROPPED, extra=extra)
        return None
    return credential


def _why_unusable(entry: Any) -> tuple[str | None, FetchCredential | None]:
    """Why this entry can't be provisioned, or the credential it provisions."""
    if not isinstance(entry, dict):
        return "the binding is not an object", None
    header = entry.get("header")
    why = _why_unusable_header(header)
    if why is not None:
        return why, None
    hosts, why = _canonical_hosts(entry.get("hosts"))
    if why is not None:
        return why, None
    value_file = entry.get("value_file")
    if not isinstance(value_file, str) or not value_file.startswith("/"):
        return "value_file must be an absolute host path to the value", None
    value, why = _read_value(value_file, header.lower())
    if why is not None:
        return why, None
    return None, FetchCredential(
        header=header.lower(), hosts=hosts, value=SecretStr(value), value_file=value_file
    )


def _why_unusable_header(header: Any) -> str | None:
    """The handler's name rules for ``headers`` (``headers.py``), plus co-core's E5."""
    if not isinstance(header, str) or not header:
        return "the binding names no header"
    folded = header.lower()
    if not HEADER_NAME.match(folded):
        return "header is not an RFC 9110 token"
    if folded in REFUSED_HEADERS or folded.startswith(REFUSED_HEADER_PREFIX):
        return f"{header!r} is not a header Replicator will send"
    if folded in UNCREDENTIALED_HEADERS:
        return f"{header!r} is never a credential (co-core E5)"
    return None


def _canonical_hosts(hosts: Any) -> tuple[frozenset[str], str | None]:
    """Each host in ``PolicyHost``'s form; a binding matches hosts exactly."""
    if not isinstance(hosts, list) or not hosts:
        return frozenset(), "hosts must be a non-empty list of hostnames"
    canonical: set[str] = set()
    for host in hosts:
        if not isinstance(host, str):
            return frozenset(), "every entry in hosts must be a hostname"
        if "*" in host:
            return frozenset(), f"host {host!r} is a wildcard; a binding matches hosts exactly"
        try:
            canonical.add(_POLICY_HOST.validate_python(host))
        except ValidationError as exc:
            return frozenset(), f"host {host!r} is not a bare hostname: {exc.errors()[0]['msg']}"
    return frozenset(canonical), None


def _read_value(value_file: str, header: str) -> tuple[str, str | None]:
    """The value, minus one trailing ``\\n``, or why it can't be sent verbatim. Never quoted."""
    try:
        content = Path(value_file).read_bytes()
    except OSError as exc:
        return "", f"value_file is unreadable ({type(exc).__name__})"
    try:
        value = content.decode("ascii")
    except UnicodeDecodeError:
        return "", "the value is not printable US-ASCII"
    value = value.removesuffix("\n")
    if not value:
        return "", "the value is empty"
    if not HEADER_VALUE.match(value):
        return "", "the value is not printable US-ASCII on one line"
    if value != value.strip():
        # The handler strips an issuer's value; a credential's is refused instead,
        # because a stripped secret is a different secret.
        return "", "the value has leading or trailing whitespace"
    # The handler's measure: name, ": ", value, CRLF (``headers.py``).
    if len(header) + len(value) + 4 > MAX_REQUEST_HEADER_BYTES:
        return "", f"the value exceeds the {MAX_REQUEST_HEADER_BYTES}-byte header bound"
    return value, None
