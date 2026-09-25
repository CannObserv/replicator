"""The alias namespace: destinations an operator provisioned on *this host*.

``credentials_alias`` on a ``content.replicate`` command is a **selector, not a
secret** (contract T1). It names a binding that exists here or it names nothing,
and the binding says *where* bytes may land — a bucket, a prefix, later a folder
id or an identifier prefix. Every provider resolves its credential locally, from
ADC or host config, and nothing here reads one. Since #114 a binding may say
*which* local key its writer loads — ``credentials_file``, a host path — so the
public and private buckets can sit behind different identities. That is a
pointer to host state, not key material, and ``_why_unusable`` refuses anything
that is not an absolute path, so a pasted key is dropped rather than carried.

**Why a file rather than settings fields.** The provisioned set is a fact about
this host, which puts it in the env channel of the charter's config taxonomy —
but it is a *table*, not a scalar, and one ``REPLICATOR_*`` variable per alias per
field is a shape env does not hold. So env carries the path
(``REPLICATOR_REPLICATION_ALIASES_FILE``) and the file carries the table. The
contract's phrase is "env-referenced host config", and this is that.

**Unset means nothing is provisioned, and that is the safe default** (T5).
Enabling replication to a destination is then an explicit operator act on the VM
rather than a consequence of a message arriving — which matters most for
archive.org, where an item cannot be deleted at all.
"""

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any

from src.core.logging import get_logger

logger = get_logger(__name__)

# One message for both unprovisioned states — unset, and a path with no file
# behind it — so the grep an operator runs is the same either way; ``detail``
# (and ``path`` in the second case) is what tells them apart (#86).
_UNPROVISIONED = "no alias table on this host — replication is not provisioned"

# Providers this host knows how to bind. A provider absent here cannot be
# provisioned, which is the other half of the ``provider_disabled`` refusal: the
# command decodes (co-core types ``provider`` as a plain ``str`` precisely so an
# unknown one can be refused rather than dead-lettered) and then finds no binding
# that could serve it.
#
# ``gcs`` alone today. ``gdrive`` needs a Shared Drive membership or domain-wide
# delegation before a binding means anything, and ``ia`` is gated on T5's
# deliberate operator act — neither is a line in this tuple, both are their own
# work (#29).
#
# **Availability is a code decision; provisioning is an operator one** (CR #25).
# T5 asks that enabling ``ia`` be an explicit act on the host, and an env-driven
# provider list would satisfy that literally — one variable and a public,
# undeletable item is reachable. Keeping the list here is stricter than the
# contract requires and deliberately so: adding a provider means adding its
# writer, its containment rule and its tests in the same change, which is the
# review an irreversible destination should get. The operator still decides
# *whether* any of it is reachable, by provisioning an alias or not.
KNOWN_PROVIDERS = ("gcs",)

# **The alias naming rule** (#114): ``<provider>-<role>``. The provider prefix
# must be the binding's, and a ``gcs`` binding must name ``co-gcs-<role>`` or its
# test twin ``co-gcs-test-<role>`` — so the name determines the bucket, and a typo
# can no longer bind the public bucket under a private name or the reverse. The
# prefixes span every provider the wire knows, not just ``KNOWN_PROVIDERS``: a
# name can be well-formed for a provider this host cannot bind yet.
#
# co-core will export this pattern (cannobserv#493) so Archiver's RepSpec schema
# validates ``credentials_alias`` against the same text; import it from there
# once it ships. ``tests/worker/test_aliases.py`` pins the string until then.
ALIAS_NAME_PATTERN = r"^(gcs|gdrive|ia)-[a-z][a-z0-9-]*$"
_ALIAS_NAME = re.compile(ALIAS_NAME_PATTERN)

# Names accepted outside the rule, each until a date. ``primary`` predates it and
# Archiver's RepSpecs name it; it goes once they name ``gcs-publication``
# (archiver#276, the publication cutover). Its bucket is unchecked, since no
# ``co-gcs-<role>`` follows from it.
#
# **Past its date a name is kept and reported, never dropped.** Dropping it would
# refuse Archiver's publications ``alias_unknown`` — a terminal answer to a date
# nobody acted on. The loud half is twofold: an ERROR at every boot, and a test
# that fails every CI run from the day after, until the name is removed or the
# date is moved on purpose.
LEGACY_ALIASES: Mapping[str, date] = MappingProxyType({"primary": date(2026, 12, 31)})


@dataclass(frozen=True, slots=True)
class AliasBinding:
    """Where one alias may write. Frozen, and it holds no credential.

    ``slots=True`` is doing real work here rather than saving memory: it means an
    operator who pastes a key into the alias file gets it **dropped at load**
    instead of carried on the object, because there is no attribute to hold it.
    T1 says no credential travels on the wire; this is the same rule one step
    further in, where the failure would be a key in a crash dump rather than on
    the bus.

    **The alias name is not a field here** (CR #39). It was, and it made the
    same key derivable two ways — ``build_writers`` keyed its drivers by the
    table's key while the handler looked them up by ``binding.alias``. Equal by
    construction through ``load_alias_table`` and free to disagree through any
    other, which silently disabled a binding whose driver had built fine. A
    binding says *where*, the table says *which*; one owner each.
    """

    provider: str
    bucket: str = ""
    prefix: str = ""
    # A host path to a service-account key, or "" for the host's ADC (#114). The
    # path, never the key: ``main.build_writers`` loads it at boot, and a value
    # that is not an absolute path is refused at load (see ``_why_unusable``).
    credentials_file: str = ""


@dataclass(frozen=True, slots=True)
class AliasTable:
    """The provisioned set, read once at boot.

    Frozen because there must be no path from the consume loop that adds an
    alias: T2 accepts "any bus writer can name any alias" only because the set of
    resolvable ones is host state a command cannot reach.

    ``frozen=True`` alone does not give that — it stops the attribute being
    *reassigned* and says nothing about the mapping behind it, so
    ``table.bindings[x] = ...`` used to succeed (CR #19). ``load_alias_table``
    wraps the dict in a ``MappingProxyType``, which makes the claim in the
    paragraph above true rather than merely intended.
    """

    bindings: Mapping[str, AliasBinding]

    def resolve(self, alias: str) -> AliasBinding | None:
        """The binding for ``alias``, or ``None`` if nobody provisioned it here."""
        return self.bindings.get(alias)

    @property
    def provisioned(self) -> tuple[str, ...]:
        """Every alias this host will accept, sorted — logged once at boot."""
        return tuple(sorted(self.bindings))


def _empty() -> AliasTable:
    """The nothing-provisioned table.

    One constructor rather than four literals (CR #23): every early return in
    ``load_alias_table`` builds one of these, and a fifth added later must get the
    immutable mapping without having to remember to. The original ``frozen=True``
    claim came untrue in exactly this way — by being asserted in one place and
    constructed in several.
    """
    return AliasTable(MappingProxyType({}))


def load_alias_table(
    path: Path | None, *, host_stores: Sequence[str] = (), today: date | None = None
) -> AliasTable:
    """Read the alias table, failing **closed** at every step.

    Three degrees of failure, and the difference between them is whether the
    structure parsed:

    - **no path, or no file** — nothing is provisioned. Not an error: this is the
      default posture of a host that does not replicate, and the overwhelmingly
      common case while #29 is in progress. Logged at INFO either way, under one
      message, so a single grep answers "is this host provisioned?" (#86 had to
      infer it from ``worker ready``, because unset used to return silently);
      ``detail`` says which of the two it was.
    - **unreadable file** — nothing is provisioned, logged at ERROR. Refusing
      everything is recoverable; provisioning whatever happened to parse before
      the syntax error would make the set of live aliases depend on where the
      JSON broke.
    - **one unusable entry** — that entry alone is dropped, logged at WARNING. A
      readable table is unambiguous about which rows are good, so the good ones
      stand and the bad one refuses exactly like an alias nobody wrote.

    Never raises. A replicate misconfiguration must not take down a worker whose
    actual job is ``content.fetch`` — the refusals report it per command, through
    the same channel every other replicate problem reaches the operator by.

    ``host_stores`` are the buckets this host keeps blobs in — temp and permanent —
    which no alias may bind (#114): aliases are publication destinations, and one
    bound to a store would let any replicate command write arbitrary keys into a
    bucket meant to hold only content-addressed blobs. ``today`` is for tests; it
    decides only whether a legacy name is overdue.
    """
    if path is None:
        logger.info(
            _UNPROVISIONED,
            extra={
                "detail": "REPLICATOR_REPLICATION_ALIASES_FILE is unset; "
                "every content.replicate command is refused alias_unknown"
            },
        )
        return _empty()
    try:
        raw = json.loads(Path(path).read_text())
    except FileNotFoundError:
        logger.info(
            _UNPROVISIONED,
            extra={
                "path": str(path),
                "detail": "no file at that path; "
                "every content.replicate command is refused alias_unknown",
            },
        )
        return _empty()
    except Exception as exc:
        logger.error(
            "alias table is unreadable",
            extra={
                "path": str(path),
                "error": f"{type(exc).__name__}: {exc}",
                "detail": "nothing is provisioned; every replicate command will be refused",
            },
        )
        return _empty()
    if not isinstance(raw, dict):
        logger.error(
            "alias table is unreadable",
            extra={"path": str(path), "detail": "expected an object of alias -> binding"},
        )
        return _empty()

    bindings: dict[str, AliasBinding] = {}
    for alias, entry in raw.items():
        binding = _binding_or_none(str(alias), entry, host_stores)
        if binding is not None:
            bindings[str(alias)] = binding
    table = AliasTable(MappingProxyType(bindings))
    _report_overdue_legacy_names(table, today or datetime.now(UTC).date())
    logger.info(
        "alias table loaded",
        extra={
            "path": str(path),
            "provisioned": list(table.provisioned),
            # Which bindings write as their own identity rather than ADC — what an
            # operator checks at the publication cutover (#114). Paths only.
            "credentials_files": {
                alias: binding.credentials_file
                for alias, binding in sorted(bindings.items())
                if binding.credentials_file
            },
        },
    )
    return table


def _report_overdue_legacy_names(table: AliasTable, today: date) -> None:
    """An ERROR per provisioned legacy name past its date; the binding stands."""
    for alias in table.provisioned:
        expiry = LEGACY_ALIASES.get(alias)
        if expiry is not None and today > expiry:
            logger.error(
                "a legacy alias is past its expiry",
                extra={
                    "alias": alias,
                    "expiry": expiry.isoformat(),
                    "detail": (
                        "still provisioned; move the RepSpecs naming it to a "
                        "<provider>-<role> alias, then remove it from the table"
                    ),
                },
            )


def _binding_or_none(alias: str, entry: Any, host_stores: Sequence[str]) -> AliasBinding | None:
    """One entry, or ``None`` with a reason in the journal.

    Only the fields ``AliasBinding`` declares are read, so anything else in the
    file — including something an operator mistook for a credential slot — never
    reaches an attribute.
    """
    why = _why_unusable(alias, entry, host_stores)
    if why is not None:
        logger.warning("ignoring an unusable alias binding", extra={"alias": alias, "detail": why})
        return None
    return AliasBinding(
        provider=entry["provider"],
        bucket=str(entry.get("bucket", "")),
        prefix=str(entry.get("prefix", "")).strip("/"),
        credentials_file=entry.get("credentials_file", ""),
    )


def _why_unusable(alias: str, entry: Any, host_stores: Sequence[str] = ()) -> str | None:
    """Why this entry cannot be provisioned, or ``None`` if it can."""
    if not alias:
        return "the alias name is empty"
    if not isinstance(entry, dict):
        return "the binding is not an object"
    provider = entry.get("provider")
    if not isinstance(provider, str) or not provider:
        return "no provider named"
    if provider not in KNOWN_PROVIDERS:
        return f"provider {provider!r} is not one this host can bind ({', '.join(KNOWN_PROVIDERS)})"
    if provider == "gcs" and not str(entry.get("bucket", "")):
        # The bucket *is* the root. Without it there is no containment check to
        # run, and a binding that cannot bound anything is worse than absent.
        return "a gcs binding needs a bucket"
    if provider == "gcs" and str(entry.get("bucket", "")) in host_stores:
        return "the bucket is a blob store this host reads; aliases are publication destinations"
    if alias not in LEGACY_ALIASES:
        why = _why_misnamed(alias, provider, str(entry.get("bucket", "")))
        if why is not None:
            return why
    if "credentials_file" in entry:
        value = entry["credentials_file"]
        # The rule, never the value: the likeliest wrong value is a pasted key,
        # and quoting it back would put the key in the journal.
        if not isinstance(value, str) or not value.startswith("/"):
            return "credentials_file must be an absolute host path to a key file"
    return None


def _why_misnamed(alias: str, provider: str, bucket: str) -> str | None:
    """Why this name breaks the naming rule for this binding, or ``None``.

    The role is everything after the first dash, so it may itself begin with
    ``test-``: ``gcs-test-publication`` binds ``co-gcs-test-publication`` as its
    own production form, a second name for the twin ``gcs-publication`` already
    reaches. Harmless — the bucket is still one the rule derives — and left legal
    because narrowing it would diverge from the pattern co-core exports.
    """
    if not _ALIAS_NAME.match(alias):
        return f"the alias name is not <provider>-<role> ({ALIAS_NAME_PATTERN})"
    named, role = alias.split("-", 1)
    if named != provider:
        return f"the alias name says {named!r} but the binding is {provider!r}"
    if provider == "gcs" and bucket not in (f"co-gcs-{role}", f"co-gcs-test-{role}"):
        return f"a gcs-{role} alias binds co-gcs-{role} or co-gcs-test-{role}, nothing else"
    return None
