"""The alias namespace: destinations an operator provisioned on *this host*.

``credentials_alias`` on a ``content.replicate`` command is a **selector, not a
secret** (contract T1). It names a binding that exists here or it names nothing,
and the binding says *where* bytes may land — a bucket, a prefix, later a folder
id or an identifier prefix. It never says how to authenticate: every provider
resolves its credential locally, from ADC or host config, so there is nowhere in
this module to put one and nothing here reads one.

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.logging import get_logger

logger = get_logger(__name__)

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
KNOWN_PROVIDERS = ("gcs",)


@dataclass(frozen=True, slots=True)
class AliasBinding:
    """Where one alias may write. Frozen, and it holds no credential.

    ``slots=True`` is doing real work here rather than saving memory: it means an
    operator who pastes a key into the alias file gets it **dropped at load**
    instead of carried on the object, because there is no attribute to hold it.
    T1 says no credential travels on the wire; this is the same rule one step
    further in, where the failure would be a key in a crash dump rather than on
    the bus.
    """

    alias: str
    provider: str
    bucket: str = ""
    prefix: str = ""


@dataclass(frozen=True, slots=True)
class AliasTable:
    """The provisioned set, read once at boot.

    Frozen because there must be no path from the consume loop that adds an
    alias: T2 accepts "any bus writer can name any alias" only because the set of
    resolvable ones is host state a command cannot reach.
    """

    bindings: dict[str, AliasBinding]

    def resolve(self, alias: str) -> AliasBinding | None:
        """The binding for ``alias``, or ``None`` if nobody provisioned it here."""
        return self.bindings.get(alias)

    @property
    def provisioned(self) -> tuple[str, ...]:
        """Every alias this host will accept, sorted — logged once at boot."""
        return tuple(sorted(self.bindings))


def load_alias_table(path: Path | None) -> AliasTable:
    """Read the alias table, failing **closed** at every step.

    Three degrees of failure, and the difference between them is whether the
    structure parsed:

    - **no path, or no file** — nothing is provisioned. Not an error: this is the
      default posture of a host that does not replicate, and the overwhelmingly
      common case while #29 is in progress.
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
    """
    if path is None:
        return AliasTable({})
    try:
        raw = json.loads(Path(path).read_text())
    except FileNotFoundError:
        logger.info("no alias table on this host — replication is not provisioned")
        return AliasTable({})
    except Exception as exc:
        logger.error(
            "alias table is unreadable",
            extra={
                "path": str(path),
                "error": f"{type(exc).__name__}: {exc}",
                "detail": "nothing is provisioned; every replicate command will be refused",
            },
        )
        return AliasTable({})
    if not isinstance(raw, dict):
        logger.error(
            "alias table is unreadable",
            extra={"path": str(path), "detail": "expected an object of alias -> binding"},
        )
        return AliasTable({})

    bindings: dict[str, AliasBinding] = {}
    for alias, entry in raw.items():
        binding = _binding_or_none(str(alias), entry)
        if binding is not None:
            bindings[str(alias)] = binding
    table = AliasTable(bindings)
    logger.info(
        "alias table loaded",
        extra={"path": str(path), "provisioned": list(table.provisioned)},
    )
    return table


def _binding_or_none(alias: str, entry: Any) -> AliasBinding | None:
    """One entry, or ``None`` with a reason in the journal.

    Only the fields ``AliasBinding`` declares are read, so anything else in the
    file — including something an operator mistook for a credential slot — never
    reaches an attribute.
    """
    why = _why_unusable(alias, entry)
    if why is not None:
        logger.warning("ignoring an unusable alias binding", extra={"alias": alias, "detail": why})
        return None
    return AliasBinding(
        alias=alias,
        provider=entry["provider"],
        bucket=str(entry.get("bucket", "")),
        prefix=str(entry.get("prefix", "")).strip("/"),
    )


def _why_unusable(alias: str, entry: Any) -> str | None:
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
    return None
