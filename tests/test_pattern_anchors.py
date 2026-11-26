"""Every compiled pattern in ``src/`` anchors on the absolute end of the string (#243).

Python's ``$`` also matches immediately *before* a trailing newline, so a
validator written ``^…$`` silently accepts ``"accept\\n"``. That's how CR #14 got
in: httpx puts such a header name on the wire with a bare LF inside it. #232 was
the same trap in another module.

This used to walk only ``src.worker.handler``, and saw the header patterns only
because that module imports them. So ``aliases.py``'s ``.match`` on a ``$``
pattern (#232) went unseen. It now walks every module under ``src/`` and keeps a
pattern whatever module defines it. #232's pattern and co-core's
``FINGERPRINT_RE`` are both imported, so a walk limited to local definitions
would miss them again.

The subjects are discovered, not listed (CR #15). A new validator is covered on
the next run. It then fails until it's enrolled in ``VALID`` (with a value it
accepts) or in ``EXEMPT`` (with a reason).
"""

import importlib
import inspect
import pkgutil
import re
from pathlib import Path

import pytest

import src

# ``\z`` won't compile until Python 3.14 (3.12 calls it a bad escape). It's
# accepted here so the check still holds after an upgrade. ``(?![\s\S])`` is
# co-core's spelling since cannobserv#763.
END_ANCHORS = ("\\Z", "\\z", "(?![\\s\\S])")

# One value each validator accepts. The probe appends ``\n`` and expects a refusal.
VALID = {
    "HEADER_NAME": "accept",
    "HEADER_VALUE": "text/html",
    "_ALIAS_NAME": "gcs-reader",
}

# Patterns that are deliberately not end-anchored, each with its reason.
EXEMPT = {
    "_NET_ERROR": (
        "an extractor, not a validator: browser.py calls .search to pull the "
        "net::ERR_* code out of a Playwright error message"
    ),
    "FINGERPRINT_RE": (
        "co-core's ^…$ pattern (cannobserv#763 changed only the alias patterns). "
        "Its one call site, replicate.fingerprint_in, uses fullmatch, pinned by "
        "test_the_fingerprint_check_needs_no_help_from_urlsplit"
    ),
}


def _raise(name: str) -> None:
    """``walk_packages`` drops a package that won't import, and its children with it."""
    raise ImportError(f"{name} failed to import during the pattern walk")


def walked_modules() -> list[str]:
    return [
        info.name
        for info in pkgutil.walk_packages(src.__path__, f"{src.__name__}.", onerror=_raise)
    ]


def discover() -> dict[str, re.Pattern[str]]:
    """Every module-level ``re.Pattern`` under ``src/``, one entry per pattern object.

    Keyed by identity first (the brief's dedupe). ``HEADER_NAME`` is bound in
    ``headers``, ``handler`` and ``credentials``, and it's still one pattern, so
    it gets one test. The key is the sorted set of names it's bound to, which is
    one name unless an import renamed it.
    """
    found: dict[int, tuple[re.Pattern[str], set[str]]] = {}
    for module_name in walked_modules():
        module = importlib.import_module(module_name)
        for name, value in inspect.getmembers(module, lambda v: isinstance(v, re.Pattern)):
            found.setdefault(id(value), (value, set()))[1].add(name)
    return {"/".join(sorted(names)): pattern for pattern, names in found.values()}


SUBJECTS = discover()


@pytest.mark.parametrize("name", sorted(SUBJECTS.keys() - EXEMPT.keys()))
def test_every_validator_anchors_on_the_absolute_end_of_the_string(name):
    pattern = SUBJECTS[name]
    assert name in VALID, f"{name} is new: enrol it in VALID with an accepted value, or in EXEMPT"
    valid = VALID[name]

    assert pattern.pattern.endswith(END_ANCHORS), f"{name} doesn't end on an absolute anchor"
    # The text check alone passes ``\\\\Z`` (an escaped backslash, then Z), so the
    # behaviour is probed as well. Without the first assert, a pattern that accepts
    # nothing would also pass.
    assert pattern.match(valid), f"{name} refuses its own VALID value {valid!r}"
    assert pattern.match(valid + "\n") is None, f"{name} accepts a trailing newline"


def test_every_enrolment_names_a_discovered_pattern():
    """The other direction: an entry for a renamed or deleted pattern is stale."""
    assert not VALID.keys() & EXEMPT.keys()
    assert sorted(VALID.keys() | EXEMPT.keys()) == sorted(SUBJECTS)


def test_the_anchoring_check_has_subjects():
    """A reflection-driven parametrize that finds nothing passes silently.

    A rename or a move that empties the walk would take the coverage with it
    while every test above stays green. Five at #243: three probed, two exempt.
    """
    assert len(SUBJECTS) >= 5


def test_the_walk_reaches_every_module_under_src():
    """``walk_packages`` skips a directory that has no ``__init__.py``.

    The patterns in such a directory would go unchecked, and nothing would say
    so. This compares the walk with the files on disk.
    """
    root = Path(src.__file__).parent
    on_disk = {
        ".".join(("src", *path.relative_to(root).with_suffix("").parts)).removesuffix(".__init__")
        for path in root.rglob("*.py")
    } - {"src"}

    assert sorted(walked_modules()) == sorted(on_disk)
