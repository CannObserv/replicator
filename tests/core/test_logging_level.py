"""REPLICATOR_LOG_LEVEL must actually reach the root logger (CR #1)."""

import logging

import pytest

from src.core.logging import configure_logging


@pytest.fixture(autouse=True)
def _restore_root_logger():
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    yield
    root.handlers, root.level = saved_handlers, saved_level


def test_defaults_to_info():
    configure_logging()
    assert logging.getLogger().level == logging.INFO


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("DEBUG", logging.DEBUG),
        ("debug", logging.DEBUG),
        ("  WARNING  ", logging.WARNING),
        ("CRITICAL", logging.CRITICAL),
    ],
)
def test_accepts_level_names(name, expected):
    configure_logging(name)
    assert logging.getLogger().level == expected


def test_accepts_int_levels():
    configure_logging(logging.ERROR)
    assert logging.getLogger().level == logging.ERROR


def test_unknown_name_falls_back_to_info():
    """A typo must not silence the worker, nor crash it at startup."""
    configure_logging("VERBOSE")
    assert logging.getLogger().level == logging.INFO


def test_non_level_module_attribute_is_rejected():
    """getattr(logging, name) would resolve 'handlers' to a module — reject it."""
    configure_logging("handlers")
    assert logging.getLogger().level == logging.INFO
