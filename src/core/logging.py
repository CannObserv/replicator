"""Structured JSON logging utilities."""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter


def _resolve_level(level: int | str) -> int:
    """Map a level name to its numeric value, falling back to INFO.

    Deliberately not ``getattr(logging, name)``: that resolves any module
    attribute, so ``LOG_LEVEL=handlers`` would return a module object and
    ``setLevel`` would raise at startup. An unrecognized name degrades to INFO
    rather than crashing the worker or silencing it.
    """
    if isinstance(level, int):
        return level
    return logging.getLevelNamesMapping().get(level.strip().upper(), logging.INFO)


def configure_logging(level: int | str = logging.INFO) -> None:
    """Configure root logger with JSON formatting. Call once at entry points.

    Accepts a numeric level or a level name (e.g. ``REPLICATOR_LOG_LEVEL``).
    """
    level = _resolve_level(level)
    handler = logging.StreamHandler(sys.stdout)
    # Keys must be named in fmt: a bare JsonFormatter() defaults to
    # "%(message)s" and emits records with no level, logger, or timestamp.
    handler.setFormatter(
        JsonFormatter(
            "%(levelname)s %(name)s %(message)s",
            timestamp=True,
            rename_fields={"levelname": "level", "name": "logger"},
        )
    )
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Use in modules as: logger = get_logger(__name__)"""
    return logging.getLogger(name)
