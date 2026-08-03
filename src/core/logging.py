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


def build_json_formatter() -> JsonFormatter:
    """The single JSON formatter definition for the whole process.

    Referenced by BOTH ``configure_logging()`` (the worker and any other
    non-uvicorn entry point) and ``src/core/log_config.json`` (uvicorn's
    ``--log-config``, via the dictConfig ``"()"`` factory key), so app records
    and uvicorn's own access/error lines serialize with one identical schema —
    no drift, one place to change.

    Keys must be named in fmt: a bare JsonFormatter() defaults to
    "%(message)s" and emits records with no level, logger, or timestamp.
    """
    return JsonFormatter(
        "%(levelname)s %(name)s %(message)s",
        timestamp=True,
        rename_fields={"levelname": "level", "name": "logger"},
    )


def configure_logging(level: int | str = logging.INFO) -> None:
    """Configure root logger with JSON formatting. Call once at entry points.

    Accepts a numeric level or a level name (e.g. ``REPLICATOR_LOG_LEVEL``).

    This is the whole story for the worker, which runs no uvicorn. Under the
    dev server, ``--log-config src/core/log_config.json`` configures the
    logging tree at boot and this call then reinstalls an identical root
    handler — harmless, and it keeps app logs JSON (at the configured level)
    even if someone launches uvicorn without ``--log-config``.
    """
    level = _resolve_level(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(build_json_formatter())
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Use in modules as: logger = get_logger(__name__)"""
    return logging.getLogger(name)
