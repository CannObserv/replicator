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


class ColorMessageFilter(logging.Filter):
    """Drop uvicorn's ``color_message`` extra before anything serializes it.

    uvicorn logs its lifecycle lines with an ANSI-coloured duplicate of the
    message attached as ``extra={"color_message": ...}``, for its own colour-
    aware default formatter. Every extra reaches the JSON payload, so without
    this the records carry a second copy of the message full of escape
    sequences — the one thing structured logging exists to avoid.

    A **filter**, not the formatter's ``reserved_attrs``, and on the **loggers**
    rather than the handler: both choices put the strip at the record's source,
    before any handler reads it. A handler that builds its payload from the
    record's ``__dict__`` instead of a ``logging.Formatter`` — which is exactly
    what OpenTelemetry's ``LoggingHandler`` does, against a reserved list that
    does not cover ``color_message`` — would otherwise resurrect the field the
    day the sink changes, silently and with no failing test. Mutating the
    record once keeps the fix independent of what consumes it.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Strip the extra if present. Never drops a record."""
        if hasattr(record, "color_message"):
            del record.color_message
        return True


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
