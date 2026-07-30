"""Structured JSON logging utilities."""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger with JSON formatting. Call once at entry points."""
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
