"""Regression tests: JSON log records carry timestamp, level, and logger name,
and uvicorn's own loggers share the app's JSON formatter (skills#69, skills#81).
"""

import json
import logging
import logging.config
from pathlib import Path

from src.core.logging import build_json_formatter, configure_logging, get_logger

# Resolved from this file, not the working directory: the suite must pin the
# shipped config wherever pytest happens to be invoked from.
REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_CONFIG_PATH = REPO_ROOT / "src" / "core" / "log_config.json"

# Every file that tells a human or a machine how to launch uvicorn. deploy/ carries
# no uvicorn invocation today (the unit runs the worker) and so passes vacuously —
# it is listed for the day the API is promoted to a deployed surface.
UVICORN_COMMAND_SOURCES = (
    "README.md",
    "AGENTS.md",
    "docs/COMMANDS.md",
    "deploy/replicator.service",
)
UVICORN_INVOCATION = "uvicorn src.api.main:app"
LOG_CONFIG_FLAG = "--log-config src/core/log_config.json"


def test_log_record_includes_structured_fields(capsys):
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    try:
        configure_logging()
        get_logger("src.some.module").warning("hello %s", "world")
    finally:
        root.handlers, root.level = saved_handlers, saved_level

    record = json.loads(capsys.readouterr().out)
    assert record["message"] == "hello world"
    assert record["level"] == "WARNING"
    assert record["logger"] == "src.some.module"
    assert "timestamp" in record


def test_uvicorn_log_config_is_valid_and_shares_formatter():
    """The uvicorn --log-config file wires uvicorn's loggers through the same
    formatter as the app, and dictConfig accepts it (a malformed file would
    fail the dev server at boot, not in review)."""
    config = json.loads(LOG_CONFIG_PATH.read_text())

    # Single source of truth: the file builds its formatter from the factory
    # configure_logging() also uses, not a duplicated fmt string.
    assert any(
        f.get("()") == "src.core.logging.build_json_formatter"
        for f in config["formatters"].values()
    )
    # All three uvicorn loggers must be present, else they keep the plain default.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        assert name in config["loggers"]

    names = ("", "uvicorn", "uvicorn.error", "uvicorn.access")
    saved = {
        n: (
            logging.getLogger(n).handlers[:],
            logging.getLogger(n).propagate,
            logging.getLogger(n).level,
        )
        for n in names
    }
    try:
        logging.config.dictConfig(config)  # raises on a malformed config
    finally:
        # Restore level too: dictConfig sets root + uvicorn loggers to INFO,
        # and leaking that into later tests would be an order-dependent flake.
        for n, (handlers, propagate, level) in saved.items():
            lg = logging.getLogger(n)
            lg.handlers, lg.propagate, lg.level = handlers, propagate, level


def test_documented_uvicorn_commands_pass_log_config():
    """Every documented uvicorn invocation carries --log-config.

    The tests above pin the config file's *content*; this pins its *delivery*.
    A command copy-pasted from before #14 reinstates the mixed-format output
    the file exists to prevent, and every other test in this module still
    passes — the docs are the only thing that actually launches the server.

    Single-line invocations only: no documented command uses a backslash
    continuation, and a wrapped one would need this scan to join lines first.
    """
    offenders = []
    for source in UVICORN_COMMAND_SOURCES:
        path = REPO_ROOT / source
        if not path.exists():  # deploy/ may be pruned in a slim checkout
            continue
        for lineno, line in enumerate(path.read_text().splitlines(), start=1):
            if UVICORN_INVOCATION in line and LOG_CONFIG_FLAG not in line:
                offenders.append(f"{source}:{lineno}: {line.strip()}")

    assert not offenders, (
        "uvicorn invocation without " + LOG_CONFIG_FLAG + ":\n" + "\n".join(offenders)
    )


def test_shared_formatter_renders_uvicorn_access_record():
    """A uvicorn.access record formats to JSON with the same fields as app logs
    — the request line lands in `message`, not a plain-text handler."""
    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='%s - "%s %s HTTP/%s" %d',
        args=("127.0.0.1:0", "GET", "/health", "1.1", 200),
        exc_info=None,
    )
    parsed = json.loads(build_json_formatter().format(record))
    assert parsed["logger"] == "uvicorn.access"
    assert parsed["level"] == "INFO"
    assert parsed["message"] == '127.0.0.1:0 - "GET /health HTTP/1.1" 200'
    assert "timestamp" in parsed
