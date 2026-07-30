"""App startup must actually run — under test as well as in production (CR #13).

`httpx.ASGITransport` does not run an app's lifespan, so an AsyncClient built
straight over it exercises routes against an app that never started. These tests
pin both halves: the lifespan itself, and the fact that the shared `client`
fixture drives it.
"""

import logging

import pytest
from pythonjsonlogger.json import JsonFormatter

from src.api.main import app, lifespan


@pytest.fixture(autouse=True)
def _restore_root_logger():
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    yield
    root.handlers, root.level = saved_handlers, saved_level


async def test_client_fixture_runs_startup(client):
    """Guards the gap itself: a client that skips startup proves nothing.

    Assert *presence*, not exclusivity — pytest re-attaches its own
    LogCaptureHandlers for the call phase, so `configure_logging`'s
    `root.handlers = [handler]` is no longer the whole list by the time a test
    body runs.
    """
    root = logging.getLogger()
    assert any(isinstance(h.formatter, JsonFormatter) for h in root.handlers), (
        "startup did not install the JSON handler"
    )
    # WARNING is pytest's default; INFO means configure_logging ran.
    assert root.level == logging.INFO


async def test_lifespan_applies_the_configured_log_level(monkeypatch):
    monkeypatch.setenv("REPLICATOR_LOG_LEVEL", "DEBUG")

    async with lifespan(app):
        assert logging.getLogger().level == logging.DEBUG


async def test_lifespan_defaults_to_info(monkeypatch):
    monkeypatch.delenv("REPLICATOR_LOG_LEVEL", raising=False)

    async with lifespan(app):
        assert logging.getLogger().level == logging.INFO


async def test_lifespan_logs_start_and_stop(capsys):
    async with lifespan(app):
        pass

    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    assert any("application starting" in ln for ln in lines)
    assert any("application stopping" in ln for ln in lines)
