"""FastAPI application entry point.

Replicator is worker-first: the primary process is the bus consumer in
``src/worker/main.py``, not this app. This surface exists for liveness checks and
a future status endpoint; it is not part of the MVP command -> fact loop.
"""

from contextlib import asynccontextmanager
from importlib.metadata import version as pkg_version

from fastapi import APIRouter, FastAPI

from src.core.config import get_settings
from src.core.logging import configure_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """One-time setup on startup, teardown on shutdown."""
    configure_logging(get_settings().log_level)
    logger.info("application starting")
    yield
    logger.info("application stopping")


# version from package metadata — a hardcoded literal here drifts from
# pyproject.toml (power-map's /openapi.json reported 0.1.0 at project v0.15.0).
app = FastAPI(title="replicator", version=pkg_version("replicator"), lifespan=lifespan)

health_router = APIRouter(tags=["health"])


@health_router.get("/health")
async def health() -> dict:
    """Liveness probe — confirms the app process is running. No external calls."""
    return {"status": "ok", "build": get_settings().build_id}


app.include_router(health_router)
