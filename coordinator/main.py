# coordinator/main.py
# FastAPI application entrypoint for the Hot Takeover coordinator.
#
# Startup sequence:
#   1. Configure logging
#   2. Create the FastAPI app and include API routes
#   3. On first request (lifespan startup): start the MonitorThread
#   4. On shutdown (lifespan shutdown): stop the MonitorThread cleanly
#
# Run with:
#   uvicorn coordinator.main:app --host 127.0.0.1 --port 8000 --reload
#
# The --reload flag is useful during development but should be omitted
# when running experiments (it restarts the process on file changes,
# which resets all in-memory state).

from __future__ import annotations

import logging
import logging.config
from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared.config import (
    COORDINATOR_HOST,
    COORDINATOR_PORT,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    LOG_LEVEL,
    WORKER_TIMEOUT_SEC,
    MONITOR_POLL_SEC,
    CHECKPOINT_EVERY_N_STEPS,
    MAX_JOB_RETRIES,
)
from coordinator.api import router
from coordinator.monitor import MonitorThread

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
)
logger = logging.getLogger(__name__)


# ── Monitor lifecycle ──────────────────────────────────────────────────────────
# The monitor thread is created once and stored here so the shutdown handler
# can stop it cleanly.
_monitor: MonitorThread | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager.
    Code before 'yield' runs at startup; code after runs at shutdown.
    """
    global _monitor

    logger.info("=" * 60)
    logger.info("Hot Takeover Coordinator starting up")
    logger.info("  worker_timeout   : %.1fs", WORKER_TIMEOUT_SEC)
    logger.info("  monitor_poll     : %.1fs", MONITOR_POLL_SEC)
    logger.info("  checkpoint_every : %d steps", CHECKPOINT_EVERY_N_STEPS)
    logger.info("  max_retries      : %d", MAX_JOB_RETRIES)
    logger.info("=" * 60)

    _monitor = MonitorThread()
    _monitor.start()
    logger.info("Monitor thread started.")

    yield  # Application runs here

    logger.info("Coordinator shutting down...")
    if _monitor:
        _monitor.stop()
        _monitor.join(timeout=5.0)
        logger.info("Monitor thread stopped.")


# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Hot Takeover Coordinator",
    description=(
        "Distributed job scheduler with application-level checkpoint-based "
        "worker recovery. Workers can fail mid-task and a second worker will "
        "resume from the last checkpoint rather than restarting from scratch."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(router)


# ── Root health check ──────────────────────────────────────────────────────────
@app.get("/", tags=["Health"])
def health():
    """Quick liveness check. Returns coordinator status and config summary."""
    from shared.models import store
    return {
        "status":   "ok",
        "service":  "hot-takeover-coordinator",
        "config": {
            "worker_timeout_sec":    WORKER_TIMEOUT_SEC,
            "monitor_poll_sec":      MONITOR_POLL_SEC,
            "checkpoint_every_steps": CHECKPOINT_EVERY_N_STEPS,
            "max_job_retries":       MAX_JOB_RETRIES,
        },
        "stats": store.stats(),
    }


# ── Dev entrypoint ─────────────────────────────────────────────────────────────
# Allows running directly with: python -m coordinator.main
# Not used in production (use uvicorn command instead).
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "coordinator.main:app",
        host=COORDINATOR_HOST,
        port=COORDINATOR_PORT,
        log_level=LOG_LEVEL.lower(),
    )