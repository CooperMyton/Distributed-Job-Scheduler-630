# worker/worker.py
# Main worker process. Runs as a standalone script — one process per worker.
#
# Lifecycle:
#   1. Parse CLI args (worker_id, coordinator URL overrides)
#   2. Register with coordinator, get assigned a worker_id
#   3. Start heartbeat thread (background, every HEARTBEAT_INTERVAL_SEC)
#   4. Main loop: poll for job → execute → report result → repeat
#
# On coordinator unavailability:
#   HTTP calls retry with exponential backoff up to WORKER_HTTP_RETRIES times.
#   If the coordinator is unreachable after all retries the worker logs the
#   error and continues polling — it never crashes on a network blip.
#
# On task execution error:
#   The worker catches the exception, reports it to the coordinator via
#   POST /workers/{id}/fail, and goes back to polling.
#
# Run as:
#   python -m worker.worker
#   python -m worker.worker --worker-id my-worker-1
#   python -m worker.worker --coordinator http://127.0.0.1:8000

from __future__ import annotations

import argparse
import logging
import sys
import time
import threading
import uuid
from typing import Optional

import httpx

# Add project root to path when running as __main__
if __name__ == "__main__":
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.config import (
    COORDINATOR_URL,
    HEARTBEAT_INTERVAL_SEC,
    WORKER_POLL_SEC,
    WORKER_HTTP_RETRIES,
    WORKER_HTTP_BACKOFF_SEC,
    CHECKPOINT_EVERY_N_STEPS,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
    LOG_LEVEL,
)
from worker.executor import TaskRunner, instantiate_task
import worker.tasks  # noqa: F401 — triggers @register_task decorators

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
)
logger = logging.getLogger(__name__)


# ── HTTP helper ────────────────────────────────────────────────────────────────

def http_post(url: str, data: dict, retries: int = WORKER_HTTP_RETRIES,
              backoff: float = WORKER_HTTP_BACKOFF_SEC) -> Optional[dict]:
    """
    POST JSON to url with exponential backoff retry.
    Returns parsed response dict on success, None on all retries exhausted.
    """
    for attempt in range(retries + 1):
        try:
            r = httpx.post(url, json=data, timeout=5.0)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries:
                wait = backoff * (2 ** attempt)
                logger.warning("POST %s failed (attempt %d/%d): %s — retrying in %.1fs",
                               url, attempt + 1, retries + 1, e, wait)
                time.sleep(wait)
            else:
                logger.error("POST %s failed after %d attempts: %s", url, retries + 1, e)
    return None


def http_get(url: str, retries: int = WORKER_HTTP_RETRIES,
             backoff: float = WORKER_HTTP_BACKOFF_SEC) -> Optional[dict]:
    """GET url with exponential backoff retry."""
    for attempt in range(retries + 1):
        try:
            r = httpx.get(url, timeout=5.0)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < retries:
                wait = backoff * (2 ** attempt)
                logger.warning("GET %s failed (attempt %d/%d): %s — retrying in %.1fs",
                               url, attempt + 1, retries + 1, e, wait)
                time.sleep(wait)
            else:
                logger.error("GET %s failed after %d attempts: %s", url, retries + 1, e)
    return None


# ── Heartbeat thread ───────────────────────────────────────────────────────────

class HeartbeatThread(threading.Thread):
    """
    Sends a heartbeat POST to the coordinator every HEARTBEAT_INTERVAL_SEC.

    If the coordinator returns acknowledged=False the worker needs to
    re-register (coordinator may have restarted and lost state).
    The re-registration flag is set here; the main loop handles it.
    """

    def __init__(self, worker_id: str, coordinator_url: str):
        super().__init__(name=f"Heartbeat-{worker_id[:8]}", daemon=True)
        self.worker_id       = worker_id
        self.coordinator_url = coordinator_url
        self.needs_reregister = False
        self._stop_event     = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        logger.info("Heartbeat thread started (every %.1fs)", HEARTBEAT_INTERVAL_SEC)
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=HEARTBEAT_INTERVAL_SEC)
            if self._stop_event.is_set():
                break
            try:
                r = httpx.post(
                    f"{self.coordinator_url}/workers/{self.worker_id}/heartbeat",
                    timeout=3.0
                )
                data = r.json()
                if not data.get("acknowledged"):
                    logger.warning("Heartbeat not acknowledged — need to re-register")
                    self.needs_reregister = True
                else:
                    logger.debug("Heartbeat OK")
            except Exception as e:
                logger.warning("Heartbeat failed: %s", e)


# ── Worker ─────────────────────────────────────────────────────────────────────

class Worker:
    """
    Encapsulates the full lifecycle of a single worker process.

    The main loop is intentionally simple:
        while True:
            poll for job
            if job: execute it
            else: sleep and try again
    """

    def __init__(self, worker_id: Optional[str], coordinator_url: str):
        self.worker_id       = worker_id or str(uuid.uuid4())
        self.coordinator_url = coordinator_url
        self.heartbeat:      Optional[HeartbeatThread] = None

    # ── Registration ───────────────────────────────────────────────────────────

    def register(self) -> bool:
        """
        Register with the coordinator. Returns True on success.
        Safe to call on reconnection — coordinator handles idempotent re-registration.
        """
        logger.info("Registering with coordinator at %s ...", self.coordinator_url)
        result = http_post(
            f"{self.coordinator_url}/workers/register",
            {"worker_id": self.worker_id}
        )
        if result is None:
            logger.error("Registration failed — coordinator unreachable.")
            return False
        self.worker_id = result["worker_id"]
        logger.info("Registered as worker_id=%s", self.worker_id)
        return True

    # ── Checkpoint callback ────────────────────────────────────────────────────

    def _push_checkpoint(self, job_id: str, step_index: int, state: dict) -> None:
        """
        Called by TaskRunner at every checkpoint interval.
        Runs in the main thread (blocking) — the coordinator is fast enough
        that this doesn't meaningfully delay execution.
        """
        result = http_post(
            f"{self.coordinator_url}/workers/{self.worker_id}/checkpoint",
            {
                "job_id":     job_id,
                "step_index": step_index,
                "state":      state,
            }
        )
        if result is None:
            logger.warning("Checkpoint push failed for job %s step %d", job_id, step_index)
        elif result.get("accepted"):
            logger.debug("Checkpoint accepted: job=%s step=%d seq=%d size=%dB",
                         job_id, step_index,
                         result.get("sequence", -1),
                         result.get("size_bytes", 0))
        else:
            # Coordinator rejected it (we may have been declared dead)
            logger.warning(
                "Checkpoint rejected by coordinator: job=%s — %s",
                job_id, result.get("message")
            )

    # ── Job execution ──────────────────────────────────────────────────────────

    def _execute_job(self, job: dict) -> None:
        """
        Execute a single job dict received from the coordinator.

        Handles both fresh assignments and takeover assignments:
        - Fresh:    latest_checkpoint is None, start from empty state
        - Takeover: latest_checkpoint contains P1's last state, resume from there
        """
        job_id      = job["id"]
        payload     = job["payload"]
        task_type   = payload.get("task_type", "unknown")
        checkpoint  = job.get("latest_checkpoint")
        is_takeover = checkpoint is not None

        if is_takeover:
            initial_state = checkpoint["state"]
            initial_step  = checkpoint["step_index"]
            logger.info(
                "TAKEOVER job=%s type=%s resuming from step=%d",
                job_id, task_type, initial_step
            )
        else:
            initial_state = None
            initial_step  = 0
            logger.info("Starting job=%s type=%s", job_id, task_type)

        # Instantiate the task from the payload
        try:
            task = instantiate_task(payload)
        except KeyError as e:
            logger.error("Unknown task type in payload: %s", e)
            http_post(
                f"{self.coordinator_url}/workers/{self.worker_id}/fail",
                {"job_id": job_id, "error": f"Unknown task type: {e}"}
            )
            return

        # Build the runner with checkpoint callback wired in
        runner = TaskRunner(
            task             = task,
            job_id           = job_id,
            initial_state    = initial_state,
            initial_step     = initial_step,
            checkpoint_every = CHECKPOINT_EVERY_N_STEPS,
            on_checkpoint    = self._push_checkpoint,
        )

        # Run the task — this blocks until completion or exception
        try:
            result = runner.run()
        except Exception as e:
            logger.error("Task raised exception: job=%s error=%s", job_id, e, exc_info=True)
            http_post(
                f"{self.coordinator_url}/workers/{self.worker_id}/fail",
                {"job_id": job_id, "error": str(e)}
            )
            return

        # Submit the result
        submitted = http_post(
            f"{self.coordinator_url}/workers/{self.worker_id}/complete",
            {
                "job_id":      job_id,
                "result":      result,
                "total_steps": runner.step_index,
            }
        )

        if submitted and submitted.get("accepted"):
            logger.info(
                "Job completed: job=%s steps=%d time=%.2fs ckpt_overhead=%.1f%%",
                job_id,
                runner.step_index,
                runner.total_time,
                runner.checkpoint_overhead_fraction * 100,
            )
        else:
            # Coordinator rejected result — we were declared dead mid-run
            logger.warning(
                "Result rejected by coordinator: job=%s "
                "(we were declared dead while executing — job will be retried)",
                job_id
            )

    # ── Main loop ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Main worker loop. Runs forever until KeyboardInterrupt (Ctrl+C).

        Loop:
            1. Re-register if needed (coordinator restart)
            2. Poll for a job
            3. If job available: execute it (blocking)
            4. If queue empty: sleep WORKER_POLL_SEC and try again
        """
        # Initial registration
        if not self.register():
            logger.error("Could not register. Is the coordinator running? Exiting.")
            sys.exit(1)

        # Start heartbeat thread
        self.heartbeat = HeartbeatThread(self.worker_id, self.coordinator_url)
        self.heartbeat.start()

        logger.info("Worker ready. Polling for jobs every %.1fs ...", WORKER_POLL_SEC)

        try:
            while True:
                # Handle coordinator restart — re-register if heartbeat was rejected
                if self.heartbeat.needs_reregister:
                    logger.info("Re-registering with coordinator...")
                    self.register()
                    self.heartbeat.needs_reregister = False

                # Poll for next job
                response = http_get(
                    f"{self.coordinator_url}/workers/{self.worker_id}/job"
                )

                if response is None:
                    # Coordinator unreachable — wait and retry
                    time.sleep(WORKER_POLL_SEC)
                    continue

                job = response.get("job")

                if job is None:
                    # Queue empty — idle wait
                    logger.debug("Queue empty, waiting...")
                    time.sleep(WORKER_POLL_SEC)
                    continue

                # Execute the job (blocks until done or exception)
                self._execute_job(job)

        except KeyboardInterrupt:
            logger.info("Worker %s shutting down (Ctrl+C).", self.worker_id)
        finally:
            if self.heartbeat:
                self.heartbeat.stop()


# ── CLI entrypoint ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hot Takeover Worker")
    parser.add_argument(
        "--worker-id",
        default=None,
        help="Stable worker ID (optional). Auto-generated UUID if not provided."
    )
    parser.add_argument(
        "--coordinator",
        default=COORDINATOR_URL,
        help=f"Coordinator base URL (default: {COORDINATOR_URL})"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    worker = Worker(
        worker_id       = args.worker_id,
        coordinator_url = args.coordinator,
    )
    worker.run()