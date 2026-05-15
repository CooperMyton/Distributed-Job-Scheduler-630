# coordinator/monitor.py
# Background thread that runs inside the coordinator process and continuously
# scans for workers that have stopped sending heartbeats.
#
# This is the implementation of the failure detector described in the
# Chandra-Toueg model. Our detector is "eventually perfect" — it may
# temporarily suspect a live worker (false positive under network delay)
# but will eventually correctly identify all dead workers.
#
# The key timing relationship that determines detector quality:
#
#   detection_latency ≈ WORKER_TIMEOUT_SEC ± MONITOR_POLL_SEC
#
# This means:
#   - Minimum detection time: WORKER_TIMEOUT_SEC
#   - Maximum detection time: WORKER_TIMEOUT_SEC + MONITOR_POLL_SEC
#   - False positive risk: decreases as WORKER_TIMEOUT increases
#
# RESEARCH NOTE (Experiment 1): This tradeoff is what you are measuring.
# A tight timeout detects failures quickly but risks false positives.
# A loose timeout is conservative but leaves jobs stranded longer.

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone

from shared.config import MONITOR_POLL_SEC, WORKER_TIMEOUT_SEC, MAX_JOB_RETRIES
from shared.models import store
from metrics.collector import emit

logger = logging.getLogger(__name__)


class MonitorThread(threading.Thread):
    """
    Daemon thread that periodically checks all ACTIVE workers for heartbeat
    timeouts and triggers hot takeover for any orphaned jobs.

    Lifecycle:
        monitor = MonitorThread()
        monitor.start()   # called in coordinator/main.py on startup
        monitor.stop()    # called on coordinator shutdown

    The thread is a daemon so it dies automatically if the main process exits.
    """

    def __init__(
        self,
        poll_interval:  float = MONITOR_POLL_SEC,
        worker_timeout: float = WORKER_TIMEOUT_SEC,
    ):
        super().__init__(name="MonitorThread", daemon=True)
        self.poll_interval  = poll_interval
        self.worker_timeout = worker_timeout
        self._stop_event    = threading.Event()

    def stop(self) -> None:
        """Signal the thread to exit on its next wake cycle."""
        self._stop_event.set()

    def run(self) -> None:
        logger.info(
            "Monitor started: poll=%.1fs timeout=%.1fs",
            self.poll_interval, self.worker_timeout
        )
        while not self._stop_event.is_set():
            try:
                self._scan()
            except Exception as e:
                # Never let a scan error kill the monitor thread
                logger.error("Monitor scan error: %s", e, exc_info=True)
            self._stop_event.wait(timeout=self.poll_interval)
        logger.info("Monitor stopped.")

    def _scan(self) -> None:
        """
        One scan cycle:
        1. Get all ACTIVE workers
        2. Compute seconds since last heartbeat for each
        3. If > worker_timeout: declare dead, trigger takeover for any held job
        """
        now     = datetime.now(timezone.utc)
        workers = store.get_active_workers()

        for worker in workers:
            # Make last_heartbeat timezone-aware for comparison
            last_hb = worker.last_heartbeat
            if last_hb.tzinfo is None:
                last_hb = last_hb.replace(tzinfo=timezone.utc)

            silence = (now - last_hb).total_seconds()

            if silence <= self.worker_timeout:
                continue  # Worker is healthy

            # ── Worker timed out ──────────────────────────────────────────────
            logger.warning(
                "Worker TIMEOUT detected: worker=%s silence=%.1fs threshold=%.1fs",
                worker.id, silence, self.worker_timeout
            )

            orphaned_job_id = store.mark_worker_dead(worker.id)

            emit({
                "event":                "worker_died",
                "worker_id":            worker.id,
                "silence_sec":          round(silence, 3),
                "detection_latency_sec": round(silence, 3),
                "had_job":              orphaned_job_id is not None,
                "orphaned_job_id":      orphaned_job_id,
            })

            if orphaned_job_id is None:
                # Worker died while idle — no job to recover
                logger.info("Dead worker %s had no active job.", worker.id)
                continue

            # ── Trigger hot takeover ──────────────────────────────────────────
            new_status = store.mark_job_takeover(
                job_id            = orphaned_job_id,
                detection_latency = round(silence, 3),
                max_retries       = MAX_JOB_RETRIES,
            )

            from shared.models import JobStatus
            job = store.get_job(orphaned_job_id)

            if new_status == JobStatus.PENDING_TAKEOVER:
                checkpoint_step = (
                    job["latest_checkpoint"]["step_index"]
                    if job and job.get("latest_checkpoint")
                    else None
                )
                logger.warning(
                    "Hot takeover queued: job=%s checkpoint_step=%s retry=%s",
                    orphaned_job_id,
                    checkpoint_step if checkpoint_step is not None else "none (full restart)",
                    job["retry_count"] if job else "?",
                )
                emit({
                    "event":            "takeover_queued",
                    "job_id":           orphaned_job_id,
                    "failed_worker":    worker.id,
                    "retry_count":      job["retry_count"] if job else None,
                    "checkpoint_step":  checkpoint_step,
                    "steps_redone":     job.get("steps_redone") if job else None,
                    "detection_latency_sec": round(silence, 3),
                })

            elif new_status == JobStatus.FAILED:
                logger.error(
                    "Job permanently failed after exhausting retries: job=%s",
                    orphaned_job_id
                )
                emit({
                    "event":   "job_permanently_failed",
                    "job_id":  orphaned_job_id,
                    "reason":  "retries_exhausted",
                    "retry_count": job["retry_count"] if job else None,
                })