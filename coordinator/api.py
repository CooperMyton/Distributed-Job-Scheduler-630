# coordinator/api.py
# FastAPI route handlers for the Hot Takeover coordinator.
#
# All routes are stateless with respect to HTTP — they read and write
# exclusively through the shared store and checkpoint_store singletons.
# This means every handler is safe to call from concurrent request threads.
#
# Route map:
#   POST /jobs                          Submit a new job
#   GET  /jobs                          List all jobs
#   GET  /jobs/{job_id}                 Get one job's status and result
#
#   POST /workers/register              Worker registers on startup
#   POST /workers/{worker_id}/heartbeat Worker sends heartbeat
#   GET  /workers/{worker_id}/job       Worker polls for next job (with checkpoint)
#   POST /workers/{worker_id}/checkpoint Worker pushes checkpoint blob
#   POST /workers/{worker_id}/complete  Worker submits final result
#   POST /workers/{worker_id}/fail      Worker reports execution error
#
#   GET  /workers                       List all workers and their status
#   GET  /metrics                       System-wide stats snapshot

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from shared.config import MAX_JOB_RETRIES, CHECKPOINT_WARN_SIZE_BYTES
from shared.models import JobStatus, store
from coordinator.checkpoint_store import checkpoint_store
from metrics.collector import emit

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Request / Response Schemas ─────────────────────────────────────────────────
# Pydantic models provide automatic validation and OpenAPI documentation.

class JobSubmitRequest(BaseModel):
    payload: dict = Field(
        ...,
        description="Must include 'task_type'. Additional fields are task-specific.",
        example={"task_type": "CountingTask", "target": 100, "step_sleep": 0.05}
    )

class JobSubmitResponse(BaseModel):
    job_id:  str
    status:  str
    message: str

class WorkerRegisterRequest(BaseModel):
    worker_id: Optional[str] = Field(
        None,
        description="Optional: supply a stable ID for re-registration after crash. "
                    "If omitted, coordinator assigns a UUID."
    )

class WorkerRegisterResponse(BaseModel):
    worker_id: str
    message:   str

class HeartbeatRequest(BaseModel):
    worker_id: str

class HeartbeatResponse(BaseModel):
    acknowledged: bool
    message:      str

class CheckpointRequest(BaseModel):
    job_id:     str
    step_index: int  = Field(..., ge=0)
    state:      dict = Field(..., description="Full serializable task state at this step")

class CheckpointResponse(BaseModel):
    accepted:   bool
    sequence:   Optional[int]
    size_bytes: Optional[int]
    message:    str

class CompleteRequest(BaseModel):
    job_id:      str
    result:      dict
    total_steps: int = Field(..., ge=0)

class CompleteResponse(BaseModel):
    accepted: bool
    message:  str

class FailRequest(BaseModel):
    job_id: str
    error:  str

class FailResponse(BaseModel):
    job_status: str
    retry_count: int
    message:    str


# ── Job Routes ─────────────────────────────────────────────────────────────────

@router.post(
    "/jobs",
    response_model=JobSubmitResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new job",
    tags=["Jobs"],
)
def submit_job(request: JobSubmitRequest):
    """
    Accept a job payload from a client and enqueue it.

    The payload must include 'task_type' matching a registered CheckpointableTask.
    Example payloads:
        {"task_type": "CountingTask",   "target": 200}
        {"task_type": "FibonacciTask",  "n": 50}
        {"task_type": "PrimeSieveTask", "limit": 1000}
        {"task_type": "SleepTask",      "total_seconds": 30, "step_sec": 1.0}
    """
    if "task_type" not in request.payload:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="payload must include 'task_type'"
        )

    job = store.enqueue_job(request.payload)

    emit({
        "event":        "job_submitted",
        "job_id":       job.id,
        "task_type":    request.payload.get("task_type"),
        "queue_depth":  store.queue_depth(),
    })

    logger.info("Job submitted: id=%s type=%s", job.id, request.payload.get("task_type"))
    return JobSubmitResponse(
        job_id=job.id,
        status=job.status.value,
        message="Job enqueued successfully."
    )


@router.get(
    "/jobs",
    summary="List all jobs",
    tags=["Jobs"],
)
def list_jobs(status_filter: Optional[str] = None):
    """
    Return all jobs. Optionally filter by status:
        ?status_filter=pending | running | pending_takeover | completed | failed
    """
    jobs = store.get_all_jobs()
    if status_filter:
        jobs = [j for j in jobs if j["status"] == status_filter]
    return {"jobs": jobs, "count": len(jobs)}


@router.get(
    "/jobs/{job_id}",
    summary="Get job status and result",
    tags=["Jobs"],
)
def get_job(job_id: str):
    """Poll a specific job. Returns full job dict including result when complete."""
    job = store.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return job


# ── Worker Routes ──────────────────────────────────────────────────────────────

@router.post(
    "/workers/register",
    response_model=WorkerRegisterResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new worker",
    tags=["Workers"],
)
def register_worker(request: WorkerRegisterRequest):
    """
    Called by each worker process on startup.

    If worker_id is provided and the worker was previously registered (e.g.
    it crashed and restarted), the coordinator reactivates it rather than
    creating a duplicate entry. This supports worker reconnection.
    """
    import uuid
    worker_id = request.worker_id or str(uuid.uuid4())
    worker    = store.register_worker(worker_id)

    emit({
        "event":     "worker_registered",
        "worker_id": worker_id,
    })

    logger.info("Worker registered: id=%s", worker_id)
    return WorkerRegisterResponse(
        worker_id=worker.id,
        message="Registered successfully."
    )


@router.post(
    "/workers/{worker_id}/heartbeat",
    response_model=HeartbeatResponse,
    summary="Send worker heartbeat",
    tags=["Workers"],
)
def heartbeat(worker_id: str):
    """
    Called periodically by each worker to signal it is alive.

    If the coordinator does not know this worker_id (e.g. coordinator
    restarted), it returns acknowledged=False and the worker should
    re-register before continuing.
    """
    known = store.heartbeat(worker_id)
    if not known:
        logger.warning("Heartbeat from unknown worker %s — needs re-registration", worker_id)
        return HeartbeatResponse(
            acknowledged=False,
            message="Unknown worker. Please re-register."
        )
    return HeartbeatResponse(acknowledged=True, message="OK")


@router.get(
    "/workers/{worker_id}/job",
    summary="Poll for next job assignment",
    tags=["Workers"],
)
def poll_job(worker_id: str):
    """
    Worker calls this to request a job. Returns the next available job with
    its checkpoint (if any) attached, or {"job": null} if the queue is empty.

    TAKEOVER SCENARIO: If the job has status PENDING_TAKEOVER, the response
    includes latest_checkpoint with the full state dict. The worker must
    detect this and pass the state to its TaskRunner as initial_state.
    """
    worker = store.get_worker(worker_id)
    if worker is None:
        raise HTTPException(status_code=404, detail=f"Worker '{worker_id}' not registered.")

    job = store.dequeue_job(worker_id)
    if job is None:
        return {"job": None, "message": "Queue empty."}

    is_takeover = job.latest_checkpoint is not None

    emit({
        "event":          "job_assigned",
        "job_id":         job.id,
        "worker_id":      worker_id,
        "task_type":      job.payload.get("task_type"),
        "is_takeover":    is_takeover,
        "checkpoint_step": job.latest_checkpoint.step_index if is_takeover else None,
    })

    logger.info(
        "Job assigned: job=%s worker=%s takeover=%s checkpoint_step=%s",
        job.id, worker_id, is_takeover,
        job.latest_checkpoint.step_index if is_takeover else "n/a"
    )
    return {"job": job.to_dict(), "is_takeover": is_takeover}


@router.post(
    "/workers/{worker_id}/checkpoint",
    response_model=CheckpointResponse,
    summary="Push a checkpoint blob",
    tags=["Workers"],
)
def push_checkpoint(worker_id: str, request: CheckpointRequest):
    """
    Worker pushes its current execution state at a checkpoint interval.

    The coordinator:
    1. Validates the worker is still the assigned owner of the job
       (rejects stale checkpoints from workers already declared dead)
    2. Stores the checkpoint in the InMemoryStore (on the Job object)
    3. Persists to the checkpoint_store backend
    4. Warns if the checkpoint state is unusually large

    Returns accepted=False if the checkpoint was rejected (stale or job not running).
    The worker should not treat a rejection as fatal — it just means the
    coordinator has already moved on (declared the worker dead, reassigned the job).
    """
    ckpt = store.accept_checkpoint(
        job_id     = request.job_id,
        worker_id  = worker_id,
        step_index = request.step_index,
        state      = request.state,
    )

    if ckpt is None:
        logger.warning(
            "Checkpoint rejected: job=%s worker=%s step=%d "
            "(worker may have been declared dead)",
            request.job_id, worker_id, request.step_index
        )
        return CheckpointResponse(
            accepted=False,
            sequence=None,
            size_bytes=None,
            message="Checkpoint rejected: job not running or worker not assigned."
        )

    # Persist to the checkpoint store backend
    checkpoint_store.save(ckpt)

    if ckpt.size_bytes > CHECKPOINT_WARN_SIZE_BYTES:
        logger.warning(
            "Large checkpoint: job=%s size=%d bytes (threshold=%d)",
            request.job_id, ckpt.size_bytes, CHECKPOINT_WARN_SIZE_BYTES
        )

    emit({
        "event":       "checkpoint_pushed",
        "job_id":      request.job_id,
        "worker_id":   worker_id,
        "step_index":  request.step_index,
        "sequence":    ckpt.sequence,
        "size_bytes":  ckpt.size_bytes,
    })

    logger.debug(
        "Checkpoint accepted: job=%s step=%d seq=%d size=%d bytes",
        request.job_id, request.step_index, ckpt.sequence, ckpt.size_bytes
    )
    return CheckpointResponse(
        accepted=True,
        sequence=ckpt.sequence,
        size_bytes=ckpt.size_bytes,
        message="Checkpoint stored."
    )


@router.post(
    "/workers/{worker_id}/complete",
    response_model=CompleteResponse,
    summary="Submit job result",
    tags=["Workers"],
)
def complete_job(worker_id: str, request: CompleteRequest):
    """
    Worker submits the final result of a completed job.

    The coordinator validates that this worker is still the assigned owner.
    If the worker was declared dead and replaced by P2, this call returns
    accepted=False (late result from a zombie worker — discarded).

    On success, the checkpoint is cleaned up (no longer needed).
    """
    accepted = store.complete_job(
        job_id      = request.job_id,
        worker_id   = worker_id,
        result      = request.result,
        total_steps = request.total_steps,
    )

    if not accepted:
        logger.warning(
            "Late result rejected: job=%s worker=%s "
            "(worker was declared dead, job already reassigned)",
            request.job_id, worker_id
        )
        return CompleteResponse(
            accepted=False,
            message="Result rejected: you are no longer the assigned worker for this job."
        )

    # Clean up checkpoint — job is done, no need to keep state
    checkpoint_store.delete(request.job_id)

    job = store.get_job(request.job_id)
    total_time = None
    if job and job["started_at"] and job["completed_at"]:
        from datetime import datetime
        t0 = datetime.fromisoformat(job["started_at"])
        t1 = datetime.fromisoformat(job["completed_at"])
        total_time = (t1 - t0).total_seconds()

    emit({
        "event":        "job_completed",
        "job_id":       request.job_id,
        "worker_id":    worker_id,
        "total_steps":  request.total_steps,
        "total_time_sec": total_time,
        "takeover":     (job["takeover_count"] > 0) if job else False,
        "steps_redone": job.get("steps_redone") if job else None,
    })

    logger.info(
        "Job completed: job=%s worker=%s steps=%d time=%.2fs",
        request.job_id, worker_id, request.total_steps, total_time or 0
    )
    return CompleteResponse(accepted=True, message="Result accepted. Job marked completed.")


@router.post(
    "/workers/{worker_id}/fail",
    response_model=FailResponse,
    summary="Report job execution error",
    tags=["Workers"],
)
def fail_job(worker_id: str, request: FailRequest):
    """
    Worker explicitly reports that a task raised an unhandled exception.

    This is distinct from silent crash (which is handled by the monitor thread).
    On retry, the job is re-queued as PENDING_TAKEOVER (with existing checkpoint)
    or PENDING (if no checkpoint exists). On final failure, it is marked FAILED.
    """
    result_status = store.fail_job_by_worker(
        job_id     = request.job_id,
        worker_id  = worker_id,
        error      = request.error,
        max_retries= MAX_JOB_RETRIES,
    )

    if result_status is None:
        raise HTTPException(
            status_code=404,
            detail=f"Job '{request.job_id}' not found or not assigned to worker '{worker_id}'."
        )

    job = store.get_job(request.job_id)

    emit({
        "event":       "job_failed_by_worker",
        "job_id":      request.job_id,
        "worker_id":   worker_id,
        "error":       request.error,
        "retry_count": job["retry_count"] if job else None,
        "new_status":  result_status.value,
    })

    logger.warning(
        "Worker reported job failure: job=%s worker=%s error=%s new_status=%s",
        request.job_id, worker_id, request.error, result_status.value
    )
    return FailResponse(
        job_status=result_status.value,
        retry_count=job["retry_count"] if job else 0,
        message=f"Job re-queued for retry." if result_status != JobStatus.FAILED
                else "Job permanently failed (retries exhausted)."
    )


# ── Worker List & Metrics ──────────────────────────────────────────────────────

@router.get(
    "/workers",
    summary="List all workers",
    tags=["Workers"],
)
def list_workers():
    return {"workers": store.get_all_workers()}


@router.get(
    "/metrics",
    summary="System-wide stats snapshot",
    tags=["Metrics"],
)
def get_metrics():
    """
    Returns aggregate counts for jobs and workers.
    Also includes checkpoint store statistics (count + total bytes).
    Used by scripts/run_experiment.py to poll system state during experiments.
    """
    stats = store.stats()

    # Add checkpoint store stats if in-memory backend
    from coordinator.checkpoint_store import InMemoryCheckpointStore
    if isinstance(checkpoint_store, InMemoryCheckpointStore):
        stats["checkpoints_stored"]     = checkpoint_store.count()
        stats["checkpoint_total_bytes"] = checkpoint_store.total_bytes()

    return stats