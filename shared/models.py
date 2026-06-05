# shared/models.py
# Core data structures and thread-safe in-memory store for the Hot Takeover
# distributed scheduler. Defines Job, Worker, and Checkpoint dataclasses,
# plus the InMemoryStore that the coordinator reads and writes exclusively.
#
# Architecture rule: workers never import this module. They talk to the
# coordinator over HTTP. This file is coordinator-only shared state.

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


# ── Enumerations ───────────────────────────────────────────────────────────────

class JobStatus(str, Enum):
    PENDING          = "pending"           # In queue, waiting for a worker
    RUNNING          = "running"           # Assigned and actively executing
    PENDING_TAKEOVER = "pending_takeover"  # Worker died; awaiting reassignment
    COMPLETED        = "completed"         # Finished successfully
    FAILED           = "failed"            # Exhausted all retries permanently


class WorkerStatus(str, Enum):
    ACTIVE = "active"  # Sending heartbeats, available for work
    DEAD   = "dead"    # Missed heartbeat deadline; monitor declared it dead


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class Checkpoint:
    """
    A single checkpoint snapshot emitted by a worker during task execution.

    The 'state' dict is the entire serializable execution state of the task
    at the moment it was checkpointed. It is opaque to the coordinator —
    the coordinator stores it and hands it to P2 verbatim on takeover.

    This design is the core research contribution: the coordinator does not
    need to understand task semantics, only that 'state' is a JSON-safe dict
    that fully describes progress so far.
    """
    job_id:     str
    worker_id:  str
    sequence:   int       # Monotonically increasing per job; used to discard stale checkpoints
    step_index: int       # How many task steps have completed at this checkpoint
    state:      dict      # Opaque task state — serializable by the task's run_step()
    timestamp:  datetime  = field(default_factory=datetime.utcnow)
    size_bytes: int       = 0  # Populated by the store after serialization

    def to_dict(self) -> dict:
        return {
            "job_id":     self.job_id,
            "worker_id":  self.worker_id,
            "sequence":   self.sequence,
            "step_index": self.step_index,
            "state":      self.state,
            "timestamp":  self.timestamp.isoformat(),
            "size_bytes": self.size_bytes,
        }


@dataclass
class Job:
    """
    Represents a single unit of work flowing through the scheduler.

    'payload' carries the task type and its initialization parameters.
    Example: {"task_type": "CountingTask", "target": 100}

    'latest_checkpoint' holds the most recently committed Checkpoint for
    this job. On takeover, the coordinator attaches this to the assignment
    response so P2 can resume without querying a separate endpoint.
    """
    id:                 str                   = field(default_factory=lambda: str(uuid.uuid4()))
    payload:            dict                  = field(default_factory=dict)
    status:             JobStatus             = JobStatus.PENDING
    assigned_worker:    Optional[str]         = None
    retry_count:        int                   = 0
    latest_checkpoint:  Optional[Checkpoint]  = None   # Most recent committed checkpoint
    checkpoint_seq:     int                   = 0      # Next sequence number to assign
    created_at:         datetime              = field(default_factory=datetime.utcnow)
    started_at:         Optional[datetime]    = None
    completed_at:       Optional[datetime]    = None
    result:             Optional[dict]        = None
    error:              Optional[str]         = None

    # Metrics fields — populated during the job lifecycle for experiment analysis
    detection_latency_sec:  Optional[float]   = None  # Experiment 1
    steps_redone:           Optional[int]     = None  # Experiment 2
    takeover_count:         int               = 0     # How many times this job was taken over
    total_steps_executed:   int               = 0     # Across all workers (including retried steps)

    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "payload":          self.payload,
            "status":           self.status.value,
            "assigned_worker":  self.assigned_worker,
            "retry_count":      self.retry_count,
            "latest_checkpoint": self.latest_checkpoint.to_dict() if self.latest_checkpoint else None,
            "checkpoint_seq":   self.checkpoint_seq,
            "created_at":       self.created_at.isoformat(),
            "started_at":       self.started_at.isoformat()   if self.started_at   else None,
            "completed_at":     self.completed_at.isoformat() if self.completed_at else None,
            "result":           self.result,
            "error":            self.error,
            "detection_latency_sec": self.detection_latency_sec,
            "steps_redone":     self.steps_redone,
            "takeover_count":   self.takeover_count,
            "total_steps_executed": self.total_steps_executed,
        }


@dataclass
class Worker:
    """
    Tracks the live state of a single worker process.

    'current_job_id' is the single job this worker holds. A worker holds
    at most one job at a time (pull-based model).

    'jobs_completed' is a lifetime counter used for utilization metrics
    in the visualization / Tableau export.
    """
    id:                str          = field(default_factory=lambda: str(uuid.uuid4()))
    status:            WorkerStatus = WorkerStatus.ACTIVE
    last_heartbeat:    datetime     = field(default_factory=datetime.utcnow)
    current_job_id:    Optional[str] = None
    jobs_completed:    int          = 0
    jobs_taken_over:   int          = 0   # Jobs this worker inherited via hot takeover
    registered_at:     datetime     = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "id":              self.id,
            "status":          self.status.value,
            "last_heartbeat":  self.last_heartbeat.isoformat(),
            "current_job_id":  self.current_job_id,
            "jobs_completed":  self.jobs_completed,
            "jobs_taken_over": self.jobs_taken_over,
            "registered_at":   self.registered_at.isoformat(),
        }


# ── Thread-Safe In-Memory Store ────────────────────────────────────────────────

class InMemoryStore:
    """
    Single source of truth for all job, worker, and checkpoint state.

    Uses a reentrant lock (RLock) so the coordinator's API request threads
    and background monitor thread can call store methods concurrently and
    safely, including methods that call other store methods internally.

    Design contract:
    - Callers receive Job/Worker objects directly when mutation is expected.
      They must mutate through store methods, not by modifying returned objects
      ad-hoc, to ensure lock discipline.
    - to_dict() snapshots are returned for read-only API responses.
    - The pending queue is FIFO for new jobs; recovered/takeover jobs are
      re-inserted at the front to minimize re-work after failure.
    """

    def __init__(self) -> None:
        self._lock:          threading.RLock    = threading.RLock()
        self._jobs:          Dict[str, Job]     = {}
        self._workers:       Dict[str, Worker]  = {}
        self._pending_queue: List[str]          = []  # Ordered list of PENDING job IDs

    # ── Worker operations ──────────────────────────────────────────────────────

    def register_worker(self, worker_id: str) -> Worker:
        """Register a new worker. Safe to call on re-registration (idempotent)."""
        with self._lock:
            if worker_id in self._workers:
                # Worker is reconnecting — mark it active, reset heartbeat
                w = self._workers[worker_id]
                w.status = WorkerStatus.ACTIVE
                w.last_heartbeat = datetime.utcnow()
                return w
            worker = Worker(id=worker_id)
            self._workers[worker_id] = worker
            return worker

    def heartbeat(self, worker_id: str) -> bool:
        """
        Refresh last_heartbeat timestamp for a worker.
        Returns False if the worker_id is unknown (caller should re-register).
        """
        with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return False
            worker.last_heartbeat = datetime.utcnow()
            worker.status = WorkerStatus.ACTIVE
            return True

    def mark_worker_dead(self, worker_id: str) -> Optional[str]:
        """
        Declare a worker dead. Detaches it from its current job and returns
        the orphaned job_id (or None if the worker had no job).
        Called by the monitor thread on heartbeat timeout.
        """
        with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return None
            worker.status = WorkerStatus.DEAD
            orphaned_job_id = worker.current_job_id
            worker.current_job_id = None
            return orphaned_job_id

    def get_worker(self, worker_id: str) -> Optional[Worker]:
        with self._lock:
            return self._workers.get(worker_id)

    def get_all_workers(self) -> List[dict]:
        with self._lock:
            return [w.to_dict() for w in self._workers.values()]

    def get_active_workers(self) -> List[Worker]:
        """Return live Worker objects for ACTIVE workers (used by monitor thread)."""
        with self._lock:
            return [w for w in self._workers.values() if w.status == WorkerStatus.ACTIVE]

    # ── Job operations ─────────────────────────────────────────────────────────

    def enqueue_job(self, payload: dict) -> Job:
        """Create a new PENDING job and append it to the back of the queue."""
        with self._lock:
            job = Job(payload=payload)
            self._jobs[job.id] = job
            self._pending_queue.append(job.id)
            return job

    def dequeue_job(self, worker_id: str) -> Optional[Job]:
        """
        Atomically pop the next available job (PENDING or PENDING_TAKEOVER)
        from the front of the queue and assign it to worker_id.

        Returns None if the queue is empty.

        PENDING_TAKEOVER jobs carry their latest_checkpoint so the assigned
        worker can resume rather than restart. The worker receives the full
        Job dict including the checkpoint in the API response.
        """
        with self._lock:
            while self._pending_queue:
                job_id = self._pending_queue.pop(0)
                job = self._jobs.get(job_id)
                if job is None:
                    continue
                if job.status not in (JobStatus.PENDING, JobStatus.PENDING_TAKEOVER):
                    continue

                is_takeover = job.status == JobStatus.PENDING_TAKEOVER

                job.status          = JobStatus.RUNNING
                job.assigned_worker = worker_id
                job.started_at      = datetime.utcnow()

                worker = self._workers.get(worker_id)
                if worker:
                    worker.current_job_id = job.id
                    if is_takeover:
                        worker.jobs_taken_over += 1

                return job
            return None

    def accept_checkpoint(self, job_id: str, worker_id: str,
                          step_index: int, state: dict) -> Optional[Checkpoint]:
        """
        Store a checkpoint for a running job.

        Validates that:
        - The job exists and is RUNNING
        - The submitting worker is the currently assigned worker
          (rejects stale checkpoints from a worker that was already declared dead)

        Returns the stored Checkpoint on success, None on rejection.
        The sequence number is monotonically increasing per job so P2 can
        verify it is loading the latest state and not a stale one.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            if job.status != JobStatus.RUNNING:
                return None
            if job.assigned_worker != worker_id:
                # Stale checkpoint from a dead worker — discard
                return None

            import json
            size = len(json.dumps(state).encode("utf-8"))

            ckpt = Checkpoint(
                job_id=job_id,
                worker_id=worker_id,
                sequence=job.checkpoint_seq,
                step_index=step_index,
                state=state,
                size_bytes=size,
            )
            job.checkpoint_seq     += 1
            job.latest_checkpoint   = ckpt
            job.total_steps_executed = step_index
            return ckpt

    def complete_job(self, job_id: str, worker_id: str,
                     result: dict, total_steps: int) -> bool:
        """
        Mark a job COMPLETED. Validates the submitting worker is still the
        assigned one (rejects a result from a worker declared dead mid-flight).
        Returns False if validation fails or job not found.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.assigned_worker != worker_id:
                # Worker was declared dead and replaced; reject this late result
                return False
            job.status           = JobStatus.COMPLETED
            job.result           = result
            job.completed_at     = datetime.utcnow()
            job.assigned_worker  = None
            job.total_steps_executed = total_steps

            worker = self._workers.get(worker_id)
            if worker:
                worker.current_job_id = None
                worker.jobs_completed += 1
            return True

    def mark_job_takeover(self, job_id: str, detection_latency: float,
                          max_retries: int) -> Optional[JobStatus]:
        """
        Called by the monitor thread when a worker dies with an active job.

        - Increments retry_count
        - Records detection_latency for Experiment 1
        - Computes steps_redone (steps since last checkpoint) for Experiment 2
        - If retries remain: puts job back as PENDING_TAKEOVER at front of queue
        - If retries exhausted: marks job permanently FAILED
        Returns the resulting JobStatus, or None if job not found.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None

            job.retry_count           += 1
            job.detection_latency_sec  = detection_latency
            job.takeover_count        += 1
            job.assigned_worker        = None

            # Compute steps_redone: how much work P2 must redo.
            # This is the number of steps between the last checkpoint and
            # the step the worker was on when the monitor detected failure.
            # Since the worker may still be running (pushing checkpoints) after
            # the heartbeat stops, we use total_steps_executed at detection time
            # minus the checkpoint step. If they are equal (worker kept up),
            # steps_redone = 0 which correctly means P2 starts from a fresh checkpoint.
            if job.latest_checkpoint is not None:
                gap = job.total_steps_executed - job.latest_checkpoint.step_index
                job.steps_redone = max(0, gap)
            else:
                job.steps_redone = job.total_steps_executed  # No checkpoint = full restart

            if job.retry_count <= max_retries:
                job.status     = JobStatus.PENDING_TAKEOVER
                job.started_at = None
                # Insert at front: recovered jobs run before new submissions
                self._pending_queue.insert(0, job.id)
            else:
                job.status       = JobStatus.FAILED
                job.completed_at = datetime.utcnow()
                job.error        = f"Exhausted {max_retries} retries via worker failure"

            return job.status

    def fail_job_by_worker(self, job_id: str, worker_id: str,
                           error: str, max_retries: int) -> Optional[JobStatus]:
        """
        Called when a worker explicitly reports a task execution error
        (as opposed to silent crash detected by the monitor).
        Follows the same retry logic as mark_job_takeover.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            if job.assigned_worker != worker_id:
                return None

            job.retry_count    += 1
            job.error           = error
            job.assigned_worker = None

            worker = self._workers.get(worker_id)
            if worker:
                worker.current_job_id = None

            if job.retry_count <= max_retries:
                job.status     = JobStatus.PENDING_TAKEOVER if job.latest_checkpoint else JobStatus.PENDING
                job.started_at = None
                self._pending_queue.insert(0, job.id)
            else:
                job.status       = JobStatus.FAILED
                job.completed_at = datetime.utcnow()

            return job.status

    def get_job(self, job_id: str) -> Optional[dict]:
        with self._lock:
            job = self._jobs.get(job_id)
            return job.to_dict() if job else None

    def get_all_jobs(self) -> List[dict]:
        with self._lock:
            return [j.to_dict() for j in self._jobs.values()]

    def queue_depth(self) -> int:
        with self._lock:
            return len(self._pending_queue)

    def stats(self) -> dict:
        """Return a snapshot of aggregate system stats for the /metrics endpoint."""
        with self._lock:
            jobs = list(self._jobs.values())
            return {
                "queue_depth":       len(self._pending_queue),
                "total_jobs":        len(jobs),
                "pending":           sum(1 for j in jobs if j.status == JobStatus.PENDING),
                "running":           sum(1 for j in jobs if j.status == JobStatus.RUNNING),
                "pending_takeover":  sum(1 for j in jobs if j.status == JobStatus.PENDING_TAKEOVER),
                "completed":         sum(1 for j in jobs if j.status == JobStatus.COMPLETED),
                "failed":            sum(1 for j in jobs if j.status == JobStatus.FAILED),
                "total_workers":     len(self._workers),
                "active_workers":    sum(1 for w in self._workers.values() if w.status == WorkerStatus.ACTIVE),
                "dead_workers":      sum(1 for w in self._workers.values() if w.status == WorkerStatus.DEAD),
                "total_takeovers":   sum(j.takeover_count for j in jobs),
            }


# ── Module-level singleton ─────────────────────────────────────────────────────
# The coordinator imports this object directly. A single instance guarantees
# that all API threads and the monitor thread share the same state.
store = InMemoryStore()