# shared/models.py
# Defines the core data structures (Job, Worker) and the thread-safe
# InMemoryStore that the coordinator reads/writes.
# The store's public interface is intentionally narrow so it can be
# swapped for a Redis-backed implementation without touching any other file.

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


#Enumerations 

class JobStatus(str, Enum):
    PENDING   = "pending"    # in queue, waiting for a worker
    RUNNING   = "running"    # assigned to a worker, currently executing
    COMPLETED = "completed"  # finished successfully
    FAILED    = "failed"     # exhausted all retries


class WorkerStatus(str, Enum):
    ACTIVE = "active"  # sending heartbeats, available for work
    DEAD   = "dead"    # missed heartbeat deadline


#Dataclasses 

@dataclass
class Job:
    """Represents a single unit of work in the scheduler."""
    id:              str            = field(default_factory=lambda: str(uuid.uuid4()))
    payload:         dict           = field(default_factory=dict)  # arbitrary job params
    status:          JobStatus      = JobStatus.PENDING
    assigned_worker: Optional[str]  = None   # worker.id currently holding this job
    retry_count:     int            = 0
    created_at:      datetime       = field(default_factory=datetime.utcnow)
    started_at:      Optional[datetime] = None
    completed_at:    Optional[datetime] = None
    result:          Optional[dict] = None   # populated on success
    error:           Optional[str]  = None   # populated on failure

    def to_dict(self) -> dict:
        return {
            "id":              self.id,
            "payload":         self.payload,
            "status":          self.status.value,
            "assigned_worker": self.assigned_worker,
            "retry_count":     self.retry_count,
            "created_at":      self.created_at.isoformat(),
            "started_at":      self.started_at.isoformat()  if self.started_at  else None,
            "completed_at":    self.completed_at.isoformat() if self.completed_at else None,
            "result":          self.result,
            "error":           self.error,
        }


@dataclass
class Worker:
    """Tracks the live state of a single worker process."""
    id:             str          = field(default_factory=lambda: str(uuid.uuid4()))
    status:         WorkerStatus = WorkerStatus.ACTIVE
    last_heartbeat: datetime     = field(default_factory=datetime.utcnow)
    current_job_id: Optional[str] = None   # job this worker is executing right now
    jobs_completed: int          = 0       # lifetime counter, used for utilization metrics
    registered_at:  datetime     = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "status":         self.status.value,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "current_job_id": self.current_job_id,
            "jobs_completed": self.jobs_completed,
            "registered_at":  self.registered_at.isoformat(),
        }


# Thread-Safe In-Memory Store 

class InMemoryStore:
    """
    Single source of truth for all job and worker state.

    All public methods acquire a reentrant lock so the coordinator's
    API threads and background monitor thread can call them safely.

    Design rule: callers always work with plain Job / Worker objects.
    The store never returns internal references. it returns copies via
    to_dict(), or the objects themselves when mutation is expected (and
    the caller must re-enter through a store method to persist changes).
    """

    def __init__(self) -> None:
        self._lock:    threading.RLock       = threading.RLock()
        self._jobs:    Dict[str, Job]        = {}
        self._workers: Dict[str, Worker]     = {}
        # Ordered pending queue: list of job IDs waiting to be picked up
        self._pending_queue: List[str]       = []

    #Worker operations 

    def register_worker(self, worker_id: str) -> Worker:
        with self._lock:
            worker = Worker(id=worker_id)
            self._workers[worker_id] = worker
            return worker

    def heartbeat(self, worker_id: str) -> bool:
        """Update last_heartbeat timestamp. Returns False if worker unknown."""
        with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return False
            worker.last_heartbeat = datetime.utcnow()
            worker.status = WorkerStatus.ACTIVE
            return True

    def mark_worker_dead(self, worker_id: str) -> Optional[str]:
        """
        Mark a worker as dead and detach it from any running job.
        Returns the job_id that was orphaned, or None.
        """
        with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                return None
            worker.status = WorkerStatus.DEAD
            orphaned_job_id = worker.current_job_id
            worker.current_job_id = None
            return orphaned_job_id

    def get_all_workers(self) -> List[dict]:
        with self._lock:
            return [w.to_dict() for w in self._workers.values()]

    def get_worker(self, worker_id: str) -> Optional[Worker]:
        with self._lock:
            return self._workers.get(worker_id)

    #Job operations

    def enqueue_job(self, payload: dict) -> Job:
        """Create a new PENDING job and add it to the back of the queue."""
        with self._lock:
            job = Job(payload=payload)
            self._jobs[job.id] = job
            self._pending_queue.append(job.id)
            return job

    def dequeue_job(self, worker_id: str) -> Optional[Job]:
        """
        Pop the next PENDING job from the front of the queue and assign
        it to worker_id. Returns None if the queue is empty.
        """
        with self._lock:
            while self._pending_queue:
                job_id = self._pending_queue.pop(0)
                job = self._jobs.get(job_id)
                # Guard: job could have been cancelled / already removed
                if job and job.status == JobStatus.PENDING:
                    job.status          = JobStatus.RUNNING
                    job.assigned_worker = worker_id
                    job.started_at      = datetime.utcnow()
                    # Link the worker → job so the monitor can find orphans
                    worker = self._workers.get(worker_id)
                    if worker:
                        worker.current_job_id = job.id
                    return job
            return None

    def complete_job(self, job_id: str, worker_id: str, result: dict) -> bool:
        """Mark a job COMPLETED and release the worker. Returns False if not found."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            job.status       = JobStatus.COMPLETED
            job.result       = result
            job.completed_at = datetime.utcnow()
            job.assigned_worker = None
            worker = self._workers.get(worker_id)
            if worker:
                worker.current_job_id = None
                worker.jobs_completed += 1
            return True

    def fail_job(self, job_id: str, error: str, max_retries: int) -> JobStatus:
        """
        Record a failure on a job.
        - If retry_count < max_retries: put it back to PENDING (re-queue).
        - Otherwise: mark it permanently FAILED.
        Returns the resulting JobStatus.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return JobStatus.FAILED
            job.retry_count    += 1
            job.assigned_worker = None
            job.error           = error
            if job.retry_count <= max_retries:
                job.status     = JobStatus.PENDING
                job.started_at = None
                # Re-insert at the front so recovered jobs run before new ones
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

    def get_active_workers(self) -> List[Worker]:
        """Return Worker objects whose status is ACTIVE (not copies)."""
        with self._lock:
            return [w for w in self._workers.values() if w.status == WorkerStatus.ACTIVE]

    def queue_depth(self) -> int:
        with self._lock:
            return len(self._pending_queue)


#Module-level singleton
# The coordinator imports this object directly.
# Workers never touch the store — they talk to the coordinator over HTTP.
store = InMemoryStore()