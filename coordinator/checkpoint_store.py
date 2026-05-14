# coordinator/checkpoint_store.py
# Manages storage and retrieval of checkpoint blobs for the coordinator.
#
# The CheckpointStore sits between the coordinator API (which receives
# checkpoint pushes from workers) and the scheduler (which attaches the
# latest checkpoint to a takeover job assignment).
#
# Two backends are provided:
#   InMemoryCheckpointStore  -- default, no dependencies, resets on restart
#   FileCheckpointStore      -- persists to disk, survives coordinator restart
#
# Both expose the same interface so the coordinator never knows which is active.
# Switch via STORAGE_BACKEND in shared/config.py.
#
# DESIGN DECISION: only the LATEST checkpoint per job is kept.
# Rationale: storing all checkpoints provides no recovery benefit (P2 always
# wants the most recent state) and grows memory unboundedly for long jobs.
# The sequence number on each checkpoint lets the coordinator detect and
# reject out-of-order or stale pushes.

from __future__ import annotations

import json
import logging
import os
import threading
from abc import ABC, abstractmethod
from typing import Optional

from shared.models import Checkpoint

logger = logging.getLogger(__name__)


# ── Abstract Interface ─────────────────────────────────────────────────────────

class BaseCheckpointStore(ABC):

    @abstractmethod
    def save(self, checkpoint: Checkpoint) -> bool:
        """
        Persist a checkpoint. Returns True on success.
        Rejects the checkpoint if its sequence number is not greater than
        the currently stored one (guards against stale writes from dead workers).
        """

    @abstractmethod
    def load(self, job_id: str) -> Optional[Checkpoint]:
        """
        Retrieve the latest checkpoint for a job.
        Returns None if no checkpoint exists for job_id.
        """

    @abstractmethod
    def delete(self, job_id: str) -> None:
        """Remove checkpoint data for a completed or permanently failed job."""

    @abstractmethod
    def exists(self, job_id: str) -> bool:
        """Return True if at least one checkpoint exists for job_id."""


# ── In-Memory Backend ──────────────────────────────────────────────────────────

class InMemoryCheckpointStore(BaseCheckpointStore):
    """
    Thread-safe in-memory checkpoint store.

    Stores one Checkpoint object per job_id. All operations are O(1).
    State is lost when the coordinator process restarts — acceptable for
    development and most experiments.
    """

    def __init__(self) -> None:
        self._lock:  threading.RLock          = threading.RLock()
        self._store: dict[str, Checkpoint]    = {}

    def save(self, checkpoint: Checkpoint) -> bool:
        with self._lock:
            existing = self._store.get(checkpoint.job_id)
            if existing and existing.sequence >= checkpoint.sequence:
                logger.warning(
                    "Rejected stale checkpoint for job %s: "
                    "incoming seq=%d <= stored seq=%d",
                    checkpoint.job_id, checkpoint.sequence, existing.sequence
                )
                return False
            self._store[checkpoint.job_id] = checkpoint
            logger.debug(
                "Checkpoint saved: job=%s seq=%d step=%d size=%d bytes",
                checkpoint.job_id, checkpoint.sequence,
                checkpoint.step_index, checkpoint.size_bytes
            )
            return True

    def load(self, job_id: str) -> Optional[Checkpoint]:
        with self._lock:
            return self._store.get(job_id)

    def delete(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._store:
                del self._store[job_id]
                logger.debug("Checkpoint deleted for job %s", job_id)

    def exists(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._store

    def count(self) -> int:
        """Number of jobs with active checkpoints. Used in /metrics."""
        with self._lock:
            return len(self._store)

    def total_bytes(self) -> int:
        """Total bytes across all stored checkpoints. Used in /metrics."""
        with self._lock:
            return sum(c.size_bytes for c in self._store.values())


# ── File-Backed Backend ────────────────────────────────────────────────────────

class FileCheckpointStore(BaseCheckpointStore):
    """
    File-backed checkpoint store. Each job gets one JSON file:
        {checkpoint_dir}/{job_id}.json

    Survives coordinator restarts, making it suitable for testing recovery
    scenarios where the coordinator itself is restarted.

    File format: the Checkpoint.to_dict() JSON representation.
    On load, the dict is reconstructed into a Checkpoint object.
    """

    def __init__(self, checkpoint_dir: str) -> None:
        self._lock = threading.RLock()
        self._dir  = checkpoint_dir
        os.makedirs(self._dir, exist_ok=True)
        logger.info("FileCheckpointStore initialized at %s", self._dir)

    def _path(self, job_id: str) -> str:
        # Sanitize job_id to prevent path traversal (job IDs are UUIDs so
        # this is defensive, not strictly necessary)
        safe_id = job_id.replace("/", "_").replace("..", "_")
        return os.path.join(self._dir, f"{safe_id}.json")

    def save(self, checkpoint: Checkpoint) -> bool:
        with self._lock:
            path = self._path(checkpoint.job_id)
            # Check sequence before writing
            existing = self._load_from_disk(path)
            if existing and existing.sequence >= checkpoint.sequence:
                logger.warning(
                    "Rejected stale checkpoint for job %s: seq=%d <= stored=%d",
                    checkpoint.job_id, checkpoint.sequence, existing.sequence
                )
                return False
            try:
                tmp_path = path + ".tmp"
                with open(tmp_path, "w") as f:
                    json.dump(checkpoint.to_dict(), f)
                # Atomic rename so a crash mid-write doesn't corrupt the file
                os.replace(tmp_path, path)
                logger.debug(
                    "Checkpoint written to disk: job=%s seq=%d step=%d",
                    checkpoint.job_id, checkpoint.sequence, checkpoint.step_index
                )
                return True
            except OSError as e:
                logger.error("Failed to write checkpoint for job %s: %s", checkpoint.job_id, e)
                return False

    def load(self, job_id: str) -> Optional[Checkpoint]:
        with self._lock:
            return self._load_from_disk(self._path(job_id))

    def _load_from_disk(self, path: str) -> Optional[Checkpoint]:
        """Internal helper — caller must hold lock."""
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r") as f:
                data = json.load(f)
            from datetime import datetime
            return Checkpoint(
                job_id     = data["job_id"],
                worker_id  = data["worker_id"],
                sequence   = data["sequence"],
                step_index = data["step_index"],
                state      = data["state"],
                timestamp  = datetime.fromisoformat(data["timestamp"]),
                size_bytes = data.get("size_bytes", 0),
            )
        except (OSError, KeyError, json.JSONDecodeError) as e:
            logger.error("Failed to read checkpoint from %s: %s", path, e)
            return None

    def delete(self, job_id: str) -> None:
        with self._lock:
            path = self._path(job_id)
            try:
                if os.path.exists(path):
                    os.remove(path)
                    logger.debug("Checkpoint file deleted: %s", path)
            except OSError as e:
                logger.warning("Could not delete checkpoint file %s: %s", path, e)

    def exists(self, job_id: str) -> bool:
        with self._lock:
            return os.path.exists(self._path(job_id))


# ── Factory ────────────────────────────────────────────────────────────────────

def make_checkpoint_store() -> BaseCheckpointStore:
    """
    Instantiate the correct backend based on shared/config.py.
    Called once in coordinator/main.py at startup.
    """
    from shared.config import STORAGE_BACKEND, CHECKPOINT_DIR
    if STORAGE_BACKEND == "file":
        return FileCheckpointStore(CHECKPOINT_DIR)
    # Default: in-memory
    return InMemoryCheckpointStore()


# Module-level singleton — imported by coordinator/api.py and coordinator/scheduler.py
checkpoint_store: BaseCheckpointStore = make_checkpoint_store()