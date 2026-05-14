# worker/executor.py
# Defines the CheckpointableTask interface — the core academic contribution
# of this project — and the TaskRunner that drives the step loop, manages
# checkpoint emission, and communicates results back to the coordinator.
#
# The CheckpointableTask interface answers the professor's research question:
# "What is the minimum state representation that enables correct hot takeover
# of general-purpose Python tasks?"
#
# Answer: any task that can serialize its progress as a JSON-safe dict and
# resume from that dict is hot-takeover compatible. The executor enforces
# this contract; the coordinator stores and forwards the state blindly.

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple

logger = logging.getLogger(__name__)


# ── The Checkpoint Contract ────────────────────────────────────────────────────

class CheckpointableTask(ABC):
    """
    Abstract base class for all tasks that can be hot-taken-over.

    IMPLEMENTING A NEW TASK:
    1. Subclass CheckpointableTask.
    2. Implement run_step(state) -> (new_state, is_done).
       - 'state' is empty {} on the very first call.
       - 'state' is the dict from the last checkpoint on resume.
       - new_state must be JSON-serializable (dicts, lists, ints, floats, strings).
       - is_done = True signals the task has completed; run_step won't be called again.
    3. Implement get_result(final_state) -> dict.
       - Extract the answer from the terminal state for submission to the coordinator.
    4. Implement from_payload(payload) classmethod.
       - Instantiate the task from the job payload dict sent by the client.

    CORRECTNESS REQUIREMENT:
    If run_step(S) is called and returns (S', False), then run_step(S') must
    continue from exactly where run_step(S) left off. The task must never
    assume it is running for the first time, it must always check 'state'
    first and initialize defaults only when keys are absent.

    IDEMPOTENCY:
    If a task has side effects (e.g. writing files, inserting DB rows), it
    must be idempotent with respect to its state. Specifically: if run_step
    is called with state S that reflects N completed steps, the task must
    not re-apply those N steps' side effects.
    """

    @abstractmethod
    def run_step(self, state: dict) -> Tuple[dict, bool]:
        """
        Execute one atomic unit of work.

        Args:
            state: Current execution state. Empty dict on first call;
                   deserialized checkpoint state on resume after takeover.

        Returns:
            (new_state, is_done):
                new_state — JSON-serializable dict describing all progress
                             made up to and including this step.
                is_done   — True if the task has finished; False to continue.
        """

    @abstractmethod
    def get_result(self, final_state: dict) -> dict:
        """
        Extract the final deliverable from the terminal state.

        Called exactly once after run_step returns is_done=True.
        Must return a JSON-serializable dict.
        """

    @classmethod
    @abstractmethod
    def from_payload(cls, payload: dict) -> "CheckpointableTask":
        """
        Instantiate this task from a job payload dict.
        Example payload: {"task_type": "CountingTask", "target": 100}
        """

    @property
    def task_type(self) -> str:
        return self.__class__.__name__


# ── Task Registry ──────────────────────────────────────────────────────────────

_TASK_REGISTRY: dict[str, type[CheckpointableTask]] = {}


def register_task(cls: type[CheckpointableTask]) -> type[CheckpointableTask]:
    """
    Class decorator that registers a task type by name.
    Usage: @register_task
           class MyTask(CheckpointableTask): ...
    """
    _TASK_REGISTRY[cls.__name__] = cls
    return cls


def get_task_class(task_type: str) -> type[CheckpointableTask]:
    """Look up a registered task class by name. Raises KeyError if not found."""
    if task_type not in _TASK_REGISTRY:
        raise KeyError(
            f"Unknown task type '{task_type}'. "
            f"Registered types: {list(_TASK_REGISTRY.keys())}"
        )
    return _TASK_REGISTRY[task_type]


def instantiate_task(payload: dict) -> CheckpointableTask:
    """
    Full pipeline: extract task_type from payload, look up class, instantiate.
    This is the single entry point used by the worker loop.
    """
    task_type = payload.get("task_type")
    if not task_type:
        raise ValueError("Job payload must include 'task_type' field.")
    cls = get_task_class(task_type)
    return cls.from_payload(payload)


# ── Task Runner ────────────────────────────────────────────────────────────────

class TaskRunner:
    """
    Drives a CheckpointableTask through its step loop.

    Responsibilities:
    - Load initial state from checkpoint (if provided) or start fresh.
    - Call task.run_step() in a loop.
    - Emit checkpoint callbacks at the configured interval.
    - Track execution metrics (steps, timing).
    - Return result or propagate exceptions cleanly.

    The runner is deliberately decoupled from HTTP — it receives callbacks
    for checkpointing and completion so it can be tested without a live
    coordinator.
    """

    def __init__(
        self,
        task:                 CheckpointableTask,
        job_id:               str,
        initial_state:        Optional[dict]    = None,
        initial_step:         int               = 0,
        checkpoint_every:     int               = 5,
        on_checkpoint:        Optional[Any]     = None,   # Callable[[str, int, dict], None]
        on_step:              Optional[Any]     = None,   # Callable[[int], None]  (for testing)
    ):
        self.task             = task
        self.job_id           = job_id
        self.state            = initial_state or {}
        self.step_index       = initial_step
        self.checkpoint_every = checkpoint_every
        self.on_checkpoint    = on_checkpoint   # coordinator.push_checkpoint(job_id, step, state)
        self.on_step          = on_step

        # Metrics
        self.start_time:      Optional[float] = None
        self.end_time:        Optional[float] = None
        self.checkpoint_times: list[float]    = []

    def run(self) -> dict:
        """
        Execute the task to completion.

        Returns the final result dict from task.get_result().
        Raises on unhandled task exceptions (worker will report these to coordinator).

        If initial_state was provided (takeover scenario), execution resumes
        from step_index rather than starting at 0.
        """
        self.start_time = time.monotonic()
        is_done = False

        logger.info(
            "TaskRunner starting: job=%s task=%s step=%d checkpoint_every=%d",
            self.job_id, self.task.task_type, self.step_index, self.checkpoint_every
        )

        while not is_done:
            step_start = time.monotonic()

            # The core contract call
            new_state, is_done = self.task.run_step(self.state)
            self.state      = new_state
            self.step_index += 1

            if self.on_step:
                self.on_step(self.step_index)

            # Emit checkpoint every N steps (and always on completion)
            if self.on_checkpoint and (
                self.step_index % self.checkpoint_every == 0 or is_done
            ):
                ckpt_start = time.monotonic()
                try:
                    self.on_checkpoint(self.job_id, self.step_index, self.state)
                    self.checkpoint_times.append(time.monotonic() - ckpt_start)
                except Exception as e:
                    # Checkpoint failure is non-fatal: log and continue.
                    # The worst case is P2 has to redo more work.
                    logger.warning("Checkpoint push failed for job %s: %s", self.job_id, e)

            logger.debug("Step %d completed in %.4fs", self.step_index, time.monotonic() - step_start)

        self.end_time = time.monotonic()
        result = self.task.get_result(self.state)

        logger.info(
            "TaskRunner finished: job=%s steps=%d total_time=%.2fs avg_ckpt_time=%.4fs",
            self.job_id,
            self.step_index,
            self.total_time,
            self.avg_checkpoint_time,
        )
        return result

    # ── Metrics accessors ──────────────────────────────────────────────────────

    @property
    def total_time(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    @property
    def avg_checkpoint_time(self) -> float:
        if not self.checkpoint_times:
            return 0.0
        return sum(self.checkpoint_times) / len(self.checkpoint_times)

    @property
    def checkpoint_overhead_fraction(self) -> float:
        """Fraction of total runtime spent on checkpoint I/O. Used in Experiment 3."""
        if self.total_time == 0:
            return 0.0
        return sum(self.checkpoint_times) / self.total_time
