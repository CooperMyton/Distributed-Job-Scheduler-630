# worker/tasks.py
# Concrete CheckpointableTask implementations used for experiments and demos.
#
# Each task is designed to validate a different aspect of the checkpoint contract:
#
#   CountingTask    -- Simplest possible stateful task. State is a single integer.
#                      Used in Experiment 5 (fidelity): easy to verify P2's result
#                      matches the uninterrupted baseline exactly.
#
#   FibonacciTask   -- Tiny state (two integers). Many fast steps. Used in
#                      Experiment 3 (overhead): frequent checkpointing of small
#                      state isolates the I/O cost from the compute cost.
#
#   PrimeSieveTask  -- CPU-bound. State grows as primes list grows. Used in
#                      Experiment 3 (overhead): shows how checkpoint size affects
#                      overhead, and whether large state breaks the contract.
#
#   FileWriterTask  -- Has a side effect (writes to disk). Demonstrates idempotency:
#                      on resume, the task seeks to the correct line rather than
#                      re-writing from the top. Critical for at-least-once semantics.
#
#   SleepTask       -- Simulates a long-running job with no useful intermediate state.
#                      Used in live demos and Experiments 1 & 4: easy to kill mid-sleep
#                      and observe takeover. Also shows what happens when checkpoint
#                      state is minimal (P2 must redo all remaining sleep).

from __future__ import annotations

import math
import os
import time
from typing import Tuple

from worker.executor import CheckpointableTask, register_task


# ── 1. CountingTask ────────────────────────────────────────────────────────────

@register_task
class CountingTask(CheckpointableTask):
    """
    Counts from 0 to 'target', incrementing by 1 per step.

    State: {"count": int, "target": int}

    Correctness guarantee: P2's final result (count == target) is identical to
    P1's result regardless of when the takeover happened. This makes it the
    primary task for Experiment 5 (fidelity verification).
    """

    def __init__(self, target: int, step_sleep: float = 0.0):
        self.target     = target
        self.step_sleep = step_sleep  # Slow down steps for easier failure injection

    @classmethod
    def from_payload(cls, payload: dict) -> "CountingTask":
        return cls(
            target=payload["target"],
            step_sleep=payload.get("step_sleep", 0.0),
        )

    def run_step(self, state: dict) -> Tuple[dict, bool]:
        # Initialize from state or start fresh
        count  = state.get("count", 0)
        target = state.get("target", self.target)

        count += 1
        if self.step_sleep > 0:
            time.sleep(self.step_sleep)

        new_state = {"count": count, "target": target}
        is_done   = count >= target
        return new_state, is_done

    def get_result(self, final_state: dict) -> dict:
        return {
            "final_count": final_state["count"],
            "target":      final_state["target"],
            "success":     final_state["count"] >= final_state["target"],
        }


# ── 2. FibonacciTask ──────────────────────────────────────────────────────────

@register_task
class FibonacciTask(CheckpointableTask):
    """
    Computes the N-th Fibonacci number iteratively.

    State: {"a": int, "b": int, "steps_done": int, "n": int}

    Tiny state (three small integers) means checkpoint serialization is nearly
    free. Used in Experiment 3 to establish the baseline overhead cost of the
    checkpoint I/O infrastructure independent of state size.
    """

    def __init__(self, n: int, step_sleep: float = 0.0):
        self.n          = n
        self.step_sleep = step_sleep

    @classmethod
    def from_payload(cls, payload: dict) -> "FibonacciTask":
        return cls(n=payload["n"], step_sleep=payload.get("step_sleep", 0.0))

    def run_step(self, state: dict) -> Tuple[dict, bool]:
        a          = state.get("a", 0)
        b          = state.get("b", 1)
        steps_done = state.get("steps_done", 0)
        n          = state.get("n", self.n)

        if self.step_sleep > 0:
            time.sleep(self.step_sleep)

        # One Fibonacci step
        a, b       = b, a + b
        steps_done += 1

        new_state = {"a": a, "b": b, "steps_done": steps_done, "n": n}
        is_done   = steps_done >= n
        return new_state, is_done

    def get_result(self, final_state: dict) -> dict:
        # After n steps, 'a' holds fib(n)
        return {
            "n":          final_state["n"],
            "fibonacci":  final_state["a"],
            "steps_done": final_state["steps_done"],
        }


# ── 3. PrimeSieveTask ─────────────────────────────────────────────────────────

@register_task
class PrimeSieveTask(CheckpointableTask):
    """
    Finds all prime numbers up to 'limit' using trial division, one candidate
    per step.

    State: {"checked_up_to": int, "primes": list[int], "limit": int}

    State size grows as primes are found. Used in Experiment 3 to measure
    how checkpoint overhead scales with state size — the larger the prime
    list, the more bytes must be serialized and transmitted per checkpoint.

    Also CPU-bound (each step does trial division), making it a realistic
    proxy for compute-heavy distributed jobs.
    """

    def __init__(self, limit: int, step_sleep: float = 0.0):
        self.limit      = limit
        self.step_sleep = step_sleep

    @classmethod
    def from_payload(cls, payload: dict) -> "PrimeSieveTask":
        return cls(limit=payload["limit"], step_sleep=payload.get("step_sleep", 0.0))

    def run_step(self, state: dict) -> Tuple[dict, bool]:
        checked = state.get("checked_up_to", 1)   # Last number fully checked
        primes  = state.get("primes", [])
        limit   = state.get("limit", self.limit)

        candidate = checked + 1

        if self.step_sleep > 0:
            time.sleep(self.step_sleep)

        # Trial division primality test for this candidate
        is_prime = candidate >= 2 and all(
            candidate % p != 0
            for p in primes
            if p <= math.isqrt(candidate)
        )

        if is_prime:
            primes = primes + [candidate]   # New list (don't mutate state in place)

        new_state = {
            "checked_up_to": candidate,
            "primes":        primes,
            "limit":         limit,
        }
        is_done = candidate >= limit
        return new_state, is_done

    def get_result(self, final_state: dict) -> dict:
        primes = final_state["primes"]
        return {
            "limit":       final_state["limit"],
            "prime_count": len(primes),
            "largest":     primes[-1] if primes else None,
            "primes":      primes,
        }


# ── 4. FileWriterTask ─────────────────────────────────────────────────────────

@register_task
class FileWriterTask(CheckpointableTask):
    """
    Writes 'total_lines' lines to a file, one line per step.

    State: {"lines_written": int, "total_lines": int, "filepath": str}

    IDEMPOTENCY: On resume after takeover, the task opens the file in write
    mode and seeks to the correct line count rather than appending. This
    prevents duplicate lines if P2 resumes from a checkpoint that reflects
    N lines written but the file already contains those lines.

    Demonstrates that at-least-once execution semantics can be made safe
    for IO-bound tasks through careful state design.
    """

    def __init__(self, total_lines: int, filepath: str, step_sleep: float = 0.0):
        self.total_lines = total_lines
        self.filepath    = filepath
        self.step_sleep  = step_sleep

    @classmethod
    def from_payload(cls, payload: dict) -> "FileWriterTask":
        return cls(
            total_lines=payload["total_lines"],
            filepath=payload.get("filepath", f"output_{payload.get('job_id', 'unknown')}.txt"),
            step_sleep=payload.get("step_sleep", 0.0),
        )

    def run_step(self, state: dict) -> Tuple[dict, bool]:
        lines_written = state.get("lines_written", 0)
        total_lines   = state.get("total_lines", self.total_lines)
        filepath      = state.get("filepath", self.filepath)

        # IDEMPOTENCY: rewrite the file up to lines_written, then append one more.
        # This ensures that if we resume from checkpoint at line N, we don't
        # produce a file with lines 0..N duplicated followed by N+1..end.
        if lines_written == 0:
            # First step or fresh start: create/truncate
            mode = "w"
        else:
            # Resuming: rewrite exactly lines_written lines then continue
            # (for simplicity in this implementation, we track and seek via
            # line count rather than byte offset)
            mode = "w"

        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)

        # Read existing correct lines if resuming
        existing_lines = []
        if lines_written > 0 and os.path.exists(filepath):
            with open(filepath, "r") as f:
                existing_lines = f.readlines()
            # Truncate to exactly lines_written (discards any lines beyond checkpoint)
            existing_lines = existing_lines[:lines_written]

        # Append the new line
        new_line = f"Line {lines_written + 1} of {total_lines}\n"
        all_lines = existing_lines + [new_line]

        with open(filepath, "w") as f:
            f.writelines(all_lines)

        if self.step_sleep > 0:
            time.sleep(self.step_sleep)

        lines_written += 1
        new_state = {
            "lines_written": lines_written,
            "total_lines":   total_lines,
            "filepath":      filepath,
        }
        is_done = lines_written >= total_lines
        return new_state, is_done

    def get_result(self, final_state: dict) -> dict:
        filepath      = final_state["filepath"]
        lines_written = final_state["lines_written"]
        # Verify the file has exactly the right number of lines
        actual_lines = 0
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                actual_lines = sum(1 for _ in f)
        return {
            "filepath":      filepath,
            "lines_written": lines_written,
            "lines_on_disk": actual_lines,
            "correct":       actual_lines == lines_written,
        }


# ── 5. SleepTask ──────────────────────────────────────────────────────────────

@register_task
class SleepTask(CheckpointableTask):
    """
    Simulates a long-running job by sleeping in small increments.

    State: {"seconds_elapsed": float, "total_seconds": float, "step_sec": float}

    Used in live demos and Experiments 1 & 4 because:
    - Easy to kill mid-execution and observe takeover
    - Duration is controlled, so failure can be injected at predictable points
    - Minimal state means checkpoint overhead is negligible (isolates detection latency)

    Note: if P1 dies at elapsed=T and last checkpoint was at elapsed=C,
    P2 must re-sleep (T-C) seconds of work. This re-work is the steps_redone
    metric tracked in Experiment 2.
    """

    def __init__(self, total_seconds: float, step_sec: float = 0.5, step_sleep: float = 0.0):
        self.total_seconds = total_seconds
        self.step_sec      = step_sec      # How many seconds each step simulates
        self.step_sleep    = step_sleep    # Additional artificial slowdown if needed

    @classmethod
    def from_payload(cls, payload: dict) -> "SleepTask":
        return cls(
            total_seconds=payload["total_seconds"],
            step_sec=payload.get("step_sec", 0.5),
            step_sleep=payload.get("step_sleep", 0.0),
        )

    def run_step(self, state: dict) -> Tuple[dict, bool]:
        elapsed       = state.get("seconds_elapsed", 0.0)
        total_seconds = state.get("total_seconds", self.total_seconds)
        step_sec      = state.get("step_sec", self.step_sec)

        time.sleep(step_sec)   # The actual simulated work
        if self.step_sleep > 0:
            time.sleep(self.step_sleep)

        elapsed += step_sec
        new_state = {
            "seconds_elapsed": round(elapsed, 4),
            "total_seconds":   total_seconds,
            "step_sec":        step_sec,
        }
        is_done = elapsed >= total_seconds
        return new_state, is_done

    def get_result(self, final_state: dict) -> dict:
        return {
            "seconds_elapsed": final_state["seconds_elapsed"],
            "total_seconds":   final_state["total_seconds"],
            "completed":       True,
        }
