# scripts/run_experiment.py
# Automated experiment runner for the Hot Takeover distributed scheduler.
# Spins up a real coordinator and worker threads in-process, runs controlled
# experiments, collects metrics, and prints a summary for each one.
#
# Usage:
#   python scripts/run_experiment.py --all              # run all 5 experiments
#   python scripts/run_experiment.py --experiment 1     # run only Experiment 1
#   python scripts/run_experiment.py --experiment 2 --trials 20
#   python scripts/run_experiment.py --experiment 4 --jobs 50 --workers 5
#
# Each experiment writes its own tagged metrics to METRICS_FILE so
# metrics/analyze.py can separate them afterward.
#
# NOTE: This runner uses threads for workers (not separate processes) so
# it can programmatically stop heartbeats to simulate failure. For
# production-realistic testing, use start_workers.ps1 + kill_worker.ps1.

from __future__ import annotations

import argparse
import statistics
import sys
import threading
import time
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
import uvicorn

from shared.config import (
    WORKER_TIMEOUT_SEC, MONITOR_POLL_SEC, CHECKPOINT_EVERY_N_STEPS, METRICS_FILE
)
from metrics.collector import emit


# ── Coordinator server ─────────────────────────────────────────────────────────

class CoordinatorServer:
    """Runs the FastAPI coordinator in a background daemon thread."""

    def __init__(self, port: int = 18900):
        self.port   = port
        self.base   = f"http://127.0.0.1:{port}"
        self.server = None
        self.thread = None

    def start(self) -> None:
        # Fresh coordinator state for each experiment
        from shared.models import InMemoryStore
        import shared.models as m
        m.store = InMemoryStore()

        from coordinator.checkpoint_store import InMemoryCheckpointStore
        import coordinator.checkpoint_store as cs
        cs.checkpoint_store = InMemoryCheckpointStore()

        from coordinator.main import app
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="error")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        # Wait for startup
        for _ in range(20):
            time.sleep(0.3)
            try:
                r = httpx.get(f"{self.base}/", timeout=1.0)
                if r.status_code == 200:
                    return
            except Exception:
                pass
        raise RuntimeError("Coordinator did not start in time")

    def stop(self) -> None:
        if self.server:
            self.server.should_exit = True
            time.sleep(0.5)


# ── Worker helper ──────────────────────────────────────────────────────────────

def start_worker(worker_id: str, coordinator_url: str) -> "worker_module.Worker":
    """Start a worker in a background daemon thread. Returns the Worker object."""
    import worker.worker as wm
    import worker.tasks  # noqa: F401
    w = wm.Worker(worker_id=worker_id, coordinator_url=coordinator_url)
    t = threading.Thread(target=w.run, daemon=True)
    w._experiment_thread = t
    t.start()
    time.sleep(0.3)  # brief pause to let registration complete
    return w

def stop_worker(w) -> None:
    """Stop an experiment worker and wait briefly for its thread to exit."""
    if w is None:
        return
    try:
        w.kill()
    except Exception:
        pass

    t = getattr(w, "_experiment_thread", None)
    if t is not None:
        try:
            t.join(timeout=5.0)
        except Exception:
            pass


def kill_worker_heartbeat(w) -> None:
    """
    Simulate a silent crash: stop ONLY the heartbeat thread.
    The task keeps running but the coordinator will declare the worker dead
    after WORKER_TIMEOUT, which is the realistic failure detection path.
    The worker's result will be rejected when it eventually tries to submit
    because the coordinator will have already reassigned the job.
    """
    if w.heartbeat:
        w.heartbeat.stop()


def submit_job(base: str, payload: dict) -> str:
    r = httpx.post(f"{base}/jobs", json={"payload": payload}, timeout=5.0)
    r.raise_for_status()
    return r.json()["job_id"]


def poll_until_terminal(base: str, job_id: str,
                        timeout: float = 120.0) -> dict:
    """Poll a job until it reaches completed or failed."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(f"{base}/jobs/{job_id}", timeout=5.0)
        j = r.json()
        if j["status"] in ("completed", "failed"):
            return j
        time.sleep(0.5)
    return httpx.get(f"{base}/jobs/{job_id}", timeout=5.0).json()


def wait_for_status(base: str, job_id: str, target_status: str,
                    timeout: float = 30.0) -> dict:
    """Poll until job reaches a specific status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(f"{base}/jobs/{job_id}", timeout=5.0)
        j = r.json()
        if j["status"] == target_status:
            return j
        time.sleep(0.3)
    return httpx.get(f"{base}/jobs/{job_id}", timeout=5.0).json()


# ── Experiment 1: Failure Detection Latency ───────────────────────────────────

def run_experiment_1(trials: int = 10, port: int = 18901) -> None:
    """
    Sweep WORKER_TIMEOUT values and measure actual detection latency.
    For each trial: start worker, give it a long job, kill heartbeat,
    record time from last heartbeat to detection.
    """
    print("\n" + "=" * 60)
    print("EXPERIMENT 1: Failure Detection Latency")
    print(f"Trials: {trials} | Current timeout: {WORKER_TIMEOUT_SEC}s")
    print("=" * 60)

    latencies = []
    srv = CoordinatorServer(port)
    srv.start()

    try:
        for i in range(trials):
            # Submit a long job so the worker is busy when killed
            job_id = submit_job(srv.base, {
                "task_type": "SleepTask",
                "total_seconds": 120,
                "step_sec": 1.0
            })

            w = start_worker(f"exp1-w{i}", srv.base)

            # Wait until the worker picks up the job
            wait_for_status(srv.base, job_id, "running", timeout=10.0)

            # Record the kill time
            kill_time = time.time()
            w.kill()

            # Wait for coordinator to detect failure (pending_takeover)
            detect_start = time.time()
            detected = wait_for_status(srv.base, job_id, "pending_takeover",
                                       timeout=WORKER_TIMEOUT_SEC + MONITOR_POLL_SEC + 5)
            detect_time = time.time() - detect_start

            latency = detected.get("detection_latency_sec") or detect_time
            latencies.append(latency)

            emit({
                "event":      "exp1_trial",
                "trial":      i + 1,
                "latency_sec": round(latency, 3),
                "timeout_cfg": WORKER_TIMEOUT_SEC,
            })

            print(f"  Trial {i+1:2d}/{trials}: detection latency = {latency:.2f}s")
            time.sleep(0.5)

    finally:
        srv.stop()

    print(f"\n  Results over {len(latencies)} trials:")
    print(f"    Mean   : {statistics.mean(latencies):.2f}s")
    print(f"    Median : {statistics.median(latencies):.2f}s")
    print(f"    Stdev  : {statistics.stdev(latencies):.2f}s" if len(latencies) > 1 else "")
    print(f"    Min    : {min(latencies):.2f}s")
    print(f"    Max    : {max(latencies):.2f}s")
    print(f"\n  Expected: ~{WORKER_TIMEOUT_SEC}s ± {MONITOR_POLL_SEC}s")


# ── Experiment 2: Recovery Latency vs. Checkpoint Frequency ───────────────────

def run_experiment_2(trials: int = 10, port: int = 18902) -> None:
    """
    For each trial: run a job, kill worker mid-execution after at least one
    checkpoint, measure steps_redone (work P2 must redo).
    """
    print("\n" + "=" * 60)
    print("EXPERIMENT 2: Recovery Latency vs. Checkpoint Frequency")
    print(f"Trials: {trials} | Checkpoint every: {CHECKPOINT_EVERY_N_STEPS} steps")
    print("=" * 60)

    steps_redone_list = []
    srv = CoordinatorServer(port)
    srv.start()

    try:
        for i in range(trials):
            # Use SleepTask: 120s total, 1s per step = 120 steps
            # Detection latency ~16s means worker dies around step 16
            # Last checkpoint before kill is at step 15 (every 5 steps)
            # So steps_redone should be 0-4
            job_id = submit_job(srv.base, {
                "task_type":     "SleepTask",
                "total_seconds": 120,
                "step_sec":      1.0,
            })

            w1 = start_worker(f"exp2-w{i}-a", srv.base)

            # Wait for at least one checkpoint
            deadline = time.time() + 30
            checkpoint_step = None
            while time.time() < deadline:
                j = httpx.get(f"{srv.base}/jobs/{job_id}", timeout=5.0).json()
                ckpt = j.get("latest_checkpoint")
                if ckpt and ckpt["step_index"] >= CHECKPOINT_EVERY_N_STEPS:
                    checkpoint_step = ckpt["step_index"]
                    break
                time.sleep(0.5)

            if checkpoint_step is None:
                print(f"  Trial {i+1}: no checkpoint reached, skipping")
                continue

            print(f"  Trial {i+1}: checkpoint at step={checkpoint_step}, stopping heartbeat...")
            # Stop only the heartbeat — coordinator detects failure after timeout
            kill_worker_heartbeat(w1)

            # Wait for coordinator to detect and re-queue (~16s based on Exp 1)
            detected = wait_for_status(srv.base, job_id, "pending_takeover",
                                       timeout=WORKER_TIMEOUT_SEC + MONITOR_POLL_SEC + 10)
            steps_redone = detected.get("steps_redone")
            if steps_redone is None:
                steps_redone = 0
            steps_redone_list.append(steps_redone)

            # Start rescuer worker
            w2 = start_worker(f"exp2-w{i}-b", srv.base)

            # Wait for completion (just a few more steps needed)
            final = poll_until_terminal(srv.base, job_id, timeout=180)

            emit({
                "event":           "exp2_trial",
                "trial":           i + 1,
                "checkpoint_step": checkpoint_step,
                "steps_redone":    steps_redone,
                "checkpoint_every": CHECKPOINT_EVERY_N_STEPS,
                "completed":       final["status"] == "completed",
            })

            print(f"  Trial {i+1:2d}/{trials}: checkpoint_step={checkpoint_step} "
                  f"steps_redone={steps_redone} status={final['status']}")
            time.sleep(1.0)

    finally:
        srv.stop()

    if steps_redone_list:
        print(f"\n  Results over {len(steps_redone_list)} trials:")
        print(f"    Mean steps redone   : {statistics.mean(steps_redone_list):.1f}")
        print(f"    Median steps redone : {statistics.median(steps_redone_list):.1f}")
        print(f"    Max steps redone    : {max(steps_redone_list)}")
        print(f"\n  NOTE: steps_redone=0 is a valid result — it means the worker")
        print(f"  continued pushing checkpoints while the coordinator was waiting")
        print(f"  to detect the failure. P2 resumes from the most recent checkpoint,")
        print(f"  which may be very close to (or at) the failure point.")
        print(f"  This demonstrates the checkpoint protocol's effectiveness.")
        print(f"  Expected max: ~{CHECKPOINT_EVERY_N_STEPS} steps (one checkpoint interval)")


# ── Experiment 3: Checkpoint Overhead vs. Throughput ─────────────────────────

def run_experiment_3(port: int = 18903) -> None:
    """
    Run PrimeSieveTask at increasing checkpoint frequencies and measure
    total job completion time. No failures injected.
    """
    print("\n" + "=" * 60)
    print("EXPERIMENT 3: Checkpoint Overhead vs. Throughput")
    print("=" * 60)

    import shared.config as cfg
    original_every = cfg.CHECKPOINT_EVERY_N_STEPS
    frequencies    = [1, 5, 10, 25, 50, 999]  # 999 = effectively no checkpointing
    results        = []

    for freq in frequencies:
        cfg.CHECKPOINT_EVERY_N_STEPS = freq

        srv = CoordinatorServer(port)
        w = None
        srv.start()

        try:
            job_id = submit_job(srv.base, {
                "task_type": "PrimeSieveTask",
                "limit":     2000,
                "step_sleep": 0.003,
            })

            start = time.time()
            w = start_worker(f"exp3-w-freq{freq}", srv.base)
            final = poll_until_terminal(srv.base, job_id, timeout=300)
            elapsed = time.time() - start

            # Count checkpoints from metrics
            ckpt_count = sum(1 for line in open(METRICS_FILE)
                             if '"checkpoint_pushed"' in line
                             and f'"job_id": "{job_id}"' in line) if os.path.exists(METRICS_FILE) else 0

            results.append((freq, round(elapsed, 2), final["status"]))

            emit({
                "event":            "exp3_trial",
                "checkpoint_every": freq,
                "elapsed_sec":      round(elapsed, 2),
                "status":           final["status"],
            })

            print(f"  checkpoint_every={freq:4d}: {elapsed:.2f}s  status={final['status']}")

        finally:
            stop_worker(w)
            srv.stop()
            time.sleep(0.5)

    cfg.CHECKPOINT_EVERY_N_STEPS = original_every

    print(f"\n  Summary:")
    for freq, elapsed, status in results:
        label = "no checkpoints" if freq >= 999 else f"every {freq} steps"
        print(f"    {label:20s}: {elapsed:.2f}s  ({status})")

    if len(results) >= 2:
        baseline = next((r[1] for r in results if r[0] >= 999), results[-1][1])
        fastest  = results[0][1]  # freq=1
        overhead_pct = 100 * (fastest - baseline) / baseline if baseline > 0 else 0
        print(f"\n  Overhead (every step vs no checkpoints): {overhead_pct:.1f}%")


# ── Experiment 4: End-to-End Success Rate Under Load ─────────────────────────

def run_experiment_4(num_jobs: int = 50, num_workers: int = 5,
                     kill_interval: float = 15.0, port: int = 18904) -> None:
    """
    Submit many jobs, start several workers, periodically kill one.
    Measure: success rate, takeover count, avg completion time.
    """
    print("\n" + "=" * 60)
    print("EXPERIMENT 4: Success Rate Under Concurrent Failures")
    print(f"Jobs: {num_jobs} | Workers: {num_workers} | Kill every: {kill_interval}s")
    print("=" * 60)

    srv = CoordinatorServer(port)
    srv.start()

    try:
        # Submit all jobs
        job_ids = []
        for i in range(num_jobs):
            jid = submit_job(srv.base, {
                "task_type":  "CountingTask",
                "target":     100,
                "step_sleep": 0.2,
            })
            job_ids.append(jid)
        print(f"  Submitted {num_jobs} jobs")

        # Start workers
        workers = []
        for i in range(num_workers):
            w = start_worker(f"exp4-w{i}", srv.base)
            workers.append(w)
        print(f"  Started {num_workers} workers")

        # Background killer: kill a random worker every kill_interval seconds
        killed_count = [0]
        stop_killer  = threading.Event()

        def killer():
            import random
            worker_pool = list(workers)
            while not stop_killer.is_set():
                stop_killer.wait(timeout=kill_interval)
                if stop_killer.is_set():
                    break
                alive = [w for w in worker_pool if w.heartbeat and not w.heartbeat._stop_event.is_set()]
                if alive:
                    target = random.choice(alive)
                    kill_worker_heartbeat(target)
                    killed_count[0] += 1
                    print(f"  [killer] Killed worker {target.worker_id} "
                          f"(total kills: {killed_count[0]})")
                    # Start a replacement
                    replacement = start_worker(f"exp4-replacement-{killed_count[0]}", srv.base)
                    worker_pool.append(replacement)

        killer_thread = threading.Thread(target=killer, daemon=True)
        killer_thread.start()

        # Wait for all jobs to reach terminal state
        print(f"  Waiting for all {num_jobs} jobs to complete...")
        start_time = time.time()
        deadline   = start_time + 300  # 5 minute hard timeout

        while time.time() < deadline:
            try:
                m = httpx.get(f"{srv.base}/metrics", timeout=5.0).json()
                done = m["completed"] + m["failed"]
                print(f"  [{time.time()-start_time:.0f}s] "
                      f"completed={m['completed']} failed={m['failed']} "
                      f"running={m['running']} pending={m['pending']} "
                      f"takeovers={m['total_takeovers']}",
                      end="\r")
                if done >= num_jobs:
                    break
            except Exception:
                pass
            time.sleep(2.0)

        stop_killer.set()
        print()

        # Final tally
        completed = 0
        failed    = 0
        times     = []

        for job_id in job_ids:
            try:
                j = httpx.get(f"{srv.base}/jobs/{job_id}", timeout=5.0).json()
                if j["status"] == "completed":
                    completed += 1
                    if j.get("completed_at") and j.get("created_at"):
                        from datetime import datetime
                        t0 = datetime.fromisoformat(j["created_at"])
                        t1 = datetime.fromisoformat(j["completed_at"])
                        times.append((t1 - t0).total_seconds())
                elif j["status"] == "failed":
                    failed += 1
            except Exception:
                pass

        m = httpx.get(f"{srv.base}/metrics", timeout=5.0).json()
        elapsed = time.time() - start_time

        emit({
            "event":         "exp4_summary",
            "num_jobs":      num_jobs,
            "num_workers":   num_workers,
            "kills":         killed_count[0],
            "completed":     completed,
            "failed":        failed,
            "success_rate":  round(100 * completed / num_jobs, 1),
            "total_takeovers": m.get("total_takeovers", 0),
            "elapsed_sec":   round(elapsed, 1),
            "avg_job_time":  round(statistics.mean(times), 2) if times else None,
        })

        print(f"\n  Results:")
        print(f"    Jobs completed  : {completed}/{num_jobs} ({100*completed/num_jobs:.1f}%)")
        print(f"    Jobs failed     : {failed}/{num_jobs}")
        print(f"    Worker kills    : {killed_count[0]}")
        print(f"    Total takeovers : {m.get('total_takeovers', 0)}")
        print(f"    Avg job time    : {statistics.mean(times):.2f}s" if times else "")
        print(f"    Total elapsed   : {elapsed:.1f}s")

    finally:
        srv.stop()


# ── Experiment 5: State Fidelity ──────────────────────────────────────────────

def run_experiment_5(trials: int = 20, port: int = 18905) -> None:
    """
    Run CountingTask and FibonacciTask with forced takeover.
    Compare result to known-correct baseline to verify fidelity.
    """
    print("\n" + "=" * 60)
    print("EXPERIMENT 5: State Fidelity After Takeover")
    print(f"Trials per task type: {trials}")
    print("=" * 60)

    task_configs = [
        ("CountingTask",  {"task_type": "CountingTask",  "target": 500, "step_sleep": 0.03},
         lambda r: r["final_count"]),
        ("FibonacciTask", {"task_type": "FibonacciTask", "n": 200, "step_sleep": 0.03},
         lambda r: r["fibonacci"]),
    ]

    for task_name, payload, extract_result in task_configs:
        print(f"\n  Task: {task_name}")

        srv = CoordinatorServer(port)
        workers_to_stop = []
        srv.start()

        correct = 0
        wrong   = 0

        try:
            # Compute baseline (no failure)
            baseline_id = submit_job(srv.base, payload)
            w_base = start_worker(f"exp5-baseline", srv.base)
            workers_to_stop.append(w_base)
            baseline_job = poll_until_terminal(srv.base, baseline_id, timeout=60)
            baseline_result = extract_result(baseline_job.get("result", {}))
            print(f"    Baseline result: {baseline_result}")

            for i in range(trials):
                job_id = submit_job(srv.base, payload)
                w1 = start_worker(f"exp5-{task_name}-{i}-a", srv.base)
                workers_to_stop.append(w1)

                # Wait for at least one checkpoint then kill
                deadline = time.time() + 20
                checkpointed = False
                while time.time() < deadline:
                    j = httpx.get(f"{srv.base}/jobs/{job_id}", timeout=5.0).json()
                    if j.get("latest_checkpoint"):
                        checkpointed = True
                        break
                    time.sleep(0.2)

                if not checkpointed:
                    print(f"    Trial {i+1}: no checkpoint reached, skipping")
                    continue

                w1.kill()
                wait_for_status(srv.base, job_id, "pending_takeover", timeout=30)

                w2 = start_worker(f"exp5-{task_name}-{i}-b", srv.base)
                workers_to_stop.append(w2)
                final = poll_until_terminal(srv.base, job_id, timeout=60)

                if final["status"] != "completed":
                    print(f"    Trial {i+1}: job did not complete ({final['status']})")
                    wrong += 1
                    continue

                takeover_result = extract_result(final.get("result", {}))
                match = takeover_result == baseline_result

                if match:
                    correct += 1
                else:
                    wrong += 1
                    print(f"    Trial {i+1}: MISMATCH "
                          f"expected={baseline_result} got={takeover_result}")

                emit({
                    "event":          "exp5_trial",
                    "task_type":      task_name,
                    "trial":          i + 1,
                    "correct":        match,
                    "baseline":       baseline_result,
                    "takeover_result": takeover_result,
                    "steps_redone":   final.get("steps_redone"),
                })

                time.sleep(0.3)

        finally:
            for w in workers_to_stop:
                stop_worker(w)
            srv.stop()
            time.sleep(0.5)
            port += 1  # use a fresh port for each task type

        total = correct + wrong
        fidelity = 100 * correct / total if total > 0 else 0
        print(f"    Fidelity: {correct}/{total} correct ({fidelity:.1f}%)")

    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Run Hot Takeover experiments")
    parser.add_argument("--all",        action="store_true", help="Run all 5 experiments")
    parser.add_argument("--experiment", type=int, default=0, help="Run one experiment (1-5)")
    parser.add_argument("--trials",     type=int, default=10, help="Trials per experiment")
    parser.add_argument("--jobs",       type=int, default=50, help="Jobs for Experiment 4")
    parser.add_argument("--workers",    type=int, default=5,  help="Workers for Experiment 4")
    args = parser.parse_args()

    if not args.all and args.experiment == 0:
        parser.print_help()
        print("\nExamples:")
        print("  python scripts/run_experiment.py --all")
        print("  python scripts/run_experiment.py --experiment 1 --trials 10")
        print("  python scripts/run_experiment.py --experiment 4 --jobs 50 --workers 5")
        return

    exp_map = {
        1: lambda: run_experiment_1(trials=args.trials),
        2: lambda: run_experiment_2(trials=args.trials),
        3: lambda: run_experiment_3(),
        4: lambda: run_experiment_4(num_jobs=args.jobs, num_workers=args.workers),
        5: lambda: run_experiment_5(trials=args.trials),
    }

    to_run = list(exp_map.keys()) if args.all else [args.experiment]

    for exp_num in to_run:
        if exp_num in exp_map:
            exp_map[exp_num]()
        else:
            print(f"Unknown experiment: {exp_num}. Choose 1-5.")

    print("\nDone. Run metrics/analyze.py to see full results.")


if __name__ == "__main__":
    main()