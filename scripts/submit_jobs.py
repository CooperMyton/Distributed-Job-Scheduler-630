# scripts/submit_jobs.py
# Submit one or many jobs to the coordinator for testing and experiments.
#
# Usage:
#   python scripts/submit_jobs.py                              # 5 CountingTasks
#   python scripts/submit_jobs.py --count 20 --task CountingTask
#   python scripts/submit_jobs.py --count 10 --task SleepTask --sleep 15
#   python scripts/submit_jobs.py --count 5  --task FibonacciTask --n 100
#   python scripts/submit_jobs.py --count 5  --task PrimeSieveTask --limit 500
#   python scripts/submit_jobs.py --mixed 20  # mix of all task types
#   python scripts/submit_jobs.py --wait       # submit and poll until all complete

import argparse
import sys
import time
import random

import httpx

DEFAULT_COORDINATOR = "http://127.0.0.1:8000"


def submit_job(coordinator: str, payload: dict) -> str:
    """Submit one job, return its job_id."""
    r = httpx.post(f"{coordinator}/jobs", json={"payload": payload}, timeout=5.0)
    r.raise_for_status()
    return r.json()["job_id"]


def get_job(coordinator: str, job_id: str) -> dict:
    r = httpx.get(f"{coordinator}/jobs/{job_id}", timeout=5.0)
    r.raise_for_status()
    return r.json()


def get_metrics(coordinator: str) -> dict:
    r = httpx.get(f"{coordinator}/metrics", timeout=5.0)
    r.raise_for_status()
    return r.json()


def make_payload(task: str, args: argparse.Namespace) -> dict:
    """Build a task payload dict from CLI args."""
    if task == "CountingTask":
        return {
            "task_type":  "CountingTask",
            "target":     args.target,
            "step_sleep": args.step_sleep,
        }
    elif task == "FibonacciTask":
        return {
            "task_type":  "FibonacciTask",
            "n":          args.n,
            "step_sleep": args.step_sleep,
        }
    elif task == "PrimeSieveTask":
        return {
            "task_type":  "PrimeSieveTask",
            "limit":      args.limit,
            "step_sleep": args.step_sleep,
        }
    elif task == "SleepTask":
        return {
            "task_type":     "SleepTask",
            "total_seconds": args.sleep,
            "step_sec":      args.step_sec,
        }
    elif task == "FileWriterTask":
        return {
            "task_type":   "FileWriterTask",
            "total_lines": args.lines,
            "filepath":    f"output/file_task_{int(time.time())}.txt",
            "step_sleep":  args.step_sleep,
        }
    else:
        print(f"Unknown task type: {task}")
        sys.exit(1)


MIXED_TASKS = ["CountingTask", "FibonacciTask", "PrimeSieveTask", "SleepTask"]


def main():
    parser = argparse.ArgumentParser(description="Submit jobs to the Hot Takeover coordinator")
    parser.add_argument("--coordinator", default=DEFAULT_COORDINATOR)
    parser.add_argument("--count",    type=int, default=5,     help="Number of jobs to submit")
    parser.add_argument("--task",     default="CountingTask",  help="Task type")
    parser.add_argument("--mixed",    type=int, default=0,     help="Submit N jobs of mixed types")
    parser.add_argument("--wait",     action="store_true",     help="Poll until all jobs complete")
    parser.add_argument("--interval", type=float, default=0.1, help="Seconds between submissions")

    # Task-specific args
    parser.add_argument("--target",     type=int,   default=100,  help="CountingTask target")
    parser.add_argument("--n",          type=int,   default=50,   help="FibonacciTask n")
    parser.add_argument("--limit",      type=int,   default=200,  help="PrimeSieveTask limit")
    parser.add_argument("--sleep",      type=float, default=20.0, help="SleepTask total_seconds")
    parser.add_argument("--step-sec",   type=float, default=1.0,  help="SleepTask step_sec")
    parser.add_argument("--lines",      type=int,   default=50,   help="FileWriterTask total_lines")
    parser.add_argument("--step-sleep", type=float, default=0.05, dest="step_sleep",
                        help="Extra sleep per step (slows tasks for failure injection)")
    args = parser.parse_args()

    coordinator = args.coordinator
    count       = args.mixed if args.mixed > 0 else args.count

    # ── Submit jobs ────────────────────────────────────────────────────────────
    print(f"Submitting {count} jobs to {coordinator}...")
    job_ids = []

    for i in range(count):
        if args.mixed > 0:
            task = random.choice(MIXED_TASKS)
        else:
            task = args.task

        payload = make_payload(task, args)

        try:
            job_id = submit_job(coordinator, payload)
            job_ids.append(job_id)
            print(f"  [{i+1}/{count}] {task} → {job_id}")
            if args.interval > 0:
                time.sleep(args.interval)
        except Exception as e:
            print(f"  [{i+1}/{count}] FAILED: {e}")

    print(f"\nSubmitted {len(job_ids)} jobs.")

    # ── Optionally wait for completion ────────────────────────────────────────
    if not args.wait:
        print("Run with --wait to poll until all jobs complete.")
        print(f"Check status at: {coordinator}/metrics")
        return

    print("\nPolling until all jobs complete...")
    start = time.time()
    terminal_statuses = {"completed", "failed"}

    while True:
        statuses = {}
        for job_id in job_ids:
            try:
                job = get_job(coordinator, job_id)
                statuses[job_id] = job["status"]
            except Exception:
                statuses[job_id] = "unknown"

        counts = {}
        for s in statuses.values():
            counts[s] = counts.get(s, 0) + 1

        elapsed = time.time() - start
        print(f"  [{elapsed:.0f}s] {counts}", end="\r")

        done = all(s in terminal_statuses for s in statuses.values())
        if done:
            break
        time.sleep(1.0)

    elapsed = time.time() - start
    print(f"\n\nAll jobs finished in {elapsed:.1f}s")

    # ── Summary ────────────────────────────────────────────────────────────────
    completed = sum(1 for s in statuses.values() if s == "completed")
    failed    = sum(1 for s in statuses.values() if s == "failed")
    print(f"  Completed : {completed}/{len(job_ids)}")
    print(f"  Failed    : {failed}/{len(job_ids)}")
    print(f"  Success % : {100 * completed / len(job_ids):.1f}%")

    try:
        m = get_metrics(coordinator)
        print(f"\nSystem metrics:")
        print(f"  Total takeovers : {m.get('total_takeovers', 0)}")
        print(f"  Active workers  : {m.get('active_workers', 0)}")
    except Exception:
        pass

    # Print any failed jobs
    if failed > 0:
        print(f"\nFailed jobs:")
        for job_id, status in statuses.items():
            if status == "failed":
                try:
                    job = get_job(coordinator, job_id)
                    print(f"  {job_id}: {job.get('error', 'unknown error')}")
                except Exception:
                    print(f"  {job_id}: could not fetch details")


if __name__ == "__main__":
    main()