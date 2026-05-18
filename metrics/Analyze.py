# metrics/analyze.py
# Reads metrics.jsonl and computes statistics for all five experiments.
#
# Usage:
#   python metrics/analyze.py                        # analyze default metrics.jsonl
#   python metrics/analyze.py --file my_run.jsonl    # analyze a specific file
#   python metrics/analyze.py --experiment 1         # only print Experiment 1
#   python metrics/analyze.py --csv                  # also write results to CSV
#
# Output: printed summary tables for each experiment, plus optional CSV files
# that can be imported directly into Tableau or Excel for plotting.

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
from collections import defaultdict
from typing import Any

from shared.config import METRICS_FILE


# ── Load events ────────────────────────────────────────────────────────────────

def load_events(filepath: str) -> list[dict]:
    """Load all events from a JSONL metrics file."""
    if not os.path.exists(filepath):
        print(f"Metrics file not found: {filepath}")
        print("Run an experiment first with: python scripts/run_experiment.py")
        return []
    events = []
    with open(filepath) as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"  Warning: skipping malformed line {i}: {e}")
    print(f"Loaded {len(events)} events from {filepath}\n")
    return events


def filter_events(events: list[dict], event_type: str) -> list[dict]:
    return [e for e in events if e.get("event") == event_type]


def safe_stats(values: list[float]) -> dict:
    """Return mean, median, stdev, min, max for a list of floats."""
    if not values:
        return {"count": 0, "mean": None, "median": None,
                "stdev": None, "min": None, "max": None}
    return {
        "count":  len(values),
        "mean":   round(statistics.mean(values), 3),
        "median": round(statistics.median(values), 3),
        "stdev":  round(statistics.stdev(values), 3) if len(values) > 1 else 0.0,
        "min":    round(min(values), 3),
        "max":    round(max(values), 3),
    }


def print_table(headers: list[str], rows: list[list[str]], title: str = "") -> None:
    """Print a formatted ASCII table."""
    if title:
        print(f"  {title}")
        print("  " + "-" * (sum(len(h) + 3 for h in headers)))
    col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
                  for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("  " + "  ".join("-" * w for w in col_widths))
    for row in rows:
        print(fmt.format(*[str(x) for x in row]))
    print()


def write_csv(filepath: str, headers: list[str], rows: list[list]) -> None:
    os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else ".", exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"  CSV written: {filepath}")


# ── Experiment 1: Failure Detection Latency ───────────────────────────────────

def experiment_1(events: list[dict], write_csv_flag: bool) -> None:
    """
    Research question: How quickly does the coordinator detect a dead worker?
    Metric: detection_latency_sec from worker_died events.
    """
    print("=" * 60)
    print("EXPERIMENT 1: Failure Detection Latency")
    print("=" * 60)

    died_events = filter_events(events, "worker_died")
    if not died_events:
        print("  No worker_died events found. Run an experiment that kills workers.\n")
        return

    latencies = [e["detection_latency_sec"] for e in died_events
                 if "detection_latency_sec" in e]

    stats = safe_stats(latencies)
    print(f"  Worker failures detected: {stats['count']}")
    print(f"  Mean detection latency  : {stats['mean']}s")
    print(f"  Median                  : {stats['median']}s")
    print(f"  Std dev                 : {stats['stdev']}s")
    print(f"  Min / Max               : {stats['min']}s / {stats['max']}s")
    print()

    # Group by silence_sec buckets if experiment swept WORKER_TIMEOUT
    rows = [[round(e.get("detection_latency_sec", 0), 2),
             e.get("worker_id", "?")[:8],
             e.get("had_job", False)]
            for e in died_events]
    print_table(["latency_sec", "worker_id", "had_job"], rows,
                "Per-failure breakdown:")

    if write_csv_flag:
        write_csv("results/exp1_detection_latency.csv",
                  ["worker_id", "detection_latency_sec", "had_job", "timestamp"],
                  [[e.get("worker_id","?"), e.get("detection_latency_sec",""),
                    e.get("had_job",""), e.get("timestamp","")]
                   for e in died_events])


# ── Experiment 2: Recovery Latency vs. Checkpoint Frequency ───────────────────

def experiment_2(events: list[dict], write_csv_flag: bool) -> None:
    """
    Research question: How does checkpoint interval affect steps P2 must redo?
    Metric: steps_redone from job_completed events where takeover=True.
    """
    print("=" * 60)
    print("EXPERIMENT 2: Recovery Latency vs. Checkpoint Frequency")
    print("=" * 60)

    completed = filter_events(events, "job_completed")
    takeover_jobs = [e for e in completed if e.get("takeover")]

    if not takeover_jobs:
        print("  No completed takeover jobs found.\n")
        return

    steps_redone_vals = [e["steps_redone"] for e in takeover_jobs
                         if e.get("steps_redone") is not None]
    latency_vals      = [e["total_time_sec"] for e in takeover_jobs
                         if e.get("total_time_sec") is not None]

    print(f"  Takeover jobs completed : {len(takeover_jobs)}")

    if steps_redone_vals:
        s = safe_stats(steps_redone_vals)
        print(f"  Steps redone (mean)     : {s['mean']} steps")
        print(f"  Steps redone (min/max)  : {s['min']} / {s['max']} steps")

    if latency_vals:
        l = safe_stats(latency_vals)
        print(f"  Total job time (mean)   : {l['mean']}s")
    print()

    rows = [[e.get("job_id","?")[:8],
             e.get("steps_redone","?"),
             round(e.get("total_time_sec", 0), 2)]
            for e in takeover_jobs]
    print_table(["job_id", "steps_redone", "total_time_sec"], rows,
                "Per-takeover breakdown:")

    if write_csv_flag:
        write_csv("results/exp2_recovery_latency.csv",
                  ["job_id", "steps_redone", "total_time_sec", "timestamp"],
                  [[e.get("job_id","?"), e.get("steps_redone",""),
                    e.get("total_time_sec",""), e.get("timestamp","")]
                   for e in takeover_jobs])


# ── Experiment 3: Checkpoint Overhead vs. Throughput ─────────────────────────

def experiment_3(events: list[dict], write_csv_flag: bool) -> None:
    """
    Research question: What is the performance cost of checkpointing?
    Metric: checkpoint size and frequency vs. job completion time.
    """
    print("=" * 60)
    print("EXPERIMENT 3: Checkpoint Overhead vs. Throughput")
    print("=" * 60)

    ckpt_events     = filter_events(events, "checkpoint_pushed")
    completed_events = filter_events(events, "job_completed")

    if not ckpt_events:
        print("  No checkpoint_pushed events found.\n")
        return

    # Checkpoints per job
    ckpts_per_job: dict[str, list] = defaultdict(list)
    for e in ckpt_events:
        ckpts_per_job[e.get("job_id","?")].append(e)

    sizes      = [e["size_bytes"] for e in ckpt_events if "size_bytes" in e]
    size_stats = safe_stats(sizes)

    print(f"  Total checkpoints pushed : {len(ckpt_events)}")
    print(f"  Jobs with checkpoints    : {len(ckpts_per_job)}")
    print(f"  Avg checkpoints/job      : {round(len(ckpt_events)/max(len(ckpts_per_job),1), 1)}")
    print(f"  Checkpoint size (mean)   : {size_stats['mean']} bytes")
    print(f"  Checkpoint size (max)    : {size_stats['max']} bytes")
    print()

    # Per-job summary
    rows = []
    for job_id, ckpts in list(ckpts_per_job.items())[:15]:
        job_sizes = [c.get("size_bytes", 0) for c in ckpts]
        completed = next((e for e in completed_events if e.get("job_id") == job_id), None)
        total_time = round(completed["total_time_sec"], 2) if completed and completed.get("total_time_sec") else "?"
        rows.append([job_id[:8], len(ckpts),
                     round(statistics.mean(job_sizes), 0) if job_sizes else "?",
                     total_time])
    print_table(["job_id", "num_ckpts", "avg_size_bytes", "total_time_sec"], rows,
                "Per-job checkpoint summary (first 15):")

    if write_csv_flag:
        write_csv("results/exp3_checkpoint_overhead.csv",
                  ["job_id", "step_index", "sequence", "size_bytes", "timestamp"],
                  [[e.get("job_id","?"), e.get("step_index",""),
                    e.get("sequence",""), e.get("size_bytes",""), e.get("timestamp","")]
                   for e in ckpt_events])


# ── Experiment 4: End-to-End Success Rate Under Load ─────────────────────────

def experiment_4(events: list[dict], write_csv_flag: bool) -> None:
    """
    Research question: Does the system maintain correctness under concurrent failures?
    Metrics: job completion rate, takeover rate, avg end-to-end time.
    """
    print("=" * 60)
    print("EXPERIMENT 4: End-to-End Success Rate Under Load")
    print("=" * 60)

    submitted  = filter_events(events, "job_submitted")
    completed  = filter_events(events, "job_completed")
    failed     = filter_events(events, "job_permanently_failed")
    takeovers  = filter_events(events, "takeover_queued")
    deaths     = filter_events(events, "worker_died")

    total      = len(submitted)
    n_complete = len(completed)
    n_failed   = len(failed)
    n_takeover = len(takeovers)
    n_deaths   = len(deaths)

    if total == 0:
        print("  No jobs found.\n")
        return

    success_rate = 100 * n_complete / total if total > 0 else 0
    times = [e["total_time_sec"] for e in completed if e.get("total_time_sec") is not None]
    time_stats = safe_stats(times)

    print(f"  Jobs submitted          : {total}")
    print(f"  Jobs completed          : {n_complete}  ({success_rate:.1f}%)")
    print(f"  Jobs permanently failed : {n_failed}")
    print(f"  Takeovers triggered     : {n_takeover}")
    print(f"  Worker failures         : {n_deaths}")
    print(f"  Avg job time            : {time_stats['mean']}s")
    print(f"  Min / Max job time      : {time_stats['min']}s / {time_stats['max']}s")
    print()

    rows = [
        ["Total jobs submitted",    total],
        ["Completed successfully",  f"{n_complete} ({success_rate:.1f}%)"],
        ["Permanently failed",      n_failed],
        ["Hot takeovers triggered", n_takeover],
        ["Worker deaths detected",  n_deaths],
        ["Avg completion time",     f"{time_stats['mean']}s" if time_stats['mean'] else "N/A"],
    ]
    print_table(["Metric", "Value"], rows, "Summary:")

    if write_csv_flag:
        write_csv("results/exp4_success_rate.csv",
                  ["metric", "value"],
                  rows)


# ── Experiment 5: State Fidelity ──────────────────────────────────────────────

def experiment_5(events: list[dict], write_csv_flag: bool) -> None:
    """
    Research question: Does P2 produce the same result as P1 would have?
    Metric: compare takeover job results against known-correct baseline results.
    The experiment runner tags baseline vs takeover jobs for comparison.
    """
    print("=" * 60)
    print("EXPERIMENT 5: State Fidelity After Takeover")
    print("=" * 60)

    completed = filter_events(events, "job_completed")
    takeover_completed = [e for e in completed if e.get("takeover")]
    clean_completed    = [e for e in completed if not e.get("takeover")]

    print(f"  Clean completions (baseline) : {len(clean_completed)}")
    print(f"  Takeover completions         : {len(takeover_completed)}")

    if not takeover_completed:
        print("  No takeover completions to analyze fidelity.\n")
        return

    # steps_redone=0 means P2 resumed from the exact last checkpoint
    zero_redo  = sum(1 for e in takeover_completed if e.get("steps_redone", -1) == 0)
    some_redo  = sum(1 for e in takeover_completed if (e.get("steps_redone") or 0) > 0)
    total_take = len(takeover_completed)

    print(f"  Perfect resume (steps_redone=0) : {zero_redo}/{total_take}")
    print(f"  Partial redo (steps_redone > 0) : {some_redo}/{total_take}")
    print()
    print("  NOTE: Full fidelity verification (result equality) requires")
    print("  run_experiment.py --experiment 5, which compares result dicts")
    print("  between baseline and takeover runs of the same task type.")
    print()

    rows = [[e.get("job_id","?")[:8],
             e.get("steps_redone","?"),
             round(e.get("total_time_sec",0), 2),
             e.get("worker_id","?")[:8]]
            for e in takeover_completed]
    print_table(["job_id", "steps_redone", "total_time_sec", "worker_id"], rows,
                "Per-takeover fidelity:")

    if write_csv_flag:
        write_csv("results/exp5_fidelity.csv",
                  ["job_id", "steps_redone", "total_time_sec", "worker_id", "timestamp"],
                  [[e.get("job_id","?"), e.get("steps_redone",""),
                    e.get("total_time_sec",""), e.get("worker_id","?"),
                    e.get("timestamp","")]
                   for e in takeover_completed])


# ── Overall summary ────────────────────────────────────────────────────────────

def overall_summary(events: list[dict]) -> None:
    print("=" * 60)
    print("OVERALL SYSTEM SUMMARY")
    print("=" * 60)

    by_type: dict[str, int] = defaultdict(int)
    for e in events:
        by_type[e.get("event", "unknown")] += 1

    rows = sorted(by_type.items(), key=lambda x: -x[1])
    print_table(["event_type", "count"], [[k, v] for k, v in rows],
                "Event counts in this metrics file:")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Hot Takeover experiment metrics")
    parser.add_argument("--file",       default=METRICS_FILE, help="Path to metrics JSONL file")
    parser.add_argument("--experiment", type=int, default=0,
                        help="Run only this experiment (1-5). Default: all.")
    parser.add_argument("--csv",        action="store_true",
                        help="Write per-experiment CSV files to results/")
    args = parser.parse_args()

    events = load_events(args.file)
    if not events:
        return

    exp_map = {
        1: experiment_1,
        2: experiment_2,
        3: experiment_3,
        4: experiment_4,
        5: experiment_5,
    }

    if args.experiment == 0:
        overall_summary(events)
        print()
        for fn in exp_map.values():
            fn(events, args.csv)
    elif args.experiment in exp_map:
        exp_map[args.experiment](events, args.csv)
    else:
        print(f"Unknown experiment: {args.experiment}. Choose 1-5.")


if __name__ == "__main__":
    main()