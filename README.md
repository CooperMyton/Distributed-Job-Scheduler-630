# Hot Takeover: Application-Level Checkpoint-Based Worker Recovery

Distributed job scheduler for CS630 that demonstrates hot takeover: when a worker fails mid-task, the coordinator detects the failure and another worker resumes from the failed worker's last application-level checkpoint instead of restarting from scratch.

## Research Question

What is the minimum checkpoint representation that enables a second worker to resume a failed worker's execution with correctness and bounded overhead, and what is the tradeoff between checkpoint frequency, recovery latency, and throughput cost?

## Core Idea

Each task defines a JSON-serializable checkpoint state. The coordinator stores that state but does not interpret it. If worker P1 fails, worker P2 receives the latest checkpoint and resumes execution.

The project explores a middleware-level recovery mechanism: higher-level than OS/process checkpointing, but more general than framework-specific dataflow recovery.

## Architecture

- `coordinator/`: FastAPI coordinator, worker registry, job queue, heartbeat monitor, checkpoint handling
- `worker/`: worker loop, heartbeat thread, task execution, checkpoint pushing
- `shared/`: config, dataclasses, thread-safe in-memory store
- `metrics/`: JSONL event collection and analysis
- `scripts/`: experiment runner, job submission, worker start/kill helpers

## Key Features

- Heartbeat-based failure detection
- Mid-task worker takeover
- Application-level checkpoint contract
- Stale checkpoint/result rejection
- At-least-once execution with retry limit
- Experiment scripts for latency, overhead, success rate, and fidelity

## CheckpointableTask Interface

```python
class CheckpointableTask(ABC):
    def run_step(self, state: dict) -> tuple[dict, bool]:
        ...

    def get_result(self, final_state: dict) -> dict:
        ...

    @classmethod
    def from_payload(cls, payload: dict) -> "CheckpointableTask":
        ...
```

The minimum checkpoint is the task-defined state dictionary needed to resume from the last completed step. The coordinator treats this state as opaque data.

## Hot Takeover Protocol

1. Worker registers with coordinator.
2. Worker polls for a job.
3. Coordinator assigns job and marks it `RUNNING`.
4. Worker executes step-by-step.
5. Worker periodically pushes checkpoints.
6. Coordinator detects missed heartbeat.
7. Coordinator marks worker dead.
8. Job becomes `PENDING_TAKEOVER`.
9. Replacement worker receives job plus latest checkpoint.
10. Replacement worker resumes and completes.
11. Late submissions from the old worker are rejected.

## Correctness and Safety Mechanisms

- `InMemoryStore` uses `threading.RLock` for thread-safe coordinator state.
- Job assignment is atomic: dequeue and assignment happen under one lock.
- Only the currently assigned worker can checkpoint or complete a job.
- Late checkpoints and results from zombie workers are rejected.
- Takeover jobs are reinserted at the front of the queue.
- Jobs have a retry limit to avoid infinite retry loops.
- Checkpoints use sequence numbers to guard against stale writes.

## Implemented Tasks

| Task | Purpose |
|---|---|
| `CountingTask` | Simple correctness baseline |
| `FibonacciTask` | Small-state deterministic task |
| `PrimeSieveTask` | Checkpoint overhead experiment |
| `FileWriterTask` | Idempotent side-effect demo |
| `SleepTask` | Long-running failure detection demo |

## Setup

Python 3.10 is recommended.

```powershell
py -3.10 -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

## Run Manually

Terminal 1:

```powershell
python -m coordinator.main
```

Terminal 2:

```powershell
.\scripts\start_workers.ps1 -n 3
```

Terminal 3:

```powershell
python scripts\submit_jobs.py --count 1 --task CountingTask --target 300 --step-sleep 0.1 --wait
```

Kill a worker:

```powershell
.\scripts\kill_worker.ps1 -random
```

Coordinator API docs:

```text
http://127.0.0.1:8000/docs
```

## Run Experiments

Clear old metrics first:

```powershell
Remove-Item -Force metrics.jsonl -ErrorAction SilentlyContinue
```

Run experiments:

```powershell
python scripts\run_experiment.py --experiment 1 --trials 5
python scripts\run_experiment.py --experiment 3
python scripts\run_experiment.py --experiment 5 --trials 2
```

Analyze:

```powershell
python -m metrics.analyze
python -m metrics.analyze --experiment 3
python -m metrics.analyze --csv
```

## Results Summary

### Failure Detection

With `WORKER_TIMEOUT_SEC = 10` and `MONITOR_POLL_SEC = 2`:

```text
Mean detection latency: 10.898s
Median: 10.914s
Std dev: 0.573s
Min / Max: 10.047s / 11.954s
```

The detector behaves within the expected 10-12 second window.

### Checkpoint Overhead

`PrimeSieveTask`, `limit=2000`, `step_sleep=0.003`:

```text
Every 1 step:   87.43s
Every 5 steps:  42.83s
Every 10 steps: 35.82s
Every 25 steps: 32.39s
Every 50 steps: 31.70s
Near none:      31.20s
```

Moderate checkpoint intervals are practical: every 25-50 steps kept overhead under about 4% in this run.

### Success Under Repeated Failure

A load experiment completed 30/30 submitted jobs with 8 worker kills and 6 hot takeovers, for a 100% completion rate and no permanent failures.

### Fidelity

Experiment 5 showed takeover completions and direct baseline comparison:

```text
Fidelity: 2/2 correct (100.0%)
```

Replacement workers produced results matching uninterrupted baseline executions.

## Limitations

- Coordinator is a single point of failure.
- In-memory scheduler state is lost on coordinator restart.
- Semantics are at-least-once, not exactly-once.
- Task authors must define correct checkpoint state.
- Side-effecting tasks must be idempotent.
- Heartbeat timeout adds recovery latency.

## Related Work

This project sits between OS-level checkpointing and framework-specific recovery.

| System | Layer |
|---|---|
| DMTCP / CRIU | OS/process checkpointing |
| Spark RDD | Dataflow lineage |
| Flink savepoints | Stream operator checkpointing |
| This project | Application middleware checkpointing |

## Submission Notes

The recommended submission package is a tarball containing the source code, this README, `MANUAL.md`, and `requirements.txt`. Do not include `venv/`, `.git/`, or `__pycache__/` directories.
