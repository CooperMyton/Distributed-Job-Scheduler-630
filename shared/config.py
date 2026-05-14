# shared/config.py
# Central configuration for the Hot Takeover distributed scheduler.
# All tunable constants live here, coordinator, workers, and experiments
# import from this single file so changing one value affects the whole system.

# ── Coordinator ────────────────────────────────────────────────────────────────
COORDINATOR_HOST = "127.0.0.1"
COORDINATOR_PORT = 8000
COORDINATOR_URL  = f"http://{COORDINATOR_HOST}:{COORDINATOR_PORT}"

# Heartbeat & Failure Detection
# How often (seconds) a worker sends a heartbeat to the coordinator.
HEARTBEAT_INTERVAL_SEC = 3

# How long (seconds) of silence before the coordinator declares a worker dead.
# Rule of thumb: at least 3x HEARTBEAT_INTERVAL_SEC to absorb transient delays.
# RESEARCH VARIABLE: Experiment 1 sweeps this value (5, 10, 20, 30).
WORKER_TIMEOUT_SEC = 10

# How often (seconds) the monitor thread scans for timed-out workers.
# Contributes to detection latency: worst-case = WORKER_TIMEOUT + MONITOR_POLL.
MONITOR_POLL_SEC = 2

# ── Checkpointing ──────────────────────────────────────────────────────────────
# Worker emits a checkpoint every N steps of task execution.
# RESEARCH VARIABLE: Experiments 2 & 3 sweep this value (1, 5, 10, 20, 50).
CHECKPOINT_EVERY_N_STEPS = 5

# Where to persist checkpoints when using file-based storage.
# Each job gets its own file: CHECKPOINT_DIR/{job_id}.json
CHECKPOINT_DIR = "checkpoints"

# Maximum size (bytes) of a serialized checkpoint blob before a warning is logged.
# Helps surface tasks whose state grows unboundedly.
CHECKPOINT_WARN_SIZE_BYTES = 1_048_576  # 1 MB

# ── Worker ─────────────────────────────────────────────────────────────────────
# How often (seconds) a worker polls the coordinator for a new job when idle.
WORKER_POLL_SEC = 2

# How many times a worker retries a failed HTTP call to the coordinator
# before giving up on that action (uses exponential backoff).
WORKER_HTTP_RETRIES = 3
WORKER_HTTP_BACKOFF_SEC = 1.0  # doubles on each retry

# ── Job Lifecycle ──────────────────────────────────────────────────────────────
# Maximum number of times a job will be retried (across all workers) before
# it is permanently marked FAILED. Caps infinite retry loops.
# At-least-once semantics: a job WILL be retried up to this limit.
MAX_JOB_RETRIES = 3

# ── Storage Backend ────────────────────────────────────────────────────────────
# "memory"  -- InMemoryStore (default, no dependencies, resets on coordinator restart)
# "file"    -- File-backed store, checkpoints survive coordinator restart
# "redis"   -- Redis-backed store (requires Redis running, best for load experiments)
STORAGE_BACKEND = "memory"

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB   = 0

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL       = "INFO"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT      = "%(asctime)s  %(levelname)-8s  %(name)s -- %(message)s"

# ── Metrics & Visualization ────────────────────────────────────────────────────
# All events (job transitions, checkpoints, failures, takeovers) are appended
# here as newline-delimited JSON. Feed this file directly into Tableau or
# metrics/analyze.py for experiment results.
METRICS_FILE = "metrics.jsonl"
