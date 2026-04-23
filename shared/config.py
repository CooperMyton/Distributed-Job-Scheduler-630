# shared/config.py
# Central configuration for the distributed scheduler.
# All tunable constants live here so coordinator and workers stay in sync.

#Coordinator 
COORDINATOR_HOST = "127.0.0.1"
COORDINATOR_PORT = 8000
COORDINATOR_URL  = f"http://{COORDINATOR_HOST}:{COORDINATOR_PORT}"

#  Heartbeat & Failure Detection 
# How often (seconds) a worker sends a heartbeat to the coordinator
HEARTBEAT_INTERVAL_SEC = 3

# How long (seconds) the coordinator waits before declaring a worker dead.
# Should be at least 2-3× HEARTBEAT_INTERVAL_SEC to tolerate transient delays.
WORKER_TIMEOUT_SEC = 10

# How often (seconds) the coordinator's monitor thread checks for dead workers
MONITOR_POLL_SEC = 2

# Job Execution 
# How often (seconds) a worker polls the coordinator for a new job
WORKER_POLL_SEC = 2

# Maximum number of times a job will be retried after worker failure.
# Enforces at-least-once semantics without infinite retry loops.
MAX_JOB_RETRIES = 3

#  Storage 
# Set to True to use Redis, False to use the in-memory store.
# Swap this without touching coordinator or worker code.
USE_REDIS = False

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_DB   = 0

#  Logging 
LOG_LEVEL       = "INFO"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FORMAT      = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s"

#Metrics / Visualization 
# Jobs + events are appended here as newline-delimited JSON (easy Tableau ingest)
METRICS_FILE = "metrics.jsonl"