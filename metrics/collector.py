# metrics/collector.py
# Append-only JSONL event writer used by coordinator/api.py and
# coordinator/monitor.py to record every significant system event.
#
# Each call to emit() writes one JSON object on its own line to METRICS_FILE.
# The file can be fed directly to metrics/analyze.py or imported into Tableau.
#
# Thread-safe: a single file lock ensures concurrent API threads don't
# interleave writes.

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any

from shared.config import METRICS_FILE

logger   = logging.getLogger(__name__)
_lock    = threading.Lock()
_enabled = True   # Set to False in unit tests to suppress file I/O


def emit(event: dict[str, Any]) -> None:
    """
    Append one event record to the metrics JSONL file.

    A 'timestamp' field (UTC ISO-8601) is injected automatically if not present.
    Any non-serializable values are converted to strings rather than crashing.
    """
    if not _enabled:
        return
    record = {"timestamp": datetime.now(timezone.utc).isoformat(), **event}
    try:
        line = json.dumps(record, default=str)
        with _lock:
            with open(METRICS_FILE, "a") as f:
                f.write(line + "\n")
    except OSError as e:
        logger.error("Failed to write metrics event: %s", e)


def disable() -> None:
    """Suppress file I/O during unit tests."""
    global _enabled
    _enabled = False


def enable() -> None:
    global _enabled
    _enabled = True