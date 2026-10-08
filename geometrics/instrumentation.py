"""
Machine-readable stage timings.

Every pipeline stage (check, gee_submit, check_status, ingest, fetch) appends one
JSON object per call to a log file, so throughput can be reported from recorded
measurements instead of scraped stdout. Default location is
~/.geometrics/timings.jsonl; override with the GEOMETRICS_TIMING_LOG environment
variable, or pass log_path.

Each line carries: stage, started_at (UTC ISO), seconds, plus whatever context
the caller supplies (row counts, source names, job ids), and "error" when the
stage raised.
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_LOG = Path.home() / ".geometrics" / "timings.jsonl"


def log_path() -> Path:
    override = os.environ.get("GEOMETRICS_TIMING_LOG")
    return Path(override) if override else DEFAULT_LOG


@contextmanager
def timed(stage: str, log: Path | None = None, **context):
    """
    Time a stage and append the record on the way out.

    The yielded dict can be updated by the caller to add results that are only
    known once the stage has run (rows inserted, jobs created, and so on).
    """
    record: dict = {
        "stage": stage,
        "started_at": datetime.now(timezone.utc).isoformat(),
        **context,
    }
    started = time.perf_counter()
    try:
        yield record
    except Exception as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        record["seconds"] = round(time.perf_counter() - started, 4)
        _append(record, log)


def _append(record: dict, log: Path | None = None) -> None:
    target = log or log_path()
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a") as handle:
            handle.write(json.dumps(record, default=str) + "\n")
    except OSError:
        # Instrumentation must never take the pipeline down with it.
        pass


def read_timings(log: Path | None = None) -> list[dict]:
    """Read back every recorded stage, oldest first. Bad lines are skipped."""
    target = log or log_path()
    if not target.exists():
        return []
    out = []
    for line in target.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out
