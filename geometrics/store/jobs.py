"""
Job tracking: submit records, status polling, and GEE task expiry handling.

GEE task lifecycle:
  READY → RUNNING → COMPLETED | FAILED | CANCELLED

GEE deletes tasks from its records after ~30 days (or on manual deletion).
When a task can no longer be found in GEE:
  - If local status was COMPLETED → file is already in Drive; status unchanged.
  - If local status was PENDING or RUNNING → job never finished; marked EXPIRED.
  - EXPIRED jobs should be resubmitted.
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.engine import Engine

from geometrics.store.schema import jobs

# Map GEE task states to local status values
_GEE_STATE_MAP = {
    "READY": "PENDING",
    "RUNNING": "RUNNING",
    "COMPLETED": "COMPLETED",
    "FAILED": "FAILED",
    "CANCELLED": "CANCELLED",
    "CANCEL_REQUESTED": "CANCELLED",
}

# Statuses that are still active (worth polling GEE for)
_ACTIVE_STATUSES = {"PENDING", "RUNNING"}

# Statuses that are terminal locally (no GEE poll needed)
_TERMINAL_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "EXPIRED", "INGESTED"}

_GEE_POLL_BATCH = 100  # ee.data.getTaskStatus accepts up to ~1000, keep batches small


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_submitted(engine: Engine, task_id: str, source_id: int, level: int,
                     date_start: str, date_end: str, gdrive_folder: str,
                     file_prefix: str, gdrive_base: str, row_count: int | None = None) -> int:
    expected_path = f"{gdrive_base.rstrip('/')}/{gdrive_folder}/{file_prefix}.csv"
    with engine.begin() as conn:
        result = conn.execute(
            jobs.insert().values(
                task_id=task_id,
                source_id=source_id,
                level=level,
                date_start=date_start,
                date_end=date_end,
                gdrive_folder=gdrive_folder,
                file_prefix=file_prefix,
                expected_path=expected_path,
                status="PENDING",
                submitted_at=_now(),
                row_count=row_count,
            )
        )
        return result.inserted_primary_key[0]


def check_status(engine: Engine) -> dict:
    """
    Poll GEE for all active jobs and update local status.

    Returns a summary dict:
      {status_label: count, ...}
    """
    import ee  # imported here so the rest of the module works without GEE auth

    with engine.connect() as conn:
        rows = conn.execute(
            select(jobs.c.job_id, jobs.c.task_id, jobs.c.status, jobs.c.started_at)
            .where(jobs.c.status.in_(_ACTIVE_STATUSES))
        ).fetchall()

    if not rows:
        print("No active jobs to check.")
        return {}

    # Poll GEE in batches
    gee_status: dict[str, str] = {}
    gee_errors: dict[str, str] = {}
    task_ids = [r.task_id for r in rows]
    for i in range(0, len(task_ids), _GEE_POLL_BATCH):
        batch = task_ids[i : i + _GEE_POLL_BATCH]
        results = ee.data.getTaskStatus(batch)
        for r in results:
            gee_status[r["id"]] = r.get("state", "UNKNOWN")
            if r.get("error_message"):
                gee_errors[r["id"]] = str(r["error_message"])[:2000]

    summary: dict[str, int] = {}
    with engine.begin() as conn:
        for row in rows:
            gee_state = gee_status.get(row.task_id, "UNKNOWN")
            new_status = _resolve_status(row.status, gee_state)
            if new_status != row.status:
                extra: dict = {}
                if new_status == "RUNNING" and not row.started_at:
                    extra["started_at"] = _now()
                if new_status == "COMPLETED":
                    extra["completed_at"] = _now()
                    # A task seen only once, already finished, still has a start.
                    if not row.started_at:
                        extra["started_at"] = _now()
                if new_status in ("FAILED", "CANCELLED") and row.task_id in gee_errors:
                    extra["error"] = gee_errors[row.task_id]
                conn.execute(
                    update(jobs)
                    .where(jobs.c.job_id == row.job_id)
                    .values(status=new_status, **extra)
                )
            summary[new_status] = summary.get(new_status, 0) + 1

    _print_summary(summary, len(rows))
    return summary


def _resolve_status(local_status: str, gee_state: str) -> str:
    if gee_state == "UNKNOWN":
        # GEE no longer knows this task
        if local_status == "COMPLETED":
            # File was already confirmed in Drive — keep COMPLETED so ingest can proceed
            return "COMPLETED"
        # Job never confirmed completion; mark as expired so user knows to resubmit
        return "EXPIRED"
    return _GEE_STATE_MAP.get(gee_state, local_status)


def _print_summary(summary: dict, total: int) -> None:
    print(f"Checked {total} active job(s):")
    for status, count in sorted(summary.items()):
        note = ""
        if status == "EXPIRED":
            note = "  ← GEE task not found; resubmit if not yet COMPLETED"
        print(f"  {status}: {count}{note}")


def list_jobs(engine: Engine, status: str | None = None) -> list:
    with engine.connect() as conn:
        q = select(jobs)
        if status:
            q = q.where(jobs.c.status == status)
        return conn.execute(q.order_by(jobs.c.submitted_at.desc())).fetchall()
