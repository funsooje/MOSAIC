"""
Tests for the store layer: schema initialisation, job tracking, and expiry logic.
All tests use an in-memory SQLite database — no external dependencies required.
"""

import pytest
from sqlalchemy import inspect, select

from sqlalchemy import create_engine

from geometrics.store.jobs import _resolve_status, list_jobs, record_submitted
from geometrics.store.schema import initialize_db, jobs, sources


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    """Fresh in-memory SQLite engine with schema applied (never cached)."""
    eng = create_engine("sqlite://")
    initialize_db(eng)
    return eng


@pytest.fixture()
def engine_with_source(engine):
    """Engine pre-loaded with one source row so foreign keys resolve."""
    with engine.begin() as conn:
        conn.execute(sources.insert().values(
            name="Landsat NDVI",
            native_level=15,
            pixel_resolution_m=30,
            source_temporal_granularity="16-day",
            temporal_granularity="year",
        ))
    return engine


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_initialize_db_creates_all_tables(engine):
    table_names = inspect(engine).get_table_names()
    assert set(table_names) == {
        "sources", "variables", "cells", "hiergp_cells",
        "spatiotemporal_units", "jobs",
    }


def test_spatiotemporal_units_unique_per_cell_and_time(engine):
    """One row per (cell, timestamp) — the constraint ingest relies on to dedup."""
    constraints = inspect(engine).get_unique_constraints("spatiotemporal_units")
    assert any(
        set(c["column_names"]) == {"cell_pk", "timestamp"}
        for c in constraints
    )


def test_cells_unique_on_cell_id(engine):
    constraints = inspect(engine).get_unique_constraints("cells")
    unique_cols = [set(c["column_names"]) for c in constraints]
    assert {"cell_id"} in unique_cols


def test_sources_columns(engine):
    cols = {c["name"] for c in inspect(engine).get_columns("sources")}
    assert cols == {
        "source_id", "name", "native_level", "pixel_resolution_m",
        "source_temporal_granularity", "temporal_granularity",
    }


def test_jobs_columns(engine):
    cols = {c["name"] for c in inspect(engine).get_columns("jobs")}
    expected = {
        "job_id", "task_id", "source_id", "level", "date_start", "date_end",
        "gdrive_folder", "file_prefix", "expected_path", "status",
        "submitted_at", "started_at", "completed_at", "error",
        "ingested_at", "row_count",
    }
    assert cols == expected


# ---------------------------------------------------------------------------
# Job recording
# ---------------------------------------------------------------------------

def test_record_submitted_inserts_row(engine_with_source):
    job_id = record_submitted(
        engine=engine_with_source,
        task_id="EE-ABC123",
        source_id=1,
        level=8,
        date_start="2020-01-01",
        date_end="2020-12-31",
        gdrive_folder="geometrics_exports",
        file_prefix="ndvi_l8_2020",
        gdrive_base="/Users/funsooje/Google Drive/My Drive",
    )
    assert isinstance(job_id, int) and job_id > 0

    with engine_with_source.connect() as conn:
        row = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).fetchone()

    assert row.task_id == "EE-ABC123"
    assert row.status == "PENDING"
    assert row.expected_path == (
        "/Users/funsooje/Google Drive/My Drive/geometrics_exports/ndvi_l8_2020.csv"
    )
    assert row.submitted_at is not None


def test_record_submitted_strips_trailing_slash_from_gdrive_base(engine_with_source):
    job_id = record_submitted(
        engine=engine_with_source,
        task_id="EE-XYZ",
        source_id=1,
        level=8,
        date_start="2021-01-01",
        date_end="2021-12-31",
        gdrive_folder="exports",
        file_prefix="ndvi_2021",
        gdrive_base="/Users/funsooje/Google Drive/My Drive/",  # trailing slash
    )
    with engine_with_source.connect() as conn:
        row = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).fetchone()
    assert "//" not in row.expected_path


def test_list_jobs_returns_all(engine_with_source):
    for i in range(3):
        record_submitted(
            engine=engine_with_source,
            task_id=f"EE-{i}",
            source_id=1,
            level=8,
            date_start="2020-01-01",
            date_end="2020-12-31",
            gdrive_folder="exports",
            file_prefix=f"ndvi_{i}",
            gdrive_base="/gdrive",
        )
    rows = list_jobs(engine_with_source)
    assert len(rows) == 3


def test_list_jobs_filters_by_status(engine_with_source):
    record_submitted(
        engine=engine_with_source,
        task_id="EE-A",
        source_id=1,
        level=8,
        date_start="2020-01-01",
        date_end="2020-12-31",
        gdrive_folder="exports",
        file_prefix="ndvi_a",
        gdrive_base="/gdrive",
    )
    # Manually mark it COMPLETED
    with engine_with_source.begin() as conn:
        conn.execute(jobs.update().values(status="COMPLETED"))

    pending = list_jobs(engine_with_source, status="PENDING")
    completed = list_jobs(engine_with_source, status="COMPLETED")
    assert len(pending) == 0
    assert len(completed) == 1


# ---------------------------------------------------------------------------
# GEE expiry / status resolution logic
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("local_status, gee_state, expected", [
    # Normal GEE state transitions
    ("PENDING",   "READY",            "PENDING"),
    ("PENDING",   "RUNNING",          "RUNNING"),
    ("RUNNING",   "RUNNING",          "RUNNING"),
    ("RUNNING",   "COMPLETED",        "COMPLETED"),
    ("RUNNING",   "FAILED",           "FAILED"),
    ("RUNNING",   "CANCELLED",        "CANCELLED"),
    ("RUNNING",   "CANCEL_REQUESTED", "CANCELLED"),
    # GEE task no longer exists (UNKNOWN = deleted/expired)
    ("PENDING",   "UNKNOWN",          "EXPIRED"),   # never ran → resubmit
    ("RUNNING",   "UNKNOWN",          "EXPIRED"),   # never confirmed done → resubmit
    ("COMPLETED", "UNKNOWN",          "COMPLETED"), # file is in Drive → still ingestable
])
def test_resolve_status(local_status, gee_state, expected):
    assert _resolve_status(local_status, gee_state) == expected
