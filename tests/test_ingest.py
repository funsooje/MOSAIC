"""
Tests for the ingest layer.

Uses in-memory SQLite and temporary CSV files — no GEE auth required.
Ingest is file-driven: ingest_file / ingest_folder read GEE exports whose rows
carry cell_id, source and timestamp, and write them into the per-source wide
observation table obs_{source}.
"""

import pytest
from sqlalchemy import create_engine, select, text

from geometrics.store.schema import initialize_db, jobs, sources, variables
from geometrics.store.jobs import record_submitted
from geometrics.store.ingest import ingest_file, ingest_folder


SOURCE = "Landsat_NDVI"
ROWS = (
    "cell_id,source,timestamp,NDVI\n"
    "13:-4256|1650,Landsat_NDVI,2020-06-15,0.72\n"
    "13:-4255|1650,Landsat_NDVI,2020-06-15,0.68\n"
    "13:-4256|1651,Landsat_NDVI,2020-06-15,0.81\n"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    initialize_db(eng)
    return eng


@pytest.fixture()
def registered(engine):
    """Engine with Landsat_NDVI and its NDVI variable registered."""
    with engine.begin() as conn:
        conn.execute(sources.insert().values(
            name=SOURCE,
            native_level=13,
            pixel_resolution_m=30,
            source_temporal_granularity="16-day",
            temporal_granularity="year",
        ))
        conn.execute(variables.insert().values(source_id=1, name="NDVI", unit="index"))
    return engine


@pytest.fixture()
def export_csv(tmp_path):
    path = tmp_path / "Landsat_NDVI_batch_001.csv"
    path.write_text(ROWS)
    return path


def stored_values(engine):
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT u.timestamp, o.NDVI
            FROM obs_landsat_ndvi o
            JOIN spatiotemporal_units u ON u.id = o.unit_pk
            ORDER BY o.NDVI
        """)).fetchall()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_ingest_file_inserts_rows(registered, export_csv):
    assert ingest_file(registered, export_csv) == 3


def test_ingest_writes_values_to_source_table(registered, export_csv):
    ingest_file(registered, export_csv)
    rows = stored_values(registered)
    assert [round(r.NDVI, 2) for r in rows] == [0.68, 0.72, 0.81]


def test_ingest_snaps_timestamp_to_granularity(registered, export_csv):
    """The source stores yearly, so 2020-06-15 lands on 2020-01-01."""
    ingest_file(registered, export_csv)
    assert all(str(r.timestamp).startswith("2020-01-01") for r in stored_values(registered))


def test_ingest_registers_cells(registered, export_csv):
    ingest_file(registered, export_csv)
    with registered.connect() as conn:
        rows = conn.execute(text("SELECT cell_id, backend, level FROM cells")).fetchall()
    assert len(rows) == 3
    assert {r.backend for r in rows} == {"hiergp"}
    assert {r.level for r in rows} == {13}


def test_ingest_drops_gee_meta_columns(registered, tmp_path):
    path = tmp_path / "with_meta.csv"
    path.write_text(
        'system:index,cell_id,source,timestamp,NDVI,.geo\n'
        '00000,13:-4256|1650,Landsat_NDVI,2020-06-15,0.72,"{""type"":""Point""}"\n'
    )
    assert ingest_file(registered, path) == 1


# ---------------------------------------------------------------------------
# Idempotence
# ---------------------------------------------------------------------------

def test_ingest_skips_duplicates_on_rerun(registered, export_csv):
    assert ingest_file(registered, export_csv) == 3
    assert ingest_file(registered, export_csv) == 0
    assert len(stored_values(registered)) == 3


# ---------------------------------------------------------------------------
# Folder ingest and job bookkeeping
# ---------------------------------------------------------------------------

def test_ingest_folder_returns_rows_per_file(registered, export_csv):
    result = ingest_folder(registered, export_csv.parent)
    assert result == {export_csv.name: 3}


def test_ingest_folder_marks_job_ingested(registered, export_csv, tmp_path):
    job_id = record_submitted(
        engine=registered, task_id="EE-TEST-001", source_id=1, level=13,
        date_start="2020-01-01", date_end="2020-12-31",
        gdrive_folder="exports", file_prefix=export_csv.stem,
        gdrive_base=str(tmp_path), row_count=3,
    )
    ingest_folder(registered, export_csv.parent, gdrive_folder="exports")
    with registered.connect() as conn:
        row = conn.execute(
            select(jobs.c.status, jobs.c.ingested_at).where(jobs.c.job_id == job_id)
        ).fetchone()
    assert row.status == "INGESTED"
    assert row.ingested_at is not None


def test_ingest_folder_leaves_other_jobs_alone(registered, export_csv, tmp_path):
    other = record_submitted(
        engine=registered, task_id="EE-TEST-002", source_id=1, level=13,
        date_start="2020-01-01", date_end="2020-12-31",
        gdrive_folder="other-folder", file_prefix=export_csv.stem,
        gdrive_base=str(tmp_path), row_count=3,
    )
    ingest_folder(registered, export_csv.parent, gdrive_folder="exports")
    with registered.connect() as conn:
        status = conn.execute(
            select(jobs.c.status).where(jobs.c.job_id == other)
        ).scalar()
    assert status == "PENDING"


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------

def test_ingest_raises_on_unregistered_source(engine, export_csv):
    with pytest.raises(ValueError, match="not registered"):
        ingest_file(engine, export_csv)


def test_ingest_raises_on_missing_required_columns(registered, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("cell_id,timestamp\n13:-4256|1650,2020-06-15\n")
    with pytest.raises(ValueError, match="missing required columns"):
        ingest_file(registered, path)


def test_ingest_raises_when_no_variable_columns(registered, tmp_path):
    path = tmp_path / "novars.csv"
    path.write_text("cell_id,source,timestamp\n13:-4256|1650,Landsat_NDVI,2020-06-15\n")
    with pytest.raises(ValueError, match="no variable columns"):
        ingest_file(registered, path)


def test_ingest_folder_raises_when_folder_missing(registered, tmp_path):
    with pytest.raises(FileNotFoundError):
        ingest_folder(registered, tmp_path / "nope")
