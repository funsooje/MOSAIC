"""
Tests for extraction modules.

GEE (ee.*) is mocked throughout — no authentication required.
Tests verify: source registration, job record creation, GEE call arguments.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import pytest
from sqlalchemy import create_engine, select

from geometrics.config import GeoMetricsConfig
from geometrics.backends.hiergp import HierGPBackend
from geometrics.store.schema import initialize_db, jobs, sources, variables
from geometrics.store.ingest import ingest_file
from geometrics.extraction.base import ensure_source
from geometrics.extraction.ndvi import submit_ndvi
from geometrics.extraction.treecover import submit_treecover


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    initialize_db(eng)
    return eng


@pytest.fixture()
def config(tmp_path):
    return GeoMetricsConfig(
        db_url="sqlite://",
        gdrive_base=str(tmp_path),
        backend="hiergp",
    )


@pytest.fixture()
def backend():
    return HierGPBackend()


@pytest.fixture()
def cell_ids(backend):
    return [
        backend.point_to_cell(47.6062, -122.3321, 13),
        backend.point_to_cell(47.6500, -122.3000, 13),
    ]


@pytest.fixture()
def items(cell_ids):
    """Resolved items as dispatch() builds them: one per (cell, timestamp)."""
    return [
        {"cell_id": cid,
         "source": "Landsat_NDVI",
         "timestamp": "2020-01-01",
         "temporal_granularity": "year",
         "requested_level": 13,
         "date_start": "2020-01-01",
         "date_end": "2020-12-31"}
        for cid in cell_ids
    ]


@pytest.fixture()
def mock_ee():
    """
    Inject a mock 'ee' module into sys.modules so all lazy 'import ee'
    calls inside extraction functions receive the mock.
    """
    import sys

    fake_task = MagicMock()
    fake_task.id = "EE-MOCK-TASK-001"

    ee_mock = MagicMock()
    ee_mock.Feature.side_effect = lambda geom, props: {"geom": geom, "props": props}
    ee_mock.Geometry.Point.side_effect = lambda coords: coords
    # FeatureCollection must behave like one: the submitters call .map() on it.
    ee_mock.FeatureCollection.side_effect = lambda feats: MagicMock(name="FeatureCollection")
    ee_mock.Reducer.mean.return_value.setOutputs.return_value = MagicMock()
    ee_mock.ImageCollection.return_value.filterDate.return_value \
        .map.return_value.map.return_value.median.return_value = MagicMock()
    ee_mock.ImageCollection.return_value.filterDate.return_value \
        .first.return_value.select.return_value.rename.return_value = MagicMock()
    ee_mock.batch.Export.table.toDrive.return_value = fake_task

    with patch.dict(sys.modules, {"ee": ee_mock}):
        yield fake_task


# ---------------------------------------------------------------------------
# ensure_source
# ---------------------------------------------------------------------------

def test_ensure_source_creates_source_and_variables(engine):
    source_id, _ = ensure_source(
        engine=engine,
        name="TestSource",
        native_level=8,
        pixel_resolution_m=100,
        source_temporal_granularity="day",
        temporal_granularity="year",
        variable_defs=[{"name": "VAR1", "unit": "m"}, {"name": "VAR2"}],
    )
    assert isinstance(source_id, int)

    with engine.connect() as conn:
        src = conn.execute(select(sources).where(sources.c.source_id == source_id)).fetchone()
        vars_ = conn.execute(select(variables).where(variables.c.source_id == source_id)).fetchall()

    assert src.name == "TestSource"
    assert src.native_level == 8
    assert len(vars_) == 2
    assert {v.name for v in vars_} == {"VAR1", "VAR2"}


def test_ensure_source_is_idempotent(engine):
    sid1, new1 = ensure_source(engine, "Dup", 8, 30, "day", "year", [{"name": "X"}])
    sid2, new2 = ensure_source(engine, "Dup", 8, 30, "day", "year", [{"name": "X"}])
    assert sid1 == sid2
    assert (new1, new2) == (True, False)

    with engine.connect() as conn:
        count = conn.execute(select(sources)).fetchall()
    assert len(count) == 1


# ---------------------------------------------------------------------------
# NDVI submission
# ---------------------------------------------------------------------------

def test_submit_ndvi_registers_source(engine, config, backend, items, mock_ee):
    submit_ndvi(engine, config, backend, items, "exports", "Landsat_NDVI_batch_001")

    with engine.connect() as conn:
        src = conn.execute(select(sources).where(sources.c.name == "Landsat_NDVI")).fetchone()
    assert src is not None
    assert src.pixel_resolution_m == 30


def test_submit_ndvi_creates_job_record(engine, config, backend, items, mock_ee):
    job_id = submit_ndvi(engine, config, backend, items, "exports", "Landsat_NDVI_batch_001")

    with engine.connect() as conn:
        job = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).fetchone()

    assert job.task_id == "EE-MOCK-TASK-001"
    assert job.status == "PENDING"
    assert job.date_start == "2020-01-01"
    assert job.date_end == "2020-12-31"
    assert job.file_prefix == "Landsat_NDVI_batch_001"
    assert job.row_count == len(items)


def test_submit_records_level_actually_extracted(engine, config, backend, items, mock_ee):
    """jobs.level follows the items' requested_level, not the catalog constant."""
    for item in items:
        item["requested_level"] = 11
    job_id = submit_ndvi(engine, config, backend, items, "exports", "batch_001")

    with engine.connect() as conn:
        job = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).fetchone()
    assert job.level == 11


def test_submit_ndvi_expected_path_uses_gdrive_base(engine, config, backend, items, mock_ee):
    submit_ndvi(engine, config, backend, items, "exports", "Landsat_NDVI_batch_001")

    with engine.connect() as conn:
        job = conn.execute(select(jobs)).fetchone()

    assert job.expected_path.startswith(config.gdrive_base)
    assert job.expected_path.endswith(".csv")


def test_submit_ndvi_calls_gee_export(engine, config, backend, items, mock_ee):
    with patch("geometrics.extraction.ndvi.submit_export", wraps=lambda **kw: "EE-X") as spy:
        with patch("geometrics.extraction.ndvi.record_submitted", return_value=1):
            submit_ndvi(engine, config, backend, items, "exports", "batch_001")
    spy.assert_called_once()
    _, kwargs = spy.call_args
    assert "cell_id" in kwargs["properties"]
    assert "NDVI" in kwargs["properties"]
    assert "timestamp" in kwargs["properties"]


# ---------------------------------------------------------------------------
# Treecover submission
# ---------------------------------------------------------------------------

def test_submit_treecover_registers_source(engine, config, backend, items, mock_ee):
    submit_treecover(engine, config, backend, items, "exports", "MODIS_Treecover_batch_001")

    with engine.connect() as conn:
        src = conn.execute(select(sources).where(sources.c.name == "MODIS_Treecover")).fetchone()
    assert src is not None
    assert src.temporal_granularity == "year"


def test_submit_treecover_creates_job_record(engine, config, backend, items, mock_ee):
    job_id = submit_treecover(engine, config, backend, items, "exports", "tc_batch_001")

    with engine.connect() as conn:
        job = conn.execute(select(jobs).where(jobs.c.job_id == job_id)).fetchone()

    assert job.task_id == "EE-MOCK-TASK-001"
    assert job.status == "PENDING"
    assert job.date_start == "2020-01-01"
    assert job.file_prefix == "tc_batch_001"


# ---------------------------------------------------------------------------
# Multiple sources coexist
# ---------------------------------------------------------------------------

def test_ndvi_and_treecover_register_separately(engine, config, backend, items, mock_ee):
    submit_ndvi(engine, config, backend, items, "exports", "ndvi_batch_001")
    submit_treecover(engine, config, backend, items, "exports", "tc_batch_001")

    with engine.connect() as conn:
        srcs = conn.execute(select(sources)).fetchall()

    assert {s.name for s in srcs} == {"Landsat_NDVI", "MODIS_Treecover"}
