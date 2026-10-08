"""
Tests for the query layer: resolve, check_availability, fetch.

Uses synthetic GEE-style CSVs for:
  - Landsat8_NDVI  : NDVI (native_level=9)
  - ERA5_Land      : temperature, humidity (native_level=8)

No GEE auth required. All data ingested from in-memory CSVs into SQLite.
"""

from __future__ import annotations

import math
import pytest
from sqlalchemy import create_engine

from geometrics.backends.hiergp import HierGPBackend
from geometrics.store.schema import initialize_db, sources, variables
from geometrics.store.ingest import ingest_file
from geometrics.store.query import (
    DataQuery,
    VariableSpec,
    check_availability,
    fetch,
    resolve,
)

# Test location: Seattle, WA
LAT, LON = 47.6062, -122.3321
TIMESTAMP = "2020-06-15"       # raw timestamp in user data
NDVI_TIMESTAMP = "2020-01-01"  # snapped to year (NDVI temporal_granularity="year")
ERA5_TIMESTAMP = TIMESTAMP      # ERA5 temporal_granularity="hour" → date part preserved

# Source / level config matching what extraction modules use
NDVI_LEVEL = 9
ERA5_LEVEL = 8


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def backend():
    return HierGPBackend()


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    initialize_db(eng)
    return eng


@pytest.fixture()
def seeded_engine(engine, backend, tmp_path):
    """
    DB seeded with:
      - Landsat8_NDVI / NDVI  at native_level=9
      - ERA5_Land / temperature, humidity  at native_level=8

    Observations ingested from synthetic GEE-style CSVs for cells around Seattle.
    """
    # Register sources and variables
    with engine.begin() as conn:
        conn.execute(sources.insert().values(
            name="Landsat8_NDVI", native_level=NDVI_LEVEL,
            pixel_resolution_m=30, source_temporal_granularity="16-day",
            temporal_granularity="year",
        ))
        conn.execute(sources.insert().values(
            name="ERA5_Land", native_level=ERA5_LEVEL,
            pixel_resolution_m=9000, source_temporal_granularity="hour",
            temporal_granularity="hour",
        ))
        conn.execute(variables.insert().values(source_id=1, name="NDVI", unit="index"))
        conn.execute(variables.insert().values(source_id=2, name="temperature", unit="K"))
        conn.execute(variables.insert().values(source_id=2, name="humidity", unit="fraction"))

    # Build NDVI cells: the native cell + its siblings (children of the parent)
    native_cell = backend.point_to_cell(LAT, LON, NDVI_LEVEL)
    parent_cell = backend.cell_parent(native_cell)
    ndvi_cells = backend.cell_children(parent_cell)  # 4 cells at NDVI_LEVEL

    ndvi_csv = "cell_id,timestamp,source,NDVI\n" + "\n".join(
        f"{cid},{NDVI_TIMESTAMP},Landsat8_NDVI,{0.60 + i * 0.05}"
        for i, cid in enumerate(ndvi_cells)
    )

    # ERA5 cells: the cell containing Seattle at ERA5_LEVEL
    era5_cell = backend.point_to_cell(LAT, LON, ERA5_LEVEL)
    era5_csv = (
        "cell_id,timestamp,source,temperature,humidity\n"
        f"{era5_cell},{ERA5_TIMESTAMP},ERA5_Land,295.3,0.72\n"
    )

    ndvi_path = tmp_path / "ndvi.csv"
    ndvi_path.write_text(ndvi_csv)
    ingest_file(engine, ndvi_path)

    era5_path = tmp_path / "era5.csv"
    era5_path.write_text(era5_csv)
    ingest_file(engine, era5_path)

    return engine, backend, native_cell, parent_cell, ndvi_cells, era5_cell


# ---------------------------------------------------------------------------
# VariableSpec
# ---------------------------------------------------------------------------

def test_variable_spec_parse():
    spec = VariableSpec.parse("Landsat8_NDVI:NDVI")
    assert spec.source == "Landsat8_NDVI"
    assert spec.parameter == "NDVI"
    assert spec.level is None


def test_variable_spec_parse_with_spaces():
    spec = VariableSpec.parse(" ERA5_Land : temperature ")
    assert spec.source == "ERA5_Land"
    assert spec.parameter == "temperature"


def test_variable_spec_parse_invalid():
    with pytest.raises(ValueError, match="source:parameter"):
        VariableSpec.parse("no_colon_here")


def test_variable_spec_str():
    assert str(VariableSpec("Landsat8_NDVI", "NDVI")) == "Landsat8_NDVI:NDVI"


# ---------------------------------------------------------------------------
# resolve
# ---------------------------------------------------------------------------

def test_resolve_returns_one_item_per_location_variable_timestamp(seeded_engine):
    engine, backend, native_cell, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI")],
    )
    items = resolve(engine, backend, query)
    assert len(items) == 1
    assert items[0]["cell_id"] == native_cell
    assert items[0]["requested_level"] == NDVI_LEVEL
    assert items[0]["native_level"] == NDVI_LEVEL


def test_resolve_uses_native_level_by_default(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("ERA5_Land:temperature")],
    )
    items = resolve(engine, backend, query)
    assert items[0]["requested_level"] == ERA5_LEVEL


def test_resolve_uses_explicit_level_override(seeded_engine):
    engine, backend, *_ = seeded_engine
    coarser = ERA5_LEVEL - 1
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("ERA5_Land:temperature", level=coarser)],
    )
    items = resolve(engine, backend, query)
    assert items[0]["requested_level"] == coarser


def test_resolve_multiple_locations_and_variables(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP), (47.65, -122.30, TIMESTAMP)],
        variables=[
            VariableSpec.parse("Landsat8_NDVI:NDVI"),
            VariableSpec.parse("ERA5_Land:temperature"),
        ],
    )
    items = resolve(engine, backend, query)
    # 2 rows × 2 variables = 4
    assert len(items) == 4


def test_resolve_raises_on_unknown_source(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("NonExistent:NDVI")],
    )
    with pytest.raises(ValueError, match="Unknown source"):
        resolve(engine, backend, query)


# ---------------------------------------------------------------------------
# check_availability
# ---------------------------------------------------------------------------

def test_check_availability_finds_stored_data(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI")],
    )
    items = resolve(engine, backend, query)
    report = check_availability(engine, backend, items)
    assert len(report["available"]) == 1
    assert len(report["missing"]) == 0


def test_check_availability_flags_missing_timestamp(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, "1999-01-01")],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI")],
    )
    items = resolve(engine, backend, query)
    report = check_availability(engine, backend, items)
    assert len(report["missing"]) == 1
    assert report["missing"][0]["reason"] == "not in store"


def test_check_availability_flags_finer_than_native(seeded_engine):
    engine, backend, *_ = seeded_engine
    finer = NDVI_LEVEL + 1
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI", level=finer)],
    )
    items = resolve(engine, backend, query)
    report = check_availability(engine, backend, items)
    assert report["missing"][0]["reason"] == "requested level finer than native"


def test_check_availability_coarser_request_uses_descendants(seeded_engine):
    engine, backend, native_cell, parent_cell, *_ = seeded_engine
    coarser = NDVI_LEVEL - 1
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI", level=coarser)],
    )
    items = resolve(engine, backend, query)
    report = check_availability(engine, backend, items)
    assert len(report["available"]) == 1


# ---------------------------------------------------------------------------
# fetch — direct lookup
# ---------------------------------------------------------------------------

def test_fetch_returns_dataframe(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI")],
    )
    items = resolve(engine, backend, query)
    result = fetch(engine, backend, items)
    assert list(result.columns) == [
        "lat", "lon", "timestamp", "resolved_timestamp",
        "source", "parameter", "value", "cell_id", "level", "aggregated", "_has_record",
    ]
    assert len(result) == 1


def test_fetch_ndvi_value(seeded_engine):
    engine, backend, native_cell, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI")],
    )
    items = resolve(engine, backend, query)
    result = fetch(engine, backend, items)
    assert result.iloc[0]["value"] is not None
    assert not math.isnan(result.iloc[0]["value"])
    assert not result.iloc[0]["aggregated"]


def test_fetch_era5_temperature_and_humidity(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[
            VariableSpec.parse("ERA5_Land:temperature"),
            VariableSpec.parse("ERA5_Land:humidity"),
        ],
    )
    items = resolve(engine, backend, query)
    result = fetch(engine, backend, items)
    assert len(result) == 2
    temp_row = result[result["parameter"] == "temperature"].iloc[0]
    hum_row = result[result["parameter"] == "humidity"].iloc[0]
    assert abs(temp_row["value"] - 295.3) < 0.01
    assert abs(hum_row["value"] - 0.72) < 0.01


def test_fetch_returns_none_for_finer_than_native(seeded_engine):
    engine, backend, *_ = seeded_engine
    finer = NDVI_LEVEL + 1
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI", level=finer)],
    )
    items = resolve(engine, backend, query)
    result = fetch(engine, backend, items)
    assert result.iloc[0]["value"] is None
    assert not result.iloc[0]["_has_record"]


# ---------------------------------------------------------------------------
# fetch — aggregation (coarser than native)
# ---------------------------------------------------------------------------

def test_fetch_aggregates_when_coarser_than_native(seeded_engine):
    engine, backend, native_cell, parent_cell, ndvi_cells, *_ = seeded_engine
    coarser = NDVI_LEVEL - 1
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP)],
        variables=[VariableSpec.parse("Landsat8_NDVI:NDVI", level=coarser)],
    )
    items = resolve(engine, backend, query)
    result = fetch(engine, backend, items)

    row = result.iloc[0]
    assert row["aggregated"]
    assert row["level"] == coarser
    # Expected: mean of 0.60, 0.65, 0.70, 0.75 = 0.675
    assert abs(row["value"] - 0.675) < 0.001


def test_fetch_multi_source_multi_location(seeded_engine):
    engine, backend, *_ = seeded_engine
    query = DataQuery(
        rows=[(LAT, LON, TIMESTAMP), (47.65, -122.30, TIMESTAMP)],
        variables=[
            VariableSpec.parse("Landsat8_NDVI:NDVI"),
            VariableSpec.parse("ERA5_Land:temperature"),
        ],
    )
    items = resolve(engine, backend, query)
    result = fetch(engine, backend, items)
    assert len(result) == 4  # 2 rows × 2 variables
    assert set(result["source"]) == {"Landsat8_NDVI", "ERA5_Land"}
