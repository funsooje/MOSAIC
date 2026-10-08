"""
ERA5-Land hourly extraction (ECMWF/ERA5_LAND/HOURLY).

ERA5-Land native resolution is ~9 km. We use HierGP level 7 (~6.4 km).
Each submitted feature carries start_date as its hourly timestamp property.
GEE filters to that exact hour and takes the first (only) image.

Output CSV columns: cell_id, timestamp, source, <variable columns>
"""

from __future__ import annotations

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import extracted_level, ensure_source, items_to_ee_feature_collection, submit_export
from geometrics.store.jobs import record_submitted
from sqlalchemy.engine import Engine

_SOURCE_NAME = "ERA5_Land"
_NATIVE_LEVEL = 7
_PIXEL_RESOLUTION_M = 9000
_COLLECTION = "ECMWF/ERA5_LAND/HOURLY"

_VARIABLE_DEFS = [
    {"name": "temperature_2m",                    "unit": "K"},
    {"name": "dewpoint_temperature_2m",           "unit": "K"},
    {"name": "surface_pressure",                  "unit": "Pa"},
    {"name": "u_component_of_wind_10m",           "unit": "m/s"},
    {"name": "v_component_of_wind_10m",           "unit": "m/s"},
    {"name": "surface_thermal_radiation_downwards","unit": "J/m^2"},
    {"name": "surface_net_solar_radiation",       "unit": "J/m^2"},
    {"name": "total_precipitation",               "unit": "m"},
]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "ERA5-Land hourly reanalysis (ECMWF), 8 surface variables at ~9 km",
    "gee_collection": _COLLECTION,
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "hour",
    "temporal_granularity": "hour",
    "variables": [
        {"name": v["name"], "unit": v["unit"], "description": v["name"].replace("_", " ")}
        for v in _VARIABLE_DEFS
    ],
}


def submit_era5_land(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    source_id, _ = ensure_source(
        engine=engine,
        name=_SOURCE_NAME,
        native_level=_NATIVE_LEVEL,
        pixel_resolution_m=_PIXEL_RESOLUTION_M,
        source_temporal_granularity="hour",
        temporal_granularity="hour",
        variable_defs=_VARIABLE_DEFS,
    )

    cells_fc = items_to_ee_feature_collection(backend, items)
    processed = cells_fc.map(_process_feature)

    date_start = min(item["date_start"] for item in items)
    date_end = max(item["date_end"] for item in items)

    var_names = [v["name"] for v in _VARIABLE_DEFS]
    task_id = submit_export(
        collection=processed,
        description=f"GeoMetrics ERA5-Land {file_prefix}",
        folder=gdrive_folder,
        file_prefix=file_prefix,
        properties=["cell_id", "timestamp", "source"] + var_names,
    )

    return record_submitted(
        engine=engine,
        task_id=task_id,
        source_id=source_id,
        level=extracted_level(items, _NATIVE_LEVEL),
        date_start=date_start,
        date_end=date_end,
        gdrive_folder=gdrive_folder,
        file_prefix=file_prefix,
        gdrive_base=config.gdrive_base,
        row_count=len(items),
    )


def _process_feature(feature):
    """GEE server-side: extract all ERA5-Land variables for the feature's hour."""
    import ee

    start = ee.Date(feature.get("start_date"))
    end = start.advance(1, "hour")

    band_names = [v["name"] for v in _VARIABLE_DEFS]

    image = (
        ee.ImageCollection(_COLLECTION)
        .filterDate(start, end)
        .select(band_names)
        .first()
    )

    result = image.reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=feature.geometry(),
        scale=_PIXEL_RESOLUTION_M,
        bestEffort=True,
        maxPixels=1e9,
    )

    props = {"source": _SOURCE_NAME}
    for name in band_names:
        props[name] = result.get(name)

    return feature.set(props)
