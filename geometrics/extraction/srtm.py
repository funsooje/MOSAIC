"""
SRTM elevation and slope extraction.

Terrain from the 30 m SRTM digital elevation model: elevation in metres and
slope in degrees, derived with ee.Terrain.slope.

SRTM is a single static image, not a collection, so there is nothing to filter
by date and every cell has one value for all time. The source is registered
with temporal_granularity "static", which the store reads back at the
1900-01-01 sentinel.
"""

from __future__ import annotations

from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import (
    extracted_level, ensure_source, items_to_ee_feature_collection, submit_export,
)
from geometrics.store.jobs import record_submitted

_SOURCE_NAME = "SRTM_Terrain"
_NATIVE_LEVEL = 13
_PIXEL_RESOLUTION_M = 30
_COLLECTION = "USGS/SRTMGL1_003"

_VARIABLE_DEFS = [
    {"name": "elevation", "unit": "m"},
    {"name": "slope", "unit": "degrees"},
]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "SRTM 30 m digital elevation model — elevation and derived slope",
    "gee_collection": _COLLECTION,
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "static",
    "temporal_granularity": "static",
    "variables": [
        {"name": "elevation", "unit": "m",
         "description": "Height above sea level in metres (SRTM GL1, 30 m)"},
        {"name": "slope", "unit": "degrees",
         "description": "Terrain slope in degrees, from ee.Terrain.slope on the DEM"},
    ],
}


def submit_srtm(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    """Submit one GEE batch job for SRTM terrain items. Returns local job_id."""
    source_id, _ = ensure_source(
        engine=engine,
        name=_SOURCE_NAME,
        native_level=_NATIVE_LEVEL,
        pixel_resolution_m=_PIXEL_RESOLUTION_M,
        source_temporal_granularity="static",
        temporal_granularity="static",
        variable_defs=_VARIABLE_DEFS,
    )

    cells_fc = items_to_ee_feature_collection(backend, items)
    processed = cells_fc.map(_process_feature)

    date_start = min(item["date_start"] for item in items)
    date_end = max(item["date_end"] for item in items)

    var_names = [v["name"] for v in _VARIABLE_DEFS]
    task_id = submit_export(
        collection=processed,
        description=f"GeoMetrics SRTM Terrain {file_prefix}",
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
    """GEE server-side: sample elevation and slope at the feature's location."""
    import ee

    dem = ee.Image(_COLLECTION)
    terrain = dem.select("elevation").addBands(
        ee.Terrain.slope(dem.select("elevation")).rename("slope")
    )

    result = terrain.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=feature.geometry(),
        scale=_PIXEL_RESOLUTION_M,
        bestEffort=True,
        maxPixels=1e9,
    )

    props = {"source": _SOURCE_NAME}
    for name in (v["name"] for v in _VARIABLE_DEFS):
        props[name] = result.get(name)

    return feature.set(props)
