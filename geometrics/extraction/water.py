"""
JRC Global Surface Water distance-to-water extraction.

Computes Euclidean distance (metres) from each cell centroid to the nearest
large permanent water body using the JRC Yearly History collection.

"Permanent water" = water class > 2 (i.e. class 3).
Small water bodies (< 10 pixels at 30 m) are excluded to avoid noise from
ponds and irrigation canals.
"""

from __future__ import annotations

from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import extracted_level, ensure_source, items_to_ee_feature_collection, submit_export
from geometrics.store.jobs import record_submitted

_SOURCE_NAME = "JRC_Water"
_NATIVE_LEVEL = 13
_PIXEL_RESOLUTION_M = 30
_COLLECTION = "JRC/GSW1_4/YearlyHistory"

_VARIABLE_DEFS = [
    {"name": "water_distance", "unit": "m"},
]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "JRC Global Surface Water — distance to nearest large permanent water body",
    "gee_collection": _COLLECTION,
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "year",
    "temporal_granularity": "year",
    "variables": [
        {
            "name": "water_distance",
            "unit": "m",
            "description": (
                "Euclidean distance to nearest large permanent water body (metres). "
                "0 = inside a water body."
            ),
        },
    ],
}


def submit_water(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    """Submit one GEE batch job for JRC water-distance items. Returns local job_id."""
    source_id, _ = ensure_source(
        engine=engine,
        name=_SOURCE_NAME,
        native_level=_NATIVE_LEVEL,
        pixel_resolution_m=_PIXEL_RESOLUTION_M,
        source_temporal_granularity="year",
        temporal_granularity="year",
        variable_defs=_VARIABLE_DEFS,
    )

    cells_fc = items_to_ee_feature_collection(backend, items)
    processed = cells_fc.map(_process_feature)

    date_start = min(item["date_start"] for item in items)
    date_end = max(item["date_end"] for item in items)

    var_names = [v["name"] for v in _VARIABLE_DEFS]
    task_id = submit_export(
        collection=processed,
        description=f"GeoMetrics JRC Water {file_prefix}",
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


_JRC_MAX_YEAR = 2021  # last full year in JRC/GSW1_4/YearlyHistory


def _process_feature(feature):
    """GEE server-side: compute distance to permanent water for the feature's year."""
    import ee

    geometry = feature.geometry()

    # Clamp to the last available JRC year; store under the requested timestamp.
    year_num = ee.Date(feature.get("start_date")).get("year").min(_JRC_MAX_YEAR)
    year_start = ee.Date.fromYMD(year_num, 1, 1)
    year_end = ee.Date.fromYMD(year_num, 12, 31)

    col = ee.ImageCollection(_COLLECTION).filterDate(year_start, year_end)

    # Gracefully handle years with no imagery (returns null instead of failing).
    fallback = ee.Image.constant(0).rename("waterYear")
    safe_min = ee.Image(ee.Algorithms.If(col.size().gt(0), col.min(), fallback))

    # Permanent water: class 3 (> 2). selfMask() sets non-water to no-data.
    water = safe_min.gt(2).selfMask()

    # Remove small water bodies (< 10 pixels × 900 m² each)
    min_area = _PIXEL_RESOLUTION_M * _PIXEL_RESOLUTION_M * 10
    pixel_count = water.connectedPixelCount(maxSize=100)
    area = pixel_count.multiply(ee.Image.pixelArea())
    large_water = water.updateMask(area.gte(min_area))

    # fastDistanceTransform returns squared-pixel distance; convert to metres
    distance = (
        large_water.mask()
        .fastDistanceTransform(neighborhood=1024)
        .multiply(ee.Image.pixelArea())
        .sqrt()
        .rename("water_distance")
    )

    result = distance.reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=geometry,
        scale=_PIXEL_RESOLUTION_M,
        maxPixels=1e9,
        bestEffort=True,
    )

    # Return null if the collection had no real imagery for this year.
    water_dist = ee.Algorithms.If(col.size().gt(0), result.get("water_distance"), None)

    return feature.set({
        "source": _SOURCE_NAME,
        "water_distance": water_dist,
    })
