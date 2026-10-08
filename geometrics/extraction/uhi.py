"""
Yale YCEO Urban Heat Island (UHI) extraction.

Extracts daytime and nighttime UHI for yearly, winter, and summer seasons.
Data is available 2003–2018; years outside that range are clamped.
The stored timestamp is the user's requested year, not the clamped year.

Collections:
  YALE/YCEO/UHI/UHI_yearly_pixel/v4
  YALE/YCEO/UHI/Winter_UHI_yearly_pixel/v4
  YALE/YCEO/UHI/Summer_UHI_yearly_pixel/v4
"""

from __future__ import annotations

from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import extracted_level, ensure_source, items_to_ee_feature_collection, submit_export
from geometrics.store.jobs import record_submitted

_SOURCE_NAME = "YALE_UHI"
_NATIVE_LEVEL = 10
_PIXEL_RESOLUTION_M = 1000
_UHI_MIN_YEAR = 2003
_UHI_MAX_YEAR = 2018

_VARIABLE_DEFS = [
    {"name": "yearly_daytime", "unit": "K"},
    {"name": "yearly_nighttime", "unit": "K"},
    {"name": "winter_daytime", "unit": "K"},
    {"name": "winter_nighttime", "unit": "K"},
    {"name": "summer_daytime", "unit": "K"},
    {"name": "summer_nighttime", "unit": "K"},
]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "Yale YCEO Urban Heat Island — yearly/winter/summer daytime and nighttime UHI",
    "gee_collection": "YALE/YCEO/UHI/UHI_yearly_pixel/v4",
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "year",
    "temporal_granularity": "year",
    "variables": [
        {"name": "yearly_daytime", "unit": "K",
         "description": "Annual mean daytime UHI (K)"},
        {"name": "yearly_nighttime", "unit": "K",
         "description": "Annual mean nighttime UHI (K)"},
        {"name": "winter_daytime", "unit": "K",
         "description": "Winter daytime UHI (K)"},
        {"name": "winter_nighttime", "unit": "K",
         "description": "Winter nighttime UHI (K)"},
        {"name": "summer_daytime", "unit": "K",
         "description": "Summer daytime UHI (K)"},
        {"name": "summer_nighttime", "unit": "K",
         "description": "Summer nighttime UHI (K)"},
    ],
}


def submit_uhi(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    """Submit one GEE batch job for Yale UHI items. Returns local job_id."""
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
        description=f"GeoMetrics UHI {file_prefix}",
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
    """GEE server-side: extract yearly/winter/summer UHI for the feature's year."""
    import ee

    geometry = feature.geometry()

    # Clamp to available data range 2003–2018.
    year_num = ee.Date(feature.get("start_date")).get("year")
    year_str = year_num.min(_UHI_MAX_YEAR).max(_UHI_MIN_YEAR).format("%d")

    def _get_uhi(collection_id, prefix):
        image = (
            ee.ImageCollection(collection_id)
            .filter(ee.Filter.eq("system:index", year_str))
            .first()
            .select(["Daytime", "Nighttime"])
        )
        result = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geometry,
            scale=_PIXEL_RESOLUTION_M,
            maxPixels=1e9,
        )
        return {
            prefix + "_daytime": result.get("Daytime"),
            prefix + "_nighttime": result.get("Nighttime"),
        }

    props = {"source": _SOURCE_NAME}
    props.update(_get_uhi("YALE/YCEO/UHI/UHI_yearly_pixel/v4", "yearly"))
    props.update(_get_uhi("YALE/YCEO/UHI/Winter_UHI_yearly_pixel/v4", "winter"))
    props.update(_get_uhi("YALE/YCEO/UHI/Summer_UHI_yearly_pixel/v4", "summer"))

    return feature.set(props)
