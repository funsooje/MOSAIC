"""
MODIS MOD44B annual tree cover extraction.

Each submitted feature carries start_date and end_date as properties.
GEE reads those per-feature at runtime, so different cells can cover
different years within one batch.

GEE bands extracted:
  Percent_Tree_Cover, Percent_NonTree_Vegetation, Percent_NonVegetated,
  Quality, Percent_Tree_Cover_SD, Percent_NonVegetated_SD, Cloud
"""

from __future__ import annotations

from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import extracted_level, ensure_source, items_to_ee_feature_collection, submit_export
from geometrics.store.jobs import record_submitted

_SOURCE_NAME = "MODIS_Treecover"
_NATIVE_LEVEL = 12
_PIXEL_RESOLUTION_M = 250
_COLLECTION = "MODIS/061/MOD44B"

# GEE band name → output variable name
_BAND_MAP = {
    "Percent_Tree_Cover": "percent_tree_cover",
    "Percent_NonTree_Vegetation": "percent_nontree_vegetation",
    "Percent_NonVegetated": "percent_nonvegetated",
    "Quality": "quality",
    "Percent_Tree_Cover_SD": "percent_tree_cover_sd",
    "Percent_NonVegetated_SD": "percent_nonvegetated_sd",
    "Cloud": "cloud",
}

_VARIABLE_DEFS = [
    {"name": "percent_tree_cover", "unit": "percent"},
    {"name": "percent_nontree_vegetation", "unit": "percent"},
    {"name": "percent_nonvegetated", "unit": "percent"},
    {"name": "quality", "unit": "flag"},
    {"name": "percent_tree_cover_sd", "unit": "percent"},
    {"name": "percent_nonvegetated_sd", "unit": "percent"},
    {"name": "cloud", "unit": "percent"},
]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "MODIS MOD44B annual vegetation continuous fields (250 m)",
    "gee_collection": _COLLECTION,
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "year",
    "temporal_granularity": "year",
    "variables": [
        {"name": "percent_tree_cover", "unit": "percent",
         "description": "Annual percent tree cover (0–100)"},
        {"name": "percent_nontree_vegetation", "unit": "percent",
         "description": "Annual percent non-tree vegetation (0–100)"},
        {"name": "percent_nonvegetated", "unit": "percent",
         "description": "Annual percent non-vegetated (0–100)"},
        {"name": "quality", "unit": "flag",
         "description": "Quality flag"},
        {"name": "percent_tree_cover_sd", "unit": "percent",
         "description": "Standard deviation of percent tree cover"},
        {"name": "percent_nonvegetated_sd", "unit": "percent",
         "description": "Standard deviation of percent non-vegetated"},
        {"name": "cloud", "unit": "percent",
         "description": "Cloud cover percentage"},
    ],
}


def submit_treecover(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    """
    Submit one GEE batch job for a batch of missing MODIS treecover items.

    Returns the local job_id.
    """
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
        description=f"GeoMetrics Treecover {file_prefix}",
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
    """GEE server-side: pick the annual treecover image for each feature's year."""
    import ee

    year = ee.Date(feature.get("start_date")).get("year")

    image = (
        ee.ImageCollection(_COLLECTION)
        .filter(ee.Filter.calendarRange(year, year, "year"))
        .select(list(_BAND_MAP.keys()))
        .first()
    )

    result = image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=feature.geometry(),
        scale=_PIXEL_RESOLUTION_M,
        maxPixels=1e9,
        bestEffort=True,
    )

    props = {"source": _SOURCE_NAME}
    for band, var in _BAND_MAP.items():
        props[var] = result.get(band)

    return feature.set(props)
