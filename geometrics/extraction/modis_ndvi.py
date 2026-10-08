"""
MODIS MOD13Q1 NDVI extraction.

MOD13Q1 is a 16-day composite at 250 m. We collect all composites within
the feature's start_date/end_date window, apply the SummaryQA quality mask,
scale the NDVI band (divide by 10000), and take the annual mean.

Each submitted feature carries start_date and end_date as properties.
GEE reads those per-feature at runtime so different cells can cover
different years within one batch.

Output CSV columns: cell_id, timestamp, source, NDVI
"""

from __future__ import annotations

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import extracted_level, ensure_source, items_to_ee_feature_collection, submit_export
from geometrics.store.jobs import record_submitted
from sqlalchemy.engine import Engine

_SOURCE_NAME = "MODIS_NDVI"
_NATIVE_LEVEL = 11
_PIXEL_RESOLUTION_M = 250
_COLLECTION = "MODIS/061/MOD13Q1"

_VARIABLE_DEFS = [{"name": "NDVI", "unit": "index"}]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "MODIS MOD13Q1 16-day NDVI composite, annual mean",
    "gee_collection": _COLLECTION,
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "16-day",
    "temporal_granularity": "year",
    "variables": [
        {
            "name": "NDVI",
            "unit": "index",
            "description": "Normalized Difference Vegetation Index, annual mean of 16-day composites (MOD13Q1)",
        },
    ],
}


def submit_modis_ndvi(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    """
    Submit one GEE batch job for a batch of missing MODIS NDVI items.

    gdrive_folder: Drive folder for this batch (caller sets per source).
    file_prefix: filename without extension, e.g. "batch_001".
    Returns the local job_id.
    """
    source_id, _ = ensure_source(
        engine=engine,
        name=_SOURCE_NAME,
        native_level=_NATIVE_LEVEL,
        pixel_resolution_m=_PIXEL_RESOLUTION_M,
        source_temporal_granularity="16-day",
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
        description=f"GeoMetrics MODIS NDVI {file_prefix}",
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
    """GEE server-side: annual mean NDVI from all 16-day composites in the feature's date range."""
    import ee

    start = feature.get("start_date")
    end = feature.get("end_date")

    collection = (
        ee.ImageCollection(_COLLECTION)
        .filterDate(start, end)
        .map(_mask_quality)
        .map(_scale_ndvi)
        .select("NDVI")
    )

    mean_image = collection.mean()
    result = mean_image.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=feature.geometry(),
        scale=_PIXEL_RESOLUTION_M,
        maxPixels=1e9,
        bestEffort=True,
    )

    return feature.set({"source": _SOURCE_NAME, "NDVI": result.get("NDVI")})


def _mask_quality(image):
    """Keep only good and marginal quality pixels (SummaryQA <= 1)."""
    return image.updateMask(image.select("SummaryQA").lte(1))


def _scale_ndvi(image):
    """Apply MODIS NDVI scale factor (raw values are integers × 10000)."""
    scaled = image.select("NDVI").toFloat().divide(10000)
    return image.addBands(scaled.rename("NDVI"), overwrite=True)
