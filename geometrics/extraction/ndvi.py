"""
Landsat NDVI extraction (Landsat 5 / 7 / 8 / 9).

Each submitted feature carries start_date and end_date as properties.
GEE reads those per-feature at runtime to filter the image collection,
so different cells can span different date ranges within one batch.

Output CSV columns: cell_id, variable_name, timestamp, value
"""

from __future__ import annotations

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import extracted_level, ensure_source, items_to_ee_feature_collection, submit_export
from geometrics.store.jobs import record_submitted
from sqlalchemy.engine import Engine

_SOURCE_NAME = "Landsat_NDVI"
_NATIVE_LEVEL = 13
_PIXEL_RESOLUTION_M = 30
_GDRIVE_FOLDER = "geometrics_ndvi"

_VARIABLE_DEFS = [{"name": "NDVI", "unit": "index"}]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "Landsat 5/7/8/9 surface reflectance NDVI, cloud-masked median composite",
    "gee_collection": "LANDSAT/LC08/C02/T1_L2",
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "16-day",
    "temporal_granularity": "year",
    "variables": [
        {
            "name": "NDVI",
            "unit": "index",
            "description": "Normalized Difference Vegetation Index, median of all Landsat missions",
        },
    ],
}


def submit_ndvi(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    """
    Submit one GEE batch job for a batch of missing Landsat NDVI items.

    gdrive_folder: Drive folder for this batch (caller sets subfolder per source).
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
        description=f"GeoMetrics Landsat NDVI {file_prefix}",
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
    """GEE server-side: build NDVI composite using each feature's own date range."""
    import ee

    start = feature.get("start_date")
    end = feature.get("end_date")
    geometry = feature.geometry()

    collections = []
    for cid in ("LANDSAT/LC08/C02/T1_L2", "LANDSAT/LC09/C02/T1_L2"):
        col = (
            ee.ImageCollection(cid)
            .filterDate(start, end)
            .filterBounds(geometry)
            .map(_mask_clouds)
            .map(_apply_scale_factors)
            .map(_compute_ndvi)
        )
        collections.append(col)

    for cid in ("LANDSAT/LE07/C02/T1_L2", "LANDSAT/LT05/C02/T1_L2"):
        col = (
            ee.ImageCollection(cid)
            .filterDate(start, end)
            .filterBounds(geometry)
            .map(_mask_clouds)
            .map(_harmonize_to_oli)
            .map(_apply_scale_factors)
            .map(_compute_ndvi)
        )
        collections.append(col)

    merged = collections[0]
    for col in collections[1:]:
        merged = merged.merge(col)

    median = merged.median()
    result = median.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=geometry,
        scale=_PIXEL_RESOLUTION_M,
        maxPixels=1e9,
        bestEffort=True,
    )

    return feature.set({"source": _SOURCE_NAME, "NDVI": result.get("NDVI")})


def _mask_clouds(image):
    qa = image.select("QA_PIXEL")
    cloud        = qa.bitwiseAnd(1 << 3).And(qa.bitwiseAnd(3 << 8).gte(2))
    cloud_shadow = qa.bitwiseAnd(1 << 4).And(qa.bitwiseAnd(3 << 10).gte(2))
    snow         = qa.bitwiseAnd(1 << 5).And(qa.bitwiseAnd(3 << 12).gte(2))
    return image.updateMask(cloud.Or(cloud_shadow).Or(snow).Not())


def _apply_scale_factors(image):
    for band in ("SR_B4", "SR_B5"):
        scaled = image.select(band).multiply(0.0000275).add(-0.2)
        image = image.addBands(scaled.rename(band), overwrite=True)
    return image


def _harmonize_to_oli(image):
    red = image.select("SR_B3").multiply(0.9825).add(-0.0022).rename("SR_B4")
    nir = image.select("SR_B4").multiply(1.0073).add(-0.0021).rename("SR_B5")
    return image.addBands(red, overwrite=True).addBands(nir, overwrite=True)


def _compute_ndvi(image):
    return image.normalizedDifference(["SR_B5", "SR_B4"]).rename("NDVI")
