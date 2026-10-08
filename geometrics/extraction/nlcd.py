"""
USGS National Land Cover Database (NLCD) extraction.

NLCD is released every 2–3 years, not annually. Available release years:
  [2001, 2004, 2006, 2008, 2011, 2013, 2016, 2019]

Each requested year is snapped to the nearest NLCD release year in Python
before submission. The stored timestamp is the user's requested year so that
repeated requests for the same year are deduplicated correctly.

Variables: landcover, impervious, impervious_descriptor
"""

from __future__ import annotations

from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.config import GeoMetricsConfig
from geometrics.extraction.base import extracted_level, ensure_source, submit_export
from geometrics.store.jobs import record_submitted

_SOURCE_NAME = "NLCD"
_NATIVE_LEVEL = 13
_PIXEL_RESOLUTION_M = 30
_COLLECTION = "USGS/NLCD_RELEASES/2019_REL/NLCD"

_NLCD_YEARS = [2001, 2004, 2006, 2008, 2011, 2013, 2016, 2019]

_VARIABLE_DEFS = [
    {"name": "landcover", "unit": "class"},
    {"name": "impervious", "unit": "percent"},
    {"name": "impervious_descriptor", "unit": "class"},
]

SOURCE_SPEC = {
    "name": _SOURCE_NAME,
    "description": "USGS National Land Cover Database — landcover, impervious surface",
    "gee_collection": _COLLECTION,
    "pixel_resolution_m": _PIXEL_RESOLUTION_M,
    "native_level": _NATIVE_LEVEL,
    "source_temporal_granularity": "2-3 years",
    "temporal_granularity": "year",
    "variables": [
        {"name": "landcover", "unit": "class",
         "description": "NLCD land cover class code (e.g. 11=open water, 41=forest)"},
        {"name": "impervious", "unit": "percent",
         "description": "Percent impervious surface (0–100)"},
        {"name": "impervious_descriptor", "unit": "class",
         "description": "Impervious surface descriptor class code"},
    ],
}


def _snap_nlcd_year(year: int) -> int:
    """Return the nearest NLCD release year to the given year."""
    return min(_NLCD_YEARS, key=lambda y: abs(y - year))


def submit_nlcd(
    engine: Engine,
    config: GeoMetricsConfig,
    backend: GridBackend,
    items: list[dict],
    gdrive_folder: str,
    file_prefix: str,
) -> int:
    """Submit one GEE batch job for NLCD items. Returns local job_id."""
    import ee

    source_id, _ = ensure_source(
        engine=engine,
        name=_SOURCE_NAME,
        native_level=_NATIVE_LEVEL,
        pixel_resolution_m=_PIXEL_RESOLUTION_M,
        source_temporal_granularity="2-3 years",
        temporal_granularity="year",
        variable_defs=_VARIABLE_DEFS,
    )

    # Build features with the pre-snapped NLCD year as a property.
    features = []
    for item in items:
        lat, lon = backend.cell_to_centroid(item["cell_id"])
        requested_year = int(item["timestamp"][:4])
        nlcd_year = str(_snap_nlcd_year(requested_year))
        feat = ee.Feature(
            ee.Geometry.Point([lon, lat]),
            {
                "cell_id": item["cell_id"],
                "start_date": item["date_start"],
                "end_date": item["date_end"],
                "timestamp": item["timestamp"],
                "nlcd_year": nlcd_year,
            },
        )
        features.append(feat)

    cells_fc = ee.FeatureCollection(features)
    processed = cells_fc.map(_process_feature)

    date_start = min(item["date_start"] for item in items)
    date_end = max(item["date_end"] for item in items)

    var_names = [v["name"] for v in _VARIABLE_DEFS]
    task_id = submit_export(
        collection=processed,
        description=f"GeoMetrics NLCD {file_prefix}",
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
    """GEE server-side: extract NLCD values for the pre-snapped release year."""
    import ee

    geometry = feature.geometry()
    nlcd_year = feature.get("nlcd_year")

    image = (
        ee.ImageCollection(_COLLECTION)
        .filter(ee.Filter.eq("system:index", nlcd_year))
        .first()
        .select(["landcover", "impervious", "impervious_descriptor"])
    )

    result = image.reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=geometry,
        scale=_PIXEL_RESOLUTION_M,
        maxPixels=1e9,
        bestEffort=True,
    )

    return feature.set({
        "source": _SOURCE_NAME,
        "landcover": result.get("landcover"),
        "impervious": result.get("impervious"),
        "impervious_descriptor": result.get("impervious_descriptor"),
    })
