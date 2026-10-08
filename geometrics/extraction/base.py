"""
Shared helpers for GEE extraction modules.

All functions that touch ee.* are isolated so the rest of the module
is importable without GEE authentication.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.store.schema import sources, variables


def ensure_source(
    engine: Engine,
    name: str,
    native_level: int,
    pixel_resolution_m: int,
    source_temporal_granularity: str,
    temporal_granularity: str,
    variable_defs: list[dict],
) -> int:
    """
    Insert the source and its variables if they don't exist yet.

    variable_defs: list of {"name": ..., "unit": ...}
    Returns source_id.
    """
    with engine.connect() as conn:
        row = conn.execute(
            select(sources.c.source_id).where(sources.c.name == name)
        ).fetchone()

    if row:
        return row.source_id, False

    with engine.begin() as conn:
        result = conn.execute(sources.insert().values(
            name=name,
            native_level=native_level,
            pixel_resolution_m=pixel_resolution_m,
            source_temporal_granularity=source_temporal_granularity,
            temporal_granularity=temporal_granularity,
        ))
        source_id = result.inserted_primary_key[0]
        for var in variable_defs:
            conn.execute(variables.insert().values(
                source_id=source_id,
                name=var["name"],
                unit=var.get("unit"),
            ))
    return source_id, True


def extracted_level(items: list[dict], fallback: int) -> int:
    """
    The grid level the batch is actually being extracted at.

    resolve() picks the level from the database's sources.native_level, which
    can differ from the module constant (migrated data sits at its own level),
    so record what was extracted rather than what the catalog assumes.
    """
    levels = {item["requested_level"] for item in items if "requested_level" in item}
    return levels.pop() if len(levels) == 1 else fallback


def items_to_ee_feature_collection(backend: GridBackend, items: list[dict]):
    """
    Build a GEE FeatureCollection from resolved missing items.

    Each Feature is the cell centroid with properties:
      cell_id, start_date, end_date, timestamp

    GEE reads start_date/end_date per-feature at runtime to filter the image
    collection, so different cells can have different date ranges in one batch.
    """
    import ee

    features = []
    for item in items:
        lat, lon = backend.cell_to_centroid(item["cell_id"])
        feat = ee.Feature(
            ee.Geometry.Point([lon, lat]),
            {
                "cell_id": item["cell_id"],
                "start_date": item["date_start"],
                "end_date": item["date_end"],
                "timestamp": item["timestamp"],
            },
        )
        features.append(feat)
    return ee.FeatureCollection(features)


def submit_export(
    collection,
    description: str,
    folder: str,
    file_prefix: str,
    properties: list[str],
) -> str:
    """
    Submit a GEE Export.table.toDrive task and return the task ID.

    properties: list of feature property names to include in the export.
    GEE always adds 'system:index' and '.geo'; ingest drops these automatically.
    """
    import ee

    task = ee.batch.Export.table.toDrive(
        collection=collection,
        description=description,
        folder=folder,
        fileNamePrefix=file_prefix,
        fileFormat="CSV",
        selectors=properties,
    )
    task.start()
    return task.id
