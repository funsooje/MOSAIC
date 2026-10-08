"""
GeoMetrics Viewer — FastAPI server.

Serves the map frontend and exposes two API endpoints:
  GET /api/sources  — list of sources, variables, and available timestamps
  GET /api/data     — bulk cell data (lats/lons/values) for a source/variable/timestamp

Start with:
  python viewer/server.py
  # or
  uvicorn viewer.server:app --reload --port 8765
"""

from __future__ import annotations

import gzip as gz
import json
import logging
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy import inspect as sa_inspect, text

from geometrics import GeoMetrics
from geometrics.backends import get_backend
from geometrics.catalog import CATALOG

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="GeoMetrics Viewer", docs_url="/api/docs")

gm = GeoMetrics()
_backend = get_backend(gm.config.backend)

_coord_cache: dict[tuple[str, str], dict] = {}
_sources_cache: list | None = None


@app.on_event("startup")
def _prefetch_sources() -> None:
    global _sources_cache  # noqa: PLW0603
    _sources_cache = _query_sources()
    log.info("Sources pre-loaded: %d source(s)", len(_sources_cache))


# ---------------------------------------------------------------------------
# API routes — must be registered BEFORE the static file catch-all
# ---------------------------------------------------------------------------

_VIEW_FILE = Path(__file__).parent / "saved_view.json"
_DEFAULT_VIEW: dict[str, Any] = {"center": [-120.5, 47.5], "zoom": 6}


@app.get("/api/view")
def get_view() -> JSONResponse:
    """Return saved map center/zoom, or the default if none saved yet."""
    if _VIEW_FILE.exists():
        return JSONResponse(json.loads(_VIEW_FILE.read_text()))
    return JSONResponse(_DEFAULT_VIEW)


@app.post("/api/view")
async def save_view(request: Request) -> JSONResponse:
    """Persist current map center and zoom to saved_view.json."""
    data = await request.json()
    center = data.get("center")
    zoom = data.get("zoom")
    if not isinstance(center, list) or len(center) != 2 or not isinstance(zoom, (int, float)):
        raise HTTPException(400, "Expected {center: [lng, lat], zoom: number}")
    _VIEW_FILE.write_text(json.dumps({"center": center, "zoom": zoom}))
    return JSONResponse({"ok": True})


@app.get("/api/sources")
def get_sources() -> JSONResponse:
    """Return all sources that have data in the DB, with variables and timestamps."""
    if _sources_cache is not None:
        return JSONResponse(_sources_cache)
    return JSONResponse(_query_sources())


@app.get("/api/data")
def get_data(source: str, variable: str, timestamp: str, bbox: str | None = None) -> Response:
    """
    Return bulk cell data for a source/variable/timestamp.

    Response body (gzip-compressed JSON):
      {
        "lats":   [float, ...],
        "lons":   [float, ...],
        "values": [float | null, ...],
        "meta":   {"min": float, "max": float, "count": int, "unit": str}
      }
    """
    t0 = time.time()
    log.info(
        "GET /api/data source=%s variable=%s timestamp=%s bbox=%s",
        source, variable, timestamp, bbox,
    )

    obs_table = f"obs_{source.lower()}"
    existing_tables = set(sa_inspect(gm.engine).get_table_names())
    if obs_table not in existing_tables:
        raise HTTPException(404, f"No observation table for source {source!r}")

    with gm.engine.connect() as conn:
        src_row = conn.execute(text(
            "SELECT source_id, native_level, pixel_resolution_m FROM sources WHERE name = :name"
        ), {"name": source}).fetchone()
        if src_row is None:
            raise HTTPException(404, f"Source {source!r} not found")

        var_row = conn.execute(text(
            "SELECT unit FROM variables WHERE source_id = :sid AND name = :vname"
        ), {"sid": src_row.source_id, "vname": variable}).fetchone()
        if var_row is None:
            raise HTTPException(404, f"Variable {variable!r} not found for source {source!r}")
        unit = var_row.unit or ""

    cache_key = (source, timestamp)
    if cache_key not in _coord_cache:
        log.info("  loading coords from DB...")
        _coord_cache[cache_key] = _load_cell_coords(obs_table, timestamp)
        log.info("  coords loaded: %d cells (%.1fs)",
                 len(_coord_cache[cache_key]["unit_pks"]), time.time() - t0)
    else:
        log.info("  coords cache hit")

    coord_data = _coord_cache[cache_key]
    if not coord_data["unit_pks"]:
        log.info("  no data for this timestamp")
        return _gzip_json({"lats": [], "lons": [], "values": [], "meta": {"count": 0}})

    all_pks  = coord_data["unit_pks"]
    lats_all = coord_data["lats"]
    lons_all = coord_data["lons"]

    # Spatial filter for focus-area requests
    if bbox:
        west, south, east, north = (float(x) for x in bbox.split(","))
        lats_arr = np.array(lats_all)
        lons_arr = np.array(lons_all)
        mask = (lats_arr >= south) & (lats_arr <= north) & \
               (lons_arr >= west)  & (lons_arr <= east)
        indices = np.where(mask)[0]
        all_pks  = [all_pks[i]  for i in indices]
        lats_all = lats_arr[mask].tolist()
        lons_all = lons_arr[mask].tolist()
        log.info("  bbox filter: %d cells in view", len(all_pks))

    total = len(all_pks)
    limit = 200_000

    if total > limit:
        rng = np.random.default_rng(seed=42)
        idx = np.sort(rng.choice(total, size=limit, replace=False))
        unit_pks = [all_pks[i]  for i in idx]
        lats_out = [lats_all[i] for i in idx]
        lons_out = [lons_all[i] for i in idx]
        log.info("  sampled %d of %d cells", limit, total)
    else:
        unit_pks = all_pks
        lats_out = lats_all
        lons_out = lons_all

    log.info("  fetching values for %d cells...", len(unit_pks))
    values = _load_values(obs_table, variable, unit_pks)
    log.info("  values fetched (%.1fs)", time.time() - t0)

    valid_vals = [v for v in values if v is not None]
    meta: dict[str, Any] = {
        "count": len(values),
        "total": total,
        "sampled": total > limit,
        "unit": unit,
        "pixel_resolution_m": src_row.pixel_resolution_m,
        "min": min(valid_vals) if valid_vals else None,
        "max": max(valid_vals) if valid_vals else None,
    }

    payload = {
        "lats": lats_out,
        "lons": lons_out,
        "values": values,
        "meta": meta,
    }
    log.info("  serializing and compressing...")
    resp = _gzip_json(payload)
    log.info("  done (total %.1fs)", time.time() - t0)
    return resp


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _query_sources() -> list:
    """Query DB for all sources with variables and timestamps."""
    existing_tables = set(sa_inspect(gm.engine).get_table_names())
    result = []
    with gm.engine.connect() as conn:
        src_rows = conn.execute(text(
            "SELECT source_id, name, native_level, temporal_granularity FROM sources ORDER BY name"
        )).fetchall()

        for src in src_rows:
            obs_table = f"obs_{src.name.lower()}"
            if obs_table not in existing_tables:
                continue

            var_rows = conn.execute(text(
                "SELECT name, unit FROM variables WHERE source_id = :sid ORDER BY name"
            ), {"sid": src.source_id}).fetchall()

            ts_rows = conn.execute(text(f"""
                SELECT DISTINCT u.timestamp::date::text AS ts
                FROM {obs_table} o
                JOIN spatiotemporal_units u ON u.id = o.unit_pk
                ORDER BY ts
            """)).fetchall()

            catalog_entry = CATALOG.get(src.name, {})
            result.append({
                "name": src.name,
                "description": catalog_entry.get("description", ""),
                "native_level": src.native_level,
                "temporal_granularity": src.temporal_granularity,
                "variables": [{"name": v.name, "unit": v.unit or ""} for v in var_rows],
                "timestamps": [r.ts for r in ts_rows],
            })
    return result


def _load_cell_coords(obs_table: str, timestamp: str) -> dict:
    """
    All cells with data for (obs_table, timestamp), as centroid lat/lon.

    HierGP has a vectorised path: x/y live in hiergp_cells and the grid library
    converts a whole frame at once. Any other backend goes cell_id by cell_id
    through the GridBackend interface, which is slower but backend-agnostic.
    """
    if gm.config.backend == "hiergp":
        return _load_cell_coords_hiergp(obs_table, timestamp)
    return _load_cell_coords_generic(obs_table, timestamp)


def _load_cell_coords_generic(obs_table: str, timestamp: str) -> dict:
    """Centroids via backend.cell_to_centroid — works for any backend."""
    with gm.engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT o.unit_pk, c.cell_id
            FROM {obs_table} o
            JOIN spatiotemporal_units u ON u.id = o.unit_pk
            JOIN cells c ON c.id = u.cell_pk
            WHERE u.timestamp = :ts
        """), {"ts": timestamp}).fetchall()

    if not rows:
        return {"lats": [], "lons": [], "unit_pks": []}

    lats, lons, unit_pks = [], [], []
    for unit_pk, cell_id in rows:
        lat, lon = _backend.cell_to_centroid(cell_id)
        lats.append(lat)
        lons.append(lon)
        unit_pks.append(unit_pk)
    return {"lats": lats, "lons": lons, "unit_pks": unit_pks}


def _load_cell_coords_hiergp(obs_table: str, timestamp: str) -> dict:
    """Vectorised HierGP path: batch-convert x/y from hiergp_cells."""
    from geometrics.backends.hiergp import _to_internal

    with gm.engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT o.unit_pk, h.x, h.y, c.level
            FROM {obs_table} o
            JOIN spatiotemporal_units u ON u.id = o.unit_pk
            JOIN cells c ON c.id = u.cell_pk
            JOIN hiergp_cells h ON h.cell_pk = c.id
            WHERE u.timestamp = :ts
        """), {"ts": timestamp}).fetchall()

    if not rows:
        return {"lats": [], "lons": [], "unit_pks": []}

    data_df = pd.DataFrame(rows, columns=["unit_pk", "x", "y", "level"])
    lats, lons, unit_pks = [], [], []
    # A source can span levels (ERA5 sits at 8 while the rest sit at 13), and
    # generateCenters is per-level, so convert one level at a time.
    for level, group in data_df.groupby("level"):
        internal_level = _to_internal(int(level))
        centers = _backend._grider.generateCenters(group[["x", "y"]], internal_level)
        lats.extend(centers[f"l{internal_level}_lat"].tolist())
        lons.extend(centers[f"l{internal_level}_lon"].tolist())
        unit_pks.extend(group["unit_pk"].tolist())

    return {"lats": lats, "lons": lons, "unit_pks": unit_pks}


def _load_values(obs_table: str, variable: str, unit_pks: list[int]) -> list:
    """Fetch values for a single variable in the order of unit_pks."""
    chunk_size = 50_000
    pk_to_val: dict[int, float | None] = {}

    with gm.engine.connect() as conn:
        for i in range(0, len(unit_pks), chunk_size):
            chunk = unit_pks[i : i + chunk_size]
            pks_str = ",".join(str(p) for p in chunk)
            rows = conn.execute(text(f"""
                SELECT unit_pk, {variable}
                FROM {obs_table}
                WHERE unit_pk IN ({pks_str})
            """)).fetchall()
            for pk, val in rows:
                pk_to_val[pk] = val

    return [pk_to_val.get(pk) for pk in unit_pks]


def _gzip_json(obj: Any) -> Response:
    """Serialize obj to JSON and gzip-compress it."""
    compressed = gz.compress(json.dumps(obj, allow_nan=False).encode(), compresslevel=6)
    return Response(
        content=compressed,
        media_type="application/json",
        headers={"Content-Encoding": "gzip"},
    )


# ---------------------------------------------------------------------------
# Static files — mount LAST so API routes take priority
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765, reload=False)
