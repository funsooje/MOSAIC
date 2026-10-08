"""
Local source computation: spatial operations against user-supplied GeoDataFrames.

Supported operations
--------------------
nearest_distance
    Distance in km from each grid-cell centroid to the nearest feature in the
    reference GeoDataFrame.  For polygon sources, uses true boundary distance
    (not centroid-to-centroid).  Returns 0 for centroids that fall inside a
    polygon.

inside
    1.0 if the centroid falls inside any feature polygon, 0.0 otherwise.

attribute_lookup
    Nearest-neighbour match on a point DataFrame with per-year value columns.
    Each location/year pair looks up the closest point in the reference dataset
    and reads the value from the corresponding year column.  Years outside the
    dataset's range are clipped to the nearest available year.

All operations write results into the standard obs_{source_name} table so that
gm.fetch() works without modification.
"""

from __future__ import annotations

from pyproj import Geod
from shapely.ops import nearest_points
from shapely.strtree import STRtree
from scipy.spatial import KDTree

import numpy as np
import pandas as pd
import geopandas as gpd
from sqlalchemy import select
from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.store.ingest import _ensure_cells, _ensure_spatiotemporal_units, _insert_wide
from geometrics.store.schema import (
    ensure_source_obs_table,
    ensure_spatiotemporal_year_partitions,
    source_table_name,
    sources,
    variables as variables_table,
)

_STATIC_TIMESTAMP = "1900-01-01"

# EPSG:3857 is metres but Web Mercator: distances are inflated by 1/cos(latitude),
# about 1.48x at 47N, so it must not be used to measure anything. Distances are
# computed in a local equidistant projection centred on the data instead.
_GEOGRAPHIC_CRS = "EPSG:4326"


_GEOD = Geod(ellps="WGS84")
_EARTH_R_M = 6_371_008.8          # mean Earth radius, for the ECEF search tree


def _ecef(lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """
    Lat/lon to earth-centred Cartesian metres.

    Nearest neighbour by straight-line (chord) distance in this space gives the
    same answer as nearest by great-circle distance, because chord length rises
    monotonically with arc length. That makes the search exact at any extent,
    unlike a KDTree on degrees or on a single projected CRS. No projection can
    serve this store: its cells span roughly 38S to 64N.
    """
    lat_r = np.radians(lats)
    lon_r = np.radians(lons)
    cos_lat = np.cos(lat_r)
    return np.column_stack([
        _EARTH_R_M * cos_lat * np.cos(lon_r),
        _EARTH_R_M * cos_lat * np.sin(lon_r),
        _EARTH_R_M * np.sin(lat_r),
    ])


def _geodesic_km(lats, lons, other_lats, other_lons) -> np.ndarray:
    """True WGS84 geodesic distance in km, elementwise."""
    _, _, metres = _GEOD.inv(lons, lats, other_lons, other_lats)
    return np.abs(metres) / 1000.0


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _load_source_meta(engine: Engine, source_name: str) -> tuple[list[dict], str]:
    """Return (variable_defs, temporal_granularity) for a registered local source."""
    with engine.connect() as conn:
        src_row = conn.execute(
            select(sources.c.source_id, sources.c.temporal_granularity)
            .where(sources.c.name == source_name)
        ).fetchone()
        if src_row is None:
            raise ValueError(
                f"Source {source_name!r} not registered. Run gm.register_local_source() first."
            )
        var_rows = conn.execute(
            select(variables_table.c.name, variables_table.c.unit).where(
                variables_table.c.source_id == src_row.source_id
            )
        ).fetchall()
    return ([{"name": r.name, "unit": r.unit} for r in var_rows],
            src_row.temporal_granularity)


def _load_source_var_defs(engine: Engine, source_name: str) -> list[dict]:
    """Variable definitions only — granularity is irrelevant to the static ops."""
    return _load_source_meta(engine, source_name)[0]


def _centroids_gdf(
    locations_df: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    backend: GridBackend,
    native_level: int,
) -> gpd.GeoDataFrame:
    """
    Snap each location to its grid cell and return one GeoDataFrame row per unique
    cell in WGS-84 with columns [cell_id, lat, lon, geometry].
    """
    records = []
    seen: set[str] = set()
    for _, row in locations_df.iterrows():
        lat, lon = float(row[lat_col]), float(row[lon_col])
        cell_id = backend.point_to_cell(lat, lon, native_level)
        if cell_id in seen:
            continue
        seen.add(cell_id)
        clat, clon = backend.cell_to_centroid(cell_id)
        records.append({"cell_id": cell_id, "lat": clat, "lon": clon})

    centroids_df = pd.DataFrame(records)
    geom = gpd.points_from_xy(centroids_df["lon"], centroids_df["lat"])
    return gpd.GeoDataFrame(centroids_df, geometry=geom, crs="EPSG:4326")


def _write_obs(
    engine: Engine,
    source_name: str,
    var_defs: list[dict],
    result_df: pd.DataFrame,
    var_cols: list[str],
    backend_name: str,
    years: set[int],
    upsert: bool = False,
) -> int:
    """Register cells + spatiotemporal units and insert (or upsert) observation rows."""
    ensure_source_obs_table(engine, source_name, var_defs)
    ensure_spatiotemporal_year_partitions(engine, years)

    pk_map = _ensure_cells(engine, result_df["cell_id"].tolist(), backend_name)
    result_df = result_df.copy()
    result_df["cell_pk"] = result_df["cell_id"].map(pk_map)
    unit_map = _ensure_spatiotemporal_units(engine, result_df)
    result_df["unit_pk"] = result_df.apply(
        lambda r: unit_map.get((int(r["cell_pk"]), str(r["timestamp"]))), axis=1
    )

    inserted = _insert_wide(engine, result_df, source_name, var_cols, upsert=upsert)
    print(f"  Inserted/updated {inserted} row(s) into {source_table_name(source_name)}.")
    return inserted


# ---------------------------------------------------------------------------
# nearest_distance
# ---------------------------------------------------------------------------

def compute_nearest_distance(
    engine: Engine,
    backend: GridBackend,
    locations_df: pd.DataFrame,
    source_name: str,
    geodataframe: gpd.GeoDataFrame,
    lat_col: str,
    lon_col: str,
    native_level: int,
    backend_name: str = "hiergp",
) -> int:
    """
    Compute nearest-feature distance (km) for every unique grid cell covered by
    locations_df and write results to obs_{source_name}.

    For polygon GeoDataFrames, centroids that fall inside a feature get distance 0.
    Returns the number of rows inserted.
    """
    var_defs = _load_source_var_defs(engine, source_name)
    var_names = [v["name"] for v in var_defs]
    if len(var_names) != 1 or var_names[0] != "nearest_distance":
        raise ValueError(
            f"Source {source_name!r} must have a single variable named 'nearest_distance'. "
            f"Got: {var_names}"
        )

    print(f"Building cell centroids for {source_name} …")
    query_gdf = _centroids_gdf(locations_df, lat_col, lon_col, backend, native_level)
    print(f"  {len(query_gdf):,} unique cells to process")

    ref = geodataframe.to_crs(_GEOGRAPHIC_CRS)

    geom_type = ref.geometry.geom_type.iloc[0] if len(ref) > 0 else "Unknown"
    is_polygon = geom_type in ("Polygon", "MultiPolygon")
    print(f"  Reference GDF: {len(ref):,} features ({geom_type}), computing distances …")

    if is_polygon:
        distances_km = _nearest_distance_polygon(query_gdf, ref)
    else:
        distances_km = _nearest_distance_point(query_gdf, ref)

    result_df = query_gdf[["cell_id"]].copy()
    result_df["nearest_distance"] = distances_km
    result_df["timestamp"] = _STATIC_TIMESTAMP
    result_df["source"] = source_name

    return _write_obs(
        engine, source_name, var_defs, result_df,
        ["nearest_distance"], backend_name, {int(_STATIC_TIMESTAMP[:4])},
    )


def _nearest_distance_polygon(
    query_gdf: gpd.GeoDataFrame, ref: gpd.GeoDataFrame
) -> np.ndarray:
    """
    Geodesic distance in km to the nearest polygon boundary; 0 when inside.

    Candidate polygons are found with an STRtree in degrees, which is cheap but
    slightly distorted, so the N_CANDIDATES closest candidates are each measured
    geodesically and the smallest wins. That keeps the reported distance exact
    even where degree-space ranking would have picked a different polygon.
    """
    n_candidates = 5
    joined = gpd.sjoin(query_gdf, ref[["geometry"]], how="left", predicate="within")
    inside_idx = set(joined[joined["index_right"].notna()].index)

    tree_geoms = ref.geometry.values
    strtree = STRtree(tree_geoms)
    distances_km = np.zeros(len(query_gdf))

    for i, (idx, row) in enumerate(query_gdf.iterrows()):
        if idx in inside_idx:
            continue
        point = row.geometry
        candidates = strtree.query_nearest(point, all_matches=True)
        candidates = np.atleast_1d(candidates)[:n_candidates]
        best = np.inf
        for cand in candidates:
            near_geom = tree_geoms[cand]
            # nearest_points gives the closest position on the polygon in
            # geographic coordinates; the geodesic to it is the true distance.
            on_poly = nearest_points(point, near_geom)[1]
            d = _geodesic_km(
                np.array([point.y]), np.array([point.x]),
                np.array([on_poly.y]), np.array([on_poly.x]),
            )[0]
            best = min(best, d)
        distances_km[i] = best

    return distances_km


def _nearest_distance_point(
    query_gdf: gpd.GeoDataFrame, ref: gpd.GeoDataFrame
) -> np.ndarray:
    """
    Geodesic distance in km to the nearest reference point.

    Selection runs on an ECEF KDTree, which is exact for nearest-neighbour at
    any extent; the returned distance is then the WGS84 geodesic to that point.
    """
    ref_lats = ref.geometry.y.to_numpy()
    ref_lons = ref.geometry.x.to_numpy()
    tree = KDTree(_ecef(ref_lats, ref_lons))

    q_lats = query_gdf.geometry.y.to_numpy()
    q_lons = query_gdf.geometry.x.to_numpy()
    _, idx = tree.query(_ecef(q_lats, q_lons))

    return _geodesic_km(q_lats, q_lons, ref_lats[idx], ref_lons[idx])


# ---------------------------------------------------------------------------
# inside
# ---------------------------------------------------------------------------

def compute_inside(
    engine: Engine,
    backend: GridBackend,
    locations_df: pd.DataFrame,
    source_name: str,
    geodataframe: gpd.GeoDataFrame,
    lat_col: str,
    lon_col: str,
    native_level: int,
    backend_name: str = "hiergp",
) -> int:
    """
    Write 1.0 (centroid inside any polygon) or 0.0 (outside) to obs_{source_name}.
    Returns the number of rows inserted.
    """
    var_defs = _load_source_var_defs(engine, source_name)
    var_names = [v["name"] for v in var_defs]
    if len(var_names) != 1 or var_names[0] != "inside":
        raise ValueError(
            f"Source {source_name!r} must have a single variable named 'inside'. "
            f"Got: {var_names}"
        )

    print(f"Building cell centroids for {source_name} …")
    query_gdf = _centroids_gdf(locations_df, lat_col, lon_col, backend, native_level)
    print(f"  {len(query_gdf):,} unique cells to process")

    ref = geodataframe.to_crs(_GEOGRAPHIC_CRS)
    joined = gpd.sjoin(query_gdf, ref[["geometry"]], how="left", predicate="within")
    inside_idx = set(joined[joined["index_right"].notna()].index)

    result_df = query_gdf[["cell_id"]].copy()
    result_df["inside"] = [1.0 if idx in inside_idx else 0.0 for idx in result_df.index]
    result_df["timestamp"] = _STATIC_TIMESTAMP
    result_df["source"] = source_name

    return _write_obs(
        engine, source_name, var_defs, result_df,
        ["inside"], backend_name, {int(_STATIC_TIMESTAMP[:4])},
    )


# ---------------------------------------------------------------------------
# attribute_lookup
# ---------------------------------------------------------------------------

def compute_attribute_lookup(
    engine: Engine,
    backend: GridBackend,
    locations_df: pd.DataFrame,
    source_name: str,
    dataframe: pd.DataFrame,
    lat_col: str,
    lon_col: str,
    native_level: int,
    backend_name: str,
    value_columns: dict[str, list[str]],
    temporal_range: tuple[int, int],
    ref_lat_col: str = "latitude",
    ref_lon_col: str = "longitude",
    timestamp_col: str = "timestamp",
) -> int:
    """
    Nearest-neighbour lookup against a point DataFrame with per-year value columns.

    value_columns: {variable_name: [year_col, ...]} where each year column name
        must be parseable as an integer year (e.g. "2000", "2001", ...).
    temporal_range: (min_year, max_year) — years outside this range are clipped.
    ref_lat_col / ref_lon_col: lat/lon column names in the reference dataframe.
    timestamp_col: timestamp column name in locations_df.

    Returns the number of rows inserted.
    """
    var_defs, granularity = _load_source_meta(engine, source_name)
    registered_vars = {v["name"] for v in var_defs}
    for var_name in value_columns:
        if var_name not in registered_vars:
            raise ValueError(
                f"Variable {var_name!r} not registered for source {source_name!r}. "
                f"Registered: {sorted(registered_vars)}"
            )

    min_year, max_year = temporal_range
    var_cols = list(value_columns.keys())

    # Nearest neighbour has to be measured in metres: a degree of longitude is
    # ~30% shorter than a degree of latitude at 47N, so a KDTree on raw degrees
    # picks the wrong reference point for east-west neighbours.
    print(f"Building KDTree on reference DataFrame ({len(dataframe):,} points) …")
    ref_lats = dataframe[ref_lat_col].to_numpy(dtype=float)
    ref_lons = dataframe[ref_lon_col].to_numpy(dtype=float)
    tree = KDTree(_ecef(ref_lats, ref_lons))

    # Collect unique (cell_id, original_year) combinations from the locations.
    # Store at the original year so fetch() with any timestamp in that year matches.
    # The clamped year is only used to pick which column to read from the reference data.
    cell_year_records: dict[tuple[str, int], tuple[str, int]] = {}
    # (cell_id, orig_year) → (timestamp_str, clamped_year)
    for _, row in locations_df.iterrows():
        lat, lon = float(row[lat_col]), float(row[lon_col])
        cell_id = backend.point_to_cell(lat, lon, native_level)
        raw_ts = str(row[timestamp_col])
        orig_year = int(raw_ts[:4])
        clamped = max(min_year, min(max_year, orig_year))
        # A source registered as static is read back at _STATIC_TIMESTAMP by
        # query._snap_timestamp, so storing {year}-01-01 would make it unfetchable.
        ts_str = _STATIC_TIMESTAMP if granularity == "static" else f"{orig_year}-01-01"
        cell_year_records[(cell_id, orig_year)] = (ts_str, clamped)

    print(f"  {len(cell_year_records):,} unique (cell × year) combinations")

    # One KDTree query per unique cell (centroid)
    unique_cells = list({cid for cid, _ in cell_year_records})
    cell_centroid: dict[str, tuple[float, float]] = {}
    for cell_id in unique_cells:
        clat, clon = backend.cell_to_centroid(cell_id)
        cell_centroid[cell_id] = (clat, clon)

    centroid_lats = np.array([cell_centroid[c][0] for c in unique_cells])
    centroid_lons = np.array([cell_centroid[c][1] for c in unique_cells])
    _, ref_indices = tree.query(_ecef(centroid_lats, centroid_lons))
    cell_ref_idx: dict[str, int] = dict(zip(unique_cells, ref_indices.tolist()))

    # Normalise value_columns: accept list (year==col name) or dict {year: col_name}
    # Output: {var_name: {year_int: col_name_in_df}}
    normalised: dict[str, dict[int, str]] = {}
    for var_name, spec in value_columns.items():
        if isinstance(spec, dict):
            normalised[var_name] = {int(yr): col for yr, col in spec.items()}
        else:
            normalised[var_name] = {int(col): col for col in spec}

    # Build result rows: one per (cell_id, orig_year)
    rows = []
    years: set[int] = set()
    for (cell_id, orig_year), (ts_str, clamped_year) in cell_year_records.items():
        ref_idx = cell_ref_idx[cell_id]
        entry = {"cell_id": cell_id, "timestamp": ts_str, "source": source_name}
        for var_name, year_col_map in normalised.items():
            if clamped_year in year_col_map:
                col = year_col_map[clamped_year]
            else:
                available = sorted(year_col_map)
                closest = min(available, key=lambda avail_y, yr=clamped_year: abs(avail_y - yr))
                col = year_col_map[closest]
            entry[var_name] = dataframe.iloc[ref_idx][col]
        rows.append(entry)
        years.add(orig_year)

    result_df = pd.DataFrame(rows)

    return _write_obs(
        engine, source_name, var_defs, result_df,
        var_cols, backend_name, years, upsert=True,
    )
