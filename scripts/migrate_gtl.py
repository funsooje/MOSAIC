"""
Migrate environmental data from the legacy GTL PostgreSQL database into GeoMetrics.

Three-stage bulk migration — no row-by-row processing, no dedup overhead:

  Stage 1 — Cells:
    COPY grids (subset referenced by l3yo for chosen years) → cells + hiergp_cells

  Stage 2 — Spatiotemporal units:
    COPY l3yo (filtered by year range) → spatiotemporal_units
    The join to cells uses the cell_id string built from grids.x/y and the
    standard level for that source group.

  Stage 3 — Observations (per source):
    For each source, JOIN obs table → l3yo → spatiotemporal_units ON (cell_pk, timestamp)
    and COPY directly into obs_{source}. No staging table, no dedup.

GTL DB     : connection string from GTL_DSN (.env or environment)
GeoMetrics : read from ~/.geometrics/config.json

GTL uses HierGP internal levels (1=finest); GeoMetrics reverses this (15=finest):
  standard_level = 16 - gtl_internal_level

Usage:
    python scripts/migrate_gtl.py stage1 [--year-start 2019] [--year-end 2022]
    python scripts/migrate_gtl.py stage2 [--year-start 2019] [--year-end 2022]
    python scripts/migrate_gtl.py stage3 --source MODIS_NDVI [--year-start 2019] [--year-end 2022]
    python scripts/migrate_gtl.py all    [--year-start 2019] [--year-end 2022]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from sqlalchemy import create_engine, inspect as sa_inspect
from tqdm import tqdm

from geometrics.config import load_config
from geometrics.store.schema import (
    ensure_source_obs_table,
    ensure_spatiotemporal_year_partitions,
    source_table_name,
)

def _gtl_dsn() -> str:
    """
    GTL connection string, from the environment or the repo .env.

    Kept out of source so the file can live in a public repository: set GTL_DSN
    in .env (see .env.example) or export it before running.
    """
    if os.environ.get("GTL_DSN"):
        return os.environ["GTL_DSN"]
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            key, _, value = line.partition("=")
            if key.strip() == "GTL_DSN":
                return value.strip().strip('"\'')
    print("[ERROR] GTL_DSN is not set. Put it in .env or export it:\n"
          "  GTL_DSN=\"host=localhost port=5436 dbname=gtl user=... password=...\"")
    sys.exit(1)


TIMING_LOG = Path(__file__).parent / "migration_timings.jsonl"


@contextmanager
def _timed(stage: str, **context):
    """Record wall-clock for a migration stage as one JSON line in TIMING_LOG."""
    started = time.time()
    record = {"stage": stage, "started_at": datetime.now(timezone.utc).isoformat(), **context}
    try:
        yield record
    except Exception as exc:
        record["error"] = repr(exc)
        raise
    finally:
        record["seconds"] = round(time.time() - started, 3)
        with TIMING_LOG.open("a") as handle:
            handle.write(json.dumps(record) + "\n")
        print(f"  [timing] {stage}: {record['seconds']:,.1f}s")


def _tg(msg: str) -> None:
    """Fire-and-forget Telegram notification — never raises."""
    try:
        subprocess.run(["tgsend", "--source", "GeoMetrics", msg], timeout=10, check=False)
    except Exception:  # pylint: disable=broad-except
        pass

DEFAULT_YEAR_START = 2019
DEFAULT_YEAR_END = 2022

# Source specs — variables: list of (gtl_col_name, "SourceName:gm_var_name") pairs
# gtl_level: GTL internal level (standard_level = 16 - gtl_level)
SOURCES: dict[str, dict] = {
    "MODIS_NDVI": {
        "obs_table": "ndvi",
        "gtl_level": 3,    # standard level 13
        "variables": [("ndvi", "MODIS_NDVI:NDVI")],
    },
    "Landsat_NDVI": {
        "obs_table": "ndvi_ls",
        "gtl_level": 3,
        "variables": [("ndvi", "Landsat_NDVI:NDVI")],
    },
    "MODIS_Treecover": {
        "obs_table": "mod44b",
        "gtl_level": 3,
        "variables": [
            ("percent_tree_cover",         "MODIS_Treecover:percent_tree_cover"),
            ("percent_nontree_vegetation",  "MODIS_Treecover:percent_nontree_vegetation"),
            ("percent_nonvegetated",        "MODIS_Treecover:percent_nonvegetated"),
            ("quality",                     "MODIS_Treecover:quality"),
            ("percent_tree_cover_sd",       "MODIS_Treecover:percent_tree_cover_sd"),
            ("percent_nonvegetated_sd",     "MODIS_Treecover:percent_nonvegetated_sd"),
            ("cloud",                       "MODIS_Treecover:cloud"),
        ],
    },
    "NLCD": {
        "obs_table": "nlcd",
        "gtl_level": 3,
        "variables": [
            ("landcover",             "NLCD:landcover"),
            ("impervious",            "NLCD:impervious"),
            ("impervious_descriptor", "NLCD:impervious_descriptor"),
        ],
    },
    "YALE_UHI": {
        "obs_table": "uhi",
        "gtl_level": 3,
        "variables": [
            ("yearlydaytime",   "YALE_UHI:yearly_daytime"),
            ("yearlynighttime", "YALE_UHI:yearly_nighttime"),
            ("winterdaytime",   "YALE_UHI:winter_daytime"),
            ("winternighttime", "YALE_UHI:winter_nighttime"),
            ("summerdaytime",   "YALE_UHI:summer_daytime"),
            ("summernighttime", "YALE_UHI:summer_nighttime"),
        ],
    },
    "JRC_Water": {
        "obs_table": "water",
        "gtl_level": 3,
        "variables": [("distance", "JRC_Water:water_distance")],
    },
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _TqdmFile:
    """File-like wrapper that advances a tqdm bar as bytes are read."""

    def __init__(self, file_handle, pbar):
        self._fh = file_handle
        self._pbar = pbar

    def read(self, size=-1):
        """Read and advance progress bar."""
        data = self._fh.read(size)
        self._pbar.update(len(data))
        return data

    def readline(self):
        """Read a line and advance progress bar."""
        line = self._fh.readline()
        self._pbar.update(len(line))
        return line

    def write(self, data):
        """Write and advance progress bar."""
        self._fh.write(data)
        self._pbar.update(len(data))


def _gm_dsn(db_url: str) -> str:
    """Convert a SQLAlchemy postgresql:// URL to a psycopg2 DSN string."""
    match = re.match(r"postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)", db_url)
    if not match:
        print(f"[ERROR] Cannot parse db_url: {db_url}")
        sys.exit(1)
    user, password, host, port, dbname = match.groups()
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def _copy_out(cur, sql: str, dest: Path, desc: str) -> None:
    """COPY a query to a local CSV file with a tqdm progress bar."""
    copy_sql = f"COPY ({sql}) TO STDOUT WITH (FORMAT CSV, HEADER)"
    with dest.open("wb") as file_handle:
        with tqdm(unit="B", unit_scale=True, unit_divisor=1024,
                  desc=f"  {desc}", dynamic_ncols=True) as pbar:
            cur.copy_expert(copy_sql, _TqdmFile(file_handle, pbar))


def _copy_in(cur, csv_path: Path, table: str, columns: list[str], desc: str) -> None:
    """COPY a local CSV file into a table with a tqdm progress bar."""
    col_list = ", ".join(columns)
    copy_sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT CSV, HEADER)"
    file_size = csv_path.stat().st_size
    with csv_path.open("rb") as file_handle:
        with tqdm(total=file_size, unit="B", unit_scale=True, unit_divisor=1024,
                  desc=f"  {desc}", dynamic_ncols=True) as pbar:
            cur.copy_expert(copy_sql, _TqdmFile(file_handle, pbar))


# ---------------------------------------------------------------------------
# Stage 1 — Cells
# ---------------------------------------------------------------------------

def stage1_cells(gtl_cur, gm_cur, year_start: int, year_end: int) -> None:
    """
    Copy the grids referenced by l3yo in [year_start, year_end] into
    cells and hiergp_cells. All sources in this year range use GTL level 3
    (standard level 13).
    """
    _tg(f"GeoMetrics Migration: Stage 1 starting — cells ({year_start}–{year_end})")
    print(f"\n[Stage 1] Loading cells for {year_start}–{year_end} …")

    gtl_cur.execute("""
        SELECT COUNT(DISTINCT g.id)
        FROM grids g
        JOIN l3yo l ON l.gridid = g.id
        WHERE l.temporal BETWEEN %s AND %s
    """, (year_start, year_end))
    total = gtl_cur.fetchone()[0]
    print(f"  Distinct grids to load: {total:,}")

    tmp = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_grids_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT DISTINCT
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                'hiergp'                                AS backend,
                13                                      AS level,
                g.x,
                g.y
            FROM grids g
            JOIN l3yo l ON l.gridid = g.id
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
        """, tmp, "dump grids")

        # Stage into a temp table so we can JOIN to cells for hiergp_cells
        gm_cur.execute("""
            CREATE TEMP TABLE _stg_grids (
                cell_id TEXT, backend TEXT, level INTEGER, x INTEGER, y INTEGER
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp, "_stg_grids",
                 ["cell_id", "backend", "level", "x", "y"], "load staging")

        print("  Inserting into cells …")
        gm_cur.execute("""
            INSERT INTO cells (cell_id, backend, level)
            SELECT cell_id, backend, level FROM _stg_grids
            ON CONFLICT (cell_id) DO NOTHING
        """)
        print(f"  Inserted {gm_cur.rowcount:,} cell(s).")

        print("  Inserting into hiergp_cells …")
        gm_cur.execute("""
            INSERT INTO hiergp_cells (cell_pk, x, y)
            SELECT c.id, s.x, s.y
            FROM _stg_grids s
            JOIN cells c ON c.cell_id = s.cell_id
            ON CONFLICT (cell_pk) DO NOTHING
        """)
        print(f"  Inserted {gm_cur.rowcount:,} hiergp_cell(s).")

    finally:
        tmp.unlink(missing_ok=True)

    print("  [Stage 1] Done.")
    _tg(f"GeoMetrics Migration: Stage 1 done — {total:,} cells loaded.")


# ---------------------------------------------------------------------------
# Stage 2 — Spatiotemporal units
# ---------------------------------------------------------------------------

def stage2_spatiotemporal(
    gtl_cur, gm_cur, gm_engine, year_start: int, year_end: int
) -> None:
    """
    Copy l3yo rows for [year_start, year_end] into spatiotemporal_units,
    joining grids to resolve the cell_id → cell_pk.
    """
    _tg(f"GeoMetrics Migration: Stage 2 starting — spatiotemporal units ({year_start}–{year_end})")
    print(f"\n[Stage 2] Loading spatiotemporal units for {year_start}–{year_end} …")

    gtl_cur.execute(
        "SELECT COUNT(*) FROM l3yo WHERE temporal BETWEEN %s AND %s",
        (year_start, year_end),
    )
    total = gtl_cur.fetchone()[0]
    print(f"  l3yo rows to load: {total:,}")

    years = set(range(year_start, year_end + 1))
    ensure_spatiotemporal_year_partitions(gm_engine, years)

    tmp = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_l3yo_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                l.temporal::text || '-01-01'            AS timestamp
            FROM l3yo l
            JOIN grids g ON g.id = l.gridid
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
        """, tmp, "dump l3yo")

        print("  Resolving cell_pk and inserting into spatiotemporal_units …")
        gm_cur.execute("""
            CREATE TEMP TABLE _stg_l3yo (
                cell_id   TEXT,
                timestamp TIMESTAMP
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp, "_stg_l3yo", ["cell_id", "timestamp"], "load staging")

        gm_cur.execute("""
            INSERT INTO spatiotemporal_units (cell_pk, timestamp)
            SELECT c.id, s.timestamp
            FROM _stg_l3yo s
            JOIN cells c ON c.cell_id = s.cell_id
            ON CONFLICT (cell_pk, timestamp) DO NOTHING
        """)
        inserted = gm_cur.rowcount
        print(f"  Inserted {inserted:,} spatiotemporal unit(s).")

    finally:
        tmp.unlink(missing_ok=True)

    print("  [Stage 2] Done.")
    _tg(f"GeoMetrics Migration: Stage 2 done — {inserted:,} spatiotemporal units loaded.")


# ---------------------------------------------------------------------------
# Stage 3 — Observations
# ---------------------------------------------------------------------------

def stage3_observations(  # pylint: disable=too-many-locals,too-many-arguments
    gtl_cur, gm_cur, gm_engine, source_name: str, year_start: int, year_end: int
) -> None:
    """
    Copy observations for one source from GTL into obs_{source}.
    Joins through spatiotemporal_units to resolve unit_pk — no ID mapping needed.
    """
    spec = SOURCES.get(source_name)
    if spec is None:
        print(f"[ERROR] Unknown source {source_name!r}. Available: {list(SOURCES)}")
        sys.exit(1)

    obs_table = spec["obs_table"]
    variable_defs = [
        {"name": qname.split(":", 1)[1]}
        for _, qname in spec["variables"]
    ]
    variable_names = [vd["name"] for vd in variable_defs]
    gtl_cols = [gtl_col for gtl_col, _ in spec["variables"]]

    ensure_source_obs_table(gm_engine, source_name, variable_defs)

    _tg(f"GeoMetrics Migration: Stage 3 starting — {source_name} ({year_start}–{year_end})")
    print(f"\n[Stage 3] Loading observations for {source_name} ({year_start}–{year_end}) …")

    gtl_cur.execute(f"""
        SELECT COUNT(*) FROM {obs_table} n
        JOIN l3yo l ON n.l3y_oid = l.id
        WHERE l.temporal BETWEEN %s AND %s
    """, (year_start, year_end))
    total = gtl_cur.fetchone()[0]
    print(f"  Rows to load: {total:,}")

    # Build SELECT for GTL obs — rename columns to GeoMetrics names
    rename_map = {
        gtl_col: qname.split(":", 1)[1]
        for gtl_col, qname in spec["variables"]
    }
    gtl_select_cols = ", ".join(
        f"n.{gtl_col} AS {rename_map[gtl_col]}" for gtl_col in gtl_cols
    )

    tmp = Path(tempfile.mktemp(suffix=".csv", prefix=f"gtl_{obs_table}_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                l.temporal::text || '-01-01'            AS timestamp,
                {gtl_select_cols}
            FROM {obs_table} n
            JOIN l3yo l ON n.l3y_oid = l.id
            JOIN grids g ON g.id = l.gridid
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
        """, tmp, f"dump {obs_table}")

        var_col_defs = ",\n    ".join(f"{col} DOUBLE PRECISION" for col in variable_names)
        gm_cur.execute(f"""
            CREATE TEMP TABLE _stg_obs (
                cell_id   TEXT,
                timestamp TIMESTAMP,
                {var_col_defs}
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp, "_stg_obs",
                 ["cell_id", "timestamp"] + variable_names, f"load {obs_table}")

        gm_obs_table = source_table_name(source_name)
        var_col_list = ", ".join(variable_names)
        src_cols = ", ".join(f"s.{col}" for col in variable_names)
        gm_cur.execute(f"""
            INSERT INTO {gm_obs_table} (unit_pk, {var_col_list})
            SELECT u.id, {src_cols}
            FROM _stg_obs s
            JOIN cells c ON c.cell_id = s.cell_id
            JOIN spatiotemporal_units u
              ON u.cell_pk = c.id AND u.timestamp = s.timestamp
            ON CONFLICT (unit_pk) DO NOTHING
        """)
        inserted = gm_cur.rowcount
        print(f"  Inserted {inserted:,} row(s) into {gm_obs_table}.")

    finally:
        tmp.unlink(missing_ok=True)

    print(f"  [Stage 3 — {source_name}] Done.")
    _tg(f"GeoMetrics Migration: Stage 3 done — {source_name}: {inserted:,} rows loaded.")


# ---------------------------------------------------------------------------
# Local sources — Parks_Distance, Walkability, CACES_Air
# ---------------------------------------------------------------------------

def _register_local_sources(config) -> None:
    """Register the three local sources in GeoMetrics sources/variables tables."""
    from geometrics.api import GeoMetrics
    gm = GeoMetrics(config)
    gm.register_local_source("Parks_Distance", operation="nearest_distance")
    gm.register_local_source(
        "Walkability",
        operation="attribute_lookup",
        variable_defs=[{"name": "natwalkind", "unit": "score"}],
        temporal_granularity="static",
    )
    gm.register_local_source(
        "CACES_Air",
        operation="attribute_lookup",
        variable_defs=[{"name": "pm25", "unit": "µg/m³"}, {"name": "no2", "unit": "ppb"}],
        temporal_granularity="annual",
    )


def stage_local_cells(gtl_cur, gm_cur, year_start: int, year_end: int) -> None:
    """Ensure all cells referenced by l3y in [year_start, year_end] are in cells table."""
    _tg(f"GeoMetrics Migration: Local Stage 1 starting — cells ({year_start}–{year_end})")
    print(f"\n[Local Stage 1] Ensuring l3y cells in cells table ({year_start}–{year_end}) …")

    tmp = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_l3y_cells_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT DISTINCT
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                'hiergp'                                AS backend,
                13                                      AS level,
                g.x,
                g.y
            FROM grids g
            JOIN l3y l ON l.gridid = g.id
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
        """, tmp, "dump l3y grids")

        gm_cur.execute("""
            CREATE TEMP TABLE _stg_l3y_cells (
                cell_id TEXT, backend TEXT, level INTEGER, x INTEGER, y INTEGER
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp, "_stg_l3y_cells",
                 ["cell_id", "backend", "level", "x", "y"], "load staging")

        gm_cur.execute("""
            INSERT INTO cells (cell_id, backend, level)
            SELECT cell_id, backend, level FROM _stg_l3y_cells
            ON CONFLICT (cell_id) DO NOTHING
        """)
        print(f"  Inserted {gm_cur.rowcount:,} new cell(s).")

        gm_cur.execute("""
            INSERT INTO hiergp_cells (cell_pk, x, y)
            SELECT c.id, s.x, s.y
            FROM _stg_l3y_cells s
            JOIN cells c ON c.cell_id = s.cell_id
            ON CONFLICT (cell_pk) DO NOTHING
        """)
        print(f"  Inserted {gm_cur.rowcount:,} new hiergp_cell(s).")

    finally:
        tmp.unlink(missing_ok=True)

    print("  [Local Stage 1] Done.")
    _tg("GeoMetrics Migration: Local Stage 1 done.")


def stage_local_units(gtl_cur, gm_cur, gm_engine, year_start: int, year_end: int) -> None:
    """
    Create spatiotemporal units for local sources:
      - Static (parks, walkability): one '1900-01-01' row per unique cell
      - Air: one annual row per (cell, year) in [year_start, year_end]
    """
    _tg("GeoMetrics Migration: Local Stage 2 starting — spatiotemporal units")
    print("\n[Local Stage 2] Creating spatiotemporal units …")

    # Static units — one row per unique cell at 1900-01-01
    ensure_spatiotemporal_year_partitions(gm_engine, {1900})
    tmp_static = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_l3y_static_units_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT DISTINCT
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                '1900-01-01'::text                      AS timestamp
            FROM grids g
            JOIN l3y l ON l.gridid = g.id
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
        """, tmp_static, "dump static units")

        gm_cur.execute("""
            CREATE TEMP TABLE _stg_static_units (
                cell_id TEXT, timestamp TIMESTAMP
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp_static, "_stg_static_units",
                 ["cell_id", "timestamp"], "load static units")

        gm_cur.execute("""
            INSERT INTO spatiotemporal_units (cell_pk, timestamp)
            SELECT c.id, s.timestamp
            FROM _stg_static_units s
            JOIN cells c ON c.cell_id = s.cell_id
            ON CONFLICT (cell_pk, timestamp) DO NOTHING
        """)
        print(f"  Static units (1900-01-01): inserted {gm_cur.rowcount:,} row(s).")
    finally:
        tmp_static.unlink(missing_ok=True)

    # Annual units for air — one row per (cell, year)
    ensure_spatiotemporal_year_partitions(gm_engine, set(range(year_start, year_end + 1)))
    tmp_air = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_l3y_air_units_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                l.temporal::text || '-01-01'            AS timestamp
            FROM l3y l
            JOIN grids g ON g.id = l.gridid
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
        """, tmp_air, "dump air units")

        gm_cur.execute("""
            CREATE TEMP TABLE _stg_air_units (
                cell_id TEXT, timestamp TIMESTAMP
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp_air, "_stg_air_units",
                 ["cell_id", "timestamp"], "load air units")

        gm_cur.execute("""
            INSERT INTO spatiotemporal_units (cell_pk, timestamp)
            SELECT c.id, s.timestamp
            FROM _stg_air_units s
            JOIN cells c ON c.cell_id = s.cell_id
            ON CONFLICT (cell_pk, timestamp) DO NOTHING
        """)
        print(f"  Air annual units: inserted {gm_cur.rowcount:,} row(s).")
    finally:
        tmp_air.unlink(missing_ok=True)

    print("  [Local Stage 2] Done.")
    _tg("GeoMetrics Migration: Local Stage 2 done.")


def stage_local_parks(gtl_cur, gm_cur, gm_engine, year_start: int, year_end: int) -> None:
    """Migrate Parks_Distance — nearest_distance in km, static."""
    source_name = "Parks_Distance"
    ensure_source_obs_table(gm_engine, source_name, [{"name": "nearest_distance", "unit": "km"}])
    _tg(f"GeoMetrics Migration: Local Stage 3a starting — {source_name}")
    print(f"\n[Local Stage 3a] Migrating {source_name} …")

    tmp = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_parks_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT DISTINCT ON (g.id)
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                '1900-01-01'::text                      AS timestamp,
                l.park_spatial_drift / 1000.0           AS nearest_distance
            FROM grids g
            JOIN l3y l ON l.gridid = g.id
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
            ORDER BY g.id
        """, tmp, "dump parks")

        obs_table = source_table_name(source_name)
        gm_cur.execute("""
            CREATE TEMP TABLE _stg_parks (
                cell_id TEXT, timestamp TIMESTAMP, nearest_distance DOUBLE PRECISION
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp, "_stg_parks",
                 ["cell_id", "timestamp", "nearest_distance"], "load parks")

        gm_cur.execute(f"""
            INSERT INTO {obs_table} (unit_pk, nearest_distance)
            SELECT u.id, s.nearest_distance
            FROM _stg_parks s
            JOIN cells c ON c.cell_id = s.cell_id
            JOIN spatiotemporal_units u ON u.cell_pk = c.id AND u.timestamp = s.timestamp
            ON CONFLICT (unit_pk) DO NOTHING
        """)
        inserted = gm_cur.rowcount
        print(f"  Inserted {inserted:,} row(s) into {obs_table}.")
    finally:
        tmp.unlink(missing_ok=True)

    print("  [Local Stage 3a] Done.")
    _tg(f"GeoMetrics Migration: Local Stage 3a done — {source_name}: {inserted:,} rows.")


def stage_local_walkability(gtl_cur, gm_cur, gm_engine, year_start: int, year_end: int) -> None:
    """Migrate Walkability — natwalkind score (1–20), static."""
    source_name = "Walkability"
    ensure_source_obs_table(gm_engine, source_name, [{"name": "natwalkind", "unit": "score"}])
    _tg(f"GeoMetrics Migration: Local Stage 3b starting — {source_name}")
    print(f"\n[Local Stage 3b] Migrating {source_name} …")

    tmp = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_walk_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT DISTINCT ON (g.id)
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                '1900-01-01'::text                      AS timestamp,
                w.natwalkind
            FROM grids g
            JOIN l3y l ON l.gridid = g.id
            JOIN walk w ON w.id = l.walk_id
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
            ORDER BY g.id
        """, tmp, "dump walkability")

        obs_table = source_table_name(source_name)
        gm_cur.execute("""
            CREATE TEMP TABLE _stg_walk (
                cell_id TEXT, timestamp TIMESTAMP, natwalkind DOUBLE PRECISION
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp, "_stg_walk",
                 ["cell_id", "timestamp", "natwalkind"], "load walkability")

        gm_cur.execute(f"""
            INSERT INTO {obs_table} (unit_pk, natwalkind)
            SELECT u.id, s.natwalkind
            FROM _stg_walk s
            JOIN cells c ON c.cell_id = s.cell_id
            JOIN spatiotemporal_units u ON u.cell_pk = c.id AND u.timestamp = s.timestamp
            ON CONFLICT (unit_pk) DO NOTHING
        """)
        inserted = gm_cur.rowcount
        print(f"  Inserted {inserted:,} row(s) into {obs_table}.")
    finally:
        tmp.unlink(missing_ok=True)

    print("  [Local Stage 3b] Done.")
    _tg(f"GeoMetrics Migration: Local Stage 3b done — {source_name}: {inserted:,} rows.")


def stage_local_air(gtl_cur, gm_cur, gm_engine, year_start: int, year_end: int) -> None:
    """Migrate CACES_Air — pm25 and no2, annual."""
    source_name = "CACES_Air"
    var_defs = [{"name": "pm25", "unit": "µg/m³"}, {"name": "no2", "unit": "ppb"}]
    ensure_source_obs_table(gm_engine, source_name, var_defs)
    _tg(f"GeoMetrics Migration: Local Stage 3c starting — {source_name} ({year_start}–{year_end})")
    print(f"\n[Local Stage 3c] Migrating {source_name} ({year_start}–{year_end}) …")

    tmp = Path(tempfile.mktemp(suffix=".csv", prefix="gtl_air_"))
    try:
        _copy_out(gtl_cur, f"""
            SELECT
                '13:' || g.x::text || '|' || g.y::text AS cell_id,
                l.temporal::text || '-01-01'            AS timestamp,
                a.pm25,
                a.no2
            FROM air a
            JOIN l3y l ON l.id = a.l3yid
            JOIN grids g ON g.id = l.gridid
            WHERE l.temporal BETWEEN {year_start} AND {year_end}
        """, tmp, "dump air")

        obs_table = source_table_name(source_name)
        gm_cur.execute("""
            CREATE TEMP TABLE _stg_air (
                cell_id TEXT, timestamp TIMESTAMP,
                pm25 DOUBLE PRECISION, no2 DOUBLE PRECISION
            ) ON COMMIT DROP
        """)
        _copy_in(gm_cur, tmp, "_stg_air",
                 ["cell_id", "timestamp", "pm25", "no2"], "load air")

        gm_cur.execute(f"""
            INSERT INTO {obs_table} (unit_pk, pm25, no2)
            SELECT u.id, s.pm25, s.no2
            FROM _stg_air s
            JOIN cells c ON c.cell_id = s.cell_id
            JOIN spatiotemporal_units u ON u.cell_pk = c.id AND u.timestamp = s.timestamp
            ON CONFLICT (unit_pk) DO NOTHING
        """)
        inserted = gm_cur.rowcount
        print(f"  Inserted {inserted:,} row(s) into {obs_table}.")
    finally:
        tmp.unlink(missing_ok=True)

    print("  [Local Stage 3c] Done.")
    _tg(f"GeoMetrics Migration: Local Stage 3c done — {source_name}: {inserted:,} rows.")


# ---------------------------------------------------------------------------
# ERA5-Land — hourly, GTL internal level 8 (standard level 8, 3.2 km)
# ---------------------------------------------------------------------------

ERA5_SOURCE = "ERA5_Land"
ERA5_LEVEL = 8            # standard level; grids.level is 8 for every l8h cell
ERA5_PIXEL_M = 9000       # ERA5-Land native resolution
ERA5_VARS = [
    "temperature_2m", "dewpoint_temperature_2m", "surface_pressure",
    "u_component_of_wind_10m", "v_component_of_wind_10m",
    "surface_thermal_radiation_downwards", "surface_net_solar_radiation",
    "total_precipitation",
]


def _era5_align_source(gm_cur) -> None:
    """
    Point the ERA5_Land source row at its real grid level.

    The catalog registered it at level 13 / 200 m like everything else; the GTL
    data is keyed at level 8 (3.2 km) and the viewer sizes circles from
    pixel_resolution_m, so both need correcting before the data lands.
    """
    gm_cur.execute(
        "UPDATE sources SET native_level = %s, pixel_resolution_m = %s WHERE name = %s",
        (ERA5_LEVEL, ERA5_PIXEL_M, ERA5_SOURCE),
    )
    print(f"  sources.{ERA5_SOURCE}: native_level={ERA5_LEVEL}, pixel_resolution_m={ERA5_PIXEL_M}")


def stage_era5(gtl_cur, gm_cur, gm_engine, year_start: int, year_end: int) -> None:
    """
    Migrate era5land into obs_era5_land, one year at a time.

    Hourly data at level 8: ~26M l8h rows and ~25M observations across 2010-2022,
    so each year is dumped and loaded separately to keep the temp CSVs modest.
    """
    var_defs = [{"name": name} for name in ERA5_VARS]
    ensure_source_obs_table(gm_engine, ERA5_SOURCE, var_defs)
    ensure_spatiotemporal_year_partitions(gm_engine, set(range(year_start, year_end + 1)))
    _era5_align_source(gm_cur)

    _tg(f"GeoMetrics Migration: ERA5 starting ({year_start}–{year_end})")
    print(f"\n[ERA5] Migrating era5land ({year_start}–{year_end}) …")

    obs_table = source_table_name(ERA5_SOURCE)
    var_col_list = ", ".join(ERA5_VARS)
    var_col_defs = ",\n    ".join(f"{col} DOUBLE PRECISION" for col in ERA5_VARS)
    era5_cols = ", ".join(f"e.{col}" for col in ERA5_VARS)
    src_cols = ", ".join(f"s.{col}" for col in ERA5_VARS)
    total_cells = total_units = total_obs = 0

    for year in range(year_start, year_end + 1):
        lo, hi = f"{year}-01-01", f"{year + 1}-01-01"
        print(f"\n  [ERA5 {year}]")

        with _timed("era5_cells", year=year) as rec:
            tmp = Path(tempfile.mktemp(suffix=".csv", prefix=f"gtl_era5_cells_{year}_"))
            try:
                _copy_out(gtl_cur, f"""
                    SELECT DISTINCT
                        '{ERA5_LEVEL}:' || g.x::text || '|' || g.y::text AS cell_id,
                        'hiergp' AS backend,
                        {ERA5_LEVEL} AS level,
                        g.x,
                        g.y
                    FROM grids g
                    JOIN l8h l ON l.gridid = g.id
                    WHERE l.temporal >= '{lo}' AND l.temporal < '{hi}'
                """, tmp, f"dump cells {year}")
                gm_cur.execute("""
                    CREATE TEMP TABLE _stg_era5_cells (
                        cell_id TEXT, backend TEXT, level INTEGER, x INTEGER, y INTEGER
                    ) ON COMMIT DROP
                """)
                _copy_in(gm_cur, tmp, "_stg_era5_cells",
                         ["cell_id", "backend", "level", "x", "y"], "load cells")
                gm_cur.execute("""
                    INSERT INTO cells (cell_id, backend, level)
                    SELECT cell_id, backend, level FROM _stg_era5_cells
                    ON CONFLICT (cell_id) DO NOTHING
                """)
                rec["cells_inserted"] = gm_cur.rowcount
                total_cells += gm_cur.rowcount
                gm_cur.execute("""
                    INSERT INTO hiergp_cells (cell_pk, x, y)
                    SELECT c.id, s.x, s.y
                    FROM _stg_era5_cells s
                    JOIN cells c ON c.cell_id = s.cell_id
                    ON CONFLICT (cell_pk) DO NOTHING
                """)
                print(f"    cells: +{rec['cells_inserted']:,}")
            finally:
                tmp.unlink(missing_ok=True)

        with _timed("era5_units", year=year) as rec:
            tmp = Path(tempfile.mktemp(suffix=".csv", prefix=f"gtl_era5_units_{year}_"))
            try:
                _copy_out(gtl_cur, f"""
                    SELECT
                        '{ERA5_LEVEL}:' || g.x::text || '|' || g.y::text AS cell_id,
                        l.temporal AS timestamp
                    FROM l8h l
                    JOIN grids g ON g.id = l.gridid
                    WHERE l.temporal >= '{lo}' AND l.temporal < '{hi}'
                """, tmp, f"dump units {year}")
                gm_cur.execute("""
                    CREATE TEMP TABLE _stg_era5_units (
                        cell_id TEXT, timestamp TIMESTAMP
                    ) ON COMMIT DROP
                """)
                _copy_in(gm_cur, tmp, "_stg_era5_units", ["cell_id", "timestamp"], "load units")
                gm_cur.execute("""
                    INSERT INTO spatiotemporal_units (cell_pk, timestamp)
                    SELECT c.id, s.timestamp
                    FROM _stg_era5_units s
                    JOIN cells c ON c.cell_id = s.cell_id
                    ON CONFLICT (cell_pk, timestamp) DO NOTHING
                """)
                rec["units_inserted"] = gm_cur.rowcount
                total_units += gm_cur.rowcount
                print(f"    units: +{rec['units_inserted']:,}")
            finally:
                tmp.unlink(missing_ok=True)

        with _timed("era5_obs", year=year) as rec:
            tmp = Path(tempfile.mktemp(suffix=".csv", prefix=f"gtl_era5_obs_{year}_"))
            try:
                _copy_out(gtl_cur, f"""
                    SELECT
                        '{ERA5_LEVEL}:' || g.x::text || '|' || g.y::text AS cell_id,
                        l.temporal AS timestamp,
                        {era5_cols}
                    FROM era5land e
                    JOIN l8h l ON e.l8hid = l.id
                    JOIN grids g ON g.id = l.gridid
                    WHERE l.temporal >= '{lo}' AND l.temporal < '{hi}'
                """, tmp, f"dump obs {year}")
                gm_cur.execute(f"""
                    CREATE TEMP TABLE _stg_era5_obs (
                        cell_id TEXT,
                        timestamp TIMESTAMP,
                        {var_col_defs}
                    ) ON COMMIT DROP
                """)
                _copy_in(gm_cur, tmp, "_stg_era5_obs",
                         ["cell_id", "timestamp"] + ERA5_VARS, "load obs")
                gm_cur.execute(f"""
                    INSERT INTO {obs_table} (unit_pk, {var_col_list})
                    SELECT u.id, {src_cols}
                    FROM _stg_era5_obs s
                    JOIN cells c ON c.cell_id = s.cell_id
                    JOIN spatiotemporal_units u
                      ON u.cell_pk = c.id AND u.timestamp = s.timestamp
                    ON CONFLICT (unit_pk) DO NOTHING
                """)
                rec["obs_inserted"] = gm_cur.rowcount
                total_obs += gm_cur.rowcount
                print(f"    obs:   +{rec['obs_inserted']:,}")
            finally:
                tmp.unlink(missing_ok=True)

        gm_cur.connection.commit()

    print(f"\n  [ERA5] Done — cells +{total_cells:,}, units +{total_units:,}, obs +{total_obs:,}.")
    _tg(f"GeoMetrics Migration: ERA5 done — {total_obs:,} observation rows.")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _check_prerequisites(gm_engine, stage: str, source_name: str | None = None) -> bool:  # pylint: disable=unused-argument
    """Verify required tables exist before running a stage."""
    existing = set(sa_inspect(gm_engine).get_table_names())
    if stage in ("stage2", "stage3", "all"):
        if "cells" not in existing:
            print("[ERROR] cells table missing. Run stage1 first.")
            return False
    if stage in ("stage3", "all"):
        if "spatiotemporal_units" not in existing:
            print("[ERROR] spatiotemporal_units table missing. Run stage2 first.")
            return False
    if stage in ("era5", "local", "all"):
        if "cells" not in existing:
            print("[ERROR] cells table missing. Run stage1 first.")
            return False
        if "spatiotemporal_units" not in existing:
            print("[ERROR] spatiotemporal_units table missing. Run stage2 first.")
            return False
    return True


def run(  # pylint: disable=too-many-arguments
    stage: str,
    year_start: int,
    year_end: int,
    source_name: str | None,
) -> None:
    """Entry point for all stages."""
    config = load_config()
    gm_engine = create_engine(config.db_url)
    gm_dsn = _gm_dsn(config.db_url)

    if not _check_prerequisites(gm_engine, stage, source_name):
        sys.exit(1)

    gtl_conn = psycopg2.connect(_gtl_dsn())
    gm_conn = psycopg2.connect(gm_dsn)
    gm_conn.autocommit = False
    gtl_cur = gtl_conn.cursor()
    gm_cur = gm_conn.cursor()

    try:
        span = {"year_start": year_start, "year_end": year_end}

        if stage in ("stage1", "all"):
            with _timed("stage1_cells", **span):
                stage1_cells(gtl_cur, gm_cur, year_start, year_end)
                gm_conn.commit()

        if stage in ("stage2", "all"):
            with _timed("stage2_units", **span):
                stage2_spatiotemporal(gtl_cur, gm_cur, gm_engine, year_start, year_end)
                gm_conn.commit()

        if stage in ("stage3", "all"):
            targets = [source_name] if source_name else list(SOURCES)
            for src in targets:
                with _timed("stage3_observations", source=src, **span):
                    stage3_observations(gtl_cur, gm_cur, gm_engine, src, year_start, year_end)
                    gm_conn.commit()

        if stage in ("era5", "all"):
            with _timed("stage_era5", **span):
                stage_era5(gtl_cur, gm_cur, gm_engine, year_start, year_end)
                gm_conn.commit()

        if stage in ("local", "all"):
            with _timed("stage_local", **span):
                _register_local_sources(config)
                stage_local_cells(gtl_cur, gm_cur, year_start, year_end)
                gm_conn.commit()
                stage_local_units(gtl_cur, gm_cur, gm_engine, year_start, year_end)
                gm_conn.commit()
                stage_local_parks(gtl_cur, gm_cur, gm_engine, year_start, year_end)
                gm_conn.commit()
                stage_local_walkability(gtl_cur, gm_cur, gm_engine, year_start, year_end)
                gm_conn.commit()
                stage_local_air(gtl_cur, gm_cur, gm_engine, year_start, year_end)
                gm_conn.commit()

        print("\nMigration complete.")

    except Exception as exc:
        gm_conn.rollback()
        _tg(f"GeoMetrics Migration: FAILED — {exc}")
        raise
    finally:
        gtl_cur.close()
        gm_cur.close()
        gtl_conn.close()
        gm_conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate GTL data into GeoMetrics (staged bulk COPY)"
    )
    parser.add_argument(
        "stage",
        choices=["stage1", "stage2", "stage3", "era5", "local", "all"],
        help=("Which stage to run (era5 = ERA5_Land at level 8; "
              "local = Parks_Distance + Walkability + CACES_Air)"),
    )
    parser.add_argument("--source", default=None,
                        help="Source name for stage3 (default: all sources)")
    parser.add_argument("--year-start", type=int, default=DEFAULT_YEAR_START)
    parser.add_argument("--year-end",   type=int, default=DEFAULT_YEAR_END)
    args = parser.parse_args()

    if args.stage == "stage3" and args.source is None:
        print("Note: no --source specified, will migrate all sources.")

    run(args.stage, args.year_start, args.year_end, args.source)


if __name__ == "__main__":
    main()
