"""SQLAlchemy table definitions and per-source observation table helpers."""

from sqlalchemy import (
    Column, ForeignKey, Integer, MetaData, Table, Text, UniqueConstraint,
    inspect as sa_inspect, text,
)
from sqlalchemy.engine import Engine

metadata = MetaData()

# One row per dataset (e.g. Landsat 8 NDVI, MODIS treecover, ERA5-Land)
sources = Table(
    "sources",
    metadata,
    Column("source_id", Integer, primary_key=True),
    Column("name", Text, nullable=False, unique=True),
    Column("native_level", Integer, nullable=False),
    Column("pixel_resolution_m", Integer, nullable=False),
    Column("source_temporal_granularity", Text, nullable=True),
    Column("temporal_granularity", Text, nullable=False),
)

# One row per band/variable within a source (registry only — not a FK in obs tables)
variables = Table(
    "variables",
    metadata,
    Column("variable_id", Integer, primary_key=True),
    Column("source_id", Integer, ForeignKey("sources.source_id"), nullable=False),
    Column("name", Text, nullable=False),
    Column("unit", Text),
)

# One row per unique grid cell (backend-agnostic)
cells = Table(
    "cells",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("cell_id", Text, nullable=False, unique=True),
    Column("backend", Text, nullable=False),   # "hiergp" | "h3"
    Column("level", Integer, nullable=False),  # standard level (higher = finer)
)

# HierGP-specific extension: parsed x/y coordinates for efficient spatial ops
hiergp_cells = Table(
    "hiergp_cells",
    metadata,
    Column("cell_pk", Integer, ForeignKey("cells.id"), primary_key=True),
    Column("x", Integer, nullable=False),
    Column("y", Integer, nullable=False),
)

# One row per (cell × timestamp) pair — normalised spatiotemporal unit.
# Partitioned by year in PostgreSQL. Preserves GTL l3yo.id on migration.
spatiotemporal_units = Table(
    "spatiotemporal_units",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("cell_pk", Integer, ForeignKey("cells.id"), nullable=False),
    Column("timestamp", Text, nullable=False),  # ISO date string
    UniqueConstraint("cell_pk", "timestamp"),
)

# One row per GEE batch export task
jobs = Table(
    "jobs",
    metadata,
    Column("job_id", Integer, primary_key=True),
    Column("task_id", Text),
    Column("source_id", Integer, ForeignKey("sources.source_id")),
    Column("level", Integer),
    Column("date_start", Text),
    Column("date_end", Text),
    Column("gdrive_folder", Text),
    Column("file_prefix", Text),
    Column("expected_path", Text),
    Column("status", Text, nullable=False, default="PENDING"),
    Column("submitted_at", Text),
    Column("started_at", Text),      # first poll that saw the task RUNNING
    Column("completed_at", Text),
    Column("error", Text),           # GEE error message when status is FAILED
    Column("ingested_at", Text),
    Column("row_count", Integer),
)

_REGISTRY_TABLES = [sources, variables, cells, hiergp_cells, spatiotemporal_units, jobs]


def migrate_jobs_columns(engine: Engine) -> None:
    """Add jobs.started_at / jobs.error to a store created before they existed."""
    existing = {c["name"] for c in sa_inspect(engine).get_columns("jobs")}
    for column, ddl_type in (("started_at", "TEXT"), ("error", "TEXT")):
        if column not in existing:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE jobs ADD COLUMN {column} {ddl_type}"))
            print(f"  jobs.{column} added")


def initialize_db(engine: Engine) -> None:
    """Create all registry tables (sources, variables, cells, spatiotemporal_units, jobs)."""
    if engine.dialect.name == "postgresql":
        _initialize_postgresql(engine)
    else:
        metadata.create_all(engine, tables=_REGISTRY_TABLES)
    migrate_jobs_columns(engine)
    print(f"Initialised schema on {engine.url.render_as_string(hide_password=True)}")


def _initialize_postgresql(engine: Engine) -> None:
    """Create registry tables; spatiotemporal_units is RANGE-partitioned by year."""
    metadata.create_all(engine, tables=[sources, variables, cells, hiergp_cells, jobs])
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS spatiotemporal_units (
                id       INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
                cell_pk  INTEGER NOT NULL REFERENCES cells(id),
                timestamp TIMESTAMP NOT NULL,
                UNIQUE (cell_pk, timestamp)
            ) PARTITION BY RANGE (timestamp)
        """))


# ---------------------------------------------------------------------------
# Spatiotemporal unit year partitions (PostgreSQL only)
# ---------------------------------------------------------------------------

def ensure_spatiotemporal_year_partitions(engine: Engine, years: set[int]) -> None:
    """Create year RANGE partitions for spatiotemporal_units as needed."""
    if engine.dialect.name != "postgresql":
        return
    with engine.begin() as conn:
        for year in sorted(years):
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS spatiotemporal_units_{year}
                PARTITION OF spatiotemporal_units
                FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01')
            """))


# ---------------------------------------------------------------------------
# Per-source wide observation tables (created lazily at ingest time)
# ---------------------------------------------------------------------------

def source_table_name(source_name: str) -> str:
    """Return the observation table name for a source, e.g. obs_landsat_ndvi."""
    return "obs_" + source_name.lower()


def ensure_source_obs_table(
    engine: Engine, source_name: str, variable_defs: list[dict]
) -> None:
    """
    Create the wide observation table for a source if it does not exist.

    Schema: unit_pk (PK, FK → spatiotemporal_units) + one DOUBLE PRECISION
    column per variable. No timestamp — that lives in spatiotemporal_units.
    Called at ingest time only.
    """
    table = source_table_name(source_name)
    col_defs = ",\n    ".join(
        f"{v['name']} DOUBLE PRECISION" for v in variable_defs
    )
    # No FK to spatiotemporal_units — PostgreSQL disallows FKs referencing
    # partitioned tables unless the partition key is part of the constraint.
    ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            unit_pk  INTEGER NOT NULL,
            {col_defs},
            PRIMARY KEY (unit_pk)
        )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
