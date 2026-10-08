"""
Ingest GEE-exported CSVs into per-source wide observation tables.

Expected CSV columns:
  cell_id        - backend cell ID string
  source         - source name, e.g. "Landsat_NDVI"
  timestamp      - ISO date/datetime string
  <variable cols> - one column per variable, e.g. NDVI

GEE also adds 'system:index' and '.geo' columns; these are silently dropped.
The obs table for each source is created on first ingest if it does not exist.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from geometrics.store.query import _snap_timestamp
from geometrics.store.schema import (
    cells,
    jobs as jobs_table,
    ensure_source_obs_table,
    ensure_spatiotemporal_year_partitions,
    hiergp_cells,
    source_table_name,
    sources,
    spatiotemporal_units,
    variables,
)

_GEE_META_COLS = {"system:index", ".geo"}
_FIXED_COLS = {"cell_id", "timestamp", "source"}


def ingest_file(engine: Engine, path: str | Path, backend_name: str = "hiergp") -> int:
    """Ingest a single GEE-exported CSV. Returns total rows inserted."""
    path = Path(path)
    print(f"Ingesting {path.name} …")
    data = _read_csv(path)
    total = 0
    for source_name, group in data.groupby("source"):
        total += _ingest_group(engine, group, str(source_name), backend_name)
    return total


def mark_job_ingested(engine: Engine, gdrive_folder: str, file_prefix: str,
                      rows: int) -> None:
    """
    Flip the matching job to INGESTED.

    Jobs are matched on (gdrive_folder, file_prefix), which is what
    record_submitted stored, so a file ingested from Drive closes out its job
    instead of leaving it COMPLETED forever.
    """
    from datetime import datetime, timezone

    with engine.begin() as conn:
        conn.execute(
            jobs_table.update()
            .where(jobs_table.c.gdrive_folder == gdrive_folder)
            .where(jobs_table.c.file_prefix == file_prefix)
            .values(status="INGESTED",
                    ingested_at=datetime.now(timezone.utc).isoformat())
        )


def ingest_folder(
    engine: Engine, folder_path: str | Path, backend_name: str = "hiergp",
    gdrive_folder: str | None = None,
) -> dict:
    """
    Ingest all GEE-exported CSVs found in folder_path.

    Returns {filename: rows_inserted} for every file processed.
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder}")

    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {folder}")
        return {}

    results: dict[str, int] = {}
    for path in csv_files:
        print(f"Ingesting {path.name} …")
        data = _read_csv(path)
        inserted = 0
        for source_name, group in data.groupby("source"):
            inserted += _ingest_group(engine, group, str(source_name), backend_name)
        results[path.name] = inserted
        if gdrive_folder:
            mark_job_ingested(engine, gdrive_folder, path.stem, inserted)

    return results


# ---------------------------------------------------------------------------
# Core ingest logic
# ---------------------------------------------------------------------------

def _ingest_group(
    engine: Engine, group: pd.DataFrame, source_name: str, backend_name: str
) -> int:
    var_cols = [c for c in group.columns if c not in _FIXED_COLS]
    var_defs, temporal_granularity = _get_source_meta(engine, source_name)

    ensure_source_obs_table(engine, source_name, var_defs)

    # Snap timestamps to the source's granularity so they match query-time lookups
    group = group.copy()
    group["timestamp"] = group["timestamp"].apply(
        lambda ts: _snap_timestamp(str(ts), temporal_granularity)
    )

    years = {int(ts[:4]) for ts in group["timestamp"]}
    ensure_spatiotemporal_year_partitions(engine, years)

    pk_map = _ensure_cells(engine, group["cell_id"].tolist(), backend_name)
    group["cell_pk"] = group["cell_id"].map(pk_map)

    unit_map = _ensure_spatiotemporal_units(engine, group)
    group["unit_pk"] = group.apply(
        lambda r: unit_map.get((int(r["cell_pk"]), r["timestamp"])), axis=1
    )

    inserted = _insert_wide(engine, group, source_name, var_cols)
    skipped = len(group) - inserted
    print(f"  {source_name}: inserted {inserted} row(s), skipped {skipped} duplicate(s).")
    return inserted


def _get_source_meta(engine: Engine, source_name: str) -> tuple[list[dict], str]:
    """Return (variable_defs, temporal_granularity) for a registered source."""
    with engine.connect() as conn:
        src_row = conn.execute(
            select(sources.c.source_id, sources.c.temporal_granularity)
            .where(sources.c.name == source_name)
        ).fetchone()
        if src_row is None:
            raise ValueError(
                f"Source {source_name!r} not registered. Run gm.register_sources() first."
            )
        var_rows = conn.execute(
            select(variables.c.name, variables.c.unit).where(
                variables.c.source_id == src_row.source_id
            )
        ).fetchall()
    var_defs = [{"name": r.name, "unit": r.unit} for r in var_rows]
    return var_defs, src_row.temporal_granularity


def _build_rows(group: pd.DataFrame, var_cols: list[str]) -> list[dict]:
    rows = []
    for row in group.itertuples(index=False):
        entry: dict = {"unit_pk": int(row.unit_pk)}
        for col in var_cols:
            val = getattr(row, col)
            entry[col] = val if pd.notna(val) else None
        rows.append(entry)
    return rows


def _insert_wide(
    engine: Engine, group: pd.DataFrame, source_name: str, var_cols: list[str],
    upsert: bool = False,
) -> int:
    """Insert observation rows. upsert=True updates only the specified var_cols on conflict."""
    table = source_table_name(source_name)
    cols = ["unit_pk"] + var_cols
    col_sql = ", ".join(cols)
    param_sql = ", ".join(f":{c}" for c in cols)
    if upsert:
        update_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in var_cols)
        stmt = text(
            f"INSERT INTO {table} ({col_sql}) VALUES ({param_sql})"
            f" ON CONFLICT (unit_pk) DO UPDATE SET {update_sql}"
        )
    elif engine.dialect.name == "postgresql":
        stmt = text(
            f"INSERT INTO {table} ({col_sql}) VALUES ({param_sql})"
            " ON CONFLICT (unit_pk) DO NOTHING"
        )
    else:
        stmt = text(f"INSERT OR IGNORE INTO {table} ({col_sql}) VALUES ({param_sql})")
    with engine.begin() as conn:
        result = conn.execute(stmt, _build_rows(group, var_cols))
    return result.rowcount


# ---------------------------------------------------------------------------
# Spatiotemporal unit registration
# ---------------------------------------------------------------------------

def _ensure_spatiotemporal_units(
    engine: Engine, group: pd.DataFrame
) -> dict[tuple[int, str], int]:
    """
    Upsert (cell_pk, timestamp) pairs into spatiotemporal_units.
    Returns {(cell_pk, timestamp_str): unit_pk}.
    """
    dialect = engine.dialect.name
    unit_rows = [
        {"cell_pk": int(row.cell_pk), "timestamp": str(row.timestamp)}
        for row in group[["cell_pk", "timestamp"]].drop_duplicates().itertuples(index=False)
    ]

    with engine.begin() as conn:
        if dialect == "sqlite":
            conn.execute(
                sqlite_insert(spatiotemporal_units)
                .prefix_with("OR IGNORE")
                .values(unit_rows)
            )
        else:
            conn.execute(
                pg_insert(spatiotemporal_units)
                .on_conflict_do_nothing(index_elements=["cell_pk", "timestamp"])
                .values(unit_rows)
            )

    cell_pks = [r["cell_pk"] for r in unit_rows]
    timestamps = [r["timestamp"] for r in unit_rows]
    with engine.connect() as conn:
        if dialect == "postgresql":
            rows = conn.execute(text("""
                SELECT id, cell_pk, timestamp
                FROM spatiotemporal_units
                WHERE cell_pk = ANY(:pks) AND timestamp = ANY(CAST(:tss AS timestamp[]))
            """), {"pks": cell_pks, "tss": timestamps}).fetchall()
        else:
            pks_sql = ",".join(str(p) for p in cell_pks)
            tss_sql = ",".join(f"'{t}'" for t in timestamps)
            rows = conn.execute(text(f"""
                SELECT id, cell_pk, timestamp
                FROM spatiotemporal_units
                WHERE cell_pk IN ({pks_sql}) AND timestamp IN ({tss_sql})
            """)).fetchall()
    return {(r.cell_pk, _dt_to_str(r.timestamp)): r.id for r in rows}


def _dt_to_str(value) -> str:
    """Normalise a timestamp returned from PostgreSQL (datetime) or SQLite (str) to
    the same string format that _snap_timestamp produces."""
    if not hasattr(value, "strftime"):
        return str(value)
    if value.hour == 0 and value.minute == 0 and value.second == 0:
        return value.strftime("%Y-%m-%d")
    return value.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Cell registration
# ---------------------------------------------------------------------------

def _cell_level(cell_id: str, backend_name: str) -> int:
    if backend_name == "hiergp":
        return int(cell_id.split(":")[0])
    if backend_name == "h3":
        import h3  # pylint: disable=import-outside-toplevel,import-error
        return h3.get_resolution(cell_id)
    raise ValueError(f"Unknown backend: {backend_name!r}")


def _parse_hiergp_xy(cell_id: str) -> tuple[int, int]:
    _, coords = cell_id.split(":")
    coord_x, coord_y = coords.split("|")
    return int(coord_x), int(coord_y)


def _ensure_hiergp_cells(
    engine: Engine, hiergp_rows: list[dict], pk_map: dict[str, int], dialect: str
) -> None:
    hgp_rows = [
        {"cell_pk": pk_map[r["cell_id"]], "x": r["x"], "y": r["y"]}
        for r in hiergp_rows
        if r["cell_id"] in pk_map
    ]
    with engine.begin() as conn:
        if dialect == "sqlite":
            conn.execute(sqlite_insert(hiergp_cells).prefix_with("OR IGNORE"), hgp_rows)
        else:
            conn.execute(
                pg_insert(hiergp_cells).on_conflict_do_nothing(index_elements=["cell_pk"]),
                hgp_rows,
            )


def _ensure_cells(
    engine: Engine, cell_ids: list[str], backend_name: str
) -> dict[str, int]:
    """Upsert cell_ids into the cells table. Returns {cell_id: cell_pk}."""
    unique_ids = list(dict.fromkeys(cell_ids))
    dialect = engine.dialect.name

    cell_rows, hiergp_rows = [], []
    for cid in unique_ids:
        level = _cell_level(cid, backend_name)
        cell_rows.append({"cell_id": cid, "backend": backend_name, "level": level})
        if backend_name == "hiergp":
            coord_x, coord_y = _parse_hiergp_xy(cid)
            hiergp_rows.append({"cell_id": cid, "x": coord_x, "y": coord_y})

    with engine.begin() as conn:
        if dialect == "sqlite":
            conn.execute(sqlite_insert(cells).prefix_with("OR IGNORE"), cell_rows)
        else:
            conn.execute(
                pg_insert(cells).on_conflict_do_nothing(index_elements=["cell_id"]),
                cell_rows,
            )

    with engine.connect() as conn:
        rows = conn.execute(
            select(cells.c.id, cells.c.cell_id).where(cells.c.cell_id.in_(unique_ids))
        ).fetchall()
    pk_map = {r.cell_id: r.id for r in rows}

    if backend_name == "hiergp" and hiergp_rows:
        _ensure_hiergp_cells(engine, hiergp_rows, pk_map, dialect)

    return pk_map


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> pd.DataFrame:
    data = pd.DataFrame(pd.read_csv(path))
    data = data.drop(columns=[c for c in _GEE_META_COLS if c in data.columns])
    missing = _FIXED_COLS - set(data.columns)
    if missing:
        raise ValueError(f"{path.name}: missing required columns: {missing}")
    var_cols = [c for c in data.columns if c not in _FIXED_COLS]
    if not var_cols:
        raise ValueError(f"{path.name}: no variable columns found.")
    data["timestamp"] = pd.to_datetime(data["timestamp"])
    return data
