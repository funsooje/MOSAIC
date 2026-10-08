"""
Query layer: resolve locations to cell_ids, check availability, fetch observations.

Typical workflow:
  query   = DataQuery(locations=[...], variables=[...], timestamps=[...])
  items   = resolve(engine, backend, query)          # lat/lon → cell_id
  report  = check_availability(engine, backend, items)
  # submit GEE jobs for report["missing"] if needed, then ingest
  data_df = fetch(engine, backend, items)            # returns a DataFrame
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sqlalchemy import inspect as sa_inspect, select, text
from sqlalchemy.engine import Engine

from geometrics.backends.base import GridBackend
from geometrics.catalog import CATALOG
from geometrics.store.schema import (
    cells as cells_table,
    source_table_name,
    sources,
    variables as variables_table,
)


# ---------------------------------------------------------------------------
# Public data classes
# ---------------------------------------------------------------------------

@dataclass
class VariableSpec:
    """A source + variable pair, optionally at a specific grid level."""

    source: str       # matches sources.name, e.g. "Landsat8_NDVI"
    parameter: str    # matches variables.name, e.g. "NDVI"
    level: int | None = None  # None → use source's native_level

    @classmethod
    def parse(cls, spec: str, level: int | None = None) -> "VariableSpec":
        """Parse 'Source:parameter' shorthand, e.g. 'Landsat8_NDVI:NDVI'."""
        parts = spec.split(":")
        if len(parts) != 2:
            raise ValueError(f"VariableSpec must be 'source:parameter', got: {spec!r}")
        return cls(source=parts[0].strip(), parameter=parts[1].strip(), level=level)

    def __str__(self) -> str:
        return f"{self.source}:{self.parameter}"


@dataclass
class DataQuery:
    """A set of locations and variables to query."""

    rows: list[tuple[float, float, str]]  # [(lat, lon, timestamp), ...]
    variables: list[VariableSpec]


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def resolve(engine: Engine, backend: GridBackend, query: DataQuery) -> list[dict]:
    """
    Resolve each (lat, lon) × variable × timestamp to a cell_id.

    Level per variable: VariableSpec.level if set, else source's native_level.
    Timestamps are snapped to each variable's temporal_granularity before lookup.
    Returns a flat list of dicts — one per (location, variable, timestamp).
    """
    source_meta = _load_source_meta(engine, [v.source for v in query.variables])

    items = []
    for lat, lon, timestamp in query.rows:
        for var_spec in query.variables:
            meta = source_meta[var_spec.source]
            level = var_spec.level if var_spec.level is not None else meta["native_level"]
            cell_id = backend.point_to_cell(lat, lon, level)
            snapped = _snap_timestamp(timestamp, meta["temporal_granularity"])
            items.append({
                "lat": lat,
                "lon": lon,
                "cell_id": cell_id,
                "source": var_spec.source,
                "parameter": var_spec.parameter,
                "original_timestamp": timestamp,
                "timestamp": snapped,
                "temporal_granularity": meta["temporal_granularity"],
                "requested_level": level,
                "native_level": meta["native_level"],
                "known_variables": meta["known_variables"],
                "table_exists": meta["table_exists"],
            })
    return items


def check_availability(engine: Engine, backend: GridBackend, resolved_items: list[dict]) -> dict:
    """
    Check which resolved items already have data in the store.

    Coarser-than-native requests: checks whether any descendant cells have data.
    Returns {"available": [...], "missing": [...]} where missing items include a "reason" key.
    """
    available, missing = [], []

    for item in resolved_items:
        if item["parameter"] not in item["known_variables"]:
            missing.append({**item, "reason": "unknown variable"})
            continue

        if not item["table_exists"]:
            missing.append({**item, "reason": "not in store"})
            continue

        requested_level = item["requested_level"]
        native_level = item["native_level"]

        if requested_level > native_level:
            missing.append({**item, "reason": "requested level finer than native"})
            continue

        cell_ids_to_check = (
            [item["cell_id"]]
            if requested_level == native_level
            else _get_descendants(backend, item["cell_id"], native_level)
        )

        if _has_data(engine, cell_ids_to_check, item["source"], item["timestamp"]):
            available.append(item)
        else:
            missing.append({**item, "reason": "not in store"})

    return {"available": available, "missing": missing}


def fetch(engine: Engine, backend: GridBackend, resolved_items: list[dict]) -> pd.DataFrame:
    """
    Fetch observations for pre-resolved items.

    - Same level as native  → direct lookup.
    - Coarser than native   → averages all stored descendant values.
    - Finer than native     → NaN (cannot synthesise finer data than stored).

    Returns a DataFrame with columns:
      lat, lon, timestamp, resolved_timestamp, source, parameter, value,
      cell_id, level, aggregated, _has_record
    """
    rows = []
    for item in resolved_items:
        if item["parameter"] not in item["known_variables"] or not item["table_exists"]:
            rows.append(_make_row(item, value=None, aggregated=False, has_record=False))
            continue

        requested_level = item["requested_level"]
        native_level = item["native_level"]

        if requested_level > native_level:
            rows.append(_make_row(item, value=None, aggregated=False, has_record=False))
        elif requested_level == native_level:
            found, value = _lookup_single(
                engine, item["cell_id"], item["source"], item["parameter"], item["timestamp"]
            )
            rows.append(_make_row(item, value=value, aggregated=False, has_record=found))
        else:
            descendants = _get_descendants(backend, item["cell_id"], native_level)
            found, value = _lookup_many(
                engine, descendants, item["source"], item["parameter"], item["timestamp"]
            )
            rows.append(_make_row(item, value=value, aggregated=True, has_record=found))

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_cell_pks(engine: Engine, cell_ids: list[str]) -> dict[str, int]:
    """Return {cell_id: cell_pk} for all known cell_ids."""
    if not cell_ids:
        return {}
    with engine.connect() as conn:
        rows = conn.execute(
            select(cells_table.c.id, cells_table.c.cell_id).where(
                cells_table.c.cell_id.in_(cell_ids)
            )
        ).fetchall()
    return {r.cell_id: r.id for r in rows}


def _load_source_meta(engine: Engine, source_names: list[str]) -> dict[str, dict]:
    unique_names = list(dict.fromkeys(source_names))
    with engine.connect() as conn:
        src_rows = conn.execute(
            select(sources).where(sources.c.name.in_(unique_names))
        ).fetchall()

        unknown = set(unique_names) - {r.name for r in src_rows}
        if unknown:
            # Local sources are in the DB but not in CATALOG — that is fine.
            unregistered = sorted(unknown & set(CATALOG))
            truly_unknown = sorted(unknown - set(CATALOG))
            parts = []
            if unregistered:
                parts.append(
                    f"Known but unregistered source(s): {unregistered}. "
                    "Run gm.register_sources() to add them to the database."
                )
            if truly_unknown:
                parts.append(
                    f"Unknown source(s) (not in catalog and not in database): {truly_unknown}. "
                    "Run gm.register_local_source() if this is a local source."
                )
            raise ValueError(" ".join(parts))

        src_meta = {r.name: r for r in src_rows}
        var_rows = conn.execute(
            select(variables_table).where(
                variables_table.c.source_id.in_([r.source_id for r in src_rows])
            )
        ).fetchall()

    id_to_name = {r.source_id: r.name for r in src_rows}
    var_by_source: dict[str, set[str]] = {name: set() for name in unique_names}
    for var in var_rows:
        var_by_source[id_to_name[var.source_id]].add(var.name)

    existing_tables = set(sa_inspect(engine).get_table_names())

    return {
        name: {
            "native_level": src_meta[name].native_level,
            "temporal_granularity": src_meta[name].temporal_granularity,
            "known_variables": var_by_source[name],
            "table_exists": source_table_name(name) in existing_tables,
        }
        for name in unique_names
    }


def _snap_timestamp(timestamp: str, granularity: str) -> str:
    """Truncate a timestamp string to the variable's storage granularity."""
    if granularity == "static":
        return "1900-01-01"
    date_part = timestamp[:10]  # YYYY-MM-DD
    year, month, _ = date_part.split("-")
    if granularity in ("year", "annual"):
        return f"{year}-01-01"
    if granularity == "month":
        return f"{year}-{month}-01"
    if granularity == "day":
        return date_part
    if granularity == "hour":
        # Accepts "YYYY-MM-DD", "YYYY-MM-DDTHH:MM:SS" and "YYYY-MM-DD HH:MM:SS".
        # A date with no time means midnight; slicing blindly would produce
        # "YYYY-MM-DD:00:00", which matches nothing in the store.
        hour = timestamp[11:13] or "00"
        return f"{date_part} {hour}:00:00"
    raise ValueError(f"Unsupported temporal_granularity: {granularity!r}")


def _get_descendants(backend: GridBackend, cell_id: str, target_level: int) -> list[str]:
    current = backend.cell_level(cell_id)
    if current == target_level:
        return [cell_id]
    if current > target_level:
        return []
    result = []
    for child in backend.cell_children(cell_id):
        result.extend(_get_descendants(backend, child, target_level))
    return result


def _has_data(engine: Engine, cell_ids: list[str], source_name: str, timestamp: str) -> bool:
    pk_map = _resolve_cell_pks(engine, cell_ids)
    if not pk_map:
        return False
    table = source_table_name(source_name)
    pks = ", ".join(str(pk) for pk in pk_map.values())
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT 1 FROM spatiotemporal_units u
            JOIN {table} o ON o.unit_pk = u.id
            WHERE u.cell_pk IN ({pks}) AND u.timestamp = :ts
            LIMIT 1
        """), {"ts": timestamp}).fetchone()
    return row is not None


def _lookup_single(
    engine: Engine, cell_id: str, source_name: str, parameter: str, timestamp: str
) -> tuple[bool, float | None]:
    pk_map = _resolve_cell_pks(engine, [cell_id])
    if not pk_map:
        return False, None
    table = source_table_name(source_name)
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT o.{parameter}
            FROM spatiotemporal_units u
            JOIN {table} o ON o.unit_pk = u.id
            WHERE u.cell_pk = :pk AND u.timestamp = :ts
        """), {"pk": pk_map[cell_id], "ts": timestamp}).fetchone()
    if row is None:
        return False, None
    return True, row[0]


def _lookup_many(
    engine: Engine,
    cell_ids: list[str],
    source_name: str,
    parameter: str,
    timestamp: str,
) -> tuple[bool, float | None]:
    pk_map = _resolve_cell_pks(engine, cell_ids)
    if not pk_map:
        return False, None
    table = source_table_name(source_name)
    pks = ", ".join(str(pk) for pk in pk_map.values())
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT o.{parameter}
            FROM spatiotemporal_units u
            JOIN {table} o ON o.unit_pk = u.id
            WHERE u.cell_pk IN ({pks}) AND u.timestamp = :ts
        """), {"ts": timestamp}).fetchall()
    if not rows:
        return False, None
    values = [r[0] for r in rows if r[0] is not None]
    return True, (sum(values) / len(values) if values else None)


def clear_observations(engine: Engine, source_name: str) -> None:
    """
    Drop the observation table for a source.

    Leaves source and variable registrations intact so corrected data can be
    re-ingested without re-running init_db. Raises ValueError if the source
    is not registered.
    """
    with engine.connect() as conn:
        src_row = conn.execute(
            select(sources.c.source_id).where(sources.c.name == source_name)
        ).fetchone()
    if src_row is None:
        raise ValueError(f"Source {source_name!r} not found in database.")

    table = source_table_name(source_name)
    # CASCADE is PostgreSQL-only; SQLite rejects it outright.
    cascade = " CASCADE" if engine.dialect.name == "postgresql" else ""
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table}{cascade}"))


def _make_row(item: dict, value: float | None, aggregated: bool, has_record: bool) -> dict:
    return {
        "lat": item["lat"],
        "lon": item["lon"],
        "timestamp": item["original_timestamp"],
        "resolved_timestamp": item["timestamp"],
        "source": item["source"],
        "parameter": item["parameter"],
        "value": value,
        "cell_id": item["cell_id"],
        "level": item["requested_level"],
        "aggregated": aggregated,
        "_has_record": has_record,
    }
