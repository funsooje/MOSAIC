"""
B1 — storage inventory of the deployment store.

Per source: rows, distinct cells, grid level, time span, table and index size.
Plus registry totals, cells by level, units by year, and the replication factor
for coarse sources (how many level-13 cells carry one native pixel's value).

Usage:
    python benchmarks/b1_inventory.py [--out benchmarks/results]
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from geometrics import GeoMetrics


def query(engine, sql, **params) -> pd.DataFrame:
    with engine.connect() as conn:
        conn.execute(text("set max_parallel_workers_per_gather=0"))
        return pd.DataFrame(conn.execute(text(sql), params).mappings().all())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="benchmarks/results")
    args = parser.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    gm = GeoMetrics()
    sources = query(gm.engine, """
        SELECT source_id, name, native_level, pixel_resolution_m, temporal_granularity
        FROM sources ORDER BY name
    """)

    rows = []
    for src in sources.itertuples():
        table = "obs_" + src.name.lower()
        exists = query(gm.engine, "SELECT to_regclass(:t) IS NOT NULL AS ok", t=table)
        if not exists["ok"].iloc[0]:
            rows.append({"source": src.name, "rows": 0, "note": "no observation table"})
            continue
        t0 = time.time()
        stats = query(gm.engine, f"""
            SELECT count(*) AS rows, count(DISTINCT u.cell_pk) AS cells,
                   min(c.level) AS level_min, max(c.level) AS level_max,
                   min(u.timestamp) AS time_min, max(u.timestamp) AS time_max,
                   count(DISTINCT u.timestamp) AS timestamps
            FROM {table} o
            JOIN spatiotemporal_units u ON u.id = o.unit_pk
            JOIN cells c ON c.id = u.cell_pk
        """).iloc[0].to_dict()
        size = query(gm.engine, f"""
            SELECT pg_table_size('{table}') AS table_bytes,
                   pg_indexes_size('{table}') AS index_bytes
        """).iloc[0].to_dict()
        stats.update(size)
        stats.update({
            "source": src.name, "native_level": src.native_level,
            "pixel_resolution_m": src.pixel_resolution_m,
            "temporal_granularity": src.temporal_granularity,
            # How many stored cells share one native pixel: a 1 km product on a
            # 100 m grid repeats each value across ~100 cells.
            "cells_per_native_pixel": round(
                (src.pixel_resolution_m / 100.0) ** 2, 2) if src.pixel_resolution_m else None,
            "rows_per_cell": round(stats["rows"] / stats["cells"], 2) if stats["cells"] else None,
            "query_seconds": round(time.time() - t0, 1),
        })
        rows.append(stats)
        print(f"{src.name:22s} {stats['rows']:>12,} rows  {stats['cells']:>10,} cells  "
              f"level {stats['level_min']}  {stats['query_seconds']:>5.1f}s", flush=True)

    inventory = pd.DataFrame(rows)
    inventory.to_csv(out_dir / "b1_inventory.csv", index=False)

    totals = query(gm.engine, """
        SELECT (SELECT count(*) FROM cells) AS cells,
               (SELECT count(*) FROM hiergp_cells) AS hiergp_cells,
               (SELECT count(*) FROM spatiotemporal_units) AS spatiotemporal_units,
               (SELECT count(*) FROM jobs) AS jobs,
               pg_database_size(current_database()) AS db_bytes
    """)
    by_level = query(gm.engine, "SELECT backend, level, count(*) AS n FROM cells "
                                "GROUP BY 1,2 ORDER BY 2")
    by_year = query(gm.engine, "SELECT extract(year from timestamp)::int AS year, "
                               "count(*) AS n FROM spatiotemporal_units GROUP BY 1 ORDER BY 1")
    index_sizes = query(gm.engine, """
        SELECT c.relname AS index_name, pg_size_pretty(pg_relation_size(c.oid)) AS size
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'i'
          AND pg_relation_size(c.oid) > 0
        ORDER BY pg_relation_size(c.oid) DESC LIMIT 15
    """)

    for name, frame in (("b1_totals", totals), ("b1_cells_by_level", by_level),
                        ("b1_units_by_year", by_year), ("b1_index_sizes", index_sizes)):
        frame.to_csv(out_dir / f"{name}.csv", index=False)

    print("\n" + totals.to_string(index=False))
    print("\n" + by_level.to_string(index=False))
    print(f"\nobservation rows across all sources: {int(inventory['rows'].sum()):,}")
    print(f"table bytes {int(inventory['table_bytes'].sum()):,}, "
          f"index bytes {int(inventory['index_bytes'].sum()):,}")
    json.dump({"generated": time.strftime("%FT%T"),
               "observation_rows": int(inventory["rows"].sum()),
               "table_bytes": int(inventory["table_bytes"].sum()),
               "index_bytes": int(inventory["index_bytes"].sum()),
               "db_bytes": int(totals["db_bytes"].iloc[0])},
              open(out_dir / "b1_summary.json", "w"), indent=2)


if __name__ == "__main__":
    main()
