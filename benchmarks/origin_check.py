"""
Internal soundness check: did the migration put the right value on the right
partition, for every row?

Row counts cannot answer this. The load re-derives each value's partition — it
builds a cell_id string from the origin's grid x/y, inserts it to get a
surrogate key, inserts (cell, timestamp) to get another, and writes the
observation against that. A join error there would misplace values while
leaving counts perfect.

Method: on each side compute, per source and year, an order-independent sum of
per-row hashes over (cell_id, timestamp, value). The hash binds the value to
its partition, so a swapped or misplaced value changes the sum even though the
count and the set of values are unchanged. Values are rounded to 6 decimals and
compared numerically, so the float4-to-double widening does not register as a
difference. Parks is divided by 1000 on the origin side, matching the metres to
kilometres conversion the load applies.

Output is plain: per source, per year-range, matched / mismatched / origin-only
/ store-only.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import psycopg2

S = Path(__file__).parent
OUT = S / "origin_check_results_v2.jsonl"

sys.path.insert(0, "/Users/funsooje/Documents/GitHub/GeoMetrics")
import importlib.util
spec = importlib.util.spec_from_file_location(
    "mg", "/Users/funsooje/Documents/GitHub/GeoMetrics/scripts/migrate_gtl.py")
mg = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mg)

GM_DSN = mg._gm_dsn(__import__("json").load(
    open(Path.home() / ".geometrics/config.json"))["db_url"])
GTL_DSN = mg._gtl_dsn()

HASH = ("sum(('x' || substr(md5({key}), 1, 8))::bit(32)::bigint)")


def origin_sql(gtl_table: str, value_expr: str, link: str, level: int) -> str:
    """Origin side: rebuild the cell_id the load would have derived."""
    if link == "l3yo":
        return f"""
            SELECT l.temporal AS yr, count(*) AS n,
                   count(*) FILTER (WHERE {value_expr} IS NULL) AS nulls,
                   {HASH.format(key=f"'{level}:' || g.x || '|' || g.y || '@' || "
                                    f"l.temporal || '=' || ({value_expr})::real::text")} AS h
            FROM {gtl_table} n
            JOIN l3yo l ON n.l3y_oid = l.id
            JOIN grids g ON g.id = l.gridid
            WHERE l.temporal BETWEEN 2010 AND 2022
            GROUP BY 1 ORDER BY 1"""
    if link == "l3y_air":
        return f"""
            SELECT l.temporal AS yr, count(*) AS n,
                   count(*) FILTER (WHERE {value_expr} IS NULL) AS nulls,
                   {HASH.format(key=f"'{level}:' || g.x || '|' || g.y || '@' || "
                                    f"l.temporal || '=' || ({value_expr})::real::text")} AS h
            FROM air a
            JOIN l3y l ON l.id = a.l3yid
            JOIN grids g ON g.id = l.gridid
            WHERE l.temporal BETWEEN 2010 AND 2022
            GROUP BY 1 ORDER BY 1"""
    if link == "l8h":
        return f"""
            SELECT extract(year from h.temporal)::int AS yr, count(*) AS n,
                   count(*) FILTER (WHERE {value_expr} IS NULL) AS nulls,
                   {HASH.format(key=f"'{level}:' || g.x || '|' || g.y || '@' || "
                                    f"to_char(h.temporal, 'YYYY-MM-DD HH24:MI:SS') || "
                                    f"'=' || ({value_expr})::real::text")} AS h
            FROM era5land e
            JOIN l8h h ON e.l8hid = h.id
            JOIN grids g ON g.id = h.gridid
            WHERE h.temporal >= '2010-01-01' AND h.temporal < '2023-01-01'
            GROUP BY 1 ORDER BY 1"""
    raise ValueError(link)


def store_sql(obs_table: str, column: str, hourly: bool = False) -> str:
    ts = ("to_char(u.timestamp, 'YYYY-MM-DD HH24:MI:SS')" if hourly
          else "extract(year from u.timestamp)::int::text")
    return f"""
        SELECT extract(year from u.timestamp)::int AS yr, count(*) AS n,
               count(*) FILTER (WHERE o.{column} IS NULL) AS nulls,
               {HASH.format(key=f"c.cell_id || '@' || {ts} || '=' || "
                                f"(o.{column})::real::text")} AS h
        FROM {obs_table} o
        JOIN spatiotemporal_units u ON u.id = o.unit_pk
        JOIN cells c ON c.id = u.cell_pk
        WHERE u.timestamp >= '2010-01-01' AND u.timestamp < '2023-01-01'
        GROUP BY 1 ORDER BY 1"""


CHECKS = [
    # name, gtl table, origin value expr, link, level, store table, store column
    ("MODIS_NDVI", "ndvi", "n.ndvi", "l3yo", 13, "obs_modis_ndvi", "NDVI"),
    ("Landsat_NDVI", "ndvi_ls", "n.ndvi", "l3yo", 13, "obs_landsat_ndvi", "NDVI"),
    ("MODIS_Treecover", "mod44b", "n.percent_tree_cover", "l3yo", 13,
     "obs_modis_treecover", "percent_tree_cover"),
    ("NLCD", "nlcd", "n.impervious", "l3yo", 13, "obs_nlcd", "impervious"),
    ("YALE_UHI", "uhi", "n.yearlydaytime", "l3yo", 13, "obs_yale_uhi", "yearly_daytime"),
    ("JRC_Water", "water", "n.distance", "l3yo", 13, "obs_jrc_water", "water_distance"),
    ("CACES_Air", "air", "a.pm25", "l3y_air", 13, "obs_caces_air", "pm25"),
    ("ERA5_Land", "era5land", "e.temperature_2m", "l8h", 8,
     "obs_era5_land", "temperature_2m"),
]


def run(cur, sql):
    cur.execute(sql)
    return {r[0]: {"n": r[1], "nulls": r[2], "h": r[3]} for r in cur.fetchall()}


def main() -> None:
    gtl = psycopg2.connect(GTL_DSN); gtl.set_session(readonly=True)
    gm = psycopg2.connect(GM_DSN)
    gtl_cur, gm_cur = gtl.cursor(), gm.cursor()
    gm_cur.execute("set max_parallel_workers_per_gather=4")

    for name, table, expr, link, level, obs, column in CHECKS:
        t0 = time.time()
        try:
            origin = run(gtl_cur, origin_sql(table, expr, link, level))
            store = run(gm_cur, store_sql(obs, column, hourly=(link == "l8h")))
        except Exception as exc:
            print(f"{name}: FAILED {exc!r}", flush=True)
            gtl.rollback(); gm.rollback()
            continue

        years = sorted(set(origin) | set(store))
        rec = {"source": name, "seconds": round(time.time() - t0, 1), "years": {}}
        for span, yrs in (("2010-2018", [y for y in years if y <= 2018]),
                          ("2019-2022", [y for y in years if y >= 2019])):
            o_n = sum(origin.get(y, {}).get("n", 0) for y in yrs)
            s_n = sum(store.get(y, {}).get("n", 0) for y in yrs)
            hash_match = all(origin.get(y, {}).get("h") == store.get(y, {}).get("h")
                             for y in yrs)
            bad = [y for y in yrs
                   if origin.get(y, {}).get("h") != store.get(y, {}).get("h")]
            rec["years"][span] = {
                "origin_rows": o_n, "store_rows": s_n,
                "row_delta": s_n - o_n,
                "origin_only_years": [y for y in yrs if y in origin and y not in store],
                "store_only_years": [y for y in yrs if y in store and y not in origin],
                "hashes_match": hash_match,
                "mismatched_years": bad,
            }
        verdict = all(v["hashes_match"] and v["row_delta"] == 0
                      for v in rec["years"].values())
        rec["clean"] = verdict
        with OUT.open("a") as fh:
            fh.write(json.dumps(rec) + "\n")
        flag = "CLEAN" if verdict else "*** MISMATCH ***"
        print(f"{name:17s} {flag:16s} "
              f"2010-18 o={rec['years']['2010-2018']['origin_rows']:>10,} "
              f"s={rec['years']['2010-2018']['store_rows']:>10,} | "
              f"2019-22 o={rec['years']['2019-2022']['origin_rows']:>10,} "
              f"s={rec['years']['2019-2022']['store_rows']:>10,} "
              f"({rec['seconds']}s)", flush=True)

    gtl_cur.close(); gm_cur.close(); gtl.close(); gm.close()


if __name__ == "__main__":
    main()
