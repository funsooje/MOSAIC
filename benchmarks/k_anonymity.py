"""
k-anonymity of released (cell, time bin) units, counting people.

Threat model: an adversary who knows a target was at a particular place within a
particular time window, and who sees the released units, tries to single out that
target's record. k is the number of DISTINCT participants whose points fall in the
same released (cell, time bin) unit, so k=1 means the unit identifies one person.

This is deliberately not the earlier computation that counted child cells inside a
parent cell. A parent cell holding 100 child cells may hold one participant or
fifty; cells are not people.

Two caveats that belong with any number this produces:

1. The cohort is a twin registry and the GTL schema records no pairing between
   participants — no twin, pair, family, sibling or zygosity column exists. The
   computation therefore assumes participants are independent. For two co-located
   twins the computed k overstates the real protection, and the assumption cannot
   be tested from this data.
2. k is reported per released unit AND per participant. "Most units are safe" and
   "nearly every participant is unique somewhere" can both be true at once, and
   they answer different questions.

Inputs come from scripts that export, read-only, from the GTL database:
  ancestors.csv        level-13 cell -> its ancestors at standard levels 12..6
  l3yo.csv             (cell13, year) unit ids
  year_unit_client.csv distinct (l3yo unit, participant)
  day_cell_client.csv  distinct (cell13, day, participant)

Usage:
    python benchmarks/k_anonymity.py load --dir <export dir>
    python benchmarks/k_anonymity.py compute --dir <export dir> --out benchmarks/results
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from geometrics import GeoMetrics

LEVELS = [13, 12, 11, 10, 9, 8, 7, 6]
CELL_EDGE_M = {level: 25 * 2 ** (15 - level) for level in LEVELS}

TABLES = {
    "kanon_ancestors": ("cell13 BIGINT, s12 BIGINT, s11 BIGINT, s10 BIGINT, "
                        "s9 BIGINT, s8 BIGINT, s7 BIGINT, s6 BIGINT", "ancestors.csv"),
    "kanon_l3yo": ("id BIGINT, gridid BIGINT, temporal INTEGER", "l3yo.csv"),
    "kanon_year": ("l3yo_id BIGINT, client_id INTEGER", "year_unit_client.csv"),
    "kanon_day": ("grid_id BIGINT, day DATE, client_id INTEGER", "day_cell_client.csv"),
    "kanon_pairs": ("client_id INTEGER, pair_id INTEGER", "pair_map.csv"),
    # locations.grid_id references level-1 (25 m) grids, while l3yo.gridid
    # references level-3 (100 m); the day bin therefore needs its own map.
    "kanon_ancestors25": ("cell25 BIGINT, s13 BIGINT, s12 BIGINT, s11 BIGINT, "
                          "s10 BIGINT, s9 BIGINT, s8 BIGINT, s7 BIGINT, s6 BIGINT",
                          "ancestors25.csv"),
}


def load(gm: GeoMetrics, export_dir: Path) -> None:
    """COPY the exported CSVs into the GeoMetrics store and index them."""
    import psycopg2

    dsn = gm.config.db_url.replace("postgresql://", "")
    creds, hostpart = dsn.split("@")
    user, password = creds.split(":")
    hostport, dbname = hostpart.split("/")
    host, port = hostport.split(":")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname,
                            user=user, password=password)
    cur = conn.cursor()

    for table, (columns, filename) in TABLES.items():
        path = export_dir / filename
        if not path.exists():
            print(f"  {filename} missing, skipping {table}")
            continue
        print(f"  {table} <- {filename} ({path.stat().st_size / 1e6:.0f} MB)", flush=True)
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute(f"CREATE UNLOGGED TABLE {table} ({columns})")
        with path.open() as handle:
            cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER)", handle)
        conn.commit()
        cur.execute(f"SELECT count(*) FROM {table}")
        print(f"    {cur.fetchone()[0]:,} rows", flush=True)

    for ddl in ("CREATE INDEX IF NOT EXISTS kanon_pair_client ON kanon_pairs (client_id)",
                "CREATE INDEX IF NOT EXISTS kanon_anc_cell ON kanon_ancestors (cell13)",
                "CREATE INDEX IF NOT EXISTS kanon_anc25_cell ON kanon_ancestors25 (cell25)",
                "CREATE INDEX IF NOT EXISTS kanon_l3yo_id ON kanon_l3yo (id)",
                "CREATE INDEX IF NOT EXISTS kanon_year_unit ON kanon_year (l3yo_id)",
                "CREATE INDEX IF NOT EXISTS kanon_day_cell ON kanon_day (grid_id)"):
        cur.execute(ddl)
    conn.commit()
    cur.execute("ANALYZE kanon_ancestors; ANALYZE kanon_l3yo; ANALYZE kanon_year")
    conn.commit()
    cur.close(); conn.close()
    print("  loaded and indexed")


def unit_sql(level: int, temporal: str) -> str:
    """(cell, bin, participant) memberships at one spatial level and time bin."""
    cell = "l.gridid" if level == 13 else f"a.s{level}"
    if temporal == "year":
        return f"""
            SELECT {cell} AS cell, l.temporal::text AS bin, y.client_id, p.pair_id
            FROM kanon_year y
            JOIN kanon_l3yo l ON l.id = y.l3yo_id
            JOIN kanon_ancestors a ON a.cell13 = l.gridid
            JOIN kanon_pairs p ON p.client_id = y.client_id
        """
    return f"""
        SELECT a.s{level} AS cell, d.day::text AS bin, d.client_id, p.pair_id
        FROM kanon_day d
        JOIN kanon_ancestors25 a ON a.cell25 = d.grid_id
        JOIN kanon_pairs p ON p.client_id = d.client_id
    """


def compute_cell(gm: GeoMetrics, level: int, temporal: str) -> dict:
    sql = f"""
        WITH membership AS ({unit_sql(level, temporal)}),
        per_unit AS (
            -- k_participants counts people; k_pairs collapses co-twins into one
            -- entity, so a unit holding only one twin pair has k_pairs = 1.
            SELECT cell, bin,
                   count(DISTINCT client_id) AS k,
                   count(DISTINCT pair_id) AS k_pair
            FROM membership GROUP BY cell, bin
        ),
        joined AS (
            SELECT m.client_id, m.pair_id, u.k, u.k_pair
            FROM membership m JOIN per_unit u ON u.cell = m.cell AND u.bin = m.bin
        )
        SELECT
            (SELECT count(*) FROM per_unit) AS units,
            (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY k) FROM per_unit) AS k_median,
            (SELECT avg(k) FROM per_unit) AS k_mean,
            (SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY k) FROM per_unit) AS k_p25,
            (SELECT max(k) FROM per_unit) AS k_max,
            (SELECT count(*) FROM per_unit WHERE k = 1) AS units_k1,
            (SELECT count(*) FROM per_unit WHERE k >= 5) AS units_k5plus,
            (SELECT count(*) FROM joined) AS memberships,
            (SELECT count(*) FROM joined WHERE k < 5) AS memberships_k_lt_5,
            (SELECT count(DISTINCT client_id) FROM membership) AS participants,
            (SELECT count(DISTINCT client_id) FROM joined WHERE k = 1) AS participants_in_a_k1_unit,
            (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY k_pair) FROM per_unit) AS kpair_median,
            (SELECT avg(k_pair) FROM per_unit) AS kpair_mean,
            (SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY k_pair) FROM per_unit) AS kpair_p25,
            (SELECT count(*) FROM per_unit WHERE k_pair = 1) AS units_kpair1,
            (SELECT count(*) FROM per_unit WHERE k_pair >= 5) AS units_kpair5plus,
            (SELECT count(*) FROM joined WHERE k_pair < 5) AS memberships_kpair_lt_5,
            (SELECT count(DISTINCT pair_id) FROM membership) AS pairs,
            (SELECT count(DISTINCT pair_id) FROM joined WHERE k_pair = 1) AS pairs_in_a_kpair1_unit
    """
    t0 = time.time()
    with gm.engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().one()
    out = dict(row)
    out.update({
        "level": level, "cell_edge_m": CELL_EDGE_M[level], "temporal_bin": temporal,
        "pct_units_k1": round(100 * out["units_k1"] / out["units"], 2) if out["units"] else None,
        "pct_units_k5plus": round(100 * out["units_k5plus"] / out["units"], 2) if out["units"] else None,
        "pct_memberships_k_lt_5": round(
            100 * out["memberships_k_lt_5"] / out["memberships"], 2) if out["memberships"] else None,
        "pct_participants_in_a_k1_unit": round(
            100 * out["participants_in_a_k1_unit"] / out["participants"], 2)
            if out["participants"] else None,
        "pct_units_kpair1": round(100 * out["units_kpair1"] / out["units"], 2)
            if out["units"] else None,
        "pct_units_kpair5plus": round(100 * out["units_kpair5plus"] / out["units"], 2)
            if out["units"] else None,
        "pct_memberships_kpair_lt_5": round(
            100 * out["memberships_kpair_lt_5"] / out["memberships"], 2)
            if out["memberships"] else None,
        "pct_pairs_in_a_kpair1_unit": round(
            100 * out["pairs_in_a_kpair1_unit"] / out["pairs"], 2) if out["pairs"] else None,
        "seconds": round(time.time() - t0, 1),
    })
    for key in ("k_median", "k_mean", "k_p25", "kpair_median", "kpair_mean", "kpair_p25"):
        if out[key] is not None:
            out[key] = round(float(out[key]), 3)
    return out


def compute(gm: GeoMetrics, out_dir: Path) -> None:
    results_path = out_dir / "k_anonymity.jsonl"
    done = set()
    if results_path.exists():
        for line in results_path.read_text().splitlines():
            row = json.loads(line)
            done.add((row["level"], row["temporal_bin"]))

    with gm.engine.connect() as conn:
        have_day = conn.execute(text("SELECT to_regclass('kanon_day') IS NOT NULL")).scalar()

    for temporal in ("year", "day"):
        if temporal == "day" and not have_day:
            print("day-bin table not loaded yet, skipping")
            continue
        for level in LEVELS:
            if (level, temporal) in done:
                continue
            row = compute_cell(gm, level, temporal)
            if not row["units"]:
                print(f"  L{level} {temporal}: NO UNITS — join produced nothing, "
                      f"not writing a result", flush=True)
                continue
            with results_path.open("a") as handle:
                handle.write(json.dumps(row, default=str) + "\n")
            print(f"  L{level:<2} ({CELL_EDGE_M[level]:>6} m) {temporal:4s}: "
                  f"units {row['units']:>10,} | k_med {row['k_median']:>5} "
                  f"k=1 {row['pct_units_k1']:>6}% k>=5 {row['pct_units_k5plus']:>6}% "
                  f"ppl-unique {row['pct_participants_in_a_k1_unit']:>6}% | "
                  f"kpair_med {row['kpair_median']:>5} kpair=1 {row['pct_units_kpair1']:>6}% "
                  f"pairs-unique {row['pct_pairs_in_a_kpair1_unit']:>6}% ({row['seconds']}s)",
                  flush=True)

    rows = [json.loads(l) for l in results_path.read_text().splitlines()]
    pd.DataFrame(rows).to_csv(out_dir / "k_anonymity.csv", index=False)
    print(f"\nwrote {out_dir / 'k_anonymity.csv'} ({len(rows)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["load", "compute"])
    parser.add_argument("--dir", required=True, help="directory holding the exported CSVs")
    parser.add_argument("--out", default="benchmarks/results")
    args = parser.parse_args()

    gm = GeoMetrics()
    if args.action == "load":
        load(gm, Path(args.dir))
    else:
        out_dir = Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        compute(gm, out_dir)


if __name__ == "__main__":
    main()
