"""
Case study: per-participant weekly exposure, recorded and then reproduced.

Pass 1 ("record") answers a question against the deployed store and writes a
provenance manifest: sources and their GEE collections, storage levels, native
sensor resolutions, variables, the partitions touched, the time range, the
participant set, and the store's identity.

Pass 2 ("reproduce") is handed the manifest and nothing else, re-runs the query
from it, and compares the result byte for byte against pass 1.

The inspection step between them is the point of the exercise: greenness and air
quality enter the store at different native resolutions and different temporal
granularities, so a weekly summary that mixed them silently would imply more
precision than the inputs carry.

Usage:
    python scripts/case_study.py record     --out benchmarks/results
    python scripts/case_study.py reproduce  --out benchmarks/results
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from geometrics import GeoMetrics

QUESTION = ("For each participant, what was their mean weekly exposure to greenness "
            "(Landsat NDVI) and to fine particulate air pollution (CACES PM2.5) "
            "during 2019?")
VARIABLES = ["Landsat_NDVI:NDVI", "CACES_Air:pm25"]
YEAR = 2019
N_PARTICIPANTS = 10
POINTS_PER_WEEK = 4          # weekly mean from about four observations
MIN_WEEKS = 40               # participants must cover most of the year


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def store_provenance(gm: GeoMetrics) -> dict:
    """Everything an analyst needs to know about the inputs, read from the store."""
    from geometrics.catalog import CATALOG

    sources = {}
    for spec in VARIABLES:
        source, variable = spec.split(":")
        with gm.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT native_level, pixel_resolution_m, temporal_granularity,
                       source_temporal_granularity
                FROM sources WHERE name = :n
            """), {"n": source}).fetchone()
            unit = conn.execute(text("""
                SELECT v.unit FROM variables v JOIN sources s ON s.source_id = v.source_id
                WHERE s.name = :n AND v.name = :v
            """), {"n": source, "v": variable}).scalar()
            partitions = [r[0] for r in conn.execute(text("""
                SELECT c.relname FROM pg_class c
                JOIN pg_inherits i ON i.inhrelid = c.oid
                WHERE i.inhparent = 'spatiotemporal_units'::regclass
                  AND c.relname LIKE :pat ORDER BY 1
            """), {"pat": f"%_{YEAR}"}).fetchall()]
        catalog = CATALOG.get(source, {})
        sources[spec] = {
            "variable_unit": unit,
            "gee_collection": catalog.get("gee_collection", "local source, not from GEE"),
            "storage_level": row.native_level,
            "storage_cell_edge_m": round(25 * 2 ** (15 - row.native_level)),
            "native_pixel_resolution_m": row.pixel_resolution_m,
            "stored_temporal_granularity": row.temporal_granularity,
            "upstream_temporal_granularity": row.source_temporal_granularity,
            "partitions_read": partitions,
        }

    with gm.engine.connect() as conn:
        db_bytes = conn.execute(text("SELECT pg_database_size(current_database())")).scalar()
        version = conn.execute(text("SELECT version()")).scalar()

    return {
        "question": QUESTION,
        "variables": VARIABLES,
        "year": YEAR,
        "sources": sources,
        "store": {
            "db_url_host": gm.config.db_url.split("@")[-1],
            "db_bytes": int(db_bytes),
            "postgres_version": version.split(" on ")[0],
            "backend": gm.config.backend,
        },
        "code_version": subprocess.run(
            ["git", "describe", "--tags", "--always", "--dirty"],
            capture_output=True, text=True,
            cwd=Path(__file__).resolve().parent.parent).stdout.strip(),
    }


# ---------------------------------------------------------------------------
# The analysis
# ---------------------------------------------------------------------------

def sample_participant_points(out_dir: Path) -> pd.DataFrame:
    """Participant locations for the year, from the source GTL database."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "mg", Path(__file__).resolve().parent / "migrate_gtl.py")
    mg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mg)

    import psycopg2
    conn = psycopg2.connect(mg._gtl_dsn())
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT client_id FROM locations ORDER BY client_id")
    clients = [r[0] for r in cur.fetchall()]

    rows = []
    for client in clients:
        # Spread the sample across the year: up to POINTS_PER_WEEK points in each
        # ISO week. Taking the first N chronologically would put every point in
        # January and make a weekly summary meaningless.
        cur.execute("""
            SELECT latitude, longitude, datetime FROM (
                SELECT latitude, longitude, datetime,
                       row_number() OVER (PARTITION BY date_trunc('week', datetime)
                                          ORDER BY datetime) AS rn
                FROM locations
                WHERE client_id = %s AND datetime >= %s AND datetime < %s
                  AND latitude IS NOT NULL
            ) ranked
            WHERE rn <= %s
            ORDER BY datetime
        """, (client, f"{YEAR}-01-01", f"{YEAR + 1}-01-01", POINTS_PER_WEEK))
        got = cur.fetchall()
        # Require most of the year present, or the weekly series is too sparse
        # to summarise.
        if len({d.isocalendar()[1] for _, _, d in got}) < MIN_WEEKS:
            continue
        for lat, lon, dt in got:
            rows.append({"client_id": client, "latitude": float(lat),
                         "longitude": float(lon),
                         "timestamp": dt.strftime("%Y-%m-%dT%H:%M:%S")})
        if len({r["client_id"] for r in rows}) == N_PARTICIPANTS:
            break
    cur.close(); conn.close()

    points = pd.DataFrame(rows)
    path = out_dir / "case_study_points.csv"
    points.to_csv(path, index=False)
    return points


def inspect(values: pd.DataFrame, provenance: dict) -> dict:
    """Missingness and resolution, examined before any interpretation."""
    report = {"rows": len(values), "participants": int(values["client_id"].nunique())}
    for spec in VARIABLES:
        column = spec.split(":")[1]
        source_meta = provenance["sources"][spec]
        present = values[column].notna().sum()
        report[spec] = {
            "values_present": int(present),
            "values_missing": int(len(values) - present),
            "missing_pct": round(100 * (1 - present / len(values)), 2),
            "native_pixel_resolution_m": source_meta["native_pixel_resolution_m"],
            "storage_cell_edge_m": source_meta["storage_cell_edge_m"],
            "stored_temporal_granularity": source_meta["stored_temporal_granularity"],
            "distinct_values_per_participant_year": int(
                values.groupby("client_id")[column].nunique().max()),
        }
    return report


def weekly_summary(values: pd.DataFrame) -> pd.DataFrame:
    values = values.copy()
    values["week"] = pd.to_datetime(values["timestamp"]).dt.isocalendar().week
    grouped = (values.groupby(["client_id", "week"])
               .agg(ndvi_mean=("NDVI", "mean"), ndvi_n=("NDVI", "count"),
                    pm25_mean=("pm25", "mean"), pm25_n=("pm25", "count"),
                    points=("timestamp", "count"))
               .reset_index())
    for column in ("ndvi_mean", "pm25_mean"):
        grouped[column] = grouped[column].round(6)
    return grouped.sort_values(["client_id", "week"]).reset_index(drop=True)


def digest(frame: pd.DataFrame) -> str:
    return hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Passes
# ---------------------------------------------------------------------------

def record(out_dir: Path) -> None:
    gm = GeoMetrics()
    provenance = store_provenance(gm)
    points = sample_participant_points(out_dir)
    print(f"{len(points):,} points from {points['client_id'].nunique()} participants")

    values = gm.fetch(points, VARIABLES)
    checks = inspect(values, provenance)
    print(json.dumps(checks, indent=2))

    summary = weekly_summary(values)
    summary.to_csv(out_dir / "case_study_summary.csv", index=False)

    provenance.update({
        "participant_ids": sorted(int(c) for c in points["client_id"].unique()),
        "points_per_week_per_participant": POINTS_PER_WEEK,
        "min_weeks_required": MIN_WEEKS,
        "points_file": "case_study_points.csv",
        "points_sha256": hashlib.sha256(
            (out_dir / "case_study_points.csv").read_bytes()).hexdigest(),
        "inspection": checks,
        "summary_rows": len(summary),
        "summary_sha256": digest(summary),
    })
    (out_dir / "case_study_manifest.json").write_text(json.dumps(provenance, indent=2))
    print(f"\nrecorded manifest; summary {len(summary)} rows, "
          f"sha256 {provenance['summary_sha256'][:16]}")


def reproduce(out_dir: Path) -> None:
    """Rebuild the summary from the manifest alone and compare."""
    manifest = json.loads((out_dir / "case_study_manifest.json").read_text())
    print("reproducing from manifest:")
    print(f"  question: {manifest['question']}")
    print(f"  variables: {manifest['variables']}")
    print(f"  participants: {manifest['participant_ids']}")
    print(f"  code version recorded: {manifest['code_version']}")

    points = pd.read_csv(out_dir / manifest["points_file"])
    points_sha = hashlib.sha256(
        (out_dir / manifest["points_file"]).read_bytes()).hexdigest()
    print(f"  input points sha256 matches: {points_sha == manifest['points_sha256']}")

    gm = GeoMetrics()
    values = gm.fetch(points, manifest["variables"])
    summary = weekly_summary(values)
    sha = digest(summary)

    identical = sha == manifest["summary_sha256"]
    print(f"\nreproduced {len(summary)} rows, sha256 {sha[:16]}")
    print(f"recorded                  sha256 {manifest['summary_sha256'][:16]}")
    print(f"IDENTICAL: {identical}")

    result = {"identical": identical, "reproduced_sha256": sha,
              "recorded_sha256": manifest["summary_sha256"],
              "reproduced_rows": len(summary),
              "recorded_rows": manifest["summary_rows"],
              "input_points_sha256_match": points_sha == manifest["points_sha256"]}
    if not identical:
        original = pd.read_csv(out_dir / "case_study_summary.csv")
        merged = original.merge(summary, on=["client_id", "week"], suffixes=("_rec", "_rep"))
        for column in ("ndvi_mean", "pm25_mean"):
            diff = (merged[f"{column}_rec"] - merged[f"{column}_rep"]).abs()
            result[f"{column}_max_abs_diff"] = float(diff.max())
            result[f"{column}_rows_differing"] = int((diff > 0).sum())
        print(json.dumps({k: v for k, v in result.items() if "diff" in k}, indent=2))
    (out_dir / "case_study_reproduction.json").write_text(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pass_name", choices=["record", "reproduce"])
    parser.add_argument("--out", default="benchmarks/results")
    args = parser.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    (record if args.pass_name == "record" else reproduce)(out_dir)


if __name__ == "__main__":
    main()
