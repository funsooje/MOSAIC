"""
B4 — compare freshly re-extracted values against what the store holds.

The store's values came from the GTL migration; these come straight from GEE
through the current pipeline. Any disagreement is either a pipeline fault or a
difference between what GTL computed years ago and what GEE returns today.

Two input modes:

  --exports DIR   read the re-extracted values straight from the exported CSVs
                  (the reproducible path: no scratch database required)
  --ingest        ingest the Drive folders into a scratch store first, then read
                  them back, which is how the original run worked

The sample file is the record of which partitions were drawn; it is required in
both modes. See the note on seeding in b4_sample.py.

Usage:
    python benchmarks/b4_compare.py --exports /path/to/exported/csvs
    python benchmarks/b4_compare.py --ingest
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
RESULTS = HERE / "results"
DRIVE_FOLDERS = ("mosaic-b4-run1", "mosaic-b4-jrc")


def fresh_from_exports(exports: Path, source: str, variable: str) -> pd.DataFrame:
    """Re-extracted values straight from the exported CSVs."""
    files = sorted(exports.glob(f"{source}_batch_*.csv"))
    if not files:
        return pd.DataFrame()
    frame = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    return frame[["cell_id", "timestamp", variable]].rename(
        columns={variable: "fresh_value"})


def fresh_from_store(gm, source: str, variable: str) -> pd.DataFrame:
    """Re-extracted values read back out of a scratch store."""
    from sqlalchemy import text

    table = "obs_" + source.lower()
    with gm.engine.connect() as conn:
        return pd.DataFrame(conn.execute(text(f"""
            SELECT c.cell_id, u.timestamp, o.{variable} AS fresh_value
            FROM {table} o
            JOIN spatiotemporal_units u ON u.id = o.unit_pk
            JOIN cells c ON c.id = u.cell_pk
        """)).mappings().all())


def compare(sample: pd.DataFrame, fetch) -> list[dict]:
    rows = []
    for source, group in sample.groupby("source"):
        variable = group["variable"].iloc[0]
        fresh = fetch(source, variable)
        if fresh.empty:
            rows.append({"source": source, "variable": variable, "compared": 0,
                         "note": "no re-extracted rows"})
            continue

        fresh["timestamp"] = fresh["timestamp"].astype(str).str.slice(0, 19)
        group = group.copy()
        group["timestamp"] = group["timestamp"].astype(str).str.slice(0, 19)
        merged = group.merge(fresh, on=["cell_id", "timestamp"], how="inner")

        both = merged.dropna(subset=["stored_value", "fresh_value"])
        diff = (both["fresh_value"] - both["stored_value"]).abs()
        # Relative difference is undefined where the stored value is 0. Dropping
        # those rows from the denominator made "within 1%" computable over a
        # smaller set than "exact", which produced the impossible result of a
        # within-tolerance rate BELOW the exact-match rate. An exact pair is
        # within any tolerance, including 0 == 0, so count it as such and keep
        # one denominator for both columns.
        rel = diff / both["stored_value"].abs().replace(0, np.nan)
        within_1pct = (diff == 0) | (rel <= 0.01)
        signed = both["fresh_value"] - both["stored_value"]

        rows.append({
            "source": source, "variable": variable,
            "sampled": len(group), "matched_cells": len(merged), "compared": len(both),
            "fresh_null": int(merged["fresh_value"].isna().sum()),
            "stored_null": int(merged["stored_value"].isna().sum()),
            "exact_match": int((diff == 0).sum()),
            "exact_match_pct": round(100 * (diff == 0).mean(), 2) if len(both) else None,
            "within_1pct": round(100 * within_1pct.mean(), 2) if len(both) else None,
            "mean_abs_diff": round(float(diff.mean()), 6) if len(both) else None,
            "median_abs_diff": round(float(diff.median()), 6) if len(both) else None,
            "max_abs_diff": round(float(diff.max()), 6) if len(both) else None,
            "mean_signed_diff": round(float(signed.mean()), 6) if len(both) else None,
            "std_signed_diff": round(float(signed.std()), 6) if len(both) else None,
            "correlation": round(float(both["fresh_value"].corr(both["stored_value"])), 6)
                            if len(both) > 2 else None,
            "stored_range": [round(float(both["stored_value"].min()), 4),
                             round(float(both["stored_value"].max()), 4)] if len(both) else None,
            "fresh_range": [round(float(both["fresh_value"].min()), 4),
                            round(float(both["fresh_value"].max()), 4)] if len(both) else None,
        })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exports", help="directory holding the exported CSVs")
    parser.add_argument("--ingest", action="store_true",
                        help="ingest the Drive folders into a scratch store first")
    parser.add_argument("--sample", default=str(RESULTS / "b4_sample.csv"))
    args = parser.parse_args()
    if not args.exports and not args.ingest:
        parser.error("choose --exports DIR or --ingest")

    sample = pd.read_csv(args.sample)

    if args.exports:
        exports = Path(args.exports)
        fetch = lambda s, v: fresh_from_exports(exports, s, v)  # noqa: E731
    else:
        from geometrics import GeoMetrics
        from geometrics.config import GeoMetricsConfig, load_config
        prod = load_config()
        gm = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{HERE}/b4_compare.db",
                                         gdrive_base=prod.gdrive_base, backend="hiergp"))
        for folder in DRIVE_FOLDERS:
            print(f"ingesting {folder} …")
            print(" ", gm.ingest(folder))
        fetch = lambda s, v: fresh_from_store(gm, s, v)  # noqa: E731

    rows = compare(sample, fetch)
    report = pd.DataFrame(rows)
    report.to_csv(RESULTS / "b4_fidelity.csv", index=False)
    (RESULTS / "b4_fidelity.json").write_text(json.dumps(rows, indent=2, default=str))
    pd.set_option("display.width", 200)
    print("\n" + report.to_string(index=False))


if __name__ == "__main__":
    main()
