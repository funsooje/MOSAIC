"""
The naive arm: extract at every raw point, as a one-off per-dataset script does.

No store, no partitioning, no deduplication to grid cells — that is the practice
being compared, not a weakness introduced for the comparison. To keep the arms
fair, this reuses the adapter modules' own server-side extraction functions
(_process_feature) and the same submit_export call, so the only difference from
the MOSAIC arm is WHERE the points come from: raw coordinates here, cell
centroids there.

Two variants, because the fair baseline matters:
  A1  every raw point, including exact duplicate coordinates
  A2  distinct (coordinate, date) tuples — one line of pandas, which any
      competent script would write, and the variant the headline compares against

Usage:
    python benchmarks/baseline_naive.py --points <csv> --variant A2 --folder <drive folder>
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd

SOURCES = {
    # module, server-side feature processor, variables exported, date granularity
    "Landsat_NDVI": ("geometrics.extraction.ndvi", ["NDVI"], "year"),
    "MODIS_Treecover": ("geometrics.extraction.treecover",
                        ["percent_tree_cover", "percent_nontree_vegetation",
                         "percent_nonvegetated", "quality", "percent_tree_cover_sd",
                         "percent_nonvegetated_sd", "cloud"], "year"),
}


def date_range(timestamp: str, granularity: str) -> tuple[str, str]:
    """Same per-feature date window the dispatcher builds."""
    year = timestamp[:4]
    if granularity == "year":
        return f"{year}-01-01", f"{year}-12-31"
    raise ValueError(granularity)


def build_features(points: pd.DataFrame, granularity: str):
    """One ee.Feature per raw point, carrying its own date range."""
    import ee

    features = []
    for row in points.itertuples(index=False):
        start, end = date_range(str(row.timestamp), granularity)
        features.append(ee.Feature(
            ee.Geometry.Point([float(row.longitude), float(row.latitude)]),
            {"point_id": int(row.point_id),
             "start_date": start, "end_date": end,
             "timestamp": str(row.timestamp)},
        ))
    return ee.FeatureCollection(features)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--points", required=True)
    parser.add_argument("--variant", choices=["A1", "A2"], default="A2")
    parser.add_argument("--folder", required=True)
    parser.add_argument("--batch-size", type=int, default=5000,
                        help="identical to the MOSAIC arm, so task sizing is not the variable")
    parser.add_argument("--out", default="benchmarks/results")
    args = parser.parse_args()

    import ee
    from geometrics import GeoMetrics
    from geometrics.extraction.base import submit_export

    GeoMetrics().init_gee()

    points = pd.read_csv(args.points)
    points["point_id"] = range(len(points))
    raw_count = len(points)

    if args.variant == "A2":
        # The dedup key must match the extraction's temporal granularity, not the
        # raw timestamp. These sources extract over a yearly window, so two points
        # at the same coordinate in the same year yield an identical extraction;
        # deduping on the per-second timestamp removes almost nothing and would
        # make A2 a relabelled A1.
        points["_year"] = points["timestamp"].str.slice(0, 4)
        points = points.drop_duplicates(
            subset=["latitude", "longitude", "_year"]).drop(columns="_year").reset_index(drop=True)
    print(f"{args.variant}: {raw_count:,} raw points -> {len(points):,} submitted")

    record = {"variant": args.variant, "raw_points": raw_count,
              "submitted_points": len(points), "batch_size": args.batch_size,
              "folder": args.folder, "tasks": [], "submitted_at": time.strftime("%FT%T")}

    for source, (module_path, variables, granularity) in SOURCES.items():
        module = __import__(module_path, fromlist=["_process_feature"])
        processor = module._process_feature          # the adapter's own GEE code
        n_batches = (len(points) + args.batch_size - 1) // args.batch_size
        for batch_index in range(n_batches):
            chunk = points.iloc[batch_index * args.batch_size:
                                (batch_index + 1) * args.batch_size]
            collection = build_features(chunk, granularity).map(processor)
            prefix = f"naive_{args.variant}_{source}_batch_{batch_index + 1:03d}"
            t0 = time.time()
            task_id = submit_export(
                collection=collection,
                description=f"Naive {args.variant} {source} {batch_index + 1:03d}",
                folder=args.folder,
                file_prefix=prefix,
                properties=["point_id", "timestamp", "source"] + variables,
            )
            record["tasks"].append({
                "source": source, "task_id": task_id, "file_prefix": prefix,
                "features": len(chunk), "submit_seconds": round(time.time() - t0, 2),
            })
            print(f"  {prefix}: {len(chunk):,} features, task {task_id}", flush=True)

    record["total_features"] = sum(t["features"] for t in record["tasks"])
    record["total_tasks"] = len(record["tasks"])
    out = Path(args.out) / f"baseline_naive_{args.variant}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2))
    print(f"\n{record['total_tasks']} tasks, {record['total_features']:,} features "
          f"-> {out}")


if __name__ == "__main__":
    main()
