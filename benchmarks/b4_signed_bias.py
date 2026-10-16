"""
B4 supplement — signed difference statistics per source.

Median absolute difference discards the sign, which hides the distinction the
fidelity argument rests on: a systematic offset (every difference pointing the
same way, so the signed mean is close to the absolute mean) versus symmetric
noise (differences cancelling, so the signed mean is far smaller than the
absolute mean).

Sign convention: re-extracted minus stored. Negative means the current pipeline
returns a lower value than the store holds.

Recomputed from the sampled units and the re-extracted exports already on disk;
no new platform extraction.

Usage:
    python benchmarks/b4_signed_bias.py --sample <b4_sample.csv> --exports <dir>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

SOURCES = {
    "NLCD": "impervious",
    "ERA5_Land": "temperature_2m",
    "JRC_Water": "water_distance",
    "MODIS_NDVI": "NDVI",
    "YALE_UHI": "yearly_daytime",
    "MODIS_Treecover": "percent_tree_cover",
    "Landsat_NDVI": "NDVI",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", required=True)
    parser.add_argument("--exports", required=True)
    parser.add_argument("--out", default="benchmarks/results/b4_signed_bias.csv")
    args = parser.parse_args()

    sample = pd.read_csv(args.sample)
    sample["timestamp"] = sample["timestamp"].astype(str).str.slice(0, 19)
    exports = Path(args.exports)

    rows = []
    for source, variable in SOURCES.items():
        files = sorted(exports.glob(f"{source}_batch_*.csv"))
        if not files:
            print(f"{source}: no export found, skipped")
            continue
        fresh = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        fresh["timestamp"] = fresh["timestamp"].astype(str).str.slice(0, 19)
        fresh = fresh[["cell_id", "timestamp", variable]].rename(
            columns={variable: "fresh"})

        stored = sample[sample["source"] == source][
            ["cell_id", "timestamp", "stored_value"]]
        merged = stored.merge(fresh, on=["cell_id", "timestamp"], how="inner").dropna()

        signed = merged["fresh"] - merged["stored_value"]
        absolute = signed.abs()
        rows.append({
            "source": source, "variable": variable, "compared": len(merged),
            "mean_signed_diff": round(float(signed.mean()), 6),
            "std_signed_diff": round(float(signed.std()), 6),
            "median_abs_diff": round(float(absolute.median()), 6),
            "mean_abs_diff": round(float(absolute.mean()), 6),
            "max_abs_diff": round(float(absolute.max()), 6),
            # |signed mean| / mean |diff|: near 1 means every difference points the
            # same way; near 0 means they cancel.
            "systematic_fraction": round(
                float(abs(signed.mean()) / absolute.mean()), 4) if absolute.mean() else None,
            "negative_share_pct": round(float((signed < 0).mean() * 100), 1),
        })
        print(f"  {source:17s} signed {rows[-1]['mean_signed_diff']:+.6f} "
              f"+/- {rows[-1]['std_signed_diff']:.6f}  "
              f"max|d| {rows[-1]['max_abs_diff']:.4f}  "
              f"systematic {rows[-1]['systematic_fraction']}", flush=True)

    frame = pd.DataFrame(rows)
    out = Path(args.out)
    frame.to_csv(out, index=False)
    out.with_suffix(".json").write_text(json.dumps(rows, indent=2))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
