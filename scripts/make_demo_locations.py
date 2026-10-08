"""
Generate the synthetic location trace that backs the shipped demo store.

No cohort data is involved. The trace is a random walk between a handful of
fixed anchors inside Pullman, WA city limits, which keeps the points plausible
for a mobility dataset without resembling anybody's real movements. The seed is
fixed so the file regenerates identically.

Usage:
    python scripts/make_demo_locations.py [--out demo/demo_locations.csv]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 20261007
N_POINTS = 1500
YEARS = [2019, 2020, 2021, 2022]

# Fixed anchors inside Pullman, WA — a campus, two residential areas, a park,
# a shopping area. Public, non-sensitive, and not the cohort's area.
ANCHORS = [
    ("campus",      46.7319, -117.1542),
    ("residential", 46.7265, -117.1680),
    ("residential", 46.7380, -117.1430),
    ("park",        46.7230, -117.1790),
    ("retail",      46.7290, -117.1350),
]

# Pullman city limits, roughly
LAT_RANGE = (46.715, 46.745)
LON_RANGE = (-117.200, -117.120)


def build(n_points: int = N_POINTS) -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    rows = []
    per_anchor = n_points // len(ANCHORS)

    for label, alat, alon in ANCHORS:
        # Random walk away from the anchor: small steps, occasionally resetting,
        # so points cluster near the anchor with a plausible tail of excursions.
        lat, lon = alat, alon
        for i in range(per_anchor):
            if i % 50 == 0:                      # back home
                lat, lon = alat, alon
            lat += rng.normal(0, 0.0009)         # ~100 m steps
            lon += rng.normal(0, 0.0013)
            lat = float(np.clip(lat, *LAT_RANGE))
            lon = float(np.clip(lon, *LON_RANGE))
            year = YEARS[rng.integers(len(YEARS))]
            month = int(rng.integers(1, 13))
            day = int(rng.integers(1, 29))
            hour = int(rng.integers(0, 24))
            rows.append({
                "synthetic_id": f"{label[:4]}-{len(rows):04d}",
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "timestamp": f"{year}-{month:02d}-{day:02d}T{hour:02d}:00:00",
                "anchor": label,
            })

    data = pd.DataFrame(rows)
    return data.sort_values("timestamp").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="demo/demo_locations.csv")
    parser.add_argument("--n", type=int, default=N_POINTS)
    args = parser.parse_args()

    data = build(args.n)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    data.to_csv(out, index=False)

    print(f"Wrote {len(data):,} synthetic locations to {out}")
    print(f"  lat {data['latitude'].min():.4f}–{data['latitude'].max():.4f}, "
          f"lon {data['longitude'].min():.4f}–{data['longitude'].max():.4f}")
    print(f"  {data['timestamp'].min()} … {data['timestamp'].max()}")
    print(data["anchor"].value_counts().to_string())


if __name__ == "__main__":
    main()
