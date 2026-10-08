"""
Build the demo store shipped with the paper.

Produces demo/geometrics_demo.db, a small SQLite GeoMetrics store holding real
environmental values for synthetic locations, so a reviewer can exercise
gm.check / gm.fetch, the viewer and the portability path with NO Earth Engine
account and no cohort data.

Contents:
  Landsat_NDVI        level 13 (100 m)   fine raster, from GEE
  YALE_UHI            level 10 (800 m)   coarse raster, from GEE
  Demo_Parks          level 13           local source, distance to synthetic parks

The GEE half needs the exports produced by the extraction submitted for the
demo folder; the local half is computed here and needs no credentials.

Usage:
    python scripts/build_demo_store.py --folder mosaic-demo-run1
"""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box

from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig, load_config

REPO = Path(__file__).resolve().parent.parent
DEMO_DB = REPO / "demo" / "geometrics_demo.db"
LOCATIONS = REPO / "demo" / "demo_locations.csv"
PARKS_SEED = 20261007

# Synthetic "parks": fixed boxes inside the Pullman extent. Not real PADUS
# geometry — the point is to exercise the local-source path reproducibly.
PARK_BOXES = [
    (46.7200, -117.1850, 46.7240, -117.1790),
    (46.7300, -117.1600, 46.7340, -117.1540),
    (46.7380, -117.1450, 46.7410, -117.1400),
    (46.7250, -117.1350, 46.7290, -117.1300),
]


def synthetic_parks() -> gpd.GeoDataFrame:
    geoms = [box(lon0, lat0, lon1, lat1) for lat0, lon0, lat1, lon1 in PARK_BOXES]
    return gpd.GeoDataFrame({"park_id": range(len(geoms))}, geometry=geoms, crs="EPSG:4326")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--folder", default="mosaic-demo-run1",
                        help="Drive folder holding the demo GEE exports")
    parser.add_argument("--skip-gee-ingest", action="store_true",
                        help="Only recompute the local source")
    args = parser.parse_args()

    prod = load_config()
    gm = GeoMetrics(GeoMetricsConfig(
        db_url=f"sqlite:///{DEMO_DB}",
        gdrive_base=prod.gdrive_base,
        backend="hiergp",
    ))

    if not args.skip_gee_ingest:
        print(f"Ingesting demo exports from {args.folder} …")
        result = gm.ingest(args.folder)
        for name, rows in result.items():
            print(f"  {name}: {rows:,} rows")

    print("\nComputing the local source (distance to synthetic parks) …")
    locations = pd.read_csv(LOCATIONS)
    gm.register_local_source("Demo_Parks", operation="nearest_distance")
    inserted = gm.compute_local(
        locations, source="Demo_Parks",
        reference_data=synthetic_parks(), operation="nearest_distance",
    )
    print(f"  {inserted:,} rows")

    print("\nVerifying the store answers queries …")
    sample = locations.head(5)
    out = gm.fetch(sample, ["Landsat_NDVI:NDVI", "YALE_UHI:yearly_daytime",
                            "Demo_Parks:nearest_distance"])
    print(out.to_string())
    print(f"\nDemo store ready: {DEMO_DB} "
          f"({DEMO_DB.stat().st_size / 1024:.0f} KiB)")


if __name__ == "__main__":
    main()
