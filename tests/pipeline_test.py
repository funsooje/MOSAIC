"""
Full end-to-end pipeline test: check → GEE submit → wait → ingest → re-check → fetch.

Requires:
  - GEE authenticated (ee.Initialize called at top of this script)

Uses an isolated SQLite database under tests/tmp/ so it never touches the
live GeoMetrics PostgreSQL store.

Usage:
    python tests/pipeline_test.py [--sources ERA5_Land,Landsat_NDVI] [--folder test-run-1]

  --sources  Comma-separated list of sources to test. Default: all registered sources.
  --folder   Drive folder name for this run. Default: test-pipeline-<timestamp>.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import ee

from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig
from geometrics.store.schema import metadata

LOCATIONS_CSV = "tests/sample_locations.csv"
LAT_COL = "latitude"
LON_COL = "longitude"
TIMESTAMP_COL = "timestamp"

POLL_INTERVAL_S = 15
MAX_WAIT_S = 600  # 10 minutes

# Variables to exercise per source
SOURCE_VARIABLES: dict[str, list[str]] = {
    "ERA5_Land": [
        "ERA5_Land:temperature_2m",
        "ERA5_Land:total_precipitation",
    ],
    "Landsat_NDVI": ["Landsat_NDVI:NDVI"],
    "MODIS_NDVI": ["MODIS_NDVI:NDVI"],
    "JRC_Water": ["JRC_Water:water_distance"],
    "YALE_UHI": [
        "YALE_UHI:yearly_daytime",
        "YALE_UHI:yearly_nighttime",
        "YALE_UHI:winter_daytime",
        "YALE_UHI:winter_nighttime",
        "YALE_UHI:summer_daytime",
        "YALE_UHI:summer_nighttime",
    ],
    "NLCD": [
        "NLCD:landcover",
        "NLCD:impervious",
        "NLCD:impervious_descriptor",
    ],
    "MODIS_Treecover": [
        "MODIS_Treecover:percent_tree_cover",
        "MODIS_Treecover:percent_nontree_vegetation",
        "MODIS_Treecover:percent_nonvegetated",
        "MODIS_Treecover:quality",
        "MODIS_Treecover:percent_tree_cover_sd",
        "MODIS_Treecover:percent_nonvegetated_sd",
        "MODIS_Treecover:cloud",
    ],
}


def _banner(text: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}")


def _step(num: int, total: int, text: str) -> None:
    print(f"\n[{num}/{total}] {text}")


def run(sources: list[str], folder: str) -> None:
    variables = []
    for src in sources:
        if src not in SOURCE_VARIABLES:
            print(f"[WARN] No test variables defined for source {src!r} — skipping.")
            continue
        variables.extend(SOURCE_VARIABLES[src])

    if not variables:
        print("[FAIL] No variables to test.")
        sys.exit(1)

    tmp_dir = Path("tests/tmp")
    tmp_dir.mkdir(exist_ok=True)
    db_path = tmp_dir / f"test_{folder}.db"
    test_config = GeoMetricsConfig(
        db_url=f"sqlite:///{db_path}",
        gdrive_base=GeoMetrics().config.gdrive_base,
    )

    _banner(f"Pipeline test: {', '.join(sources)}")
    print(f"  Locations : {LOCATIONS_CSV}")
    print(f"  Variables : {variables}")
    print(f"  GDrive    : {folder}")
    print(f"  Test DB   : {db_path}")

    gm = GeoMetrics(config=test_config)

    # ------------------------------------------------------------------ #
    # 0. Init fresh test DB                                                #
    # ------------------------------------------------------------------ #
    _step(0, 5, "Initialising fresh test database...")
    metadata.create_all(gm.engine)
    gm.init_db()

    # ------------------------------------------------------------------ #
    # 1. Check — expect everything missing                                 #
    # ------------------------------------------------------------------ #
    _step(1, 5, "Checking availability (expect: all missing)...")
    res = gm.check(
        LOCATIONS_CSV, variables=variables,
        lat_col=LAT_COL, lon_col=LON_COL, timestamp_col=TIMESTAMP_COL,
    )
    print(f"  available={len(res['available'])}, missing={len(res['missing'])}")
    if res["available"]:
        print(f"[FAIL] Expected 0 available after reset, got {len(res['available'])}.")
        sys.exit(1)
    if not res["missing"]:
        print("[FAIL] Nothing is missing — nothing to submit.")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 2. Submit to GEE                                                     #
    # ------------------------------------------------------------------ #
    _step(2, 5, f"Submitting {len(res['missing'])} item(s) to GEE...")
    job_ids = gm.gee_submit(res["missing"], gdrive_folder=folder)
    if not job_ids:
        print("[FAIL] No jobs were submitted.")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 3. Wait for GEE completion                                           #
    # ------------------------------------------------------------------ #
    _step(3, 5, f"Waiting for {len(job_ids)} job(s) to complete (max {MAX_WAIT_S}s)...")
    elapsed = 0
    while elapsed < MAX_WAIT_S:
        status = gm.check_status()
        pending = status.get("PENDING", 0) + status.get("RUNNING", 0)
        if pending == 0:
            print(f"  Done: {status}")
            break
        print(f"  {pending} job(s) still active... ({elapsed}s elapsed)")
        time.sleep(POLL_INTERVAL_S)
        elapsed += POLL_INTERVAL_S
    else:
        print(f"[FAIL] GEE jobs did not complete within {MAX_WAIT_S}s.")
        sys.exit(1)

    failed = gm.jobs(status="FAILED")
    if not failed.empty:
        print(f"[FAIL] {len(failed)} job(s) failed:\n{failed}")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 4. Ingest                                                            #
    # ------------------------------------------------------------------ #
    _step(4, 5, f"Ingesting from Drive folder '{folder}'...")
    results = gm.ingest(folder)
    total_rows = sum(results.values())
    print(f"  Ingested {total_rows} row(s) from {len(results)} file(s).")
    if total_rows == 0:
        print("[FAIL] No rows were ingested.")
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 5. Re-check and fetch                                                #
    # ------------------------------------------------------------------ #
    _step(5, 5, "Re-checking and fetching results...")
    res2 = gm.check(
        LOCATIONS_CSV, variables=variables,
        lat_col=LAT_COL, lon_col=LON_COL, timestamp_col=TIMESTAMP_COL,
    )
    print(f"  available={len(res2['available'])}, missing={len(res2['missing'])}")

    df_wide = gm.fetch(
        LOCATIONS_CSV, variables=variables,
        lat_col=LAT_COL, lon_col=LON_COL, timestamp_col=TIMESTAMP_COL,
        output_format="wide", preserve_cols=True,
    )
    print(f"  Wide  fetch: {df_wide.shape[0]} rows x {df_wide.shape[1]} cols")
    print(f"  Columns    : {list(df_wide.columns)}")

    df_long = gm.fetch(
        LOCATIONS_CSV, variables=variables,
        lat_col=LAT_COL, lon_col=LON_COL, timestamp_col=TIMESTAMP_COL,
        output_format="long", preserve_cols=True,
    )
    print(f"  Long  fetch: {df_long.shape[0]} rows x {df_long.shape[1]} cols")

    # Validate: at least one non-null value in each requested variable column
    var_names = [v.split(":")[1] for v in variables]
    failures = []
    for vname in var_names:
        if vname not in df_wide.columns:
            failures.append(f"  Column {vname!r} missing from wide output.")
        elif df_wide[vname].notna().sum() == 0:
            failures.append(f"  Column {vname!r} is entirely NaN.")

    if failures:
        print("\n[FAIL] Validation errors:")
        for msg in failures:
            print(msg)
        sys.exit(1)

    _banner("PASS — all steps completed successfully.")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="GeoMetrics full pipeline test")
    parser.add_argument(
        "--sources",
        default=",".join(SOURCE_VARIABLES),
        help="Comma-separated list of sources to test.",
    )
    parser.add_argument(
        "--folder",
        default=f"test-pipeline-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        help="Google Drive folder name for this test run.",
    )
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    ee.Initialize(project="ee-funsooje")
    run(sources, args.folder)


if __name__ == "__main__":
    main()
