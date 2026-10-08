"""Wait for the SRTM task, ingest it, fetch the values back, and record RQ1."""
import json, time
from pathlib import Path
import pandas as pd
from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig, load_config

S = Path(__file__).parent
prod = load_config()
gm = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{S}/rq1_srtm.db",
                                 gdrive_base=prod.gdrive_base, backend="hiergp",
                                 gee_project=prod.gee_project))
t0 = time.time()
summary = {}
while time.time() - t0 < 3600:
    summary = gm.check_status() or {}
    if not ({"PENDING", "RUNNING"} & set(summary)):
        break
    time.sleep(30)
gee_seconds = time.time() - t0
print(f"GEE wall-clock: {gee_seconds:.1f}s, status {summary}")

t1 = time.time()
ingested = gm.ingest("mosaic-rq1-srtm")
ingest_seconds = time.time() - t1
print(f"ingest: {ingested} in {ingest_seconds:.2f}s")

locs = pd.read_csv("/Users/funsooje/Documents/GitHub/GeoMetrics/demo/demo_locations.csv").head(100)
t2 = time.time()
out = gm.fetch(locs, ["SRTM_Terrain:elevation", "SRTM_Terrain:slope"])
fetch_seconds = time.time() - t2
print(out[["synthetic_id", "elevation", "slope"]].head(6).to_string())
print(f"\nelevation {out['elevation'].min():.1f}-{out['elevation'].max():.1f} m, "
      f"slope {out['slope'].min():.1f}-{out['slope'].max():.1f} deg, "
      f"non-null {out['elevation'].notna().sum()}/{len(out)}")
res = gm.check(locs, ["SRTM_Terrain:elevation"])
print(f"re-check: available={len(res['available'])} missing={len(res['missing'])}")

json.dump({"gee_seconds": round(gee_seconds, 1), "ingest_seconds": round(ingest_seconds, 2),
           "fetch_seconds": round(fetch_seconds, 2), "rows_ingested": sum(ingested.values()),
           "elevation_range_m": [float(out["elevation"].min()), float(out["elevation"].max())],
           "slope_range_deg": [float(out["slope"].min()), float(out["slope"].max())],
           "non_null": int(out["elevation"].notna().sum()), "points": len(out),
           "recheck_available": len(res["available"]), "recheck_missing": len(res["missing"])},
          open(S / "rq1_result.json", "w"), indent=2)
