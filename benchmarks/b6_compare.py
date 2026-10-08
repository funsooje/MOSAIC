"""B6 — HierGP vs H3 on the same workload.

Waits for the H3 exports, ingests them, then asks both stores for the same
2,500 points and compares what each returns. The two grids place a point in
differently shaped cells, so values are not expected to be identical; the
question is whether a researcher's answer changes materially.
"""
import json, os, time
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import text
from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig, load_config

S = Path(__file__).parent
PAIRS = [("Landsat_NDVI", "NDVI"), ("MODIS_Treecover", "percent_tree_cover")]
prod_cfg = load_config()

h3 = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{S}/b6_h3.db",
                                 gdrive_base=prod_cfg.gdrive_base, backend="h3",
                                 gee_project=prod_cfg.gee_project))
hiergp = GeoMetrics(prod_cfg)
points = pd.read_csv(S / "b6_points.csv")

# wait for both tasks
t0 = time.time()
while time.time() - t0 < 7200:
    summary = h3.check_status() or {}
    if not ({"PENDING", "RUNNING"} & set(summary)):
        break
    time.sleep(60)
print(f"H3 tasks terminal after {(time.time()-t0)/60:.1f} min: {summary}", flush=True)

t1 = time.time()
ingested = h3.ingest("mosaic-b6-h3")
h3_ingest_s = time.time() - t1
print("ingested:", ingested, f"in {h3_ingest_s:.2f}s", flush=True)

result = {"points": len(points), "h3_ingest_seconds": round(h3_ingest_s, 2),
          "h3_rows_ingested": sum(ingested.values()), "sources": {}}

for source, variable in PAIRS:
    spec = f"{source}:{variable}"
    t = time.time(); h3_vals = h3.fetch(points, [spec]); h3_fetch = time.time() - t
    t = time.time(); hg_vals = hiergp.fetch(points, [spec]); hg_fetch = time.time() - t

    col = variable
    merged = pd.DataFrame({
        "h3": h3_vals[col] if col in h3_vals else np.nan,
        "hiergp": hg_vals[col] if col in hg_vals else np.nan,
    })
    both = merged.dropna()
    diff = (both["h3"] - both["hiergp"]).abs()
    denom = both["hiergp"].abs().replace(0, np.nan)
    rel = (diff / denom).dropna()

    with h3.engine.connect() as c:
        h3_cells = c.execute(text("SELECT count(*) FROM cells")).scalar()
        h3_rows = c.execute(text(f"SELECT count(*) FROM obs_{source.lower()}")).scalar()
        h3_level = c.execute(text("SELECT native_level FROM sources WHERE name=:n"),
                             {"n": source}).scalar()
    with hiergp.engine.connect() as c:
        hg_level = c.execute(text("SELECT native_level FROM sources WHERE name=:n"),
                             {"n": source}).scalar()

    result["sources"][spec] = {
        "h3_resolution": h3_level, "hiergp_level": hg_level,
        "h3_rows": h3_rows, "h3_distinct_cells_in_store": h3_cells,
        "points_with_both": len(both),
        "h3_null": int(merged["h3"].isna().sum()),
        "hiergp_null": int(merged["hiergp"].isna().sum()),
        "identical": int((diff == 0).sum()),
        "within_1pct": round(100 * float((rel <= 0.01).mean()), 2) if len(rel) else None,
        "within_5pct": round(100 * float((rel <= 0.05).mean()), 2) if len(rel) else None,
        "mean_abs_diff": round(float(diff.mean()), 6) if len(both) else None,
        "median_abs_diff": round(float(diff.median()), 6) if len(both) else None,
        "max_abs_diff": round(float(diff.max()), 6) if len(both) else None,
        "correlation": round(float(both["h3"].corr(both["hiergp"])), 6) if len(both) > 2 else None,
        "h3_fetch_seconds": round(h3_fetch, 2), "hiergp_fetch_seconds": round(hg_fetch, 2),
    }
    print(f"{spec}: H3 res {h3_level} vs HierGP level {hg_level} | "
          f"median diff {result['sources'][spec]['median_abs_diff']} | "
          f"within 5% {result['sources'][spec]['within_5pct']}% | "
          f"corr {result['sources'][spec]['correlation']}", flush=True)

result["h3_store_bytes"] = os.path.getsize(S / "b6_h3.db")
json.dump(result, open(S / "b6_result.json", "w"), indent=2)
print(json.dumps(result, indent=2))
