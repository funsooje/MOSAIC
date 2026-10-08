"""B6 — portability: the same workload under H3 instead of HierGP.

Reduced as agreed for the shortened deadline: 2,500 of B2's 10,000 points
(every fourth), two sources. Levels come from backends/levels.py, which maps
each catalog HierGP level to the H3 resolution closest in cell area.
"""
import json, time
from pathlib import Path
import numpy as np
import pandas as pd
from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig, load_config

S = Path(__file__).parent
SOURCES = ["Landsat_NDVI:NDVI", "MODIS_Treecover:percent_tree_cover"]
prod = load_config()

db = S / "b6_h3.db"; db.unlink(missing_ok=True)
gm = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{db}", gdrive_base=prod.gdrive_base,
                                 backend="h3", gee_project=prod.gee_project))
gm.init_db()
print("backend:", type(gm.backend).__name__)

with gm.engine.connect() as conn:
    from sqlalchemy import text
    rows = conn.execute(text(
        "SELECT name, native_level FROM sources WHERE name IN "
        "('Landsat_NDVI','MODIS_Treecover','ERA5_Land','YALE_UHI')")).fetchall()
print("registered H3 levels:", {r.name: r.native_level for r in rows})

# Same geometry as B2, every fourth point
lats = 46.80 + 0.0009 * np.arange(100)
lons = -118.50 + 0.0013 * np.arange(100)
points = pd.DataFrame({"latitude": [round(a, 6) for a in lats for _ in lons],
                       "longitude": [round(o, 6) for _ in lats for o in lons],
                       "timestamp": "2021-06-15"}).iloc[::4].reset_index(drop=True)
points.to_csv(S / "b6_points.csv", index=False)
print(f"workload: {len(points):,} points x {len(SOURCES)} sources")

t0 = time.time()
res = gm.check(points, SOURCES)
check_s = time.time() - t0
print(f"check: {check_s:.2f}s, missing={len(res['missing'])}")
sample_cells = sorted({i["cell_id"] for i in res["missing"]})[:3]
print("example H3 cell ids:", sample_cells)

t1 = time.time()
jobs = gm.gee_submit(res["missing"], gdrive_folder="mosaic-b6-h3", batch_size=5000)
submit_s = time.time() - t1
print("jobs:", jobs)

json.dump({"points": len(points), "sources": SOURCES,
           "h3_levels": {r.name: r.native_level for r in rows},
           "check_seconds": round(check_s, 2), "submit_seconds": round(submit_s, 2),
           "unique_cells_h3": len({i["cell_id"] for i in res["missing"]}),
           "jobs": jobs, "folder": "mosaic-b6-h3"},
          open(S / "b6_submit.json", "w"), indent=2)
print(gm.jobs()[["job_id", "file_prefix", "level", "status", "row_count"]].to_string())
