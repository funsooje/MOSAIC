"""Submit B4 fidelity re-extractions and the demo extraction in one GEE batch.

Both write into their own scratch stores, so the 13 GB deployment store is
untouched:
  b4_compare.db  — re-extracted values, compared against the store afterwards
  demo store     — demo/geometrics_demo.db, the artifact shipped with the paper
"""
import json, time
from pathlib import Path
import pandas as pd
from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig, load_config
from geometrics.extraction.dispatch import dispatch

S = Path(__file__).parent
REPO = Path("/Users/funsooje/Documents/GitHub/GeoMetrics")
TIMINGS = S / "b4_timings.jsonl"
prod = load_config()


def record(stage, seconds, **extra):
    row = {"stage": stage, "seconds": round(seconds, 3), "at": time.strftime("%FT%T"), **extra}
    with TIMINGS.open("a") as fh:
        fh.write(json.dumps(row) + "\n")
    print(f"[timing] {stage}: {seconds:,.1f}s {extra}", flush=True)


def fresh_store(path: Path) -> GeoMetrics:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    gm = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{path}",
                                     gdrive_base=prod.gdrive_base,
                                     backend=prod.backend,
                                     gee_project=prod.gee_project))
    gm.init_db()
    return gm


# ---------------------------------------------------------------- B4
sample = pd.read_csv(S / "b4_sample.csv")
print(f"B4 sample: {len(sample)} rows, {sample['source'].nunique()} sources")

gm_b4 = fresh_store(S / "b4_compare.db")
gm_b4.init_gee()

items = [
    {"source": r.source,
     "cell_id": r.cell_id,
     "timestamp": str(r.timestamp),
     "temporal_granularity": r.temporal_granularity,
     "requested_level": int(r.cell_id.split(":")[0])}
    for r in sample.itertuples()
]

t0 = time.time()
b4_jobs = dispatch(gm_b4.engine, gm_b4.config, gm_b4.backend, items,
                   gdrive_folder="mosaic-b4-run1", batch_size=5000)
record("b4_submit", time.time() - t0, jobs=len(b4_jobs), items=len(items))

# ---------------------------------------------------------------- demo
demo_locs = pd.read_csv(REPO / "demo/demo_locations.csv")
DEMO_SOURCES = ["Landsat_NDVI:NDVI",            # fine raster, level 13 (100 m)
                "YALE_UHI:yearly_daytime"]      # coarse raster, level 10 (800 m)
print(f"\ndemo: {len(demo_locs)} synthetic locations, sources {DEMO_SOURCES}")

gm_demo = fresh_store(REPO / "demo/geometrics_demo.db")
gm_demo.init_gee()

t1 = time.time()
res = gm_demo.check(demo_locs, DEMO_SOURCES)
record("demo_check", time.time() - t1,
       available=len(res["available"]), missing=len(res["missing"]))

t2 = time.time()
demo_jobs = gm_demo.gee_submit(res["missing"], gdrive_folder="mosaic-demo-run1",
                               batch_size=5000)
record("demo_submit", time.time() - t2, jobs=len(demo_jobs), items=len(res["missing"]))

json.dump({"b4_jobs": b4_jobs, "b4_folder": "mosaic-b4-run1",
           "demo_jobs": demo_jobs, "demo_folder": "mosaic-demo-run1",
           "submitted_at": time.strftime("%FT%T")},
          open(S / "b4_demo_jobs.json", "w"), indent=2)

print("\nB4 jobs:")
print(gm_b4.jobs()[["job_id", "file_prefix", "level", "status", "row_count"]].to_string())
print("\nDemo jobs:")
print(gm_demo.jobs()[["job_id", "file_prefix", "level", "status", "row_count"]].to_string())
