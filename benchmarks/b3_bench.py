"""B3 — query latency on the frozen store.

Grid: participants {1,10,100,all} x variables {1,5,all} x range {month,year,full}.
Each cell: 1 cold run (PostgreSQL restarted, shared_buffers emptied) then 5 warm
runs. Reports median and p95 per cell.

"Cold" means the database's own buffers are cold. The Docker VM page cache is
not dropped, so these are warm-OS numbers and should be described that way.
"""
import json, subprocess, time
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import text

from geometrics import GeoMetrics

S = Path(__file__).parent
OUT_CSV = S / "b3_results.csv"
OUT_JSONL = S / "b3_runs.jsonl"
WARM_REPS = 5

VARSETS = {
    "1": ["Landsat_NDVI:NDVI"],
    "5": ["Landsat_NDVI:NDVI", "MODIS_NDVI:NDVI",
          "MODIS_Treecover:percent_tree_cover", "NLCD:impervious",
          "YALE_UHI:yearly_daytime"],
}

REAL_SOURCES = ["Landsat_NDVI", "MODIS_NDVI", "MODIS_Treecover", "ERA5_Land",
                "JRC_Water", "YALE_UHI", "NLCD", "Parks_Distance",
                "Walkability", "CACES_Air"]


def all_variables(gm) -> list[str]:
    with gm.engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.name AS source, v.name AS variable
            FROM sources s JOIN variables v ON v.source_id = s.source_id
            WHERE s.name = ANY(:names)
            ORDER BY s.name, v.name
        """), {"names": REAL_SOURCES}).fetchall()
    return [f"{r.source}:{r.variable}" for r in rows]


def restart_db() -> float:
    """Restart PostgreSQL so its buffers are empty. Returns seconds to ready."""
    t0 = time.time()
    subprocess.run(["/usr/local/bin/docker", "restart", "geometrics"],
                   capture_output=True, check=True)
    while time.time() - t0 < 180:
        probe = subprocess.run(["/opt/homebrew/bin/pg_isready", "-h", "localhost",
                                "-p", "5434", "-q"], capture_output=True)
        if probe.returncode == 0:
            break
        time.sleep(2)
    return time.time() - t0


def run_cell(gm, name, points, variables, label):
    """One cold run plus WARM_REPS warm runs of gm.fetch."""
    restart_s = restart_db()
    gm.engine.dispose()                      # force fresh connections

    timings = []
    t0 = time.time()
    out = gm.fetch(points, variables)
    cold = time.time() - t0
    rows_returned = len(out)
    value_cols = [c for c in out.columns
                  if c not in ("latitude", "longitude", "timestamp", "client_id")]
    non_null = int(out[value_cols].notna().sum().sum()) if value_cols else 0

    for _ in range(WARM_REPS):
        t = time.time()
        gm.fetch(points, variables)
        timings.append(time.time() - t)

    record = {
        "workload": name, "varset": label,
        "points": len(points), "variables": len(variables),
        "items": len(points) * len(variables),
        "rows_returned": rows_returned, "non_null_values": non_null,
        "restart_seconds": round(restart_s, 2),
        "cold_seconds": round(cold, 3),
        "warm_median_seconds": round(float(np.median(timings)), 3),
        "warm_p95_seconds": round(float(np.percentile(timings, 95)), 3),
        "warm_min_seconds": round(min(timings), 3),
        "warm_max_seconds": round(max(timings), 3),
        "warm_reps": WARM_REPS,
    }
    with OUT_JSONL.open("a") as fh:
        fh.write(json.dumps(record) + "\n")
    print(f"  {name:12s} vars={label:4s} items={record['items']:>7,} "
          f"cold={cold:7.2f}s warm_med={record['warm_median_seconds']:7.2f}s "
          f"p95={record['warm_p95_seconds']:7.2f}s", flush=True)
    return record


def done_cells() -> set:
    if not OUT_JSONL.exists():
        return set()
    return {(json.loads(l)["workload"], json.loads(l)["varset"])
            for l in OUT_JSONL.read_text().splitlines() if l.strip()}


gm = GeoMetrics()
VARSETS["all"] = all_variables(gm)
print(f"variable sets: 1, 5, all={len(VARSETS['all'])}")

manifest = json.load(open(S / "b3_workloads.json"))
already = done_cells()
if already:
    print(f"resuming, {len(already)} cells already measured")

records = []
for name, meta in manifest.items():
    points = pd.read_csv(S / meta["file"])
    if points.empty:
        print(f"  {name}: empty workload, skipped")
        continue
    for label, variables in VARSETS.items():
        if (name, label) in already:
            continue
        records.append(run_cell(gm, name, points, variables, label))

rows = [json.loads(l) for l in OUT_JSONL.read_text().splitlines() if l.strip()]
pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
print(f"\nwrote {OUT_CSV} ({len(rows)} cells)")
