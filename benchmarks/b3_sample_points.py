"""B3 step 1 — build the participant workloads.

The GeoMetrics store has no participant column: cells are shared and anonymous.
So a "participants" workload means the points those participants actually
visited, taken from the GTL locations table, then queried against GeoMetrics by
location and time. That is the real researcher workflow.

Points per participant is fixed so the participant axis scales cleanly.
"""
import json
from pathlib import Path
import pandas as pd
import psycopg2

S = Path(__file__).parent
POINTS_PER_PARTICIPANT = 10
PARTICIPANT_COUNTS = [1, 10, 100, "all"]
RANGES = {
    "month": ("2019-06-01", "2019-07-01"),
    "year":  ("2019-01-01", "2020-01-01"),
    "full":  ("2010-01-01", "2023-01-01"),
}

import os, sys
sys.path.insert(0, str(Path("/Users/funsooje/Documents/GitHub/GeoMetrics/scripts")))
import importlib.util
spec = importlib.util.spec_from_file_location(
    "mg", "/Users/funsooje/Documents/GitHub/GeoMetrics/scripts/migrate_gtl.py")
mg = importlib.util.module_from_spec(spec); spec.loader.exec_module(mg)

conn = psycopg2.connect(mg._gtl_dsn())
cur = conn.cursor()
cur.execute("SELECT DISTINCT client_id FROM locations ORDER BY client_id")
clients = [r[0] for r in cur.fetchall()]
print(f"{len(clients)} participants with points")

manifest = {}
for count in PARTICIPANT_COUNTS:
    chosen = clients if count == "all" else clients[:count]
    for range_name, (lo, hi) in RANGES.items():
        rows = []
        for client in chosen:
            cur.execute("""
                SELECT latitude, longitude, datetime
                FROM locations
                WHERE client_id = %s AND datetime >= %s AND datetime < %s
                  AND latitude IS NOT NULL
                LIMIT %s
            """, (client, lo, hi, POINTS_PER_PARTICIPANT))
            for lat, lon, dt in cur.fetchall():
                rows.append({"latitude": float(lat), "longitude": float(lon),
                             "timestamp": dt.strftime("%Y-%m-%dT%H:%M:%S"),
                             "client_id": client})
        name = f"p{count}_{range_name}"
        path = S / f"b3_points_{name}.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        manifest[name] = {"participants_requested": count,
                          "participants_with_points": len({r["client_id"] for r in rows}),
                          "points": len(rows), "range": [lo, hi], "file": path.name}
        print(f"  {name}: {len(rows):,} points from "
              f"{len({r['client_id'] for r in rows})} participants", flush=True)

json.dump(manifest, open(S / "b3_workloads.json", "w"), indent=2)
cur.close(); conn.close()
