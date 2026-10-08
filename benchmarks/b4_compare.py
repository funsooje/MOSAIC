"""B4 — compare freshly re-extracted values against what the store holds.

The store's values came from the GTL migration; these come straight from GEE
through the current pipeline. Any disagreement is either a pipeline fault or a
difference between what GTL computed years ago and what GEE returns today.
"""
import json
from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import text
from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig, load_config

S = Path(__file__).parent
prod = load_config()
gm = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{S}/b4_compare.db",
                                 gdrive_base=prod.gdrive_base, backend="hiergp"))

for folder in ("mosaic-b4-run1", "mosaic-b4-jrc"):
    print(f"ingesting {folder} …")
    print(" ", gm.ingest(folder))

sample = pd.read_csv(S / "b4_sample.csv")
CATEGORICAL = {"NLCD": {"impervious"}}      # integer percent, treat exact match as meaningful
rows = []

for source, group in sample.groupby("source"):
    variable = group["variable"].iloc[0]
    table = "obs_" + source.lower()
    with gm.engine.connect() as conn:
        fresh = pd.DataFrame(conn.execute(text(f"""
            SELECT c.cell_id, u.timestamp, o.{variable} AS fresh_value
            FROM {table} o
            JOIN spatiotemporal_units u ON u.id = o.unit_pk
            JOIN cells c ON c.id = u.cell_pk
        """)).mappings().all())
    if fresh.empty:
        rows.append({"source": source, "variable": variable, "compared": 0,
                     "note": "no re-extracted rows"})
        continue

    fresh["timestamp"] = fresh["timestamp"].astype(str).str.slice(0, 19)
    group = group.copy()
    group["timestamp"] = group["timestamp"].astype(str).str.slice(0, 19)
    merged = group.merge(fresh, on=["cell_id", "timestamp"], how="inner")

    both = merged.dropna(subset=["stored_value", "fresh_value"])
    diff = (both["fresh_value"] - both["stored_value"]).abs()
    denom = both["stored_value"].abs().replace(0, np.nan)
    rel = (diff / denom).dropna()

    rows.append({
        "source": source, "variable": variable,
        "sampled": len(group), "matched_cells": len(merged), "compared": len(both),
        "fresh_null": int(merged["fresh_value"].isna().sum()),
        "stored_null": int(merged["stored_value"].isna().sum()),
        "exact_match": int((diff == 0).sum()),
        "exact_match_pct": round(100 * (diff == 0).mean(), 2) if len(both) else None,
        "within_1pct": round(100 * (rel <= 0.01).mean(), 2) if len(rel) else None,
        "mean_abs_diff": round(float(diff.mean()), 6) if len(both) else None,
        "median_abs_diff": round(float(diff.median()), 6) if len(both) else None,
        "max_abs_diff": round(float(diff.max()), 6) if len(both) else None,
        "correlation": round(float(both["fresh_value"].corr(both["stored_value"])), 6)
                        if len(both) > 2 else None,
        "stored_range": [round(float(both["stored_value"].min()), 4),
                         round(float(both["stored_value"].max()), 4)] if len(both) else None,
        "fresh_range": [round(float(both["fresh_value"].min()), 4),
                        round(float(both["fresh_value"].max()), 4)] if len(both) else None,
    })

report = pd.DataFrame(rows)
report.to_csv(S / "b4_fidelity.csv", index=False)
pd.set_option("display.width", 200)
print("\n" + report.to_string(index=False))
json.dump(rows, open(S / "b4_fidelity.json", "w"), indent=2, default=str)
