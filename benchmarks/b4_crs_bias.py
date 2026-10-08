"""B4 component — quantify the EPSG:3857 distance bias against geodesic truth.

Takes real level-13 cell centroids from the store, a synthetic reference point
set, and computes nearest-feature distance three ways:
  3857  — what local/compute.py used to do (Web Mercator)
  aeqd  — what it does now (local azimuthal equidistant)
  geod  — WGS84 geodesic, the ground truth
"""
import json
from pathlib import Path
import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Geod
from scipy.spatial import KDTree
from sqlalchemy import text

from geometrics import GeoMetrics
from geometrics.local.compute import _local_equidistant_crs, _GEOGRAPHIC_CRS

S = Path(__file__).parent
RNG = np.random.default_rng(42)
N_CELLS = 500
N_REF = 2000

gm = GeoMetrics()
with gm.engine.connect() as c:
    rows = c.execute(text("""
        SELECT c.cell_id FROM cells c
        WHERE c.level = 13
        ORDER BY random() LIMIT :n
    """), {"n": N_CELLS}).fetchall()
cells = [r.cell_id for r in rows]
coords = [gm.backend.cell_to_centroid(cid) for cid in cells]
lats = np.array([c[0] for c in coords])
lons = np.array([c[1] for c in coords])
print(f"{len(cells)} cell centroids, lat {lats.min():.2f}..{lats.max():.2f}")

# Synthetic reference features spread over the same extent
ref_lats = RNG.uniform(lats.min(), lats.max(), N_REF)
ref_lons = RNG.uniform(lons.min(), lons.max(), N_REF)

query = gpd.GeoDataFrame(geometry=gpd.points_from_xy(lons, lats), crs=_GEOGRAPHIC_CRS)
ref = gpd.GeoDataFrame(geometry=gpd.points_from_xy(ref_lons, ref_lats), crs=_GEOGRAPHIC_CRS)


def nearest_km(crs):
    q = query.to_crs(crs)
    r = ref.to_crs(crs)
    tree = KDTree(np.column_stack([r.geometry.x, r.geometry.y]))
    dist, idx = tree.query(np.column_stack([q.geometry.x, q.geometry.y]))
    return dist / 1000.0, idx


merc_km, merc_idx = nearest_km("EPSG:3857")
aeqd_crs = _local_equidistant_crs(query)
aeqd_km, aeqd_idx = nearest_km(aeqd_crs)

# Geodesic truth: distance to the reference each method picked, measured properly
geod = Geod(ellps="WGS84")
def geodesic_km(idx):
    _, _, d = geod.inv(lons, lats, ref_lons[idx], ref_lats[idx])
    return np.abs(d) / 1000.0

truth_merc = geodesic_km(merc_idx)
truth_aeqd = geodesic_km(aeqd_idx)

out = {
    "n_cells": len(cells),
    "n_reference_points": N_REF,
    "lat_range": [round(float(lats.min()), 3), round(float(lats.max()), 3)],
    "mercator": {
        "mean_reported_km": round(float(merc_km.mean()), 4),
        "mean_true_km": round(float(truth_merc.mean()), 4),
        "mean_inflation_factor": round(float((merc_km / truth_merc).mean()), 4),
        "max_inflation_factor": round(float((merc_km / truth_merc).max()), 4),
        "mean_abs_error_km": round(float(np.abs(merc_km - truth_merc).mean()), 4),
        "max_abs_error_km": round(float(np.abs(merc_km - truth_merc).max()), 4),
    },
    "aeqd": {
        "mean_reported_km": round(float(aeqd_km.mean()), 4),
        "mean_true_km": round(float(truth_aeqd.mean()), 4),
        "mean_inflation_factor": round(float((aeqd_km / truth_aeqd).mean()), 4),
        "max_inflation_factor": round(float((aeqd_km / truth_aeqd).max()), 4),
        "mean_abs_error_km": round(float(np.abs(aeqd_km - truth_aeqd).mean()), 4),
        "max_abs_error_km": round(float(np.abs(aeqd_km - truth_aeqd).max()), 4),
    },
    "different_nearest_feature_chosen": int((merc_idx != aeqd_idx).sum()),
}
print(json.dumps(out, indent=2))
json.dump(out, open(S / "b4_crs_bias.json", "w"), indent=2)
