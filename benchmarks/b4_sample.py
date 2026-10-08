"""B4 step 1 — stratified fidelity sample from the store.

Per source: 4 years x {urban, rural} x 25 cells. Urban/rural proxy is the static
EPA walkability index at the same level-13 cell (natwalkind >= 10 = urban).

ERA5-Land is the exception: its cells are level 8 (3.2 km), so there is no
1:1 level-13 walkability row to join against. It is stratified by year only and
its rows are marked urban=unknown, which is also the honest treatment — an
urban/rural split means little at 3.2 km.

Writes each (source, year) batch to disk as it goes and skips batches already
present, so an interrupted run resumes instead of starting over.
"""
import json, os, time
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

S = Path(__file__).parent
PARTIAL = S / "b4_sample_partial.csv"
FINAL = S / "b4_sample.csv"
URL = json.load(open(os.path.expanduser('~/.geometrics/config.json')))['db_url']
engine = create_engine(URL)

YEARS = [2013, 2016, 2019, 2022]
PER_STRATUM = 25
POOL = 400

SOURCES = {
    "Landsat_NDVI":    ("obs_landsat_ndvi", "NDVI", "year"),
    "MODIS_NDVI":      ("obs_modis_ndvi", "NDVI", "year"),
    "MODIS_Treecover": ("obs_modis_treecover", "percent_tree_cover", "year"),
    "NLCD":            ("obs_nlcd", "impervious", "year"),
    "YALE_UHI":        ("obs_yale_uhi", "yearly_daytime", "year"),
    "JRC_Water":       ("obs_jrc_water", "water_distance", "year"),
    "ERA5_Land":       ("obs_era5_land", "temperature_2m", "hour"),
}
STRATIFIED_BY_WALKABILITY = {s for s in SOURCES if s != "ERA5_Land"}


def q(sql, **kw):
    with engine.connect() as c:
        c.execute(text("set max_parallel_workers_per_gather=0"))
        return pd.DataFrame(c.execute(text(sql), kw).mappings().all())


def already_done() -> set:
    if not PARTIAL.exists():
        return set()
    prior = pd.read_csv(PARTIAL)
    return set(zip(prior["source"], prior["year"]))


def append(df: pd.DataFrame) -> None:
    df.to_csv(PARTIAL, mode="a", header=not PARTIAL.exists(), index=False)


done = already_done()
if done:
    print(f"resuming — {len(done)} (source, year) batches already sampled")

for source, (table, var, granularity) in SOURCES.items():
    for year in YEARS:
        if (source, year) in done:
            continue
        t0 = time.time()
        if source in STRATIFIED_BY_WALKABILITY:
            got = q(f"""
                SELECT c.cell_id, u.timestamp, o.{var} AS stored_value,
                       (w.natwalkind >= 10) AS urban
                FROM {table} o
                JOIN spatiotemporal_units u ON u.id = o.unit_pk
                JOIN cells c ON c.id = u.cell_pk
                JOIN spatiotemporal_units su ON su.cell_pk = c.id
                     AND su.timestamp = '1900-01-01'
                JOIN obs_walkability w ON w.unit_pk = su.id
                WHERE u.timestamp >= :lo AND u.timestamp < :hi
                  AND o.{var} IS NOT NULL
                ORDER BY random() LIMIT {POOL}
            """, lo=f"{year}-01-01", hi=f"{year+1}-01-01")
            if got.empty:
                print(f"{source} {year}: no rows", flush=True)
                continue
            sel = pd.concat([got[got["urban"] == flag].head(PER_STRATUM)
                             for flag in (True, False)])
        else:
            sel = q(f"""
                SELECT c.cell_id, u.timestamp, o.{var} AS stored_value,
                       NULL::boolean AS urban
                FROM {table} o
                JOIN spatiotemporal_units u ON u.id = o.unit_pk
                JOIN cells c ON c.id = u.cell_pk
                WHERE u.timestamp >= :lo AND u.timestamp < :hi
                  AND o.{var} IS NOT NULL
                ORDER BY random() LIMIT {PER_STRATUM * 2}
            """, lo=f"{year}-01-01", hi=f"{year+1}-01-01")
            if sel.empty:
                print(f"{source} {year}: no rows", flush=True)
                continue

        sel = sel.assign(source=source, variable=var,
                         temporal_granularity=granularity, year=year)
        sel["timestamp"] = sel["timestamp"].astype(str)
        append(sel)
        urban_n = int(sel["urban"].fillna(False).sum())
        print(f"{source} {year}: {len(sel)} sampled (urban {urban_n}) "
              f"in {time.time()-t0:.1f}s", flush=True)

sample = pd.read_csv(PARTIAL)
sample.to_csv(FINAL, index=False)
print(f"\nTOTAL {len(sample)} rows, {sample['source'].nunique()} sources")
print(sample.groupby("source").size().to_string())
