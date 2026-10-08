"""
Dispatch layer: routes res["missing"] items to the correct GEE extraction function.

Groups by source — one GEE job per batch per source. Each feature carries its own
start_date/end_date so GEE processes each cell against its own dates.

Folder structure: gdrive_folder/SourceName/batch_001.csv, batch_002.csv, ...
"""

from __future__ import annotations
import calendar
import math

from geometrics.extraction.era5_land import submit_era5_land
from geometrics.extraction.modis_ndvi import submit_modis_ndvi
from geometrics.extraction.ndvi import submit_ndvi
from geometrics.extraction.srtm import submit_srtm
from geometrics.extraction.treecover import submit_treecover
from geometrics.extraction.nlcd import submit_nlcd
from geometrics.extraction.uhi import submit_uhi
from geometrics.extraction.water import submit_water

SUBMITTERS = {
    "Landsat_NDVI": submit_ndvi,
    "MODIS_NDVI": submit_modis_ndvi,
    "MODIS_Treecover": submit_treecover,
    "ERA5_Land": submit_era5_land,
    "JRC_Water": submit_water,
    "YALE_UHI": submit_uhi,
    "NLCD": submit_nlcd,
    "SRTM_Terrain": submit_srtm,
}

DEFAULT_BATCH_SIZE = 1000


def _date_range(resolved_timestamp: str, temporal_granularity: str) -> tuple[str, str]:
    year = resolved_timestamp[:4]
    month = resolved_timestamp[5:7]

    if temporal_granularity == "year":
        return f"{year}-01-01", f"{year}-12-31"
    if temporal_granularity == "month":
        last_day = calendar.monthrange(int(year), int(month))[1]
        return f"{year}-{month}-01", f"{year}-{month}-{last_day:02d}"
    if temporal_granularity == "hour":
        # Stored hourly timestamps are "YYYY-MM-DD HH:00:00"; GEE's ee.Date wants
        # ISO 8601, and rejects both the space separator and a bare hour.
        hour = f"{resolved_timestamp[:10]}T{resolved_timestamp[11:13]}:00:00"
        return hour, hour
    day = resolved_timestamp[:10]
    return day, day


def dispatch(
    engine,
    config,
    backend,
    missing_items: list[dict],
    gdrive_folder: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> list[int]:
    """
    Enrich missing items with per-item date_start/date_end, deduplicate on
    (cell_id, timestamp), group by source, split into batches, and submit one
    GEE job per batch.

    Each source gets its own subfolder: gdrive_folder/SourceName/
    Each batch file is named: batch_001, batch_002, ...

    Returns a list of local job_ids in submission order.
    """
    # Enrich each item with its own date range
    enriched = []
    for item in missing_items:
        date_start, date_end = _date_range(item["timestamp"], item["temporal_granularity"])
        enriched.append({**item, "date_start": date_start, "date_end": date_end})

    # Group by source, deduplicate by (cell_id, timestamp)
    groups: dict[str, dict[tuple, dict]] = {}
    for item in enriched:
        source = item["source"]
        key = (item["cell_id"], item["timestamp"])
        groups.setdefault(source, {})[key] = item

    job_ids = []
    for source, keyed_items in groups.items():
        submit_fn = SUBMITTERS.get(source)
        if submit_fn is None:
            raise ValueError(f"No submitter registered for source {source!r}")

        items = list(keyed_items.values())
        n_batches = math.ceil(len(items) / batch_size)

        for i in range(n_batches):
            batch = items[i * batch_size : (i + 1) * batch_size]
            file_prefix = f"{source}_batch_{i + 1:03d}"
            print(f"  {source}/batch_{i + 1:03d} | {len(batch)} (cell, timestamp) pair(s)")

            job_id = submit_fn(engine, config, backend, batch, gdrive_folder, file_prefix)
            job_ids.append(job_id)
            print(f"    job_id={job_id}")

    return job_ids
