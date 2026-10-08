# GeoMetrics

A multi-resolution environmental data store backed by Google Earth Engine (GEE), with a built-in map viewer for exploration.

GeoMetrics solves a common research problem: you have a list of field sites and dates, and you need environmental covariates — vegetation indices, land cover, climate, water proximity — for each one. Pulling that data manually from different sources is slow and produces inconsistent spatial representations. GeoMetrics automates the full pipeline: it snaps your locations to a consistent spatial grid, checks what you already have in the database, submits missing extractions to GEE as batch jobs, ingests the results, and serves them back as a clean DataFrame.

---

## How it works

```
Your locations CSV
       │
       ▼
  [snap to grid]          ← HierGP rectangular grid, resolution-aware
       │
       ▼
  [check store]           ← which (cell, variable, year) are already cached?
       │
       ├─── available ─── gm.fetch() ──► DataFrame
       │
       └─── missing ──── gm.gee_submit() ──► GEE batch export jobs
                                │
                         (jobs run in GEE)
                                │
                         gm.ingest() ──► PostgreSQL
                                │
                         gm.fetch()  ──► DataFrame
```

**Spatial grid.** All sources share a common HierGP rectangular grid. Each location is snapped to the nearest cell at the source's native resolution, so repeated queries for nearby points are automatically deduplicated and every dataset lines up spatially.

**Temporal snapping.** Timestamps are resolved to each dataset's temporal granularity — any date in 2023 maps to `2023-01-01` for annual datasets, to the nearest hour for ERA5-Land, and so on.

**Backend-agnostic.** The grid layer is pluggable. HierGP (rectangular) is the default; H3 (hexagonal) is also supported. The rest of the system — schema, ingest, query — works identically regardless of backend.

---

## Supported datasets

### GEE sources

| Source | Variable(s) | Native resolution | Temporal | GEE collection |
|--------|-------------|:-----------------:|----------|----------------|
| `Landsat_NDVI` | `NDVI` | 30 m | Annual median | Landsat 5/7/8/9 (USGS SR) |
| `MODIS_NDVI` | `NDVI` | 250 m | Annual | MOD13Q1 |
| `MODIS_Treecover` | `percent_tree_cover`, `percent_nontree_vegetation`, `percent_nonvegetated`, `quality`, `percent_tree_cover_sd`, `percent_nonvegetated_sd`, `cloud` | 250 m | Annual | MOD44B |
| `ERA5_Land` | `temperature_2m`, `dewpoint_temperature_2m`, `surface_pressure`, `u_component_of_wind_10m`, `v_component_of_wind_10m`, `surface_thermal_radiation_downwards`, `surface_net_solar_radiation`, `total_precipitation` | ~9 km | Hourly | ERA5-Land (ECMWF) |
| `JRC_Water` | `water_distance` | 30 m | Annual | JRC Global Surface Water |
| `YALE_UHI` | `yearly_daytime`, `yearly_nighttime`, `winter_daytime`, `winter_nighttime`, `summer_daytime`, `summer_nighttime` | 1 km | Annual | Yale Urban Heat Island |
| `NLCD` | `landcover`, `impervious`, `impervious_descriptor` | 30 m | Annual | NLCD (USGS) |

### Local sources

Local sources are computed from shapefiles or tabular data you supply — no GEE required. Values are stored in the same observation tables as GEE sources and appear in the viewer automatically.

| Source | Variable(s) | Native resolution | Temporal | Input data |
|--------|-------------|:-----------------:|----------|------------|
| `Parks_Distance` | `nearest_distance` (km) | 100 m | Static | PADUS protected areas (polygon shapefile) |
| `Walkability` | `natwalkind` (1–20) | 100 m | Static | EPA National Walkability Index (tabular) |
| `CACES_Air` | `pm25` (µg/m³), `no2` (ppb) | 100 m | Annual | CACES land-use regression estimates (tabular) |

---

## Prerequisites

- Python 3.10+
- PostgreSQL 14+ database
- Google Earth Engine account with project access (`ee.Initialize()`)
- Google Drive mounted locally (for ingest step only)

---

## Installation

```bash
conda env create -f environment.yml
conda activate geometrics
pip install -e .
```

---

## Setup

### 1. Configure

Run once. Saves connection settings to `~/.geometrics/config.json`.

```python
from geometrics import GeoMetrics

gm = GeoMetrics.configure(
    db_url="postgresql://user:password@localhost:5432/geometrics",
    gdrive_base="/path/to/My Drive",
    backend="hiergp",
)
```

All subsequent calls to `GeoMetrics()` load the saved config automatically.

### 2. Initialize the database

```python
gm = GeoMetrics()
gm.init_db()
# Database initialized.
# Registered 7 new source(s): ['Landsat_NDVI', 'MODIS_NDVI', ...]
```

Safe to call on an existing database — creates tables only if they don't exist, and skips sources that are already registered.

---

## Core workflow

### Prepare your locations CSV

At minimum, three columns are required. Column names are configurable.

```
site_id,latitude,longitude,timestamp
WA001,47.6062,-122.3321,2023-07-15T14:30:00
WA002,46.8523,-121.7603,2023-07-15T11:00:00
BR001,-2.4297,-54.7083,2023-08-01T10:00:00
```

### Check availability

```python
import ee
ee.Initialize()

gm = GeoMetrics()
report = gm.check(
    "locations.csv",
    variables=["Landsat_NDVI:NDVI", "MODIS_Treecover:percent_tree_cover"],
)

# report["available"] — already in the store
# report["missing"]   — need to be extracted from GEE
```

Each row in both lists includes the original coordinates, the resolved grid cell, the requested timestamp, and the resolved timestamp.

### Submit missing items to GEE

```python
job_ids = gm.gee_submit(report["missing"], gdrive_folder="extract-01")
# Submitted 2 job(s). Track with: gm.jobs()
```

GEE runs the export tasks asynchronously. Results are written as CSVs to the specified Drive folder.

### Track job status

```python
gm.jobs()               # DataFrame of all submitted jobs
gm.jobs("RUNNING")      # filter by status

gm.check_status()       # poll GEE and update local DB
```

Status values: `PENDING` → `RUNNING` → `COMPLETED` / `FAILED` / `CANCELLED` / `EXPIRED` → `INGESTED`

### Ingest completed results

Once GEE jobs are `COMPLETED` and the Drive folder has synced locally:

```python
gm.ingest("extract-01")
# Ingesting Landsat_NDVI_batch_001.csv ... inserted 7 row(s), skipped 0 duplicate(s).
# Ingesting MODIS_Treecover_batch_001.csv ... inserted 7 row(s), skipped 0 duplicate(s).
```

Re-running ingest is safe — duplicates are detected and skipped.

### Fetch stored data

```python
df = gm.fetch(
    "locations.csv",
    variables=["Landsat_NDVI:NDVI", "MODIS_Treecover:percent_tree_cover"],
)
```

Returns a DataFrame joined back to your original input, with one column per variable. Rows with no data are `NaN` by default.

**Output format options:**

```python
# Long format (one row per location × variable)
df = gm.fetch("locations.csv", variables=[...], output_format="long")

# Drop rows with no data at all
df = gm.fetch("locations.csv", variables=[...], preserve_rows=False)

# Strip extra input columns from output
df = gm.fetch("locations.csv", variables=[...], preserve_cols=False)

# Long format with grid metadata
df = gm.fetch("locations.csv", variables=[...],
               output_format="long", include_metadata=True)
# Extra columns: cell_id, level, aggregated, resolved_timestamp
```

---

## Map viewer

GeoMetrics ships a browser-based map viewer for exploring what's in the database.

```bash
python viewer/server.py
# or with live reload:
uvicorn viewer.server:app --reload --port 8765
```

Open `http://localhost:8765` in your browser.

**Features:**
- Browse all sources, variables, and available years from a sidebar
- Load up to 200,000 cells for the full dataset or just the current viewport ("Load focus area")
- Circle radius scales with the dataset's spatial resolution
- 12 colormaps with a **Flip** checkbox to reverse any colormap
- Adjustable opacity
- Hover tooltip showing value and coordinates
- Auto-updating legend with data min/max
- **Save view / Load saved view** — persist the current map center and zoom to `viewer/saved_view.json` for reproducible screenshots

The viewer API is also accessible directly:
- `GET /api/sources` — all sources with variables and available timestamps
- `GET /api/data?source=&variable=&timestamp=&bbox=` — cell data for a selection
- `GET /api/view` — saved map center and zoom (or default if none saved)
- `POST /api/view` — save current center and zoom (`{center: [lng, lat], zoom: number}`)

---

## API reference

### Configuration and setup

```python
GeoMetrics.configure(db_url, gdrive_base, backend)  # save config, return instance
gm.show_config()                                     # print current config as dict
gm.init_db()                                         # create tables + register sources
gm.register_sources()                                # register/update catalog in DB
```

### Discovery

```python
GeoMetrics.list_sources()          # list[dict] — all catalog entries
GeoMetrics.list_variables(source)  # list[dict] — variables for one source
gm.jobs(status=None)               # DataFrame of submitted jobs
```

### Data pipeline

```python
gm.check(locations, variables, lat_col, lon_col, timestamp_col)
# → {"available": [...], "missing": [...]}

gm.gee_submit(missing_items, gdrive_folder, batch_size=1000)
# → list[int] of job_ids

gm.check_status()
# → {status: count} summary dict

gm.ingest(gdrive_folder)
# → {filename: rows_inserted}

gm.fetch(locations, variables, lat_col, lon_col, timestamp_col,
         output_format="wide", preserve_rows=True, preserve_cols=True,
         include_metadata=False)
# → pd.DataFrame
```

### Maintenance

```python
gm.clear(source)       # drop all observations for a source (keeps schema)
gm.reset_db()          # drop all observation tables and reinitialize
```

---

## Architecture

### Spatial grid

The grid layer is pluggable via the `backend` config option. Two backends are provided:

**HierGP** (`backend="hiergp"`, default) — recursive rectangular grid, base cell size 25 m, 15 levels. Higher standard level = finer resolution.

| Standard level | Cell size |
|:--------------:|-----------|
| 15 | 25 m |
| 14 | 50 m |
| 13 | 100 m |
| 12 | 200 m |
| 11 | 400 m |
| 10 | 800 m |
| 9 | 1.6 km |
| ... | ... |
| 1 | ~410 km |

**H3** (`backend="h3"`) — Uber's hexagonal hierarchical grid, 15 resolutions (0 = coarsest, 14 = finest). `cell_id` is the native H3 index string. Useful when hexagonal neighbourhood relationships matter.

| H3 resolution | Approx. edge length |
|:-------------:|---------------------|
| 11 | 25 m |
| 10 | 66 m |
| 9 | 174 m |
| 8 | 461 m |
| 7 | 1.2 km |

Both backends implement the same `GridBackend` interface (`point_to_cell`, `cell_to_centroid`, `cell_parent`, `cell_children`). The rest of the system — schema, ingest, query, viewer — is identical regardless of which backend is active.

Each source is registered with a `native_level` that matches its pixel footprint. Locations are snapped to that level, so two sites that fall in the same cell share a single database row for that source.

### Database schema

```
sources              — one row per dataset (name, native_level, pixel_resolution_m, ...)
variables            — one row per band within a source (name, unit)
cells                — one row per unique grid cell (cell_id, backend, level)
hiergp_cells         — HierGP-specific: x/y integer coordinates for each cell
spatiotemporal_units — one row per (cell × timestamp) pair; RANGE-partitioned by year
obs_{source_name}    — wide observation table: unit_pk + one column per variable
jobs                 — GEE export task registry (status, file paths, row counts)
```

The observation tables are intentionally denormalized (wide format) so that fetching multiple variables for the same location requires only one join. The `spatiotemporal_units` table is partitioned by year in PostgreSQL for fast range scans.

### Adding a new GEE source

1. Create `geometrics/extraction/my_source.py` and define a `SOURCE_SPEC` dict and a `build_ee_image()` function following the pattern in `geometrics/extraction/ndvi.py`.
2. Import `SOURCE_SPEC` in `geometrics/catalog.py` and add it to `CATALOG`.
3. Run `gm.register_sources()` to add the source and its variables to the database.

### Adding a local source

Local sources are computed from a shapefile or tabular dataset you supply and stored like any other source. Three spatial operations are supported:

| Operation | Use when |
|-----------|----------|
| `nearest_distance` | Reference is a polygon layer (e.g. parks, water bodies); computes km distance to nearest boundary, 0 if inside |
| `inside` | Reference is a polygon layer; writes 1.0 (inside) or 0.0 (outside) |
| `attribute_lookup` | Reference is a point layer with per-year value columns; nearest-neighbour match |

**Step 1 — Register the source**

```python
gm.register_local_source(
    "Parks_Distance",
    operation="nearest_distance",
    # variable_defs defaults to [{"name": "nearest_distance", "unit": "km"}]
)

gm.register_local_source(
    "Walkability",
    operation="attribute_lookup",
    variable_defs=[{"name": "natwalkind", "unit": "score"}],
    temporal_granularity="static",
)
```

**Step 2 — Compute and store values**

Pass your locations DataFrame and the reference GeoDataFrame (loaded however you like — `geopandas.read_file()`, a PostGIS query, etc.):

```python
import geopandas as gpd
from geometrics.local.compute import compute_nearest_distance

parks_gdf = gpd.read_file("path/to/padus.shp")

compute_nearest_distance(
    engine=gm.engine,
    backend=gm._backend,
    locations_df=locations_df,
    source_name="Parks_Distance",
    geodataframe=parks_gdf,
    lat_col="latitude",
    lon_col="longitude",
    native_level=13,
)
```

**Step 3 — Add a catalog entry** so the viewer shows a description:

```python
# geometrics/local/sources.py
MY_SOURCE_SPEC = {
    "name": "Parks_Distance",
    "description": "Distance (km) to nearest protected area boundary (PADUS)",
    "native_level": 13,
    "temporal_granularity": "static",
    "variables": [{"name": "nearest_distance", "unit": "km", "description": "..."}],
}
```

Then import and add it to `CATALOG` in `geometrics/catalog.py`.

The source file (shapefile, GDB, CSV) is not stored in the database — only the computed values are. Re-run `compute_*` with the updated file if you add new locations or want to refresh values.

---

## Experiments

### 5d — Privacy analysis (k-anonymity proxy)

**Script:** `scripts/experiment_5d_privacy.py`

**Question:** If each level-13 cell (100 m) is treated as a raw observation, how many raw observations fall into each cell when the grid is coarsened to level L? The average count per coarser cell (k) is a proxy for k-anonymity — higher k means an individual observation is harder to link back to a specific location.

**Method:** 3,091,917 unique level-13 cells are taken as raw observations. For each target level 1–12, cells are grouped into their parent at that level by integer-dividing the x/y coordinates by 2^(13−L). The distribution of group sizes is recorded.

**Results:**

| Level | Cell size | Unique cells | Mean k | Median k |
|:-----:|----------:|-------------:|-------:|---------:|
| 13 | 100 m | 3,091,917 | 1.0 | 1.0 |
| 12 | 200 m | 1,823,340 | 1.7 | 1.0 |
| 11 | 400 m | 1,027,539 | 3.0 | 2.0 |
| 10 | 800 m | 535,867 | 5.8 | 3.0 |
| 9 | 1.6 km | 267,735 | 11.5 | 5.0 |
| 8 | 3.2 km | 133,431 | 23.2 | 8.0 |
| 7 | 6.4 km | 65,673 | 47.1 | 12.0 |
| 6 | 12.8 km | 31,409 | 98.4 | 20.0 |
| 5 | 25.6 km | 14,403 | 214.7 | 33.0 |
| 4 | 51.2 km | 6,161 | 501.9 | 60.0 |
| 3 | 102.4 km | 2,564 | 1,205.9 | 88.5 |

**Key findings:**

- Mean k grows approximately 4× per level (consistent with each level halving both x and y), reaching k ≈ 11.5 at level 9 (1.6 km) and k ≈ 23.2 at level 8 (3.2 km).
- Median k grows more slowly than mean k at every level, revealing a right-skewed distribution: monitored regions are spatially clustered, so a few cells aggregate many observations while most cells in sparse regions contain very few.
- A commonly cited k-anonymity threshold of k ≥ 5 is met at the **median** only from level 9 (1.6 km) onward. The mean exceeds k = 5 already at level 10 (800 m), but the median-vs-mean gap warns that the guarantee does not hold uniformly across space.
- The practical trade-off: aggregating from 100 m (level 13) to 1.6 km (level 9) reduces unique cells by 11.5× while providing median k-anonymity of 5. Moving to 3.2 km (level 8) reduces cells by 23× with median k = 8.

![Privacy analysis figure](figures/experiment_5d_privacy.png)

---

## License

See [LICENSE](LICENSE).
