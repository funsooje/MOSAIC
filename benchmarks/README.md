# Benchmarks

The scripts that produced the evaluation numbers, and their raw output under
`results/`. Each writes CSV or JSON so figures and tables can be regenerated
without re-running the measurements.

## Environment the numbers were measured on

| | |
|---|---|
| Host | MacBook Pro, Apple M1 (4 performance + 4 efficiency cores), 16 GB RAM, macOS 15 |
| Container runtime | Docker Desktop VM, 8 vCPU, 7.65 GiB |
| Database | PostgreSQL 16.9 (`postgres:16`, arm64), `--shm-size=2g`, port 5434 |
| Store | 9 GEE/local sources at HierGP level 13 (100 m) plus ERA5-Land at level 8 (3.2 km), 2010–2022 |

PostgreSQL settings were frozen before timing began and are recorded verbatim in
`results/pg_frozen_config.json`. `jit` is off: JIT compilation added variance
without changing throughput.

"Cold" means the database's own buffers are empty, via a container restart. The
Docker VM page cache is **not** dropped, so cold numbers are cold-database,
warm-OS. State it that way when citing them.

## Scripts

| Script | What it measures | Output |
|---|---|---|
| `b1_inventory.py` | Storage inventory: rows, cells, levels, time span, table/index size per source; replication factor for coarse sources | `results/b1_*.csv`, `b1_summary.json` |
| `b3_sample_points.py` | Builds the participant workloads from the legacy GTL database | `results/b3_workloads.json` |
| `b3_bench.py` | Query latency grid: participants × variables × time range, 1 cold + 5 warm runs each | `results/b3_results.csv`, `b3_runs.jsonl` |
| `b3_viewer.py` | Viewer `/api/data` at four viewport sizes, plus startup prefetch cost | `results/b3_viewer.json` |
| `b4_sample.py` | Draws the stratified fidelity sample from the store | `results/b4_sample.csv` |
| `b4_demo_submit.py` | Submits the fidelity re-extractions and the demo extraction | — |
| `b4_compare.py` | Fidelity: re-extracted values vs stored values, per source | `results/b4_fidelity.csv`, `b4_bias.json` |
| `b4_crs_bias.py` | Distance error from measuring in Web Mercator instead of geodesically | `results/b4_crs_bias.json` |
| `b5_recovery.py` | Kill an ingest at 50%, restart, compare row counts and per-table checksums against a clean run | `results/b5_result.json` |
| `b6_submit.py` / `b6_compare.py` | Portability: the same workload under H3 at mapped resolutions | `results/b6_*.json` |
| `rq1_finish.py` | End-to-end timing for onboarding a new source | `results/rq1_result.json` |

## Caveats worth carrying into any write-up

- **Query latency is not I/O bound.** Cold and warm runs are indistinguishable
  across all 36 grid cells, and throughput holds near 550–570 items/s whatever
  the workload or store size. `fetch()` issues one query per
  (location, variable), so cost tracks round-trips, not data volume. The
  frozen tuning therefore buys very little query speed.
- **The store has no participant column.** Cells are shared and anonymous, so a
  "participants" workload means the points those participants visited, drawn
  from the source GTL database and then queried by location and time.
- **ERA5 is not stratified by urban/rural.** Its cells are level 8 (3.2 km) and
  the urban proxy is a level-13 attribute, so no row exists to join against.
- **B6 is a reduced experiment**: 2,500 of B2's 10,000 points, two sources.
- **B5 runs on a scratch store**, not the deployment store. Recovery rests on
  the same `ON CONFLICT` clauses and unique keys either way.
- The kill in B5 fires once 50% of rows are observably on disk. An earlier
  timer-based version killed the process during interpreter startup, before any
  write, and measured nothing.

## What is deliberately not here

Three cohort-derived files are not distributed:

- `results/case_study_points.csv` — real participant coordinates, timestamped to
  the second. A movement trace re-identifies in a way a pseudonymous id does not.
- `results/b4_sample.csv` — the 100 m grid cells the fidelity sample drew from,
  which are places participants visited.
- `results/case_study_summary.csv` — per-participant weekly exposure means. The
  cohort is a small twin registry, so even aggregate per-participant series carry
  re-identification risk, and the study commits to releasing no cohort-level
  location or exposure distributions. Relabelling the ids would not change what
  the file is.

All three regenerate from the source database with the scripts above, and the
manifests retain their SHA-256 values so an authorised rebuild can still be
verified. Everything else here is aggregate across sources, synthetic, or derived
from public datasets.

## Running them

Requires the `gee` environment (see `environment.yml`), a populated store
configured in `~/.geometrics/config.json`, and for `b3_sample_points.py` the
legacy GTL database reachable via `GTL_DSN`. The GEE-submitting scripts need
Earth Engine credentials and a `gee_project` in the config.

```bash
python benchmarks/b1_inventory.py
python benchmarks/b3_sample_points.py && python benchmarks/b3_bench.py
python benchmarks/b5_recovery.py          # no GEE needed
```
