"""
Minimal CLI for GeoMetrics setup and job management.

Usage:
  python -m geometrics setup                              # create/update config interactively
  python -m geometrics config                             # show current config and its file path
  python -m geometrics initdb                             # create tables in the configured database
  python -m geometrics status                             # poll GEE for active job statuses
  python -m geometrics jobs [STATUS]                      # list jobs, optionally filtered by status
  python -m geometrics list-sources                       # show all available data sources
  python -m geometrics list-variables SOURCE              # show variables for a source
  python -m geometrics check CSV --variables V [--year Y] # check data availability for locations
  python -m geometrics register-sources                   # register all catalog sources in the DB
"""

import dataclasses
import json
import sys


def _print_config(config, path):
    print(f"Config file: {path}")
    print(json.dumps(dataclasses.asdict(config), indent=2))


def cmd_setup():
    from geometrics.config import DEFAULT_CONFIG_PATH, GeoMetricsConfig, load_config, save_config

    current = load_config()
    print("GeoMetrics setup (press Enter to keep current value)\n")

    db_url = input(f"  db_url [{current.db_url}]: ").strip() or current.db_url
    gdrive_base = input(f"  gdrive_base [{current.gdrive_base}]: ").strip() or current.gdrive_base
    backend = input(f"  backend [{current.backend}]: ").strip() or current.backend

    config = GeoMetricsConfig(db_url=db_url, gdrive_base=gdrive_base, backend=backend)
    save_config(config)
    print()
    _print_config(config, DEFAULT_CONFIG_PATH)
    print("\nEdit this file directly at any time to update your settings.")


def cmd_config():
    from geometrics.config import DEFAULT_CONFIG_PATH, load_config

    config = load_config()
    _print_config(config, DEFAULT_CONFIG_PATH)
    if not DEFAULT_CONFIG_PATH.exists():
        print("\n(file not found — showing defaults; run 'setup' to create it)")


def cmd_initdb():
    from geometrics.config import load_config
    from geometrics.store.db import get_engine
    from geometrics.store.schema import initialize_db

    config = load_config()
    engine = get_engine(config.db_url)
    initialize_db(engine)


def cmd_status():
    from geometrics.config import load_config
    from geometrics.store.db import get_engine
    from geometrics.store.jobs import check_status

    config = load_config()
    engine = get_engine(config.db_url)
    check_status(engine)


def cmd_jobs(args):
    from geometrics.config import load_config
    from geometrics.store.db import get_engine
    from geometrics.store.jobs import list_jobs

    status_filter = args[0].upper() if args else None
    config = load_config()
    engine = get_engine(config.db_url)
    rows = list_jobs(engine, status=status_filter)

    if not rows:
        print("No jobs found.")
        return

    cols = (
        f"{'ID':>6}  {'STATUS':<12}  {'TASK_ID':<25}"
        f"  {'DATE_START':<12}  {'DATE_END':<12}  SUBMITTED_AT"
    )
    print(cols)
    print("-" * len(cols))
    for row in rows:
        task_short = (row.task_id or "")[:24]
        dates = f"{row.date_start or '':<12}  {row.date_end or '':<12}"
        line = f"{row.job_id:>6}  {row.status:<12}  {task_short:<25}  {dates}"
        print(f"{line}  {row.submitted_at or ''}")


def cmd_list_sources():
    from geometrics.backends import get_backend
    from geometrics.backends.levels import native_level_for_backend
    from geometrics.catalog import CATALOG
    from geometrics.config import load_config

    config = load_config()
    backend = get_backend(config.backend)
    header = (
        f"{'SOURCE':<20}  {'DATA RES':>8}  {'COLLECT RES':>12}"
        f"  {'SRC TEMPORAL':<14}  {'COLLECT TEMPORAL'}"
    )
    print(header)
    print("-" * len(header))
    for spec in CATALOG.values():
        # Local sources carry no GEE pixel size or upstream cadence.
        level = native_level_for_backend(spec["native_level"], config.backend)
        level_km = backend.level_to_approx_resolution_km(level)
        level_res = f"L{level} (~{level_km * 1000:.0f}m)"
        pixel = spec.get("pixel_resolution_m")
        pixel_txt = f"{pixel:>6}m" if pixel else "     --"
        print(
            f"{spec['name']:<20}  {pixel_txt}  "
            f"{level_res:>12}  {spec.get('source_temporal_granularity') or '--':<14}  "
            f"{spec['temporal_granularity']}"
        )


def cmd_list_variables(args):
    from geometrics.backends import get_backend
    from geometrics.backends.levels import native_level_for_backend
    from geometrics.catalog import CATALOG, get_source
    from geometrics.config import load_config

    if not args:
        print("Usage: list-variables SOURCE")
        print(f"Available: {', '.join(CATALOG)}")
        return

    spec = get_source(args[0])
    config = load_config()
    backend = get_backend(config.backend)
    level = native_level_for_backend(spec["native_level"], config.backend)
    level_km = backend.level_to_approx_resolution_km(level)

    print(f"{spec['name']} — {spec['description']}")
    print(f"  GEE collection : {spec.get('gee_collection', '-- (local source)')}")
    print(f"  Data resolution: {spec.get('pixel_resolution_m', '--')} m")
    print(f"  Collect level  : L{level} (~{level_km * 1000:.0f} m)")
    print(f"  Source temporal: {spec.get('source_temporal_granularity') or '--'}")
    print(f"  Stored temporal: {spec['temporal_granularity']}")
    print()
    print(f"  {'VARIABLE':<25}  {'UNIT':<12}  DESCRIPTION")
    print(f"  {'-'*25}  {'-'*12}  {'-'*30}")
    for var in spec["variables"]:
        print(f"  {var['name']:<25}  {var.get('unit', ''):<12}  {var.get('description', '')}")


def cmd_check(args):
    import argparse
    import pandas as pd
    from geometrics import GeoMetrics

    parser = argparse.ArgumentParser(prog="geometrics check")
    parser.add_argument("csv_path", help="Path to CSV file with location data")
    parser.add_argument("--variables", required=True,
                        help="Comma-separated 'Source:parameter' e.g. 'Landsat8_NDVI:NDVI'")
    parser.add_argument("--year", type=int, default=None,
                        help="Use YYYY-01-01 as timestamp for all rows (yearly sources)")
    parser.add_argument("--lat-col", default="latitude", help="Latitude column name")
    parser.add_argument("--lon-col", default="longitude", help="Longitude column name")
    parser.add_argument("--timestamp-col", default="timestamp", help="Timestamp column name")
    parsed = parser.parse_args(args)

    variables = [v.strip() for v in parsed.variables.split(",")]
    data: pd.DataFrame = pd.read_csv(parsed.csv_path)

    gm = GeoMetrics()
    try:
        report = gm.check(
            data,
            variables=variables,
            year=parsed.year,
            lat_col=parsed.lat_col,
            lon_col=parsed.lon_col,
            timestamp_col=parsed.timestamp_col,
        )
    except ValueError as exc:
        print(f"Error: {exc}")
        print("Tip: run 'python -m geometrics register-sources' to register all sources.")
        return

    # Build a display label per (lat, lon) — use any identifying columns if present
    label_cols = [c for c in ("site_id", "site_name") if c in data.columns]
    lat_col, lon_col = parsed.lat_col, parsed.lon_col

    def _label(lat, lon):
        match = data[(data[lat_col] == lat) & (data[lon_col] == lon)]
        if match.empty or not label_cols:
            return f"{lat},{lon}"
        return "  ".join(str(match.iloc[0][c]) for c in label_cols)

    all_items = report["available"] + report["missing"]
    timestamps = sorted({item["timestamp"] for item in all_items})
    print(f"\nChecking {', '.join(variables)} for {len(data)} location(s)"
          f" | timestamp(s): {', '.join(timestamps)}\n")

    header = f"  {'LOCATION':<35}  {'VARIABLE':<25}  STATUS"
    print(header)
    print("  " + "-" * (len(header) - 2))

    for item in report["available"]:
        loc = _label(item["lat"], item["lon"])
        print(f"  {loc:<35}  {item['source']+':'+item['parameter']:<25}  available")

    for item in report["missing"]:
        loc = _label(item["lat"], item["lon"])
        reason = item["reason"]
        print(f"  {loc:<35}  {item['source']+':'+item['parameter']:<25}  MISSING ({reason})")

    n_avail = len(report["available"])
    n_total = len(all_items)
    print(f"\nAvailable: {n_avail} / {n_total}")
    if report["missing"]:
        print("Run 'submit' to queue GEE extraction for missing locations.")


def cmd_register_sources():
    from geometrics.catalog import CATALOG
    from geometrics.config import load_config
    from geometrics.extraction.base import ensure_source
    from geometrics.store.db import get_engine

    config = load_config()
    engine = get_engine(config.db_url)

    for spec in CATALOG.values():
        ensure_source(
            engine=engine,
            name=spec["name"],
            native_level=spec["native_level"],
            pixel_resolution_m=spec["pixel_resolution_m"],
            source_temporal_granularity=spec["source_temporal_granularity"],
            temporal_granularity=spec["temporal_granularity"],
            variable_defs=spec["variables"],
        )
        print(f"  registered: {spec['name']} ({len(spec['variables'])} variable(s))")

    print(f"\n{len(CATALOG)} source(s) registered.")


_COMMANDS = {
    "setup": (cmd_setup, []),
    "config": (cmd_config, []),
    "initdb": (cmd_initdb, []),
    "status": (cmd_status, []),
    "jobs": (cmd_jobs, None),
    "list-sources": (cmd_list_sources, []),
    "list-variables": (cmd_list_variables, None),
    "check": (cmd_check, None),
    "register-sources": (cmd_register_sources, []),
}


def main():
    args = sys.argv[1:]
    if not args or args[0] not in _COMMANDS:
        print(__doc__)
        sys.exit(1)

    cmd, fixed_args = _COMMANDS[args[0]]
    if fixed_args is None:
        cmd(args[1:])
    else:
        cmd()


if __name__ == "__main__":
    main()
