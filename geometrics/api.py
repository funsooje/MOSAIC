"""
High-level Python API for GeoMetrics.

Example usage::

    from geometrics import GeoMetrics

    gm = GeoMetrics()
    report = gm.check(
        "locations.csv",
        variables=["Landsat8_NDVI:NDVI"],
        lat_col="lat", lon_col="lon", timestamp_col="date",
    )
    # report["available"] and report["missing"] are lists of dicts

    df = gm.fetch(
        "locations.csv",
        variables=["Landsat8_NDVI:NDVI"],
    )
    # df is a pandas DataFrame with columns:
    # lat, lon, cell_id, source, parameter, timestamp, value, level, aggregated
"""

from __future__ import annotations

from typing import Union

import pandas as pd
from sqlalchemy import inspect as sa_inspect, text

from geometrics.backends import get_backend
from geometrics.backends.levels import native_level_for_backend
from geometrics.catalog import CATALOG
from geometrics.config import GeoMetricsConfig, load_config
from geometrics.instrumentation import timed
from geometrics.store.db import get_engine
from geometrics.store.query import (
    DataQuery, VariableSpec, check_availability, clear_observations, fetch, resolve,
)
from geometrics.store.schema import initialize_db, source_table_name


class GeoMetrics:
    """
    Main entry point for the GeoMetrics package.

    Loads config from ~/.geometrics/config.json by default.
    Pass a GeoMetricsConfig object to override.
    """

    def __init__(self, config: GeoMetricsConfig | None = None):
        self.config = config or load_config()
        self.engine = get_engine(self.config.db_url)
        self.backend = get_backend(self.config.backend)

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def init_gee(self) -> None:
        """
        Initialise Earth Engine against the configured project.

        A bare ee.Initialize() picks up the gcloud application-default project,
        which is rarely the one the EE account can use, so pass gee_project
        explicitly when it is configured.
        """
        import ee

        if getattr(ee.data, "_credentials", None) is not None:
            return  # already initialised
        if self.config.gee_project:
            ee.Initialize(project=self.config.gee_project)
        else:
            ee.Initialize()

    def check(
        self,
        locations: Union[str, pd.DataFrame],
        variables: list[str],
        lat_col: str = "latitude",
        lon_col: str = "longitude",
        timestamp_col: str = "timestamp",
    ) -> dict:
        """
        Check which (location, variable, timestamp) combinations have data in the store.

        locations: CSV file path or DataFrame with columns: lat, lon, timestamp.
        variables: list of "Source:parameter" strings, e.g. ["Landsat8_NDVI:NDVI"].

        Returns {"available": [...], "missing": [...]} where each item is a dict
        with keys: lat, lon, cell_id, source, parameter, timestamp, requested_level,
        native_level, variable_id, and (for missing) reason.
        """
        data = self._load(locations)
        rows = self._extract(data, lat_col, lon_col, timestamp_col)
        query = DataQuery(
            rows=rows,
            variables=[VariableSpec.parse(v) for v in variables],
        )
        with timed("check", locations=len(rows), variables=variables) as rec:
            try:
                items = resolve(self.engine, self.backend, query)
            except ValueError as e:
                print(f"[gm.check] {e}")
                return {"available": [], "missing": []}
            report = check_availability(self.engine, self.backend, items)
            rec["available"] = len(report["available"])
            rec["missing"] = len(report["missing"])
        return report

    def fetch(
        self,
        locations: Union[str, pd.DataFrame],
        variables: list[str],
        lat_col: str = "latitude",
        lon_col: str = "longitude",
        timestamp_col: str = "timestamp",
        output_format: str = "wide",
        preserve_rows: bool = True,
        preserve_cols: bool = True,
        include_metadata: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch stored observations for the given locations and variables.

        output_format: "wide" (default) — one column per variable joined back to
                       input; "long" — one row per (location, variable).
        preserve_rows: True (default) — all input rows appear; variables with no DB
                       record are NaN. False — rows with no DB record at all are
                       dropped; rows with a real NULL value in the DB are kept.
        preserve_cols: True (default) — extra columns from the input are carried
                       through. False — output contains only lat/lon/timestamp +
                       requested variable columns.
        include_metadata: long format only — include cell_id, level, aggregated,
                          resolved_timestamp columns.
        """
        data = self._load(locations)
        rows = self._extract(data, lat_col, lon_col, timestamp_col)
        query = DataQuery(
            rows=rows,
            variables=[VariableSpec.parse(v) for v in variables],
        )
        with timed("fetch", locations=len(rows), variables=variables,
                   output_format=output_format) as rec:
            try:
                items = resolve(self.engine, self.backend, query)
            except ValueError as e:
                print(f"[gm.fetch] {e}")
                return pd.DataFrame()
            long_df = fetch(self.engine, self.backend, items)
            rec["rows_returned"] = len(long_df)

        if output_format == "long":
            return self._format_long(
                long_df, data, lat_col, lon_col, timestamp_col,
                preserve_rows, preserve_cols, include_metadata,
            )
        return self._format_wide(
            long_df, data, lat_col, lon_col, timestamp_col,
            preserve_rows, preserve_cols,
        )

    def gee_submit(
        self,
        missing_items: list[dict],
        gdrive_folder: str,
        batch_size: int = 1000,
    ) -> list[int]:
        """
        Submit GEE extraction jobs for items returned by check()["missing"].

        gdrive_folder: root Drive folder. Each source gets a subfolder inside it,
            e.g. gdrive_folder/Landsat_NDVI/batch_001.csv
        batch_size: max features per GEE export task (default 1000).
        Returns a list of local job_ids in submission order.
        """
        from geometrics.extraction.dispatch import dispatch

        self.init_gee()
        if not missing_items:
            print("Nothing to submit.")
            return []

        with timed("gee_submit", items=len(missing_items), batch_size=batch_size) as rec:
            job_ids = dispatch(
                self.engine, self.config, self.backend, missing_items,
                gdrive_folder=gdrive_folder,
                batch_size=batch_size,
            )
            rec["jobs"] = len(job_ids)
        print(f"\nSubmitted {len(job_ids)} job(s). Track with: gm.jobs()")
        return job_ids

    def jobs(self, status: str | None = None) -> pd.DataFrame:
        """
        List submitted jobs, optionally filtered by status.

        Status values: PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, EXPIRED, INGESTED
        """
        from geometrics.store.jobs import list_jobs
        rows = list_jobs(self.engine, status=status)
        df = pd.DataFrame([r._asdict() for r in rows])
        return df.drop(columns=["ingested_at"], errors="ignore")

    def ingest(self, gdrive_folder: str) -> dict:
        """
        Ingest GEE-exported CSVs from a Drive folder into the database.

        gdrive_folder: the folder name passed to gee_submit (e.g. "extract-20").
            Resolved against gdrive_base from config to find the local path.

        Scans for all *.csv files in the folder, parses variable_name as
        "SourceName:variable", and inserts rows into observations.

        Returns {filename: rows_inserted} for each file processed.
        """
        from pathlib import Path
        from geometrics.store.ingest import ingest_folder

        folder_path = Path(self.config.gdrive_base.strip().replace("\\ ", " ")) / gdrive_folder
        with timed("ingest", folder=gdrive_folder) as rec:
            result = ingest_folder(
                self.engine, folder_path, backend_name=self.config.backend,
                gdrive_folder=gdrive_folder,
            )
            rec["files"] = len(result)
            rec["rows_inserted"] = sum(result.values())
        return result

    def register_local_source(
        self,
        name: str,
        operation: str,
        variable_defs: list[dict] | None = None,
        temporal_granularity: str | None = None,
        native_level: int = 13,
        pixel_resolution_m: int = 200,
    ) -> None:
        """
        Register a local (non-GEE) source in the database.

        operation: "nearest_distance", "inside", or "attribute_lookup".
            - "nearest_distance" and "inside" use fixed variable names; no extra args needed.
            - "attribute_lookup" requires variable_defs and temporal_granularity.
        variable_defs: list of {"name": ..., "unit": ...} — required for attribute_lookup.
        temporal_granularity: "static" (default for nearest_distance/inside), "annual",
            "month", "day", etc.  Required for attribute_lookup.
        native_level: HierGP standard level (default 13 = 100 m).
        pixel_resolution_m: used by the viewer for circle sizing.
        """
        from geometrics.extraction.base import ensure_source

        builtin_vars = {
            "nearest_distance": (
                [{"name": "nearest_distance", "unit": "km"}], "static"
            ),
            "inside": (
                [{"name": "inside", "unit": "binary"}], "static"
            ),
        }

        if operation in builtin_vars:
            resolved_vars, resolved_tg = builtin_vars[operation]
            resolved_vars = variable_defs or resolved_vars
            resolved_tg = temporal_granularity or resolved_tg
        elif operation == "attribute_lookup":
            if not variable_defs:
                raise ValueError(
                    "attribute_lookup requires variable_defs, e.g. "
                    "[{'name': 'pm25', 'unit': 'µg/m³'}, ...]"
                )
            if not temporal_granularity:
                raise ValueError(
                    "attribute_lookup requires temporal_granularity, e.g. 'annual'."
                )
            resolved_vars = variable_defs
            resolved_tg = temporal_granularity
        else:
            raise ValueError(
                f"Unknown operation {operation!r}. "
                "Choose from: 'nearest_distance', 'inside', 'attribute_lookup'."
            )

        _, is_new = ensure_source(
            engine=self.engine,
            name=name,
            native_level=native_level,
            pixel_resolution_m=pixel_resolution_m,
            source_temporal_granularity=None,
            temporal_granularity=resolved_tg,
            variable_defs=resolved_vars,
        )
        status = "Registered" if is_new else "Already registered"
        print(f"{status} local source '{name}' (operation={operation}).")

    def compute_local(
        self,
        locations: Union[str, pd.DataFrame],
        source: str,
        reference_data,
        operation: str,
        lat_col: str = "latitude",
        lon_col: str = "longitude",
        timestamp_col: str = "timestamp",
        value_columns: dict | None = None,
        temporal_range: tuple | None = None,
        ref_lat_col: str = "latitude",
        ref_lon_col: str = "longitude",
    ) -> int:
        """
        Run a local spatial operation and write results to the database.

        locations: CSV path or DataFrame — the locations you want values for.
        source: name of a source registered with gm.register_local_source().
        reference_data: GeoDataFrame (nearest_distance / inside) or plain DataFrame
            (attribute_lookup) containing the reference features.
        operation: "nearest_distance", "inside", or "attribute_lookup".

        attribute_lookup only:
          value_columns: {variable_name: [year_col, ...]} mapping each registered
              variable to the list of year columns in reference_data.
          temporal_range: (min_year, max_year) for clipping out-of-range timestamps.
          ref_lat_col / ref_lon_col: lat/lon column names in reference_data.

        Returns the number of rows inserted.
        """
        from geometrics.local.compute import (
            compute_nearest_distance, compute_inside, compute_attribute_lookup,
        )
        from sqlalchemy import select as sa_select
        from geometrics.store.schema import sources as sources_table

        data = self._load(locations)

        with self.engine.connect() as conn:
            src_row = conn.execute(
                sa_select(sources_table.c.native_level).where(sources_table.c.name == source)
            ).fetchone()
        if src_row is None:
            raise ValueError(
                f"Source {source!r} not found. Run gm.register_local_source() first."
            )
        native_level = src_row.native_level

        if operation == "nearest_distance":
            return compute_nearest_distance(
                engine=self.engine, backend=self.backend, locations_df=data,
                source_name=source, geodataframe=reference_data,
                lat_col=lat_col, lon_col=lon_col,
                native_level=native_level, backend_name=self.config.backend,
            )
        if operation == "inside":
            return compute_inside(
                engine=self.engine, backend=self.backend, locations_df=data,
                source_name=source, geodataframe=reference_data,
                lat_col=lat_col, lon_col=lon_col,
                native_level=native_level, backend_name=self.config.backend,
            )
        if operation == "attribute_lookup":
            if not value_columns:
                raise ValueError("attribute_lookup requires value_columns.")
            if not temporal_range:
                raise ValueError("attribute_lookup requires temporal_range=(min_year, max_year).")
            return compute_attribute_lookup(
                engine=self.engine, backend=self.backend, locations_df=data,
                source_name=source, dataframe=reference_data,
                lat_col=lat_col, lon_col=lon_col,
                native_level=native_level, backend_name=self.config.backend,
                value_columns=value_columns, temporal_range=temporal_range,
                ref_lat_col=ref_lat_col, ref_lon_col=ref_lon_col,
                timestamp_col=timestamp_col,
            )
        raise ValueError(
            f"Unknown operation {operation!r}. "
            "Choose from: 'nearest_distance', 'inside', 'attribute_lookup'."
        )

    def clear(self, source: str) -> None:
        """
        Drop all observations for a source.

        Source and variable registrations are kept intact — you can
        re-ingest corrected data without re-running init_db.
        """
        clear_observations(self.engine, source)
        print(f"Cleared all observations for {source}.")

    def check_status(self) -> dict:
        """
        Poll GEE for all active (PENDING/RUNNING) jobs and update local status.

        Returns a summary dict {status: count}.
        """
        from geometrics.store.jobs import check_status

        self.init_gee()
        with timed("check_status") as rec:
            summary = check_status(self.engine)
            rec["summary"] = summary
        return summary

    def register_sources(self) -> None:
        """Register all catalog sources and their variables in the database."""
        from geometrics.extraction.base import ensure_source

        added, existing = [], []
        for spec in CATALOG.values():
            _, is_new = ensure_source(
                engine=self.engine,
                name=spec["name"],
                native_level=native_level_for_backend(
                    spec["native_level"], self.config.backend
                ),
                # Local sources have no GEE pixel or source cadence of their own.
                pixel_resolution_m=spec.get("pixel_resolution_m", 200),
                source_temporal_granularity=spec.get("source_temporal_granularity"),
                temporal_granularity=spec["temporal_granularity"],
                variable_defs=spec["variables"],
            )
            (added if is_new else existing).append(spec["name"])

        if added:
            print(f"Registered {len(added)} new source(s): {added}")
        if existing:
            print(f"Already registered ({len(existing)}): {existing}")

    def init_db(self) -> None:
        """Create all database tables and register all known sources."""
        initialize_db(self.engine)
        print("Database initialized.")
        self.register_sources()

    def reset_db(self, *, confirm: bool = False) -> None:
        """
        Drop all per-source observation tables and recreate the schema.

        Leaves sources, variables, cells, and jobs intact.
        Pass confirm=True to skip the prompt, e.g. reset_db(confirm=True).
        """
        if not confirm:
            ans = input("Drop all observation tables? All observation data will be lost. [y/N] ")
            if ans.strip().lower() != "y":
                print("Aborted.")
                return

        existing = set(sa_inspect(self.engine).get_table_names())
        dropped = []
        with self.engine.begin() as conn:
            for src_name in CATALOG:
                table = source_table_name(src_name)
                if table in existing:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                    dropped.append(table)
        if dropped:
            print(f"Dropped {len(dropped)} observation table(s).")

        initialize_db(self.engine)
        self.register_sources()
        print("Reset complete.")

    def show_config(self) -> dict:
        """Return the current configuration as a plain dict."""
        import dataclasses
        return dataclasses.asdict(self.config)

    @staticmethod
    def list_sources() -> list[dict]:
        """
        Return catalog metadata for all implemented sources.

        Each dict includes: name, description, gee_collection, pixel_resolution_m,
        native_level, source_temporal_granularity, temporal_granularity, variables.
        """
        return list(CATALOG.values())

    @staticmethod
    def list_variables(source: str) -> list[dict]:
        """
        Return variable metadata for a single source.

        Each dict includes: name, unit, description.
        Raises ValueError if the source is not in the catalog.
        """
        from geometrics.catalog import get_source
        return get_source(source)["variables"]

    @staticmethod
    def configure(
        db_url: str | None = None,
        gdrive_base: str | None = None,
        backend: str | None = None,
        gee_project: str | None = None,
    ) -> "GeoMetrics":
        """
        Save a new config and return a GeoMetrics instance pointing at it.

        Only the fields you pass are updated; the rest keep their current values.

        Example::

            gm = GeoMetrics.configure(db_url="postgresql://user:pass@localhost/mydb")
        """
        from geometrics.config import DEFAULT_CONFIG_PATH, save_config
        current = load_config()
        def _clean_path(p: str) -> str:
            return p.strip().replace("\\ ", " ")

        updated = GeoMetricsConfig(
            db_url=db_url if db_url is not None else current.db_url,
            gdrive_base=(
                _clean_path(gdrive_base) if gdrive_base is not None else current.gdrive_base
            ),
            backend=backend if backend is not None else current.backend,
            gee_project=(
                gee_project if gee_project is not None else current.gee_project
            ),
        )
        save_config(updated, DEFAULT_CONFIG_PATH)
        import dataclasses
        print(f"Config saved to {DEFAULT_CONFIG_PATH}")
        print(dataclasses.asdict(updated))
        return GeoMetrics(updated)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_wide(
        long_df: pd.DataFrame,
        input_df: pd.DataFrame,
        lat_col: str,
        lon_col: str,
        timestamp_col: str,
        preserve_rows: bool,
        preserve_cols: bool,
    ) -> pd.DataFrame:
        if long_df.empty:
            if preserve_rows:
                result = input_df.copy()
                return result if preserve_cols else result[[lat_col, lon_col, timestamp_col]]
            return pd.DataFrame()

        record_any = (
            long_df.groupby(["lat", "lon", "timestamp"])["_has_record"]
            .any()
            .reset_index()
        )

        # Two sources can share a variable name (Landsat_NDVI:NDVI and
        # MODIS_NDVI:NDVI both call it "NDVI"). Pivoting on the bare parameter
        # collapses them into one column and silently drops a source, so
        # qualify the ambiguous ones as "Source:parameter".
        sources_per_param = long_df.groupby("parameter")["source"].nunique()
        ambiguous = set(sources_per_param[sources_per_param > 1].index)
        long_df = long_df.copy()
        long_df["_column"] = [
            f"{src}:{param}" if param in ambiguous else param
            for src, param in zip(long_df["source"], long_df["parameter"])
        ]

        value_pivot = long_df.pivot_table(
            index=["lat", "lon", "timestamp"],
            columns="_column",
            values="value",
            aggfunc="first",
        ).reset_index()
        value_pivot.columns.name = None

        merged = value_pivot.merge(record_any, on=["lat", "lon", "timestamp"])
        if not preserve_rows:
            merged = merged[merged["_has_record"]]
        merged = merged.drop(columns=["_has_record"])
        merged = merged.rename(
            columns={"lat": lat_col, "lon": lon_col, "timestamp": timestamp_col}
        )

        result = input_df.merge(
            merged,
            on=[lat_col, lon_col, timestamp_col],
            how="left" if preserve_rows else "inner",
        )
        if not preserve_cols:
            extra = [c for c in input_df.columns if c not in [lat_col, lon_col, timestamp_col]]
            result = result.drop(columns=extra, errors="ignore")
        return result.reset_index(drop=True)

    @staticmethod
    def _format_long(
        long_df: pd.DataFrame,
        input_df: pd.DataFrame,
        lat_col: str,
        lon_col: str,
        timestamp_col: str,
        preserve_rows: bool,
        preserve_cols: bool,
        include_metadata: bool,
    ) -> pd.DataFrame:
        if not preserve_rows:
            long_df = long_df[long_df["_has_record"]].copy()

        drop_cols = ["_has_record"]
        if not include_metadata:
            drop_cols += ["cell_id", "level", "aggregated", "resolved_timestamp"]
        long_df = long_df.drop(columns=[c for c in drop_cols if c in long_df.columns])

        if preserve_cols:
            extra = [c for c in input_df.columns if c not in [lat_col, lon_col, timestamp_col]]
            if extra:
                input_extra = input_df[[lat_col, lon_col, timestamp_col] + extra].rename(
                    columns={lat_col: "lat", lon_col: "lon", timestamp_col: "timestamp"}
                )
                long_df = long_df.merge(input_extra, on=["lat", "lon", "timestamp"], how="left")

        return long_df.reset_index(drop=True)

    @staticmethod
    def _load(locations: Union[str, pd.DataFrame]) -> pd.DataFrame:
        if isinstance(locations, pd.DataFrame):
            return locations
        return pd.read_csv(locations)

    @staticmethod
    def _extract(
        data: pd.DataFrame,
        lat_col: str,
        lon_col: str,
        timestamp_col: str,
    ) -> list[tuple[float, float, str]]:
        required = [c for c in (lat_col, lon_col, timestamp_col) if c not in data.columns]
        if required:
            raise ValueError(
                f"Required column(s) not found in data: {required}. "
                f"Expected columns: {lat_col!r}, {lon_col!r}, {timestamp_col!r}."
            )
        return list(zip(data[lat_col], data[lon_col], data[timestamp_col].astype(str)))
