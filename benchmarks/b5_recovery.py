"""B5 — ingest, kill at ~50%, restart, compare against a clean run.

Runs against a scratch SQLite store, never the deployment store: the point is
to interrupt a write mid-flight, which is not something to do to the 12.4 GiB
store the paper's numbers come from. Recovery behaviour is identical either
way — it rests on the same ON CONFLICT clauses and the same unique keys.

Procedure:
  1. clean run   — ingest all N files, checksum every table
  2. killed run  — fresh store, ingest in a child process, SIGKILL it at ~50%
  3. restart     — ingest the same folder again on the surviving store
  4. compare     — row counts and per-table checksums against the clean run
"""
import hashlib, json, os, signal, subprocess, sys, time
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

S = Path(__file__).parent
FOLDER = S / "b5_exports"
N_FILES = 40
ROWS_PER_FILE = 2000


def make_exports() -> None:
    """GEE-style export CSVs: 12 files, 500 rows each, deterministic."""
    FOLDER.mkdir(exist_ok=True)
    for f in range(N_FILES):
        rows = ["cell_id,source,timestamp,NDVI"]
        for i in range(ROWS_PER_FILE):
            cell = f"13:{-120000 - f * 1000 - i}|{48000 + f}"
            value = round(0.1 + ((f * ROWS_PER_FILE + i) % 800) / 1000, 4)
            rows.append(f"{cell},Landsat_NDVI,2021-06-15,{value}")
        (FOLDER / f"batch_{f:03d}.csv").write_text("\n".join(rows) + "\n")


def fresh_store(path: Path):
    path.unlink(missing_ok=True)
    sys.path.insert(0, "/Users/funsooje/Documents/GitHub/GeoMetrics")
    from geometrics import GeoMetrics
    from geometrics.config import GeoMetricsConfig
    gm = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{path}", backend="hiergp"))
    gm.init_db()
    return gm


def checksums(db_path: Path) -> dict:
    """Row count and a content hash per table, so a partial write is visible."""
    engine = create_engine(f"sqlite:///{db_path}")
    out = {}
    with engine.connect() as conn:
        tables = [r[0] for r in conn.execute(text(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")).fetchall()]
        for table in tables:
            if table.startswith("sqlite_"):
                continue
            rows = conn.execute(text(f"SELECT * FROM {table}")).fetchall()
            payload = "\n".join("|".join(str(v) for v in row) for row in sorted(
                rows, key=lambda r: tuple(str(v) for v in r)))
            out[table] = {"rows": len(rows),
                          "sha256": hashlib.sha256(payload.encode()).hexdigest()[:16]}
    engine.dispose()
    return out


INGEST_CHILD = '''
import sys
sys.path.insert(0, "/Users/funsooje/Documents/GitHub/GeoMetrics")
from geometrics import GeoMetrics
from geometrics.config import GeoMetricsConfig
gm = GeoMetrics(GeoMetricsConfig(db_url="sqlite:///%s", backend="hiergp"))
print(gm.ingest_folder_marker if False else "", flush=True)
from geometrics.store.ingest import ingest_folder
ingest_folder(gm.engine, "%s", backend_name="hiergp")
'''


def main() -> None:
    make_exports()
    result = {}

    # 1. clean run
    clean_db = S / "b5_clean.db"
    gm = fresh_store(clean_db)
    t0 = time.time()
    clean = gm.ingest_folder_result = None
    from geometrics.store.ingest import ingest_folder
    clean = ingest_folder(gm.engine, FOLDER, backend_name="hiergp")
    clean_seconds = time.time() - t0
    gm.engine.dispose()
    clean_sums = checksums(clean_db)
    result["clean"] = {"seconds": round(clean_seconds, 2),
                       "rows": sum(clean.values()), "files": len(clean),
                       "checksums": clean_sums}
    print(f"clean run: {sum(clean.values()):,} rows from {len(clean)} files "
          f"in {clean_seconds:.2f}s", flush=True)

    # 2. killed run — SIGKILL partway through
    killed_db = S / "b5_killed.db"
    fresh_store(killed_db).engine.dispose()
    script = S / "b5_child.py"
    script.write_text(INGEST_CHILD % (killed_db, FOLDER))
    proc = subprocess.Popen(
        ["/Users/funsooje/miniforge3/envs/gee/bin/python", str(script)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    # Wait until the child has actually written about half the rows, then kill.
    # A timed guess killed it during interpreter startup, before any write.
    target = sum(clean.values()) * 0.5
    probe = create_engine(f"sqlite:///{killed_db}")
    kill_t0 = time.time()
    observed = 0
    while time.time() - kill_t0 < 120:
        try:
            with probe.connect() as conn:
                observed = conn.execute(text(
                    "SELECT count(*) FROM obs_landsat_ndvi")).scalar() or 0
        except Exception:
            observed = 0
        if observed >= target:
            break
        time.sleep(0.05)
    probe.dispose()
    kill_after = time.time() - kill_t0
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()
    print(f"  child killed with {observed:,} rows visible (target {target:,.0f})", flush=True)
    partial_sums = checksums(killed_db)
    partial_rows = partial_sums.get("obs_landsat_ndvi", {}).get("rows", 0)
    print(f"killed after {kill_after:.2f}s with {partial_rows:,} rows written "
          f"({partial_rows / sum(clean.values()):.0%} of the clean total)", flush=True)
    result["killed"] = {"killed_after_seconds": round(kill_after, 2),
                        "rows_at_kill": partial_rows, "checksums": partial_sums}

    # 3. restart on the surviving store
    sys.path.insert(0, "/Users/funsooje/Documents/GitHub/GeoMetrics")
    from geometrics import GeoMetrics
    from geometrics.config import GeoMetricsConfig
    gm2 = GeoMetrics(GeoMetricsConfig(db_url=f"sqlite:///{killed_db}", backend="hiergp"))
    t1 = time.time()
    again = ingest_folder(gm2.engine, FOLDER, backend_name="hiergp")
    recovery_seconds = time.time() - t1
    gm2.engine.dispose()
    recovered_sums = checksums(killed_db)
    result["restart"] = {"seconds": round(recovery_seconds, 2),
                         "rows_inserted_on_restart": sum(again.values()),
                         "checksums": recovered_sums}
    print(f"restart: inserted {sum(again.values()):,} more rows in "
          f"{recovery_seconds:.2f}s", flush=True)

    # 4. compare
    identical = {t: recovered_sums.get(t, {}).get("sha256") == v["sha256"]
                 for t, v in clean_sums.items()}
    row_match = {t: recovered_sums.get(t, {}).get("rows") == v["rows"]
                 for t, v in clean_sums.items()}
    duplicates = (recovered_sums.get("obs_landsat_ndvi", {}).get("rows", 0)
                  - clean_sums["obs_landsat_ndvi"]["rows"])
    result["comparison"] = {"checksums_identical": identical,
                            "row_counts_identical": row_match,
                            "duplicate_rows": duplicates,
                            "all_identical": all(identical.values())}
    print("\nper-table checksum match:", identical)
    print("duplicate rows after recovery:", duplicates)
    print("ALL TABLES IDENTICAL TO CLEAN RUN:", all(identical.values()))

    json.dump(result, open(S / "b5_result.json", "w"), indent=2, default=str)


if __name__ == "__main__":
    main()
