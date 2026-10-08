"""B3 — viewer /api/data latency at four viewport sizes."""
import json, subprocess, time, urllib.request, gzip
from pathlib import Path

S = Path(__file__).parent
PORT = 8767
BASE = f"http://127.0.0.1:{PORT}"

VIEWPORTS = {
    "city (10x10 km)":      "-117.25,46.70,-117.10,46.80",
    "county (100x100 km)":  "-118.0,46.3,-117.0,47.2",
    "state (800x600 km)":   "-124.8,45.5,-116.9,49.0",
    "full extent":          None,
}
LABEL = "Landsat_NDVI/NDVI/2021-01-01"


def get(url):
    t0 = time.time()
    with urllib.request.urlopen(url, timeout=1800) as r:
        raw = r.read()
    seconds = time.time() - t0
    body = gzip.decompress(raw) if r.headers.get("Content-Encoding") == "gzip" else raw
    return seconds, len(raw), json.loads(body)


proc = subprocess.Popen(
    ["/Users/funsooje/miniforge3/envs/gee/bin/python", "-m", "uvicorn",
     "viewer.server:app", "--host", "127.0.0.1", "--port", str(PORT)],
    cwd="/Users/funsooje/Documents/GitHub/GeoMetrics",
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

t0 = time.time()
while time.time() - t0 < 600:
    try:
        urllib.request.urlopen(f"{BASE}/api/sources", timeout=20).read()
        break
    except Exception:
        time.sleep(2)
startup = time.time() - t0

results = {"startup_seconds": round(startup, 1), "viewports": {}}
print(f"viewer startup (prefetch over every obs table): {startup:.1f}s", flush=True)

for name, bbox in VIEWPORTS.items():
    url = f"{BASE}/api/data?source=Landsat_NDVI&variable=NDVI&timestamp=2021-01-01"
    if bbox:
        url += f"&bbox={bbox}"
    cold_s, nbytes, payload = get(url)          # first call: coord cache empty
    warm = []
    for _ in range(3):
        s, _, _ = get(url)
        warm.append(s)
    meta = payload["meta"]
    results["viewports"][name] = {
        "first_call_seconds": round(cold_s, 2),
        "cached_median_seconds": round(sorted(warm)[len(warm) // 2], 2),
        "cells_returned": meta.get("count"), "cells_available": meta.get("total"),
        "sampled_to_cap": meta.get("sampled"), "gzip_bytes": nbytes,
    }
    print(f"  {name:22s} first={cold_s:7.2f}s cached={sorted(warm)[1]:6.2f}s "
          f"cells={meta.get('count'):>7,} of {meta.get('total'):>9,} "
          f"gzip={nbytes/1024:.0f} KiB", flush=True)

proc.terminate(); proc.wait(timeout=30)
json.dump(results, open(S / "b3_viewer.json", "w"), indent=2)
