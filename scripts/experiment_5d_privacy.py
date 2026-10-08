"""
Experiment 5d — Privacy analysis (k-anonymity proxy).

For each grid level 1–13, treats every level-13 cell as a raw observation
and computes how many raw points fall into each coarser cell.  The average
count per coarser cell is a proxy for k-anonymity: higher k means an
individual cell is harder to link back to a specific raw location.

Usage:
    python scripts/experiment_5d_privacy.py [--out figures/]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from sqlalchemy import text

from geometrics import GeoMetrics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cell_size_m(level: int) -> float:
    """Approximate cell edge length in metres for a HierGP standard level."""
    base_m = 25
    internal = 15 + 1 - level   # standard → internal (1 = finest)
    return base_m * (2 ** (internal - 1))


def _level_label(level: int) -> str:
    m = _cell_size_m(level)
    if m >= 1_000:
        return f"L{level}\n({m/1000:.0f} km)"
    return f"L{level}\n({m:.0f} m)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(out_dir: Path) -> pd.DataFrame:
    gm = GeoMetrics()

    print("Loading level-13 cell coordinates from DB…")
    with gm.engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT h.x, h.y
            FROM hiergp_cells h
            JOIN cells c ON c.id = h.cell_pk
            WHERE c.level = 13
        """)).fetchall()

    coords = pd.DataFrame(rows, columns=["x", "y"])
    n_raw = len(coords)
    print(f"  {n_raw:,} level-13 cells (raw observations)\n")

    # For each target level, group level-13 cells into parent cells.
    # HierGP doubles resolution each step, so going from level 13 → level L
    # divides both x and y by 2^(13-L).
    records = []
    for level in range(13, 0, -1):
        shift = 13 - level
        factor = 2 ** shift

        px = coords["x"] // factor
        py = coords["y"] // factor
        cell_counts = (
            pd.DataFrame({"px": px, "py": py})
            .groupby(["px", "py"])
            .size()
        )

        k_vals = cell_counts.values
        records.append({
            "level":            level,
            "cell_size_m":      _cell_size_m(level),
            "n_unique_cells":   len(cell_counts),
            "reduction_factor": n_raw / len(cell_counts),
            "k_mean":           k_vals.mean(),
            "k_median":         float(np.median(k_vals)),
            "k_p95":            float(np.percentile(k_vals, 95)),
            "k_max":            int(k_vals.max()),
        })
        print(
            f"  Level {level:2d} ({_cell_size_m(level):>8.0f} m)  "
            f"unique cells: {len(cell_counts):>10,}  "
            f"mean k: {k_vals.mean():>10.1f}  "
            f"median k: {float(np.median(k_vals)):>8.1f}"
        )

    results = pd.DataFrame(records).sort_values("level")

    # -----------------------------------------------------------------------
    # Figure
    # -----------------------------------------------------------------------
    out_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(
        "Privacy analysis — k-anonymity proxy across grid levels\n"
        f"(n = {n_raw:,} level-13 cells as raw observations)",
        fontsize=12,
    )

    # Panel A: mean k vs level
    ax = axes[0]
    ax.plot(results["level"], results["k_mean"],   "o-", color="#2171b5", label="Mean k")
    ax.plot(results["level"], results["k_median"], "s--", color="#6baed6", label="Median k")
    ax.fill_between(
        results["level"],
        results["k_median"],
        results["k_p95"],
        alpha=0.15, color="#2171b5", label="Median → 95th pct",
    )
    ax.set_yscale("log")
    ax.set_xlabel("Grid level  (13 = 100 m, 1 ≈ 410 km)")
    ax.set_ylabel("k  (raw observations per coarse cell, log scale)")
    ax.set_title("(A)  Average k-anonymity vs. grid level")
    ax.legend(fontsize=9)
    ax.grid(True, which="both", alpha=0.3)
    ax.invert_xaxis()
    ax.xaxis.set_major_locator(mticker.MultipleLocator(2))

    # Panel B: unique cell count vs level
    ax2 = axes[1]
    ax2.bar(
        results["level"],
        results["n_unique_cells"],
        color="#41ab5d", edgecolor="white", linewidth=0.4,
    )
    ax2.set_yscale("log")
    ax2.set_xlabel("Grid level  (13 = 100 m, 1 ≈ 410 km)")
    ax2.set_ylabel("Unique cells (log scale)")
    ax2.set_title("(B)  Unique cells remaining at each level")
    ax2.grid(True, axis="y", which="both", alpha=0.3)
    ax2.invert_xaxis()
    ax2.xaxis.set_major_locator(mticker.MultipleLocator(2))

    plt.tight_layout()
    fig_path = out_dir / "experiment_5d_privacy.png"
    fig.savefig(fig_path, dpi=150, bbox_inches="tight")
    print(f"\nFigure saved → {fig_path}")

    csv_path = out_dir / "experiment_5d_privacy.csv"
    results.to_csv(csv_path, index=False, float_format="%.2f")
    print(f"Results table saved → {csv_path}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="figures", help="Output directory for figure and CSV")
    args = parser.parse_args()
    run(Path(args.out))
