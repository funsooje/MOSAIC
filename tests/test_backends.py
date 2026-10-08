"""
Tests for GridBackend implementations.

Each backend is exercised against the same checklist:
  1. point_to_cell returns a non-empty string
  2. cell_to_centroid returns coordinates within ~resolution of the input point
  3. cell_parent returns a cell one level coarser that is consistent with the original
  4. cell_children returns cells one level finer; original cell must be among them
  5. level_to_approx_resolution_km returns a positive number
  6. Parent/child relationship is internally consistent (child's parent == original)
"""

import math

from geometrics.backends.hiergp import HierGPBackend
from geometrics.backends.h3 import H3Backend


# Test coordinates: Seattle, WA
LAT, LON = 47.6062, -122.3321


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def run_backend_checks(backend, level, name):
    print(f"\n=== {name} | level={level} ===")

    # 1. point_to_cell
    cell_id = backend.point_to_cell(LAT, LON, level)
    assert isinstance(cell_id, str) and cell_id, "cell_id must be a non-empty string"
    print(f"  cell_id:   {cell_id}")

    # 2. cell_to_centroid — centroid should be within ~1 resolution of input
    clat, clon = backend.cell_to_centroid(cell_id)
    res_km = backend.level_to_approx_resolution_km(level)
    dist = haversine_km(LAT, LON, clat, clon)
    print(f"  centroid:  ({clat:.5f}, {clon:.5f})")
    print(f"  distance to centroid: {dist:.4f} km  (resolution: {res_km:.4f} km)")
    assert dist < res_km * 2, f"centroid too far from input point: {dist:.4f} km > {res_km*2:.4f} km"

    # 3. cell_parent
    parent_id = backend.cell_parent(cell_id)
    assert isinstance(parent_id, str) and parent_id, "parent_id must be a non-empty string"
    assert parent_id != cell_id, "parent must differ from child"
    print(f"  parent:    {parent_id}")

    # 4. cell_children — original cell must appear among parent's children
    children = backend.cell_children(parent_id)
    assert isinstance(children, list) and len(children) > 0, "children must be a non-empty list"
    assert cell_id in children, f"original cell {cell_id} not found in parent's children: {children}"
    print(f"  children of parent ({len(children)}): {children}")

    # 5. resolution
    assert res_km > 0, "resolution must be positive"
    print(f"  resolution: {res_km:.4f} km")

    # 6. child's parent round-trips back to the same parent
    for child in children:
        assert backend.cell_parent(child) == parent_id, \
            f"child {child} does not round-trip to parent {parent_id}"

    print("  OK")


def test_hiergp_backend():
    backend = HierGPBackend()
    for level in [5, 8, 11]:
        run_backend_checks(backend, level, "HierGP")


def test_h3_backend():
    backend = H3Backend()
    for level in [4, 7, 9]:
        run_backend_checks(backend, level, "H3")


def test_resolution_ordering_hiergp():
    backend = HierGPBackend()
    resolutions = [backend.level_to_approx_resolution_km(l) for l in [1, 5, 9, 12]]
    assert resolutions == sorted(resolutions, reverse=True), \
        "HierGP: higher level should mean finer (smaller) resolution"


def test_resolution_ordering_h3():
    backend = H3Backend()
    resolutions = [backend.level_to_approx_resolution_km(l) for l in [2, 5, 8, 12]]
    assert resolutions == sorted(resolutions, reverse=True), \
        "H3: higher level should mean finer (smaller) resolution"


if __name__ == "__main__":
    test_hiergp_backend()
    test_h3_backend()
    test_resolution_ordering_hiergp()
    test_resolution_ordering_h3()
    print("\nAll checks passed.")
