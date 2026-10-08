"""
Level mapping between grid backends.

Source catalogs declare their native level as a HierGP standard level (15 = 25 m,
each step down doubling the edge). Those integers mean something entirely
different under H3, where resolution 13 is about 3 m, so a store built on H3
needs its own level per source.

The mapping below pairs each HierGP level with the H3 resolution whose average
cell area is closest to it. Areas are from https://h3geo.org/docs/core-library/restable/

    HierGP level  edge      area        H3 res   H3 avg area
    15            25 m      0.000625    11       0.00215 km2
    14            50 m      0.0025      11       0.00215 km2
    13            100 m     0.01        10       0.0150 km2
    12            200 m     0.04        9        0.105 km2
    11            400 m     0.16        9        0.105 km2
    10            800 m     0.64        8        0.737 km2
    9             1.6 km    2.56        7        5.16 km2
    8             3.2 km    10.24       7        5.16 km2
    7             6.4 km    40.96       6        36.1 km2
    6             12.8 km   163.8       5        252.9 km2
    5             25.6 km   655.4       4        1770 km2

The pairing is approximate by construction: square and hexagonal tilings of
different sizes never line up exactly, which is why B6 reports per-point
agreement rather than claiming equivalence.
"""

from __future__ import annotations

HIERGP_TO_H3: dict[int, int] = {
    15: 11, 14: 11, 13: 10, 12: 9, 11: 9, 10: 8,
    9: 7, 8: 7, 7: 6, 6: 5, 5: 4, 4: 3, 3: 2, 2: 1, 1: 0,
}


def h3_resolution_for_hiergp_level(level: int) -> int:
    """Return the H3 resolution closest in cell area to a HierGP standard level."""
    if level not in HIERGP_TO_H3:
        raise ValueError(
            f"No H3 resolution mapped for HierGP level {level!r}. "
            f"Known levels: {sorted(HIERGP_TO_H3)}"
        )
    return HIERGP_TO_H3[level]


def native_level_for_backend(hiergp_level: int, backend_name: str) -> int:
    """
    Translate a catalog's HierGP native level into the level for backend_name.

    hiergp: returned unchanged. h3: area-matched resolution.
    """
    if backend_name == "hiergp":
        return hiergp_level
    if backend_name == "h3":
        return h3_resolution_for_hiergp_level(hiergp_level)
    raise ValueError(f"Unknown backend: {backend_name!r}")
