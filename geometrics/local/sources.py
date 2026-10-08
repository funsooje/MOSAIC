"""
Catalog entries for local (non-GEE) sources.

These are registered in the DB via gm.register_local_source() or the
migration script, not via GEE extraction.  They appear here so the viewer
can show descriptions and so query.py treats them as known sources rather
than unknown ones.
"""

PARKS_DISTANCE_SPEC: dict = {
    "name": "Parks_Distance",
    "description": "Distance (km) from each grid-cell centroid to the nearest protected area boundary (PADUS)",
    "native_level": 13,
    "temporal_granularity": "static",
    "variables": [
        {
            "name": "nearest_distance",
            "unit": "km",
            "description": "Distance to nearest park or protected area boundary; 0 if centroid is inside",
        },
    ],
}

WALKABILITY_SPEC: dict = {
    "name": "Walkability",
    "description": "EPA National Walkability Index score (1–20) at the census block-group level",
    "native_level": 13,
    "temporal_granularity": "static",
    "variables": [
        {
            "name": "natwalkind",
            "unit": "score",
            "description": "National Walkability Index: higher values indicate more walkable areas",
        },
    ],
}

CACES_AIR_SPEC: dict = {
    "name": "CACES_Air",
    "description": "CACES land-use regression estimates of ambient PM2.5 and NO2 concentrations, annual",
    "native_level": 13,
    "temporal_granularity": "annual",
    "variables": [
        {
            "name": "pm25",
            "unit": "µg/m³",
            "description": "Annual mean PM2.5 concentration (CACES block-level model)",
        },
        {
            "name": "no2",
            "unit": "ppb",
            "description": "Annual mean NO2 concentration (CACES block-level model)",
        },
    ],
}
