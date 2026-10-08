from setuptools import setup, find_packages

setup(
    name='GeoMetrics',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        "earthengine-api",
        "hiergp",               # HierGP grid backend
        "h3>=4",                # H3 grid backend
        "pandas",
        "numpy",
        "sqlalchemy>=2",
        "psycopg2-binary",      # PostgreSQL store
        "geopandas",            # local sources
        "shapely>=2",
        "pyproj",               # geodesic distances
        "scipy",                # nearest-neighbour search
        "fastapi",              # map viewer
        "uvicorn",
        "tqdm",
    ],
    python_requires=">=3.10",
    description=(
        'Multi-resolution environmental data store: extract from Google Earth '
        'Engine onto a hierarchical grid, then query by location and time'
    ),
    author='Funso Oje',
    license='MIT',
)