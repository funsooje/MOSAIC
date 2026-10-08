"""Grid backend registry — resolve a backend name from config to an instance."""

from __future__ import annotations

from geometrics.backends.base import GridBackend

_BACKENDS = {"hiergp", "h3"}


def get_backend(name: str) -> GridBackend:
    """
    Instantiate the grid backend named in config.

    Imports lazily so a store running on one backend does not need the other
    backend's library installed.
    """
    if name == "hiergp":
        from geometrics.backends.hiergp import HierGPBackend
        return HierGPBackend()
    if name == "h3":
        from geometrics.backends.h3 import H3Backend
        return H3Backend()
    raise ValueError(f"Unknown backend {name!r}. Available: {sorted(_BACKENDS)}")
