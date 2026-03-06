"""
DataSource registry for ProvenanceFlow ingestion adapters.

Add new sources here — they become available via config (pipeline_source setting)
without touching runner.py.
"""
from __future__ import annotations

from .base import DataSource
from .nasa_gistemp import NASAGISTEMPSource
from .local_csv import LocalCSVSource

SOURCE_REGISTRY: dict[str, type[DataSource]] = {
    "nasa_gistemp": NASAGISTEMPSource,
    "local_csv": LocalCSVSource,
}


def get_source(name: str, **kwargs) -> DataSource:
    """Instantiate a DataSource by registry name.

    Args:
        name: Registry key (e.g. "nasa_gistemp", "local_csv").
        **kwargs: Forwarded to the DataSource constructor.

    Raises:
        ValueError: If name is not in the registry.
    """
    if name not in SOURCE_REGISTRY:
        available = list(SOURCE_REGISTRY)
        raise ValueError(f"Unknown source: {name!r}. Available: {available}")
    return SOURCE_REGISTRY[name](**kwargs)
