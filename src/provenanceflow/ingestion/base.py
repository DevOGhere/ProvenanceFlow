"""
Abstract base class for all ProvenanceFlow data sources.

Any data source — HTTP endpoint, local file, S3 bucket, Zenodo API — must
implement DataSource.fetch() and return a typed IngestionResult.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import IngestionResult


class DataSource(ABC):
    """Contract that every ingestion adapter must satisfy."""

    @abstractmethod
    def fetch(self) -> IngestionResult:
        """Download or read data and return a standardised IngestionResult."""
        ...

    @property
    @abstractmethod
    def source_id(self) -> str:
        """Stable identifier for this source type (used in PROV lineage labels)."""
        ...
