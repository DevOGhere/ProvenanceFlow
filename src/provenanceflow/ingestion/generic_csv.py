"""
GenericCSVSource — reads ANY CSV file, no column-name assumptions.

Use this for any tabular dataset that is not NASA GISTEMP formatted.
Pairs with BasicValidator for domain-agnostic null-rate checks.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from .base import DataSource
from ..models import IngestionResult
from ..utils.checksums import sha256_file
from ..utils.identifiers import generate_pid


class GenericCSVSource(DataSource):
    """Ingests any CSV file using pandas read_csv with configurable options.

    Args:
        path:            Path to the CSV file.
        title:           Human-readable dataset name (used in PROV metadata).
        license:         License name or URI (used in PROV Dublin Core metadata).
        **read_csv_kwargs: Passed directly to pd.read_csv — encoding, sep, etc.
    """

    def __init__(self, path: Path | str, title: str | None = None,
                 license: str = "Unknown", **read_csv_kwargs) -> None:
        self._path = Path(path)
        self._title = title or self._path.stem
        self._license = license
        self._kwargs = read_csv_kwargs

    @property
    def source_id(self) -> str:
        return "generic_csv"

    @property
    def dataset_title(self) -> str:
        return self._title

    @property
    def dataset_license(self) -> str:
        return self._license

    def fetch(self) -> IngestionResult:
        if not self._path.exists():
            raise FileNotFoundError(f"CSV not found: {self._path}")
        df = pd.read_csv(self._path, **self._kwargs)
        return IngestionResult(
            source_url=f"file://{self._path.resolve()}",
            local_path=self._path,
            row_count=len(df),
            checksum_sha256=sha256_file(str(self._path)),
            dataset_pid=generate_pid("dataset"),
            ingest_timestamp=datetime.utcnow(),
        )
