"""
LocalCSVSource — reads an already-present CSV file instead of hitting the network.

Primary use cases:
  - Tests (no HTTP mocking needed)
  - Replaying data from a local archive
  - CI pipelines where data is pre-staged
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .base import DataSource
from .nasa_gistemp import parse_gistemp
from ..models import IngestionResult
from ..utils.checksums import sha256_file
from ..utils.identifiers import generate_pid


class LocalCSVSource(DataSource):
    """Ingests a GISTEMP-formatted CSV from the local filesystem."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)

    @property
    def source_id(self) -> str:
        return "local_csv"

    @property
    def dataset_title(self) -> str:
        return "NASA GISTEMP v4 Global Surface Temperature"

    @property
    def dataset_license(self) -> str:
        return "https://data.giss.nasa.gov/gistemp/"

    @property
    def _parse(self):
        return parse_gistemp

    def fetch(self) -> IngestionResult:
        if not self._path.exists():
            raise FileNotFoundError(f"CSV not found: {self._path}")
        df = parse_gistemp(str(self._path))
        return IngestionResult(
            source_url=f"file://{self._path.resolve()}",
            local_path=self._path,
            row_count=len(df),
            checksum_sha256=sha256_file(str(self._path)),
            dataset_pid=generate_pid("dataset"),
            ingest_timestamp=datetime.utcnow(),
        )
