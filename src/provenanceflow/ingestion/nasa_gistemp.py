from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from .base import DataSource
from ..models import IngestionResult
from ..utils.checksums import sha256_file
from ..utils.identifiers import generate_pid


MONTHLY_COLS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def download_gistemp(url: str, local_path: str) -> pd.DataFrame:
    """Download GISTEMP CSV from NASA and return parsed DataFrame."""
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    with open(local_path, 'wb') as f:
        f.write(response.content)
    return parse_gistemp(local_path)


def parse_gistemp(local_path: str) -> pd.DataFrame:
    """Parse a locally saved GISTEMP CSV into a clean DataFrame."""
    df = pd.read_csv(local_path, skiprows=1, na_values=['****'])
    # Drop any trailing summary rows that have non-integer Year values
    df = df[pd.to_numeric(df['Year'], errors='coerce').notna()].copy()
    df['Year'] = df['Year'].astype(int)
    return df.reset_index(drop=True)


class NASAGISTEMPSource(DataSource):
    """Downloads the NASA GISTEMP v4 global surface temperature dataset."""

    def __init__(self, url: str, output_dir: Path | str = "data/raw") -> None:
        self._url = url
        self._output_dir = Path(output_dir)

    @property
    def source_id(self) -> str:
        return "nasa_gistemp"

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
        local_path = self._output_dir / "gistemp_global.csv"
        df = download_gistemp(self._url, str(local_path))
        return IngestionResult(
            source_url=self._url,
            local_path=local_path,
            row_count=len(df),
            checksum_sha256=sha256_file(str(local_path)),
            dataset_pid=generate_pid("dataset"),
            ingest_timestamp=datetime.utcnow(),
        )
