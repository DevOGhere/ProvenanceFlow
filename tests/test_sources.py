"""Tests for DataSource abstraction, LocalCSVSource, and the source registry."""
import textwrap
from pathlib import Path

import pytest

from src.provenanceflow.ingestion.base import DataSource
from src.provenanceflow.ingestion.local_csv import LocalCSVSource
from src.provenanceflow.ingestion.nasa_gistemp import NASAGISTEMPSource
from src.provenanceflow.ingestion import SOURCE_REGISTRY, get_source
from src.provenanceflow.models import IngestionResult
from src.provenanceflow.utils.checksums import sha256_file


FIXTURE_CSV = textwrap.dedent("""\
    Global-mean monthly, seasonal, and annual means, 1880-present
    Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D,D-N,DJF,MAM,JJA,SON
    1880,-.15,-.21,-.16,-.10,-.11,-.22,-.18,-.26,-.20,-.24,-.19,-.17,-.18,****,****,-.12,-.22,-.21
    1881,-.19,-.14,  .02,  .05,  .06,-.20,  .00,-.03,-.14,-.22,-.17,-.07,-.08,-.09,-.17,  .04,-.08,-.18
""")


@pytest.fixture
def fixture_csv(tmp_path):
    p = tmp_path / "gistemp.csv"
    p.write_text(FIXTURE_CSV)
    return p


# ── DataSource ABC ────────────────────────────────────────────────────────────

def test_data_source_is_abstract():
    """Cannot instantiate DataSource directly."""
    with pytest.raises(TypeError):
        DataSource()


def test_nasa_gistemp_source_implements_data_source_abc():
    assert issubclass(NASAGISTEMPSource, DataSource)


def test_local_csv_source_implements_data_source_abc():
    assert issubclass(LocalCSVSource, DataSource)


# ── LocalCSVSource ────────────────────────────────────────────────────────────

def test_local_csv_source_returns_ingestion_result(fixture_csv):
    source = LocalCSVSource(path=fixture_csv)
    result = source.fetch()
    assert isinstance(result, IngestionResult)


def test_local_csv_source_row_count(fixture_csv):
    source = LocalCSVSource(path=fixture_csv)
    result = source.fetch()
    assert result.row_count == 2


def test_local_csv_source_checksum_matches_file(fixture_csv):
    source = LocalCSVSource(path=fixture_csv)
    result = source.fetch()
    expected = sha256_file(str(fixture_csv))
    assert result.checksum_sha256 == expected


def test_local_csv_source_id():
    source = LocalCSVSource(path="/tmp/x.csv")
    assert source.source_id == "local_csv"


def test_local_csv_source_dataset_pid_format(fixture_csv):
    source = LocalCSVSource(path=fixture_csv)
    result = source.fetch()
    assert result.dataset_pid.startswith("dataset_")


def test_local_csv_source_nonexistent_raises():
    source = LocalCSVSource(path="/tmp/does_not_exist_xyz.csv")
    with pytest.raises(FileNotFoundError):
        source.fetch()


def test_local_csv_source_url_is_file_scheme(fixture_csv):
    source = LocalCSVSource(path=fixture_csv)
    result = source.fetch()
    assert result.source_url.startswith("file://")


# ── Source Registry ───────────────────────────────────────────────────────────

def test_source_registry_contains_nasa_gistemp():
    assert "nasa_gistemp" in SOURCE_REGISTRY


def test_source_registry_contains_local_csv():
    assert "local_csv" in SOURCE_REGISTRY


def test_get_source_unknown_name_raises_value_error():
    with pytest.raises(ValueError, match="Unknown source"):
        get_source("s3_bucket")


def test_get_source_local_csv_returns_instance(fixture_csv):
    source = get_source("local_csv", path=fixture_csv)
    assert isinstance(source, LocalCSVSource)


def test_get_source_nasa_gistemp_returns_instance():
    source = get_source("nasa_gistemp", url="https://example.com/data.csv")
    assert isinstance(source, NASAGISTEMPSource)
