"""Tests for GenericCSVSource and BasicValidator."""
import textwrap
import pytest
import pandas as pd

from src.provenanceflow.ingestion.generic_csv import GenericCSVSource
from src.provenanceflow.validation.basic_validator import BasicValidator
from src.provenanceflow.pipeline.runner import run_pipeline


# ── fixtures ──────────────────────────────────────────────────────────────────

GENERIC_CSV = textwrap.dedent("""\
    patient_id,age,blood_pressure,temperature
    1,45,120,36.7
    2,52,130,37.1
    3,,,
    4,38,110,36.5
""")


@pytest.fixture
def generic_csv(tmp_path):
    p = tmp_path / "patients.csv"
    p.write_text(GENERIC_CSV)
    return str(p)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "lineage.db")


# ── GenericCSVSource ──────────────────────────────────────────────────────────

def test_generic_csv_source_reads_any_csv(generic_csv):
    source = GenericCSVSource(generic_csv)
    result = source.fetch()
    assert result.row_count == 4   # all 4 rows (no GISTEMP-specific skipping)
    assert result.source_url.startswith("file://")


def test_generic_csv_source_checksum_is_reproducible(generic_csv):
    source = GenericCSVSource(generic_csv)
    r1 = source.fetch()
    r2 = source.fetch()
    assert r1.checksum_sha256 == r2.checksum_sha256
    assert len(r1.checksum_sha256) == 64


def test_generic_csv_source_title(generic_csv):
    source = GenericCSVSource(generic_csv, title="Patient Study 2026")
    assert source.dataset_title == "Patient Study 2026"


def test_generic_csv_source_license(generic_csv):
    source = GenericCSVSource(generic_csv, license="CC-BY-4.0")
    assert source.dataset_license == "CC-BY-4.0"


def test_generic_csv_source_id(generic_csv):
    source = GenericCSVSource(generic_csv)
    assert source.source_id == "generic_csv"


def test_generic_csv_source_missing_file():
    source = GenericCSVSource("/tmp/does_not_exist_xyz.csv")
    with pytest.raises(FileNotFoundError):
        source.fetch()


# ── BasicValidator ────────────────────────────────────────────────────────────

def test_basic_validator_no_flags_on_clean_df():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    v = BasicValidator()
    results = v.validate(df)
    assert results == []


def test_basic_validator_flags_high_null_row():
    df = pd.DataFrame({"a": [None, 2], "b": [None, 5], "c": [None, 6]})
    v = BasicValidator(row_null_threshold=0.5)
    results = v.validate(df)
    row_flags = [r for r in results if r.rule_name == "row_null_rate"]
    assert len(row_flags) == 1
    assert row_flags[0].row_index == 0


def test_basic_validator_hard_reject_mostly_null_row():
    df = pd.DataFrame({"a": [None], "b": [None], "c": [None], "d": [None], "e": [1]})
    v = BasicValidator(row_null_threshold=0.5)
    results = v.validate(df)
    hard = [r for r in results if r.severity == "hard_rejection"]
    assert len(hard) == 1   # 80% null → hard_rejection


def test_basic_validator_flags_high_null_column():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [None, None, None]})
    v = BasicValidator(col_null_threshold=0.3)
    results = v.validate(df)
    col_flags = [r for r in results if r.rule_name == "column_completeness"]
    assert len(col_flags) == 1
    assert "b" in col_flags[0].reason


def test_basic_validator_get_clean_removes_hard_rejections():
    df = pd.DataFrame({
        "a": [None, 2, None],
        "b": [None, 5, None],
        "c": [None, 6, None],
        "d": [None, 7, None],
        "e": [None, 8, 1],    # row 0 and 2: 80% null → hard_rejection
    })
    v = BasicValidator(row_null_threshold=0.5)
    results = v.validate(df)
    clean = v.get_clean(df, results)
    assert len(clean) == 1   # only row 1 survives


def test_basic_validator_rejection_summary():
    df = pd.DataFrame({"a": [None, 2], "b": [None, 5], "c": [None, 6],
                       "d": [None, 7], "e": [None, 8]})
    v = BasicValidator(row_null_threshold=0.5)
    results = v.validate(df)
    summary = v.rejection_summary(results)
    assert summary.get("row_null_rate", 0) == 1


# ── End-to-end pipeline with GenericCSVSource ─────────────────────────────────

def test_run_pipeline_with_generic_csv_source(generic_csv, tmp_db):
    source = GenericCSVSource(generic_csv, title="Patient Study")
    result = run_pipeline(source, db_path=tmp_db)
    assert result.run_id.startswith("run_")
    assert result.validation.rows_in == 4
    assert result.validation.rules_applied == BasicValidator.RULE_NAMES


def test_run_pipeline_generic_csv_prov_has_correct_title(generic_csv, tmp_db):
    from src.provenanceflow.provenance.store import ProvenanceStore
    from src.provenanceflow.utils.prov_helpers import get_ingestion_entity, unwrap
    source = GenericCSVSource(generic_csv, title="My Study")
    result = run_pipeline(source, db_path=tmp_db)
    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(result.run_id)
    _, ing = get_ingestion_entity(doc)
    assert ing.get("dc:title") == "My Study"
    assert ing.get("schema:name") == "My Study"
