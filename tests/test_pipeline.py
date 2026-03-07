"""
Integration test for the full pipeline using local fixture data.
Uses LocalCSVSource — no HTTP mocking or network calls needed.
"""
import ast
import textwrap
import pytest

from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.ingestion.local_csv import LocalCSVSource
from src.provenanceflow.models import PipelineResult


FIXTURE_CSV = textwrap.dedent("""\
    Global-mean monthly, seasonal, and annual means, 1880-present
    Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D,D-N,DJF,MAM,JJA,SON
    1880,-.15,-.21,-.16,-.10,-.11,-.22,-.18,-.26,-.20,-.24,-.19,-.17,-.18,****,****,-.12,-.22,-.21
    1881,-.19,-.14,  .02,  .05,  .06,-.20,  .00,-.03,-.14,-.22,-.17,-.07,-.08,-.09,-.17,  .04,-.08,-.18
    1882,-.28,  .05,  .02, -.17,-.13,-.24,-.28,-.14,-.20,-.33,-.27,-.35,-.20,-.20,-.09, -.09,-.22,-.27
""")


@pytest.fixture
def fixture_csv(tmp_path):
    p = tmp_path / "gistemp.csv"
    p.write_text(FIXTURE_CSV)
    return str(p)


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "lineage.db")


def test_pipeline_returns_run_id(fixture_csv, tmp_db):
    result = run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)
    assert isinstance(result, PipelineResult)
    assert result.run_id.startswith('run_')


def test_pipeline_provenance_stored(fixture_csv, tmp_db):
    result = run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)

    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(result.run_id)
    assert doc is not None
    assert 'entity' in doc
    assert 'activity' in doc
    assert 'agent' in doc


def test_pipeline_prov_has_lineage_relationships(fixture_csv, tmp_db):
    result = run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)

    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(result.run_id)
    assert 'wasDerivedFrom' in doc
    assert 'wasGeneratedBy' in doc
    assert 'used' in doc


def test_pipeline_list_runs(fixture_csv, tmp_db):
    run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)
    run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)

    store = ProvenanceStore(db_path=tmp_db)
    runs = store.list_runs()
    assert len(runs) == 2


def _get_validate_activity(doc):
    for aid, attrs in doc.get('activity', {}).items():
        if 'validate_' in aid:
            return attrs
    return {}


def test_pipeline_rejections_by_rule_parseable(fixture_csv, tmp_db):
    """pf:rejections_by_rule must be parseable by ast.literal_eval (dashboard dependency)."""
    result = run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)

    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(result.run_id)
    val = _get_validate_activity(doc)
    rejections_str = val.get('pf:rejections_by_rule', '{}')
    warnings_str = val.get('pf:warnings_by_rule', '{}')
    assert isinstance(ast.literal_eval(rejections_str), dict)
    assert isinstance(ast.literal_eval(warnings_str), dict)


def test_pipeline_result_has_typed_ingestion(fixture_csv, tmp_db):
    """PipelineResult.ingestion must carry row_count and checksum."""
    result = run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)
    assert result.ingestion.row_count == 3
    assert len(result.ingestion.checksum_sha256) == 64


def test_pipeline_result_has_typed_validation(fixture_csv, tmp_db):
    """PipelineResult.validation must carry rows_in and rejection_rate."""
    result = run_pipeline(LocalCSVSource(fixture_csv), db_path=tmp_db)
    assert result.validation.rows_in == 3
    assert 0.0 <= result.validation.rejection_rate <= 1.0
