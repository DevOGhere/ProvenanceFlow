"""
Integration test for the full pipeline using local fixture data.
No network calls — download_gistemp is patched to use parse_gistemp directly.
"""
import ast
import textwrap
import pytest
from unittest.mock import patch

from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.ingestion.nasa_gistemp import parse_gistemp


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
    with patch('src.provenanceflow.pipeline.runner.download_gistemp',
               side_effect=lambda url, path: parse_gistemp(fixture_csv)):
        run_id = run_pipeline('https://example.com/data.csv', fixture_csv, db_path=tmp_db)
    assert run_id.startswith('run_')


def test_pipeline_provenance_stored(fixture_csv, tmp_db):
    with patch('src.provenanceflow.pipeline.runner.download_gistemp',
               side_effect=lambda url, path: parse_gistemp(fixture_csv)):
        run_id = run_pipeline('https://example.com/data.csv', fixture_csv, db_path=tmp_db)

    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(run_id)
    assert doc is not None
    assert 'entity' in doc
    assert 'activity' in doc
    assert 'agent' in doc


def test_pipeline_prov_has_lineage_relationships(fixture_csv, tmp_db):
    with patch('src.provenanceflow.pipeline.runner.download_gistemp',
               side_effect=lambda url, path: parse_gistemp(fixture_csv)):
        run_id = run_pipeline('https://example.com/data.csv', fixture_csv, db_path=tmp_db)

    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(run_id)
    # PROV lineage relations must be present
    assert 'wasDerivedFrom' in doc
    assert 'wasGeneratedBy' in doc
    assert 'used' in doc


def test_pipeline_list_runs(fixture_csv, tmp_db):
    with patch('src.provenanceflow.pipeline.runner.download_gistemp',
               side_effect=lambda url, path: parse_gistemp(fixture_csv)):
        run_pipeline('https://example.com/data.csv', fixture_csv, db_path=tmp_db)
        run_pipeline('https://example.com/data.csv', fixture_csv, db_path=tmp_db)

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
    with patch('src.provenanceflow.pipeline.runner.download_gistemp',
               side_effect=lambda url, path: parse_gistemp(fixture_csv)):
        run_id = run_pipeline('https://example.com/data.csv', fixture_csv, db_path=tmp_db)

    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(run_id)
    val = _get_validate_activity(doc)
    rejections_str = val.get('pf:rejections_by_rule', '{}')
    warnings_str = val.get('pf:warnings_by_rule', '{}')
    assert isinstance(ast.literal_eval(rejections_str), dict)
    assert isinstance(ast.literal_eval(warnings_str), dict)
