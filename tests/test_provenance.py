import os
import tempfile
import textwrap
import pytest
import pandas as pd

from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.tracker import ProvenanceTracker
from src.provenanceflow.provenance.query import get_run, list_runs, get_entities, get_activities


# ── fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_store(tmp_path):
    return ProvenanceStore(db_path=str(tmp_path / 'test.db'))


@pytest.fixture
def tmp_csv(tmp_path):
    """Write a minimal CSV so tracker.track_ingestion can checksum it."""
    csv = tmp_path / 'gistemp.csv'
    csv.write_text("Year,Jan\n1990,0.10\n")
    return str(csv)


# ── ProvenanceStore ──────────────────────────────────────────────────────────

def test_store_save_and_get(tmp_store):
    tmp_store.save('run_001', {'test': True})
    result = tmp_store.get('run_001')
    assert result == {'test': True}


def test_store_get_missing_returns_none(tmp_store):
    assert tmp_store.get('nonexistent') is None


def test_store_list_runs(tmp_store):
    tmp_store.save('run_a', {})
    tmp_store.save('run_b', {})
    runs = tmp_store.list_runs()
    ids = [r['run_id'] for r in runs]
    assert 'run_a' in ids and 'run_b' in ids


def test_store_upsert_replaces(tmp_store):
    tmp_store.save('run_x', {'v': 1})
    tmp_store.save('run_x', {'v': 2})
    assert tmp_store.get('run_x')['v'] == 2


# ── ProvenanceTracker ────────────────────────────────────────────────────────

def test_tracker_run_id_assigned(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    assert t.run_id.startswith('run_')


def test_tracker_custom_run_id(tmp_store, tmp_csv):
    t = ProvenanceTracker(run_id='custom_run_99')
    assert t.run_id == 'custom_run_99'


def test_tracker_track_ingestion_returns_entity(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    entity = t.track_ingestion(
        source_url='https://example.com/data.csv',
        local_path=tmp_csv,
        row_count=10,
    )
    assert entity is not None


def test_tracker_finalize_persists(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    t.track_ingestion('https://example.com/data.csv', tmp_csv, 10)
    run_id = t.finalize(tmp_store)
    doc = tmp_store.get(run_id)
    assert doc is not None
    assert 'entity' in doc


def test_tracker_prov_json_has_agent(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    t.track_ingestion('https://example.com/data.csv', tmp_csv, 10)
    t.finalize(tmp_store)
    doc = tmp_store.get(t.run_id)
    assert 'agent' in doc
    agents = list(doc['agent'].keys())
    assert any('provenanceflow' in a for a in agents)


def test_tracker_full_pipeline(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    raw = t.track_ingestion('https://example.com/data.csv', tmp_csv, 100)
    validated = t.track_validation(
        input_entity=raw,
        rows_in=100,
        rows_passed=92,
        rejections={'range_check': 5, 'completeness_check': 3},
        warnings={'null_check': 4},
    )
    run_id = t.finalize(tmp_store)
    doc = tmp_store.get(run_id)
    # Should have at least 2 entities (raw + validated)
    assert len(doc['entity']) >= 2
    # Should have wasDerivedFrom relationship
    assert 'wasDerivedFrom' in doc


def test_tracker_prov_json_valid_structure(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    t.track_ingestion('https://example.com/data.csv', tmp_csv, 50)
    t.finalize(tmp_store)
    doc = tmp_store.get(t.run_id)
    for key in ('entity', 'activity', 'agent'):
        assert key in doc


# ── query helpers ────────────────────────────────────────────────────────────

def test_query_get_run(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    t.track_ingestion('https://example.com/data.csv', tmp_csv, 10)
    t.finalize(tmp_store)
    doc = get_run(tmp_store, t.run_id)
    assert doc is not None


def test_query_list_runs(tmp_store, tmp_csv):
    for _ in range(3):
        t = ProvenanceTracker()
        t.track_ingestion('https://example.com/data.csv', tmp_csv, 5)
        t.finalize(tmp_store)
    runs = list_runs(tmp_store)
    assert len(runs) == 3


def test_query_get_entities(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    t.track_ingestion('https://example.com/data.csv', tmp_csv, 5)
    t.finalize(tmp_store)
    entities = get_entities(tmp_store, t.run_id)
    assert len(entities) >= 1


def test_query_get_activities(tmp_store, tmp_csv):
    t = ProvenanceTracker()
    t.track_ingestion('https://example.com/data.csv', tmp_csv, 5)
    t.finalize(tmp_store)
    activities = get_activities(tmp_store, t.run_id)
    assert len(activities) >= 1
