"""Tests for date-range and dataset-id querying in query.py."""
import pytest
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.tracker import ProvenanceTracker
from src.provenanceflow.provenance.query import (
    get_by_date_range,
    get_by_dataset_id,
    list_runs,
)


@pytest.fixture
def tmp_store(tmp_path):
    return ProvenanceStore(db_path=str(tmp_path / 'test.db'))


@pytest.fixture
def tmp_csv(tmp_path):
    p = tmp_path / 'data.csv'
    p.write_text("Year,Jan\n1990,0.10\n")
    return str(p)


def _make_run(tmp_store, tmp_csv) -> tuple[str, str]:
    """Create one pipeline run and return (run_id, fair_identifier)."""
    t = ProvenanceTracker()
    t.track_ingestion('https://example.com/data.csv', tmp_csv, 5)
    t.finalize(tmp_store)
    # Extract fair:identifier from stored doc
    doc = tmp_store.get(t.run_id)
    fair_id = None
    for attrs in doc.get('entity', {}).values():
        if 'fair:identifier' in attrs:
            fair_id = attrs['fair:identifier']
            break
    return t.run_id, fair_id


def test_query_by_date_range_returns_all_runs(tmp_store, tmp_csv):
    """Runs with any created_at should be included in a wide date range."""
    _make_run(tmp_store, tmp_csv)
    _make_run(tmp_store, tmp_csv)
    runs = get_by_date_range(tmp_store, start='2000-01-01', end='2099-12-31')
    assert len(runs) == 2


def test_query_by_date_range_empty_result(tmp_store, tmp_csv):
    """Date range in the past should return nothing."""
    _make_run(tmp_store, tmp_csv)
    runs = get_by_date_range(tmp_store, start='2000-01-01', end='2000-01-02')
    assert runs == []


def test_query_by_date_range_returns_correct_structure(tmp_store, tmp_csv):
    """Each returned item should have run_id and created_at keys."""
    _make_run(tmp_store, tmp_csv)
    runs = get_by_date_range(tmp_store, start='2000-01-01', end='2099-12-31')
    assert 'run_id' in runs[0]
    assert 'created_at' in runs[0]


def test_query_by_dataset_id_finds_run(tmp_store, tmp_csv):
    """Should locate the run containing a specific dataset fair:identifier."""
    run_id, fair_id = _make_run(tmp_store, tmp_csv)
    assert fair_id is not None
    results = get_by_dataset_id(tmp_store, fair_id)
    assert len(results) == 1
    assert results[0]['run_id'] == run_id


def test_query_by_dataset_id_missing_returns_empty(tmp_store, tmp_csv):
    """Looking up a non-existent dataset_id should return empty list."""
    _make_run(tmp_store, tmp_csv)
    results = get_by_dataset_id(tmp_store, 'dataset_doesnotexist')
    assert results == []


def test_query_by_date_range_excludes_past_runs(tmp_store, tmp_csv):
    """Runs created NOW must not appear when querying a range ending in the past."""
    _make_run(tmp_store, tmp_csv)
    # Query a window that definitively ended before this test ran
    runs = get_by_date_range(tmp_store, start='2000-01-01', end='2000-12-31')
    assert runs == []


def test_query_by_dataset_id_multiple_runs(tmp_store, tmp_csv):
    """Multiple runs should not bleed into each other's dataset_id lookup."""
    run_id1, fair_id1 = _make_run(tmp_store, tmp_csv)
    run_id2, fair_id2 = _make_run(tmp_store, tmp_csv)
    assert fair_id1 != fair_id2  # each run gets its own PID
    assert get_by_dataset_id(tmp_store, fair_id1)[0]['run_id'] == run_id1
    assert get_by_dataset_id(tmp_store, fair_id2)[0]['run_id'] == run_id2
