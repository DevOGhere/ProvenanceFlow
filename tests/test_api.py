"""Tests for the FastAPI REST layer."""
import pytest
from fastapi.testclient import TestClient

from src.provenanceflow.api.app import app
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.api.routers.runs import _get_store


@pytest.fixture
def client(tmp_path):
    """TestClient with a fresh in-memory store, isolated per test."""
    tmp_db = str(tmp_path / "test.db")
    test_store = ProvenanceStore(db_path=tmp_db)
    app.dependency_overrides[_get_store] = lambda: test_store
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def populated_client(tmp_path):
    """TestClient with one pre-saved run."""
    tmp_db = str(tmp_path / "test.db")
    test_store = ProvenanceStore(db_path=tmp_db)
    test_store.save("run_001", {"entity": {"e1": {}}, "activity": {"a1": {}}})
    app.dependency_overrides[_get_store] = lambda: test_store
    yield TestClient(app), test_store
    app.dependency_overrides.clear()


# ── /health ──────────────────────────────────────────────────────────────────

def test_health_returns_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ── GET /runs ─────────────────────────────────────────────────────────────────

def test_list_runs_empty_store_returns_empty_list(client):
    r = client.get("/runs")
    assert r.status_code == 200
    assert r.json() == []


def test_list_runs_after_save_returns_one_record(populated_client):
    client, _ = populated_client
    r = client.get("/runs")
    assert r.status_code == 200
    runs = r.json()
    assert len(runs) == 1
    assert runs[0]["run_id"] == "run_001"


# ── GET /runs/{run_id} ────────────────────────────────────────────────────────

def test_get_run_not_found_returns_404(client):
    r = client.get("/runs/nonexistent_run")
    assert r.status_code == 404


def test_get_run_by_id_returns_prov_doc(populated_client):
    client, _ = populated_client
    r = client.get("/runs/run_001")
    assert r.status_code == 200
    doc = r.json()
    assert "entity" in doc
    assert "activity" in doc


# ── GET /runs/{run_id}/entities ───────────────────────────────────────────────

def test_get_entities_returns_entity_list(populated_client):
    client, _ = populated_client
    r = client.get("/runs/run_001/entities")
    assert r.status_code == 200
    assert isinstance(r.json(), list)
    assert len(r.json()) >= 1


def test_get_entities_not_found_returns_404(client):
    r = client.get("/runs/ghost/entities")
    assert r.status_code == 404


# ── GET /runs/{run_id}/activities ─────────────────────────────────────────────

def test_get_activities_returns_activity_list(populated_client):
    client, _ = populated_client
    r = client.get("/runs/run_001/activities")
    assert r.status_code == 200
    assert len(r.json()) >= 1


# ── GET /runs/search ──────────────────────────────────────────────────────────

def test_search_by_date_range_returns_results(populated_client):
    client, _ = populated_client
    r = client.get("/runs/search?start=2000-01-01&end=2099-12-31")
    assert r.status_code == 200
    assert len(r.json()) == 1


def test_search_by_date_range_excludes_out_of_range(populated_client):
    client, _ = populated_client
    r = client.get("/runs/search?start=1990-01-01&end=1999-12-31")
    assert r.status_code == 200
    assert r.json() == []


def test_search_missing_params_returns_400(client):
    r = client.get("/runs/search")
    assert r.status_code == 400


def test_search_only_start_returns_400(client):
    r = client.get("/runs/search?start=2025-01-01")
    assert r.status_code == 400


# ── GET /runs/{run_id}/rejections  &  GET /runs/{run_id}/report ──────────────

_PROV_WITH_REJECTIONS = {
    "prefix": {"pf": "http://provenanceflow.org/"},
    "entity": {
        "pf:dataset_x": {
            "dc:title": "Test",
            "fair:source_url": "http://example.com/t.csv",
            "fair:identifier": "uuid-x",
            "dc:format": "text/csv",
            "dc:license": "Public Domain",
            "pf:ingest_timestamp": "2026-03-07T10:00:00Z",
            "pf:row_count": {"$": 100, "type": "xsd:integer"},
            "pf:checksum_sha256": "abc123",
        },
    },
    "activity": {
        "pf:validate_x": {
            "pf:rows_in":        {"$": 100, "type": "xsd:integer"},
            "pf:rows_passed":    {"$": 98,  "type": "xsd:integer"},
            "pf:rows_rejected":  {"$": 2,   "type": "xsd:integer"},
            "pf:rejection_rate": {"$": 0.02, "type": "xsd:double"},
            "pf:rules_applied":  "range_check",
        },
    },
    "agent": {},
    "wasGeneratedBy": {},
    "used": {},
    "wasDerivedFrom": {},
    "wasAssociatedWith": {},
}

_SAMPLE_REJECTIONS = [
    {
        "rule": "range_check",
        "severity": "hard_rejection",
        "message": "Annual mean 9.9°C outside range [-3.0, 3.0]",
        "row_index": 42,
        "row_data": '{"Year": 1999, "J-D": 9.9}',
    },
    {
        "rule": "null_check",
        "severity": "hard_rejection",
        "message": "Missing values in columns: ['Jan', 'Feb']",
        "row_index": 57,
        "row_data": '{"Year": 2001, "Jan": null}',
    },
]


@pytest.fixture
def run_with_rejections_client(tmp_path):
    """TestClient with a run that has two saved rejections."""
    tmp_db = str(tmp_path / "rej.db")
    test_store = ProvenanceStore(db_path=tmp_db)
    test_store.save("run-rej-001", _PROV_WITH_REJECTIONS)
    test_store.save_rejections("run-rej-001", _SAMPLE_REJECTIONS)
    app.dependency_overrides[_get_store] = lambda: test_store
    yield TestClient(app), test_store
    app.dependency_overrides.clear()


def test_get_rejections_returns_list(run_with_rejections_client):
    client, _ = run_with_rejections_client
    r = client.get("/runs/run-rej-001/rejections")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert data[0]["rule"] == "range_check"


def test_get_rejections_empty_run(run_with_rejections_client):
    client, store = run_with_rejections_client
    store.save("run-no-rej", _PROV_WITH_REJECTIONS)
    r = client.get("/runs/run-no-rej/rejections")
    assert r.status_code == 200
    assert r.json() == []


def test_get_rejections_unknown_run_404(client):
    r = client.get("/runs/does-not-exist/rejections")
    assert r.status_code == 404


def test_get_report_returns_markdown(run_with_rejections_client):
    client, _ = run_with_rejections_client
    r = client.get("/runs/run-rej-001/report")
    assert r.status_code == 200
    assert "Run ID" in r.text


def test_get_report_unknown_run_404(client):
    r = client.get("/runs/does-not-exist/report")
    assert r.status_code == 404
