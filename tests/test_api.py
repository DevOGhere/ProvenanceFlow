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
