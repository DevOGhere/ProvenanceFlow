"""Tests for Sprint 6 — rejected-row persistence and retrieval."""
from __future__ import annotations

import json
import pytest
import pandas as pd

from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.validation.rules import ValidationResult as RuleResult
from src.provenanceflow.validation.validator import collect_rejected_rows


# ── collect_rejected_rows unit tests ─────────────────────────────────────────

@pytest.fixture
def simple_df():
    return pd.DataFrame({
        "Year": [1980, 1981, 1982],
        "J-D":  [0.1,  9.9,  0.2],
        "Jan":  [0.5,  None, 0.3],
    })


def make_hard_rejection(rule, row_index, reason="bad value"):
    return RuleResult(
        passed=False,
        rule_name=rule,
        severity="hard_rejection",
        row_index=row_index,
        value=None,
        reason=reason,
    )


def make_warning(rule, row_index, reason="minor issue"):
    return RuleResult(
        passed=False,
        rule_name=rule,
        severity="warning",
        row_index=row_index,
        value=None,
        reason=reason,
    )


def make_dataset_level(rule, reason="dataset-level issue"):
    return RuleResult(
        passed=False,
        rule_name=rule,
        severity="hard_rejection",
        row_index=None,
        value=None,
        reason=reason,
    )


def test_collect_rejected_rows_returns_only_hard_rejections(simple_df):
    results = [
        make_hard_rejection("range_check", 1, "Annual mean 9.9 out of range"),
        make_warning("null_check", 0, "missing one month"),
    ]
    rejected = collect_rejected_rows(simple_df, results)
    assert len(rejected) == 1
    assert rejected[0]["rule"] == "range_check"
    assert rejected[0]["severity"] == "hard_rejection"


def test_collect_rejected_rows_excludes_none_row_index(simple_df):
    results = [
        make_dataset_level("temporal_continuity", "gap in years"),
        make_hard_rejection("range_check", 1, "out of range"),
    ]
    rejected = collect_rejected_rows(simple_df, results)
    assert len(rejected) == 1
    assert rejected[0]["row_index"] == 1


def test_collect_rejected_rows_row_data_is_valid_json(simple_df):
    results = [make_hard_rejection("range_check", 1, "Annual mean 9.9 out of range")]
    rejected = collect_rejected_rows(simple_df, results)
    assert len(rejected) == 1
    row_data = json.loads(rejected[0]["row_data"])
    assert row_data["Year"] == 1981


def test_collect_rejected_rows_empty_results(simple_df):
    assert collect_rejected_rows(simple_df, []) == []


def test_collect_rejected_rows_all_warnings(simple_df):
    results = [make_warning("null_check", 0), make_warning("null_check", 2)]
    assert collect_rejected_rows(simple_df, results) == []


def test_collect_rejected_rows_bad_row_index_handled(simple_df):
    # row_index 999 doesn't exist — should produce empty row_data dict
    results = [make_hard_rejection("range_check", 999, "out of range")]
    rejected = collect_rejected_rows(simple_df, results)
    assert len(rejected) == 1
    assert json.loads(rejected[0]["row_data"]) == {}


def test_collect_rejected_rows_message_field(simple_df):
    results = [make_hard_rejection("range_check", 0, "Annual mean 5.5 out of range")]
    rejected = collect_rejected_rows(simple_df, results)
    assert rejected[0]["message"] == "Annual mean 5.5 out of range"


# ── ProvenanceStore.save_rejections / get_rejections ─────────────────────────

SAMPLE_REJECTIONS = [
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
def store(tmp_path):
    return ProvenanceStore(db_path=str(tmp_path / "test.db"))


def test_save_and_get_rejections_round_trip(store):
    store.save("run-abc", {"entity": {}, "activity": {}})
    store.save_rejections("run-abc", SAMPLE_REJECTIONS)
    result = store.get_rejections("run-abc")
    assert len(result) == 2
    assert result[0]["rule"] == "range_check"
    assert result[0]["row_index"] == 42
    assert result[1]["rule"] == "null_check"
    assert result[1]["row_index"] == 57


def test_get_rejections_returns_empty_for_unknown_run(store):
    assert store.get_rejections("no-such-run") == []


def test_get_rejections_returns_empty_when_none_saved(store):
    store.save("run-xyz", {"entity": {}, "activity": {}})
    assert store.get_rejections("run-xyz") == []


def test_save_rejections_is_idempotent(store):
    store.save("run-idem", {"entity": {}, "activity": {}})
    store.save_rejections("run-idem", SAMPLE_REJECTIONS)
    store.save_rejections("run-idem", SAMPLE_REJECTIONS)  # second call
    result = store.get_rejections("run-idem")
    assert len(result) == 2  # not doubled


def test_save_rejections_empty_list(store):
    store.save("run-empty", {"entity": {}, "activity": {}})
    store.save_rejections("run-empty", [])
    assert store.get_rejections("run-empty") == []


def test_get_rejections_multiple_runs_isolated(store):
    store.save("run-1", {"entity": {}, "activity": {}})
    store.save("run-2", {"entity": {}, "activity": {}})
    store.save_rejections("run-1", [SAMPLE_REJECTIONS[0]])
    store.save_rejections("run-2", [SAMPLE_REJECTIONS[1]])
    assert store.get_rejections("run-1")[0]["rule"] == "range_check"
    assert store.get_rejections("run-2")[0]["rule"] == "null_check"


# ── API endpoint tests ────────────────────────────────────────────────────────

from fastapi.testclient import TestClient
from src.provenanceflow.api.app import app
from src.provenanceflow.api.routers.runs import _get_store

MINIMAL_PROV = {
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


@pytest.fixture
def api_client(tmp_path):
    test_store = ProvenanceStore(db_path=str(tmp_path / "api.db"))
    test_store.save("run-api-001", MINIMAL_PROV)
    test_store.save_rejections("run-api-001", SAMPLE_REJECTIONS)
    app.dependency_overrides[_get_store] = lambda: test_store
    yield TestClient(app), test_store
    app.dependency_overrides.clear()


def test_get_rejections_returns_list(api_client):
    client, _ = api_client
    r = client.get("/runs/run-api-001/rejections")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert data[0]["rule"] == "range_check"


def test_get_rejections_empty_run(api_client):
    client, store = api_client
    store.save("run-no-rej", MINIMAL_PROV)
    r = client.get("/runs/run-no-rej/rejections")
    assert r.status_code == 200
    assert r.json() == []


def test_get_rejections_404_unknown_run(api_client):
    client, _ = api_client
    r = client.get("/runs/does-not-exist/rejections")
    assert r.status_code == 404


def test_get_report_returns_markdown(api_client):
    client, _ = api_client
    r = client.get("/runs/run-api-001/report")
    assert r.status_code == 200
    assert "Run ID" in r.text
    assert "Rejected Rows by Rule" in r.text


def test_get_report_404_unknown_run(api_client):
    client, _ = api_client
    r = client.get("/runs/does-not-exist/report")
    assert r.status_code == 404
