"""Tests for src.provenanceflow.utils.report.render_report()."""
import pytest

from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.utils.report import render_report


# ── minimal PROV-JSON fixture ─────────────────────────────────────────────────

MINIMAL_PROV = {
    "prefix": {"pf": "http://provenanceflow.org/"},
    "entity": {
        "pf:dataset_abc123": {
            "dc:title": "NASA GISTEMP v4 — Global Temperature",
            "fair:source_url": "https://example.com/gistemp.csv",
            "fair:identifier": "uuid-test-1234",
            "dc:format": "text/csv",
            "dc:license": "Public Domain",
            "pf:ingest_timestamp": "2026-03-07T10:00:00Z",
            "pf:row_count": {"$": 1680, "type": "xsd:integer"},
            "pf:checksum_sha256": "deadbeef" * 8,
        },
        "pf:validated_abc123": {
            "dc:title": "Validated GISTEMP",
            "fair:identifier": "uuid-validated-5678",
            "pf:row_count": {"$": 1623, "type": "xsd:integer"},
        },
    },
    "activity": {
        "pf:validate_abc123": {
            "pf:rows_in":        {"$": 1680, "type": "xsd:integer"},
            "pf:rows_passed":    {"$": 1623, "type": "xsd:integer"},
            "pf:rows_rejected":  {"$": 57,   "type": "xsd:integer"},
            "pf:rejection_rate": {"$": 0.034, "type": "xsd:double"},
            "pf:rules_applied":  "null_check, range_check, completeness_check",
        },
    },
    "agent": {},
    "wasGeneratedBy": {},
    "used": {},
    "wasDerivedFrom": {},
    "wasAssociatedWith": {},
}


@pytest.fixture
def store_with_run(tmp_path):
    store = ProvenanceStore(db_path=str(tmp_path / "test.db"))
    store.save("run-test-uuid", MINIMAL_PROV)
    return store, "run-test-uuid"


# ── tests ─────────────────────────────────────────────────────────────────────

def test_render_report_returns_string(store_with_run):
    store, run_id = store_with_run
    result = render_report(run_id, store)
    assert isinstance(result, str)
    assert len(result) > 100


def test_render_report_contains_run_id(store_with_run):
    store, run_id = store_with_run
    result = render_report(run_id, store)
    assert run_id in result


def test_render_report_contains_source_url(store_with_run):
    store, run_id = store_with_run
    result = render_report(run_id, store)
    assert "https://example.com/gistemp.csv" in result


def test_render_report_contains_validation_metrics(store_with_run):
    store, run_id = store_with_run
    result = render_report(run_id, store)
    assert "1,680" in result or "1680" in result
    assert "1,623" in result or "1623" in result
    assert "57" in result
    assert "3.40%" in result


def test_render_report_contains_prov_json(store_with_run):
    store, run_id = store_with_run
    result = render_report(run_id, store)
    assert "```json" in result
    assert "pf:validate_abc123" in result


def test_render_report_contains_reproduce_command(store_with_run):
    store, run_id = store_with_run
    result = render_report(run_id, store)
    assert "provenanceflow run" in result


def test_render_report_raises_for_unknown_run(tmp_path):
    store = ProvenanceStore(db_path=str(tmp_path / "empty.db"))
    with pytest.raises(ValueError, match="not found"):
        render_report("no-such-run", store)


# ── rejections-by-rule table tests ───────────────────────────────────────────

@pytest.fixture
def store_with_rejections(tmp_path):
    store = ProvenanceStore(db_path=str(tmp_path / "test2.db"))
    store.save("run-test-rej", MINIMAL_PROV)
    store.save_rejections("run-test-rej", [
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
            "message": "Missing values in columns: ['Jan', 'Feb', 'Mar', 'Apr']",
            "row_index": 57,
            "row_data": '{"Year": 2001, "Jan": null}',
        },
    ])
    return store, "run-test-rej"


def test_report_with_rejections_contains_table(store_with_rejections):
    store, run_id = store_with_rejections
    report = render_report(run_id, store)
    assert "Rejected Rows by Rule" in report
    assert "range_check" in report
    assert "null_check" in report


def test_report_with_rejections_contains_sample_rows(store_with_rejections):
    store, run_id = store_with_rejections
    report = render_report(run_id, store)
    assert "Sample Rejected Rows" in report
    assert "42" in report
    assert "57" in report


def test_report_no_rejections_omits_table(store_with_run):
    store, run_id = store_with_run
    # store_with_run does not call save_rejections
    report = render_report(run_id, store)
    assert "Rejected Rows by Rule" not in report
