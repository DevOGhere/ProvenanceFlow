"""Tests for compare_runs() and RunDiff."""
import pytest

from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.compare import compare_runs, RunDiff


# ── minimal PROV-JSON builder ─────────────────────────────────────────────────

def _make_prov(sha: str, rate: float, rows_in: int = 100,
               rules: str = "null_check,range_check") -> dict:
    rows_passed = int(rows_in * (1 - rate))
    rows_rejected = rows_in - rows_passed
    return {
        "prefix": {"pf": "http://provenanceflow.org/"},
        "entity": {
            "pf:dataset_abc": {
                "pf:checksum_sha256": sha,
                "fair:source_url": f"file:///data/test_{sha[:4]}.csv",
            },
            "pf:validated_abc": {},
        },
        "activity": {
            "pf:validate_abc": {
                "pf:rows_in":        {"$": rows_in,        "type": "xsd:integer"},
                "pf:rows_passed":    {"$": rows_passed,    "type": "xsd:integer"},
                "pf:rows_rejected":  {"$": rows_rejected,  "type": "xsd:integer"},
                "pf:rejection_rate": {"$": rate,           "type": "xsd:double"},
                "pf:rules_applied":  rules,
            },
        },
        "agent": {},
        "wasGeneratedBy": {},
        "used": {},
        "wasDerivedFrom": {},
        "wasAssociatedWith": {},
    }


@pytest.fixture
def store_with_two_runs(tmp_path):
    store = ProvenanceStore(db_path=str(tmp_path / "cmp.db"))
    store.save("run-a", _make_prov(sha="deadbeef", rate=0.034))
    store.save("run-b", _make_prov(sha="deadbeef", rate=0.020))
    return store


@pytest.fixture
def store_different_datasets(tmp_path):
    store = ProvenanceStore(db_path=str(tmp_path / "cmp2.db"))
    store.save("run-x", _make_prov(sha="aabbccdd", rate=0.034))
    store.save("run-y", _make_prov(sha="11223344", rate=0.040))
    return store


# ── tests ─────────────────────────────────────────────────────────────────────

def test_compare_runs_returns_run_diff(store_with_two_runs):
    diff = compare_runs("run-a", "run-b", store_with_two_runs)
    assert isinstance(diff, RunDiff)


def test_compare_runs_same_dataset_true(store_with_two_runs):
    diff = compare_runs("run-a", "run-b", store_with_two_runs)
    assert diff.same_dataset is True


def test_compare_runs_different_datasets(store_different_datasets):
    diff = compare_runs("run-x", "run-y", store_different_datasets)
    assert diff.same_dataset is False


def test_compare_runs_same_rules_true(store_with_two_runs):
    diff = compare_runs("run-a", "run-b", store_with_two_runs)
    assert diff.same_rules is True


def test_compare_runs_delta_rejection_rate(store_with_two_runs):
    diff = compare_runs("run-a", "run-b", store_with_two_runs)
    assert diff.rejection_rate_a == pytest.approx(0.034, abs=1e-6)
    assert diff.rejection_rate_b == pytest.approx(0.020, abs=1e-6)
    assert diff.delta_rejection_rate == pytest.approx(-0.014, abs=1e-6)


def test_compare_runs_summary_says_improved(store_with_two_runs):
    diff = compare_runs("run-a", "run-b", store_with_two_runs)
    assert "improved" in diff.summary


def test_compare_runs_summary_says_degraded(store_different_datasets):
    diff = compare_runs("run-x", "run-y", store_different_datasets)
    assert "degraded" in diff.summary


def test_compare_runs_unchanged(tmp_path):
    store = ProvenanceStore(db_path=str(tmp_path / "same.db"))
    prov = _make_prov(sha="aabbccdd", rate=0.034)
    store.save("run-1", prov)
    store.save("run-2", prov)
    diff = compare_runs("run-1", "run-2", store)
    assert diff.delta_rejection_rate == pytest.approx(0.0, abs=1e-6)
    assert "unchanged" in diff.summary


def test_compare_runs_unknown_a_raises(store_with_two_runs):
    with pytest.raises(ValueError, match="not found"):
        compare_runs("no-such-run", "run-b", store_with_two_runs)


def test_compare_runs_unknown_b_raises(store_with_two_runs):
    with pytest.raises(ValueError, match="not found"):
        compare_runs("run-a", "no-such-run", store_with_two_runs)


def test_compare_runs_run_ids_recorded(store_with_two_runs):
    diff = compare_runs("run-a", "run-b", store_with_two_runs)
    assert diff.run_id_a == "run-a"
    assert diff.run_id_b == "run-b"
