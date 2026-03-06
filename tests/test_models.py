"""Tests for Pydantic data contract models."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.provenanceflow.models import (
    IngestionResult,
    ValidationResult,
    ProvenanceRecord,
    PipelineResult,
)


# ── IngestionResult ──────────────────────────────────────────────────────────

def test_ingestion_result_valid():
    r = IngestionResult(
        source_url="https://example.com/data.csv",
        local_path=Path("/tmp/data.csv"),
        row_count=100,
        checksum_sha256="a" * 64,
        dataset_pid="dataset_abc123",
        ingest_timestamp=datetime.utcnow(),
    )
    assert r.row_count == 100


def test_ingestion_result_rejects_negative_row_count():
    with pytest.raises(ValidationError):
        IngestionResult(
            source_url="https://example.com",
            local_path=Path("/tmp/x.csv"),
            row_count=-1,
            checksum_sha256="a" * 64,
            dataset_pid="dataset_x",
            ingest_timestamp=datetime.utcnow(),
        )


def test_ingestion_result_local_path_is_path_object():
    r = IngestionResult(
        source_url="https://example.com",
        local_path="/tmp/data.csv",  # str input → coerced to Path
        row_count=0,
        checksum_sha256="b" * 64,
        dataset_pid="dataset_y",
        ingest_timestamp=datetime.utcnow(),
    )
    assert isinstance(r.local_path, Path)


# ── ValidationResult ─────────────────────────────────────────────────────────

def test_validation_result_valid():
    r = ValidationResult(
        rows_in=100,
        rows_passed=90,
        rows_rejected=10,
        rejection_rate=0.1,
        rules_applied=["null_check", "range_check"],
        rejections_by_rule={"range_check": 10},
        warnings_by_rule={},
    )
    assert r.rejection_rate == 0.1


def test_validation_result_rejection_rate_rounded():
    r = ValidationResult(
        rows_in=3,
        rows_passed=2,
        rows_rejected=1,
        rejection_rate=0.33333333,
        rules_applied=[],
        rejections_by_rule={},
        warnings_by_rule={},
    )
    assert r.rejection_rate == 0.3333


def test_validation_result_rejects_rate_above_one():
    with pytest.raises(ValidationError):
        ValidationResult(
            rows_in=10,
            rows_passed=5,
            rows_rejected=5,
            rejection_rate=1.5,
            rules_applied=[],
            rejections_by_rule={},
            warnings_by_rule={},
        )


def test_validation_result_clean_path_optional():
    r = ValidationResult(
        rows_in=5,
        rows_passed=5,
        rows_rejected=0,
        rejection_rate=0.0,
        rules_applied=[],
        rejections_by_rule={},
        warnings_by_rule={},
    )
    assert r.clean_path is None


# ── ProvenanceRecord ──────────────────────────────────────────────────────────

def test_provenance_record_prov_doc_is_dict():
    r = ProvenanceRecord(
        run_id="run_abc",
        created_at=datetime.utcnow(),
        prov_doc={"entity": {}, "activity": {}},
    )
    assert isinstance(r.prov_doc, dict)


def test_provenance_record_summary_defaults_to_empty():
    r = ProvenanceRecord(
        run_id="run_abc",
        created_at=datetime.utcnow(),
        prov_doc={},
    )
    assert r.summary == {}


# ── PipelineResult ────────────────────────────────────────────────────────────

def _make_pipeline_result() -> PipelineResult:
    now = datetime.utcnow()
    return PipelineResult(
        run_id="run_test",
        ingestion=IngestionResult(
            source_url="https://example.com",
            local_path=Path("/tmp/x.csv"),
            row_count=10,
            checksum_sha256="c" * 64,
            dataset_pid="dataset_z",
            ingest_timestamp=now,
        ),
        validation=ValidationResult(
            rows_in=10,
            rows_passed=9,
            rows_rejected=1,
            rejection_rate=0.1,
            rules_applied=["null_check"],
            rejections_by_rule={"null_check": 1},
            warnings_by_rule={},
        ),
        provenance=ProvenanceRecord(
            run_id="run_test",
            created_at=now,
            prov_doc={"entity": {}},
        ),
    )


def test_pipeline_result_round_trip_json():
    result = _make_pipeline_result()
    serialised = result.model_dump_json()
    restored = PipelineResult.model_validate_json(serialised)
    assert restored.run_id == result.run_id
    assert restored.ingestion.row_count == result.ingestion.row_count
    assert restored.validation.rejection_rate == result.validation.rejection_rate


def test_pipeline_result_run_id_matches_provenance():
    result = _make_pipeline_result()
    assert result.run_id == result.provenance.run_id
