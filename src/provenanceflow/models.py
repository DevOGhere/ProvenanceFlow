"""
Canonical Pydantic data contracts for ProvenanceFlow.

Every data boundary in the system — ingestion output, validation output,
provenance records, and full pipeline results — is typed here.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field, field_validator


class IngestionResult(BaseModel):
    """Data produced by a DataSource.fetch() call."""
    source_url: str
    local_path: Path
    row_count: int = Field(ge=0)
    checksum_sha256: str
    dataset_pid: str
    ingest_timestamp: datetime


class ValidationResult(BaseModel):
    """Data produced by running Validator over an ingested dataset."""
    rows_in: int = Field(ge=0)
    rows_passed: int = Field(ge=0)
    rows_rejected: int = Field(ge=0)
    rejection_rate: float = Field(ge=0.0, le=1.0)
    rules_applied: list[str]
    rejections_by_rule: dict[str, int]
    warnings_by_rule: dict[str, int]
    clean_path: Path | None = None

    @field_validator('rejection_rate', mode='before')
    @classmethod
    def round_rate(cls, v: float) -> float:
        return round(float(v), 4)


class ProvenanceRecord(BaseModel):
    """A stored W3C PROV-JSON document with its run metadata."""
    run_id: str
    created_at: datetime
    prov_doc: dict
    summary: dict = Field(default_factory=dict)


class PipelineResult(BaseModel):
    """Full result of a run_pipeline() call — ingestion + validation + provenance."""
    run_id: str
    ingestion: IngestionResult
    validation: ValidationResult
    provenance: ProvenanceRecord
