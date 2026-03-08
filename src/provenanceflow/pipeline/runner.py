from __future__ import annotations

from datetime import datetime

import pandas as pd

from ..ingestion.nasa_gistemp import download_gistemp
from ..ingestion.base import DataSource
from ..validation.validator import Validator
from ..validation.basic_validator import BasicValidator
from ..provenance.tracker import ProvenanceTracker
from ..provenance.store import ProvenanceStore
from ..models import (
    ValidationResult as ValidationResultModel,
    PipelineResult,
    ProvenanceRecord,
)


def run_pipeline(source=None, local_path=None,
                 db_path: str = 'provenance_store/lineage.db'):
    """Run the full ingestion → validation → provenance pipeline.

    Old API (backward-compat):
        run_pipeline(url: str, local_path: str, db_path=...) -> str

    New DataSource API:
        run_pipeline(source: DataSource, db_path=...) -> PipelineResult
    """
    if isinstance(source, str):
        return _run_legacy(source, local_path, db_path)
    return _run_source(source, db_path)


# ── Legacy string-based API ───────────────────────────────────────────────────

def _run_legacy(source_url: str, local_path: str, db_path: str) -> str:
    store = ProvenanceStore(db_path=db_path)
    tracker = ProvenanceTracker()

    df = download_gistemp(source_url, local_path)
    raw_entity = tracker.track_ingestion(
        source_url=source_url,
        local_path=local_path,
        row_count=len(df),
    )

    validator = Validator()
    results = validator.validate(df)
    clean_df = validator.get_clean(df, results)
    rejections = validator.rejection_summary(results)
    warnings = validator.warning_summary(results)

    tracker.track_validation(
        input_entity=raw_entity,
        rows_in=len(df),
        rows_passed=len(clean_df),
        rejections=rejections,
        warnings=warnings,
        rules_applied=Validator.RULE_NAMES,
    )

    return tracker.finalize(store)


# ── DataSource-based API ──────────────────────────────────────────────────────

def _run_source(source: DataSource, db_path: str) -> PipelineResult:
    store = ProvenanceStore(db_path=db_path)
    tracker = ProvenanceTracker()

    # Step 1: Ingest
    ingestion = source.fetch()

    parse_fn = source._parse
    if parse_fn is not None:
        df = parse_fn(str(ingestion.local_path))
    else:
        df = pd.read_csv(str(ingestion.local_path))

    raw_entity = tracker.track_ingestion(
        source_url=ingestion.source_url,
        local_path=str(ingestion.local_path),
        row_count=ingestion.row_count,
        title=source.dataset_title,
        license_url=source.dataset_license,
    )

    # Step 2: Validate — BasicValidator for generic CSV, Validator for GISTEMP
    if source.source_id == "generic_csv":
        validator: Validator | BasicValidator = BasicValidator()
        rule_names = BasicValidator.RULE_NAMES
    else:
        validator = Validator()
        rule_names = Validator.RULE_NAMES

    results = validator.validate(df)
    clean_df = validator.get_clean(df, results)
    rejections = validator.rejection_summary(results)
    warnings = validator.warning_summary(results)

    rows_rejected = len(df) - len(clean_df)
    rejection_rate = rows_rejected / len(df) if len(df) else 0.0

    tracker.track_validation(
        input_entity=raw_entity,
        rows_in=len(df),
        rows_passed=len(clean_df),
        rejections=rejections,
        warnings=warnings,
        rules_applied=rule_names,
    )

    run_id = tracker.finalize(store)

    validation_model = ValidationResultModel(
        rows_in=len(df),
        rows_passed=len(clean_df),
        rows_rejected=rows_rejected,
        rejection_rate=rejection_rate,
        rules_applied=rule_names,
        rejections_by_rule=rejections,
        warnings_by_rule=warnings,
    )

    provenance_record = ProvenanceRecord(
        run_id=run_id,
        created_at=datetime.utcnow(),
        prov_doc=store.get(run_id),
    )

    return PipelineResult(
        run_id=run_id,
        ingestion=ingestion,
        validation=validation_model,
        provenance=provenance_record,
    )
