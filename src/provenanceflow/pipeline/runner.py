from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ..ingestion.nasa_gistemp import download_gistemp
from ..validation.validator import Validator
from ..provenance.tracker import ProvenanceTracker
from ..provenance.store import ProvenanceStore
from ..models import IngestionResult, ValidationResult, ProvenanceRecord, PipelineResult
from ..utils.checksums import sha256_file


def run_pipeline(
    source_url: str,
    local_path: str,
    db_path: str = 'provenance_store/lineage.db',
) -> PipelineResult:
    """
    Full pipeline: download → validate → track provenance → store.
    Returns a typed PipelineResult containing ingestion, validation, and provenance data.
    """
    store = ProvenanceStore(db_path=db_path)
    tracker = ProvenanceTracker()

    # Step 1: Ingest
    df = download_gistemp(source_url, local_path)
    raw_entity = tracker.track_ingestion(
        source_url=source_url,
        local_path=local_path,
        row_count=len(df),
    )

    # Step 2: Validate
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

    # Step 3: Finalize provenance
    run_id = tracker.finalize(store)

    # Step 4: Build typed result models from stored PROV doc
    prov_doc = store.get(run_id)

    # Extract dataset_pid from the PROV entity the tracker recorded
    dataset_pid = ''
    for eid, attrs in prov_doc.get('entity', {}).items():
        if 'dataset_' in eid:
            raw_fid = attrs.get('fair:identifier', '')
            dataset_pid = raw_fid['$'] if isinstance(raw_fid, dict) else str(raw_fid)
            break

    rows_in = len(df)
    rows_rejected = rows_in - len(clean_df)

    ingestion = IngestionResult(
        source_url=source_url,
        local_path=Path(local_path),
        row_count=rows_in,
        checksum_sha256=sha256_file(local_path),
        dataset_pid=dataset_pid,
        ingest_timestamp=datetime.utcnow(),
    )

    validation = ValidationResult(
        rows_in=rows_in,
        rows_passed=len(clean_df),
        rows_rejected=rows_rejected,
        rejection_rate=round(rows_rejected / rows_in, 4) if rows_in else 0.0,
        rules_applied=list(Validator.RULE_NAMES),
        rejections_by_rule=rejections,
        warnings_by_rule=warnings,
    )

    provenance = ProvenanceRecord(
        run_id=run_id,
        created_at=datetime.utcnow(),
        prov_doc=prov_doc,
        summary={
            'rows_in': rows_in,
            'rows_passed': len(clean_df),
            'rejection_rate': validation.rejection_rate,
        },
    )

    print(f"Pipeline complete. Run ID: {run_id}")
    print(f"Rows in: {rows_in} | Rows passed: {len(clean_df)} | Rejected: {rows_rejected}")

    return PipelineResult(
        run_id=run_id,
        ingestion=ingestion,
        validation=validation,
        provenance=provenance,
    )
