from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from ..ingestion.base import DataSource
from ..ingestion.nasa_gistemp import parse_gistemp
from ..validation.validator import Validator
from ..provenance.tracker import ProvenanceTracker
from ..provenance.store import ProvenanceStore
from ..models import IngestionResult, ValidationResult, ProvenanceRecord, PipelineResult
from ..utils.prov_helpers import unwrap, get_ingestion_entity

logger = logging.getLogger(__name__)


def run_pipeline(source: DataSource,
                 db_path: str = 'provenance_store/lineage.db') -> PipelineResult:
    """
    Full pipeline: fetch → validate → track provenance → store.

    Args:
        source: Any DataSource implementation (NASAGISTEMPSource, LocalCSVSource, …)
        db_path: Path to the SQLite provenance store.

    Returns:
        PipelineResult with typed ingestion, validation, and provenance data.
    """
    store = ProvenanceStore(db_path=db_path)
    tracker = ProvenanceTracker()

    # Step 1: Ingest via DataSource
    ingestion = source.fetch()
    df = parse_gistemp(str(ingestion.local_path))

    raw_entity = tracker.track_ingestion(
        source_url=ingestion.source_url,
        local_path=str(ingestion.local_path),
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
    _, ing_attrs = get_ingestion_entity(prov_doc)
    dataset_pid = str(unwrap(ing_attrs.get('fair:identifier', '')))

    rows_in = len(df)
    rows_rejected = rows_in - len(clean_df)

    ingestion_result = IngestionResult(
        source_url=ingestion.source_url,
        local_path=ingestion.local_path,
        row_count=rows_in,
        checksum_sha256=ingestion.checksum_sha256,
        dataset_pid=dataset_pid,
        ingest_timestamp=ingestion.ingest_timestamp,
    )

    validation_result = ValidationResult(
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
            'rejection_rate': validation_result.rejection_rate,
        },
    )

    logger.info("Pipeline complete. run_id=%s", run_id)
    logger.info(
        "Rows in: %d | Passed: %d | Rejected: %d",
        rows_in, len(clean_df), rows_rejected,
    )

    return PipelineResult(
        run_id=run_id,
        ingestion=ingestion_result,
        validation=validation_result,
        provenance=provenance,
    )
