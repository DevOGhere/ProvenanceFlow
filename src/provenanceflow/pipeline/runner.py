from ..ingestion.nasa_gistemp import download_gistemp
from ..validation.validator import Validator
from ..provenance.tracker import ProvenanceTracker
from ..provenance.store import ProvenanceStore


def run_pipeline(source_url: str, local_path: str,
                 db_path: str = 'provenance_store/lineage.db') -> str:
    """
    Full pipeline: download → validate → track provenance → store.
    Returns run_id for querying the provenance record.
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

    print(f"Pipeline complete. Run ID: {run_id}")
    print(f"Rows in: {len(df)} | Rows passed: {len(clean_df)} | Rejected: {len(df) - len(clean_df)}")
    print(f"Rejections by rule: {rejections}")
    print(f"Warnings by rule:   {warnings}")
    print(f"Provenance stored. Query with: store.get('{run_id}')")

    return run_id
