"""
Run comparison: compare two provenance runs and surface the diff.

Usage:
    from src.provenanceflow.provenance.compare import compare_runs

    diff = compare_runs(run_id_a, run_id_b, store)
    print(diff.summary)          # "Rejection rate improved by 0.24%..."
    print(diff.same_dataset)     # True if both runs used the same SHA-256 input
"""
from __future__ import annotations

from dataclasses import dataclass

from .store import ProvenanceStore
from ..utils.prov_helpers import unwrap, get_ingestion_entity, get_validation_activity


@dataclass
class RunDiff:
    """Structured diff between two pipeline runs."""
    run_id_a: str
    run_id_b: str
    same_dataset: bool           # True if SHA-256 fingerprints match
    same_rules: bool             # True if rules_applied strings match
    rows_passed_a: int
    rows_passed_b: int
    delta_rows_passed: int       # rows_passed_b - rows_passed_a
    rejection_rate_a: float
    rejection_rate_b: float
    delta_rejection_rate: float  # rejection_rate_b - rejection_rate_a
    source_url_a: str
    source_url_b: str
    summary: str                 # Human-readable one-liner


def compare_runs(run_id_a: str, run_id_b: str,
                 store: ProvenanceStore) -> RunDiff:
    """Compare two stored pipeline runs and return a structured diff.

    Args:
        run_id_a: Run ID of the baseline run.
        run_id_b: Run ID of the run to compare against the baseline.
        store:    ProvenanceStore containing both runs.

    Returns:
        RunDiff dataclass with field-by-field comparison.

    Raises:
        ValueError: If either run_id is not found in the store.
    """
    doc_a = store.get(run_id_a)
    doc_b = store.get(run_id_b)
    if not doc_a:
        raise ValueError(f"Run '{run_id_a}' not found in provenance store.")
    if not doc_b:
        raise ValueError(f"Run '{run_id_b}' not found in provenance store.")

    _, ing_a = get_ingestion_entity(doc_a)
    _, ing_b = get_ingestion_entity(doc_b)
    _, val_a = get_validation_activity(doc_a)
    _, val_b = get_validation_activity(doc_b)

    sha_a = ing_a.get("pf:checksum_sha256", "")
    sha_b = ing_b.get("pf:checksum_sha256", "")
    same_dataset = bool(sha_a and sha_a == sha_b)

    rules_a = val_a.get("pf:rules_applied", "")
    rules_b = val_b.get("pf:rules_applied", "")
    same_rules = rules_a == rules_b

    rp_a = int(unwrap(val_a.get("pf:rows_passed", 0)) or 0)
    rp_b = int(unwrap(val_b.get("pf:rows_passed", 0)) or 0)

    rate_a = float(unwrap(val_a.get("pf:rejection_rate", 0)) or 0)
    rate_b = float(unwrap(val_b.get("pf:rejection_rate", 0)) or 0)
    delta = rate_b - rate_a

    if delta < 0:
        direction = "improved"
    elif delta > 0:
        direction = "degraded"
    else:
        direction = "unchanged"

    summary = (
        f"Rejection rate {direction} by {abs(delta) * 100:.2f}% "
        f"({rate_a * 100:.2f}% → {rate_b * 100:.2f}%)"
    )

    return RunDiff(
        run_id_a=run_id_a,
        run_id_b=run_id_b,
        same_dataset=same_dataset,
        same_rules=same_rules,
        rows_passed_a=rp_a,
        rows_passed_b=rp_b,
        delta_rows_passed=rp_b - rp_a,
        rejection_rate_a=rate_a,
        rejection_rate_b=rate_b,
        delta_rejection_rate=delta,
        source_url_a=ing_a.get("fair:source_url", "—"),
        source_url_b=ing_b.get("fair:source_url", "—"),
        summary=summary,
    )
