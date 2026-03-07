import json
import pandas as pd
from .rules import (
    ValidationResult,
    check_null_values,
    check_temperature_range,
    check_completeness,
    check_temporal_continuity,
    check_baseline_integrity,
)


class Validator:
    """Runs all validation rules against a GISTEMP DataFrame."""

    RULE_NAMES = [
        'null_check',
        'range_check',
        'completeness_check',
        'temporal_continuity',
        'baseline_integrity',
    ]

    def validate(self, df: pd.DataFrame) -> list[ValidationResult]:
        results: list[ValidationResult] = []

        # Row-level rules
        for idx, row in df.iterrows():
            results.extend(check_null_values(row, idx))
            results.extend(check_temperature_range(row, idx))
            results.extend(check_completeness(row, idx))

        # DataFrame-level rules
        results.extend(check_temporal_continuity(df))
        results.extend(check_baseline_integrity(df))

        return results

    def get_clean(self, df: pd.DataFrame,
                  results: list[ValidationResult]) -> pd.DataFrame:
        """Return rows with no hard_rejection results."""
        rejected_indices = {
            r.row_index for r in results
            if r.severity == 'hard_rejection' and r.row_index is not None
        }
        return df.drop(index=list(rejected_indices)).reset_index(drop=True)

    def rejection_summary(self, results: list[ValidationResult]) -> dict:
        summary: dict[str, int] = {}
        for r in results:
            if r.severity == 'hard_rejection':
                summary[r.rule_name] = summary.get(r.rule_name, 0) + 1
        return summary

    def warning_summary(self, results: list[ValidationResult]) -> dict:
        summary: dict[str, int] = {}
        for r in results:
            if r.severity == 'warning':
                summary[r.rule_name] = summary.get(r.rule_name, 0) + 1
        return summary


def collect_rejected_rows(
    df: pd.DataFrame,
    results: list[ValidationResult],
) -> list[dict]:
    """Join hard-rejected ValidationResult entries with actual DataFrame rows.

    Only rows with severity == 'hard_rejection' and a non-None row_index
    are included.  Dataset-level rules (row_index is None) are excluded
    because they do not correspond to a single row.

    Args:
        df:      The raw DataFrame (before get_clean).
        results: Full list of ValidationResult from Validator.validate().

    Returns:
        List of dicts with keys: rule, severity, message, row_index, row_data.
    """
    rejected: list[dict] = []
    for r in results:
        if r.severity != "hard_rejection" or r.row_index is None:
            continue
        try:
            row_dict = df.loc[r.row_index].to_dict()
        except KeyError:
            row_dict = {}
        rejected.append({
            "rule":      r.rule_name,
            "severity":  r.severity,
            "message":   r.reason,
            "row_index": int(r.row_index),
            "row_data":  json.dumps(row_dict, default=str),
        })
    return rejected
