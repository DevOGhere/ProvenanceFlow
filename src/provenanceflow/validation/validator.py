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
