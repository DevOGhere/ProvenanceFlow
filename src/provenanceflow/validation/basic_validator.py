"""
BasicValidator — domain-agnostic validation for any tabular DataFrame.

Unlike Validator (which assumes NASA GISTEMP column structure), BasicValidator
works on any CSV: it checks null rates per row and per column without assuming
specific column names.

Pairs with GenericCSVSource for non-GISTEMP datasets.
"""
from __future__ import annotations

import pandas as pd

from .rules import ValidationResult


class BasicValidator:
    """Domain-agnostic validator: checks null rates on any DataFrame.

    Rules applied:
      - row_null_rate:        Row has more than `row_null_threshold` fraction of null cols
      - column_completeness:  Column has more than `col_null_threshold` fraction of nulls

    Args:
        row_null_threshold: Fraction (0–1) of null columns that triggers a row warning.
                            Above 0.8 → hard_rejection; above threshold → warning.
        col_null_threshold: Fraction (0–1) of null values in a column that triggers
                            a column-level warning.
    """

    RULE_NAMES = ["row_null_rate", "column_completeness"]

    def __init__(self, row_null_threshold: float = 0.5,
                 col_null_threshold: float = 0.3) -> None:
        self._row_thresh = row_null_threshold
        self._col_thresh = col_null_threshold

    def validate(self, df: pd.DataFrame) -> list[ValidationResult]:
        results: list[ValidationResult] = []
        n_cols = len(df.columns)

        if n_cols == 0:
            return results

        # Row-level: flag rows with high null fraction
        for idx, row in df.iterrows():
            null_rate = row.isna().sum() / n_cols
            if null_rate > self._row_thresh:
                severity = "hard_rejection" if null_rate >= 0.8 else "warning"
                results.append(ValidationResult(
                    passed=False,
                    rule_name="row_null_rate",
                    severity=severity,
                    row_index=int(idx),
                    value=null_rate,
                    reason=f"{null_rate * 100:.0f}% of columns are null",
                ))

        # Column-level: flag columns with high null fraction
        for col in df.columns:
            col_null_rate = float(df[col].isna().mean())
            if col_null_rate > self._col_thresh:
                results.append(ValidationResult(
                    passed=False,
                    rule_name="column_completeness",
                    severity="warning",
                    row_index=None,
                    value=col_null_rate,
                    reason=f"Column '{col}': {col_null_rate * 100:.0f}% null",
                ))

        return results

    def get_clean(self, df: pd.DataFrame,
                  results: list[ValidationResult]) -> pd.DataFrame:
        """Return rows with no hard_rejection results."""
        rejected = {
            r.row_index for r in results
            if r.severity == "hard_rejection" and r.row_index is not None
        }
        return df.drop(index=list(rejected)).reset_index(drop=True)

    def rejection_summary(self, results: list[ValidationResult]) -> dict[str, int]:
        summary: dict[str, int] = {}
        for r in results:
            if r.severity == "hard_rejection":
                summary[r.rule_name] = summary.get(r.rule_name, 0) + 1
        return summary

    def warning_summary(self, results: list[ValidationResult]) -> dict[str, int]:
        summary: dict[str, int] = {}
        for r in results:
            if r.severity == "warning":
                summary[r.rule_name] = summary.get(r.rule_name, 0) + 1
        return summary
