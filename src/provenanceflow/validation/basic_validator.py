"""
BasicValidator — domain-agnostic validation for any tabular DataFrame.

Works on any CSV without assuming specific column names. Rules are null-rate
checks at the row and column level, with configurable thresholds.

    from provenanceflow.validation.basic_validator import BasicValidator

    v = BasicValidator(row_null_threshold=0.5, col_null_threshold=0.3)
    results = v.validate(df)
    clean = v.get_clean(df, results)

Internally built on Validator + @rule, so results are standard ValidationResult
objects and the full rejection/warning API is available.
"""
from __future__ import annotations

import pandas as pd

from .rule import rule, RuleFunction
from .validator import Validator


# ── Rule names exposed as a class-level constant for external reference ───────

RULE_NAMES = ["row_null_rate", "column_completeness"]


def _make_row_null_rule(threshold: float) -> RuleFunction:
    """Build a row-level null-rate rule with a configurable threshold.

    - null_rate > 0.8          → hard_rejection (row is mostly empty)
    - threshold < null_rate ≤ 0.8 → warning (row is partially sparse)
    """
    @rule(name="row_null_rate", severity="warning")
    def row_null_rate(row, idx):
        total = len(row)
        if total == 0:
            return None
        null_count = int(row.isna().sum())
        null_rate = null_count / total
        if null_rate >= 0.8:
            return (
                f"{null_count}/{total} values null ({null_rate:.0%})",
                "hard_rejection",
            )
        if null_rate > threshold:
            return f"{null_count}/{total} values null ({null_rate:.0%})"
        return None

    return row_null_rate


def _make_column_null_rule(threshold: float) -> RuleFunction:
    """Build a dataframe-level rule that flags columns with high null rates."""
    @rule(name="column_completeness", severity="warning")
    def column_completeness(df: pd.DataFrame):
        results = []
        for col in df.columns:
            rate = float(df[col].isna().mean())
            if rate > threshold:
                results.append((
                    None,
                    f"Column '{col}': {rate:.0%} null (threshold: {threshold:.0%})",
                ))
        return results

    return column_completeness


class BasicValidator(Validator):
    """Null-rate validator for any tabular DataFrame.

    Args:
        row_null_threshold: Fraction of null values that triggers a row warning.
                            Rows above 0.8 are always hard-rejected regardless.
        col_null_threshold: Fraction of null values in a column that triggers
                            a column-level warning.
    """

    RULE_NAMES = RULE_NAMES  # class-level alias for external inspection

    def __init__(
        self,
        row_null_threshold: float = 0.5,
        col_null_threshold: float = 0.3,
    ) -> None:
        rules = [
            _make_row_null_rule(row_null_threshold),
            _make_column_null_rule(col_null_threshold),
        ]
        super().__init__(rules)
