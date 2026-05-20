"""
Validator — runs a list of RuleFunction objects against a DataFrame.

The validator is dataset-agnostic: it knows nothing about column names,
domains, or data formats. All domain knowledge lives in the rules you pass in.

    from provenanceflow.validation.validator import Validator
    from provenanceflow.validation.contrib.gistemp import GISTEMP_RULES

    validator = Validator(rules=GISTEMP_RULES)
    results = validator.validate(df)
    clean_df = validator.get_clean(df, results)
    print(validator.rejection_summary(results))
"""
from __future__ import annotations

import pandas as pd

from .rule import RuleFunction
from .rules import ValidationResult


def collect_rejected_rows(df: pd.DataFrame, results: list[ValidationResult]) -> list[dict]:
    """Return structured dicts for every hard-rejected row that has a row_index.

    Used by the pipeline to persist per-row rejection detail to the store's
    rejections table, which feeds the dashboard and /runs/{id}/rejections API.
    """
    collected = []
    for r in results:
        if r.severity != "hard_rejection" or r.row_index is None:
            continue
        try:
            row_data = df.iloc[r.row_index].to_json()
        except (IndexError, ValueError):
            row_data = "{}"
        collected.append({
            "rule": r.rule_name,
            "severity": r.severity,
            "message": r.reason,
            "row_index": r.row_index,
            "row_data": row_data,
        })
    return collected


class Validator:
    """Run a list of RuleFunctions against a DataFrame.

    Row-level rules are applied once per row.
    DataFrame-level rules are applied once to the full DataFrame.

    Args:
        rules: Ordered list of RuleFunction objects produced by the @rule decorator.
    """

    def __init__(self, rules: list[RuleFunction]) -> None:
        if not rules:
            raise ValueError(
                "Validator requires at least one rule. "
                "Pass a list of @rule-decorated functions, e.g. "
                "Validator(rules=GISTEMP_RULES)."
            )
        self._row_rules = [r for r in rules if r.kind == "row"]
        self._df_rules = [r for r in rules if r.kind == "dataframe"]
        self.rule_names: list[str] = [r.name for r in rules]

    def validate(self, df: pd.DataFrame) -> list[ValidationResult]:
        """Run all rules against df. Returns every failure as a ValidationResult."""
        results: list[ValidationResult] = []

        for idx, row in df.iterrows():
            for rule in self._row_rules:
                results.extend(rule(row, int(idx)))

        for rule in self._df_rules:
            results.extend(rule(df))

        return results

    def get_clean(self, df: pd.DataFrame,
                  results: list[ValidationResult]) -> pd.DataFrame:
        """Return df with hard-rejected rows removed."""
        rejected = {
            r.row_index
            for r in results
            if r.severity == "hard_rejection" and r.row_index is not None
        }
        return df.drop(index=list(rejected)).reset_index(drop=True)

    def rejection_summary(self, results: list[ValidationResult]) -> dict[str, int]:
        """Count hard rejections per rule name."""
        summary: dict[str, int] = {}
        for r in results:
            if r.severity == "hard_rejection":
                summary[r.rule_name] = summary.get(r.rule_name, 0) + 1
        return summary

    def warning_summary(self, results: list[ValidationResult]) -> dict[str, int]:
        """Count warnings per rule name."""
        summary: dict[str, int] = {}
        for r in results:
            if r.severity == "warning":
                summary[r.rule_name] = summary.get(r.rule_name, 0) + 1
        return summary
