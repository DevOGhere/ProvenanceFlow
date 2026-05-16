"""
Rule registration system for ProvenanceFlow validators.

Usage — row-level rule (signature: row, idx):

    @rule(severity="hard_rejection")
    def no_negative_prices(row, idx):
        if row.get("price", 0) < 0:
            return f"Negative price: {row['price']}"
        # return None implicitly → passed

    # Override severity per-result by returning a (reason, severity) tuple:
    @rule(name="null_check", severity="warning")
    def null_check(row, idx):
        missing = [c for c in COLS if pd.isna(row.get(c))]
        if not missing:
            return None
        sev = "hard_rejection" if len(missing) > 3 else "warning"
        return (f"Missing: {missing}", sev)

Usage — dataframe-level rule (signature: df only):

    @rule(severity="warning")
    def no_year_gaps(df):
        years = df["year"].sort_values().tolist()
        gaps = []
        for i in range(1, len(years)):
            if years[i] - years[i - 1] > 1:
                gaps.append((None, f"Gap between {years[i-1]} and {years[i]}"))
        return gaps  # [] → all rows pass

Kind is inferred from the function signature:
  - 2+ parameters → row-level  (called once per row)
  - 1 parameter   → dataframe-level (called once on the whole DataFrame)
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Callable, Literal

import pandas as pd

from .rules import ValidationResult

RuleKind = Literal["row", "dataframe"]


@dataclass
class RuleFunction:
    """A validated, callable rule with attached metadata.

    Returned by the @rule decorator. Passed to Validator(rules=[...]).
    """
    fn: Callable
    name: str
    severity: str   # default severity; individual results may override via tuple return
    kind: RuleKind  # inferred from fn signature — do not set manually

    # Make it look like a plain function in repr / debugging
    def __repr__(self) -> str:
        return f"<RuleFunction {self.name!r} kind={self.kind} severity={self.severity!r}>"

    def __call__(self, *args) -> list[ValidationResult]:
        if self.kind == "row":
            return self._call_row(args[0], args[1])
        return self._call_df(args[0])

    # ── Row-level ──────────────────────────────────────────────────────────────

    def _call_row(self, row, idx: int) -> list[ValidationResult]:
        result = self.fn(row, idx)
        if result is None:
            return []
        if isinstance(result, tuple):
            reason, severity = result
        else:
            reason, severity = result, self.severity
        return [ValidationResult(
            passed=False,
            rule_name=self.name,
            severity=severity,
            row_index=idx,
            value=None,
            reason=reason,
        )]

    # ── DataFrame-level ────────────────────────────────────────────────────────

    def _call_df(self, df: pd.DataFrame) -> list[ValidationResult]:
        raw = self.fn(df)
        if not raw:
            return []
        results = []
        for item in raw:
            if len(item) == 3:
                row_idx, reason, severity = item
            else:
                row_idx, reason = item
                severity = self.severity
            results.append(ValidationResult(
                passed=False,
                rule_name=self.name,
                severity=severity,
                row_index=row_idx,
                value=None,
                reason=reason,
            ))
        return results


def rule(
    _fn: Callable | None = None,
    *,
    severity: str = "warning",
    name: str | None = None,
) -> RuleFunction | Callable[[Callable], RuleFunction]:
    """Decorator: register a function as a ProvenanceFlow validation rule.

    Args:
        severity: Default severity for failures produced by this rule.
                  Either 'warning' or 'hard_rejection'.
                  Individual results can override this by returning a
                  (reason, severity) tuple instead of a plain string.
        name:     Rule name recorded in ValidationResult.rule_name and
                  PROV lineage metadata. Defaults to the function name.

    Works both bare (@rule) and parametrised (@rule(severity=...)).
    """
    def decorator(fn: Callable) -> RuleFunction:
        params = list(inspect.signature(fn).parameters)
        kind: RuleKind = "row" if len(params) >= 2 else "dataframe"
        return RuleFunction(
            fn=fn,
            name=name or fn.__name__,
            severity=severity,
            kind=kind,
        )

    if _fn is not None:
        return decorator(_fn)
    return decorator
