from dataclasses import dataclass
from typing import Optional
import pandas as pd


MONTHLY_COLS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


@dataclass
class ValidationResult:
    passed: bool
    rule_name: str
    severity: str          # 'hard_rejection' | 'warning'
    row_index: Optional[int]
    value: Optional[float]
    reason: str


def check_null_values(row, row_index: int) -> list[ValidationResult]:
    """Flag rows where monthly temperature values are NaN."""
    null_cols = [c for c in MONTHLY_COLS if pd.isna(row.get(c))]
    if not null_cols:
        return []
    severity = 'hard_rejection' if len(null_cols) > 3 else 'warning'
    return [ValidationResult(
        passed=False,
        rule_name='null_check',
        severity=severity,
        row_index=row_index,
        value=None,
        reason=f"Missing values in columns: {null_cols}",
    )]


def check_temperature_range(row, row_index: int,
                             min_val: float = -3.0,
                             max_val: float = 3.0) -> list[ValidationResult]:
    """Flag annual mean temperature anomalies outside physically plausible range."""
    annual_mean = row.get('J-D')
    if pd.isna(annual_mean) or (min_val <= float(annual_mean) <= max_val):
        return []
    return [ValidationResult(
        passed=False,
        rule_name='range_check',
        severity='hard_rejection',
        row_index=row_index,
        value=float(annual_mean),
        reason=f"Annual mean {annual_mean}°C outside range [{min_val}, {max_val}]",
    )]


def check_completeness(row, row_index: int, max_missing: int = 3) -> list[ValidationResult]:
    """Hard-reject rows with more than max_missing monthly values absent."""
    null_cols = [c for c in MONTHLY_COLS if pd.isna(row.get(c))]
    if len(null_cols) <= max_missing:
        return []
    return [ValidationResult(
        passed=False,
        rule_name='completeness_check',
        severity='hard_rejection',
        row_index=row_index,
        value=None,
        reason=f"{len(null_cols)} monthly values missing (threshold: {max_missing})",
    )]


def check_temporal_continuity(df: pd.DataFrame) -> list[ValidationResult]:
    """Flag gaps in the year sequence."""
    results = []
    years = df['Year'].sort_values().tolist()
    for i in range(1, len(years)):
        gap = years[i] - years[i - 1]
        if gap > 1:
            results.append(ValidationResult(
                passed=False,
                rule_name='temporal_continuity',
                severity='warning',
                row_index=None,
                value=None,
                reason=f"Year gap between {years[i - 1]} and {years[i]} ({gap - 1} year(s) missing)",
            ))
    return results


def check_baseline_integrity(df: pd.DataFrame,
                              start: int = 1951,
                              end: int = 1980) -> list[ValidationResult]:
    """Verify the 1951-1980 baseline period has full monthly coverage."""
    baseline = df[(df['Year'] >= start) & (df['Year'] <= end)]
    expected = end - start + 1
    if len(baseline) < expected:
        return [ValidationResult(
            passed=False,
            rule_name='baseline_integrity',
            severity='warning',
            row_index=None,
            value=None,
            reason=f"Baseline period {start}-{end} has {len(baseline)}/{expected} years",
        )]
    incomplete = []
    for _, row in baseline.iterrows():
        missing = [c for c in MONTHLY_COLS if pd.isna(row.get(c))]
        if missing:
            incomplete.append(int(row['Year']))
    if incomplete:
        return [ValidationResult(
            passed=False,
            rule_name='baseline_integrity',
            severity='warning',
            row_index=None,
            value=None,
            reason=f"Baseline years with missing monthly values: {incomplete}",
        )]
    return []
