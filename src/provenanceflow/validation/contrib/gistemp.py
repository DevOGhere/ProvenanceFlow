"""
Built-in validation rules for NASA GISTEMP v4 CSV data.

These are domain-specific rules that understand GISTEMP's column structure
(monthly anomaly columns Jan–Dec, annual mean J-D, Year column).
Import GISTEMP_RULES and pass to Validator to validate GISTEMP-format data.

    from provenanceflow.validation.contrib.gistemp import GISTEMP_RULES
    from provenanceflow.validation.validator import Validator

    validator = Validator(rules=GISTEMP_RULES)
    results = validator.validate(df)
"""
from __future__ import annotations

import pandas as pd

from ..rule import rule

MONTHLY_COLS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


@rule(name="null_check", severity="warning")
def null_check(row, idx):
    """Flag rows with missing monthly temperature values.

    Severity scales with how many values are absent:
      - 1–3 missing → warning  (partial data, still usable)
      - 4+  missing → hard_rejection  (row too sparse for meaningful analysis)
    """
    missing = [c for c in MONTHLY_COLS if pd.isna(row.get(c))]
    if not missing:
        return None
    severity = "hard_rejection" if len(missing) > 3 else "warning"
    return (f"Missing values in columns: {missing}", severity)


@rule(name="range_check", severity="hard_rejection")
def range_check(row, idx):
    """Reject rows whose annual mean anomaly exceeds physically plausible bounds.

    The range [-3.0, +3.0]°C covers the full observed historical variability
    in the GISTEMP record. Values outside it indicate data errors, not climate.
    """
    annual = row.get("J-D")
    if pd.isna(annual):
        return None
    val = float(annual)
    if not (-3.0 <= val <= 3.0):
        return f"Annual mean {val}°C outside plausible range [-3.0, 3.0]"


@rule(name="completeness_check", severity="hard_rejection")
def completeness_check(row, idx):
    """Hard-reject rows missing more than 3 monthly values.

    A row with 4+ missing months cannot contribute a meaningful annual
    statistic and must be excluded from downstream analysis.
    Threshold of 3 matches the GISTEMP data quality convention.
    """
    missing = [c for c in MONTHLY_COLS if pd.isna(row.get(c))]
    if len(missing) > 3:
        return f"{len(missing)} monthly values missing (threshold: 3)"


@rule(name="temporal_continuity", severity="warning")
def temporal_continuity(df):
    """Warn when the year sequence contains gaps.

    Gaps indicate missing data years, which can bias trend calculations
    in climate analysis. Flagged as warnings rather than rejections because
    gaps in the GISTEMP record are a known historical artefact.
    """
    years = df["Year"].sort_values().tolist()
    results = []
    for i in range(1, len(years)):
        gap = int(years[i]) - int(years[i - 1])
        if gap > 1:
            results.append((
                None,
                f"Year gap between {years[i-1]} and {years[i]} "
                f"({gap - 1} year(s) missing)",
            ))
    return results


@rule(name="baseline_integrity", severity="warning")
def baseline_integrity(df):
    """Verify the 1951–1980 anomaly baseline period has complete monthly coverage.

    GISTEMP expresses all values as anomalies relative to the 1951–1980 mean.
    If that period is incomplete or contains missing monthly values, every
    anomaly in the record is potentially miscalibrated.
    """
    baseline = df[(df["Year"] >= 1951) & (df["Year"] <= 1980)]
    expected = 30

    if len(baseline) < expected:
        return [(
            None,
            f"Baseline period 1951–1980 has {len(baseline)}/{expected} years",
        )]

    incomplete_years = []
    for _, row in baseline.iterrows():
        if any(pd.isna(row.get(c)) for c in MONTHLY_COLS):
            incomplete_years.append(int(row["Year"]))

    if incomplete_years:
        return [(
            None,
            f"Baseline years with missing monthly values: {incomplete_years}",
        )]
    return []


# Ordered list passed to Validator(rules=GISTEMP_RULES).
# Row-level rules run first (per-row), then dataframe-level rules.
GISTEMP_RULES = [
    null_check,
    range_check,
    completeness_check,
    temporal_continuity,
    baseline_integrity,
]

GISTEMP_RULE_NAMES = [r.name for r in GISTEMP_RULES]
