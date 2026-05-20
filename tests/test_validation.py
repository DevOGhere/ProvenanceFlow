import pandas as pd
import pytest

from src.provenanceflow.validation.contrib.gistemp import (
    null_check,
    range_check,
    completeness_check,
    temporal_continuity,
    baseline_integrity,
    MONTHLY_COLS,
    GISTEMP_RULES,
)
from src.provenanceflow.validation.validator import Validator


def make_row(year=1990, monthly_val=0.10, annual=0.10, nulls=None):
    data = {col: monthly_val for col in MONTHLY_COLS}
    data["Year"] = year
    data["J-D"] = annual
    if nulls:
        for col in nulls:
            data[col] = float("nan")
    return pd.Series(data)


def make_df(years, monthly_val=0.10):
    return pd.DataFrame([make_row(year=y, monthly_val=monthly_val) for y in years])


# ── null_check ─────────────────────────────────────────────────────────────────

def test_null_check_clean_row():
    assert null_check(make_row(), 0) == []


def test_null_check_warning_few_nulls():
    results = null_check(make_row(nulls=["Jan", "Feb"]), 0)
    assert len(results) == 1
    assert results[0].severity == "warning"
    assert results[0].rule_name == "null_check"


def test_null_check_hard_rejection_many_nulls():
    results = null_check(make_row(nulls=["Jan", "Feb", "Mar", "Apr"]), 0)
    assert results[0].severity == "hard_rejection"


def test_null_check_exactly_3_nulls_is_warning():
    results = null_check(make_row(nulls=["Jan", "Feb", "Mar"]), 0)
    assert len(results) == 1
    assert results[0].severity == "warning"


# ── range_check ────────────────────────────────────────────────────────────────

def test_range_check_valid():
    assert range_check(make_row(annual=1.5), 0) == []


def test_range_check_exceeds_max():
    results = range_check(make_row(annual=3.1), 0)
    assert len(results) == 1
    assert results[0].severity == "hard_rejection"
    assert "3.1" in results[0].reason


def test_range_check_below_min():
    results = range_check(make_row(annual=-3.5), 0)
    assert results[0].severity == "hard_rejection"


def test_range_check_nan_annual_skipped():
    row = make_row()
    row["J-D"] = float("nan")
    assert range_check(row, 0) == []


def test_range_check_exactly_at_bounds_passes():
    assert range_check(make_row(annual=-3.0), 0) == []
    assert range_check(make_row(annual=3.0), 0) == []


# ── completeness_check ─────────────────────────────────────────────────────────

def test_completeness_ok():
    assert completeness_check(make_row(nulls=["Jan"]), 0) == []


def test_completeness_exactly_3_nulls_passes():
    assert completeness_check(make_row(nulls=["Jan", "Feb", "Mar"]), 0) == []


def test_completeness_exactly_4_nulls_hard_rejection():
    results = completeness_check(make_row(nulls=["Jan", "Feb", "Mar", "Apr"]), 0)
    assert len(results) == 1
    assert results[0].severity == "hard_rejection"
    assert results[0].rule_name == "completeness_check"


def test_completeness_hard_rejection():
    results = completeness_check(make_row(nulls=["Jan", "Feb", "Mar", "Apr"]), 0)
    assert results[0].severity == "hard_rejection"


# ── temporal_continuity ────────────────────────────────────────────────────────

def test_temporal_continuity_no_gaps():
    assert temporal_continuity(make_df([1880, 1881, 1882])) == []


def test_temporal_continuity_detects_gap():
    results = temporal_continuity(make_df([1880, 1882]))
    assert len(results) == 1
    assert "1880" in results[0].reason and "1882" in results[0].reason


# ── baseline_integrity ─────────────────────────────────────────────────────────

def test_baseline_integrity_pass():
    assert baseline_integrity(make_df(list(range(1951, 1981)))) == []


def test_baseline_integrity_incomplete_years():
    results = baseline_integrity(make_df(list(range(1951, 1970))))
    assert len(results) == 1
    assert results[0].rule_name == "baseline_integrity"


def test_baseline_integrity_missing_monthly_within_baseline():
    rows = [make_row(year=y) for y in range(1951, 1981)]
    df = pd.DataFrame(rows)
    df.loc[df["Year"] == 1960, "Jun"] = float("nan")
    results = baseline_integrity(df)
    assert len(results) == 1
    assert "1960" in results[0].reason


# ── Validator integration ──────────────────────────────────────────────────────

def test_validator_requires_rules():
    with pytest.raises(ValueError, match="at least one rule"):
        Validator(rules=[])


def test_validator_get_clean_removes_hard_rejections():
    df = pd.DataFrame([
        make_row(year=1990, annual=0.5),
        make_row(year=1991, annual=9.9),
    ])
    v = Validator(rules=GISTEMP_RULES)
    results = v.validate(df)
    clean = v.get_clean(df, results)
    assert len(clean) == 1
    assert clean.iloc[0]["Year"] == 1990


def test_validator_get_clean_with_zero_rejections():
    df = pd.DataFrame([make_row(year=y) for y in range(1990, 1995)])
    v = Validator(rules=GISTEMP_RULES)
    results = v.validate(df)
    rejections = [r for r in results if r.severity == "hard_rejection"]
    assert rejections == []
    assert len(v.get_clean(df, results)) == len(df)


def test_validator_rejection_summary():
    df = pd.DataFrame([make_row(year=1990, annual=9.9)])
    v = Validator(rules=GISTEMP_RULES)
    summary = v.rejection_summary(v.validate(df))
    assert summary.get("range_check", 0) >= 1


def test_validator_warning_summary():
    df = pd.DataFrame([make_row(year=1990, nulls=["Jan"])])
    v = Validator(rules=GISTEMP_RULES)
    summary = v.warning_summary(v.validate(df))
    assert "null_check" in summary


def test_validator_rule_names_match_gistemp():
    v = Validator(rules=GISTEMP_RULES)
    assert v.rule_names == [r.name for r in GISTEMP_RULES]
