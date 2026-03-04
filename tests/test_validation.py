import pandas as pd
import pytest
from src.provenanceflow.validation.rules import (
    check_null_values,
    check_temperature_range,
    check_completeness,
    check_temporal_continuity,
    check_baseline_integrity,
    MONTHLY_COLS,
)
from src.provenanceflow.validation.validator import Validator


def make_row(year=1990, monthly_val=0.10, annual=0.10, nulls=None):
    """Build a minimal GISTEMP-style row."""
    data = {col: monthly_val for col in MONTHLY_COLS}
    data['Year'] = year
    data['J-D'] = annual
    if nulls:
        for col in nulls:
            data[col] = float('nan')
    return pd.Series(data)


def make_df(years, monthly_val=0.10):
    rows = [make_row(year=y, monthly_val=monthly_val) for y in years]
    return pd.DataFrame(rows)


# --- check_null_values ---

def test_null_check_clean_row():
    assert check_null_values(make_row(), 0) == []


def test_null_check_warning_few_nulls():
    results = check_null_values(make_row(nulls=['Jan', 'Feb']), 0)
    assert len(results) == 1
    assert results[0].severity == 'warning'
    assert results[0].rule_name == 'null_check'


def test_null_check_hard_rejection_many_nulls():
    results = check_null_values(make_row(nulls=['Jan', 'Feb', 'Mar', 'Apr']), 0)
    assert results[0].severity == 'hard_rejection'


# --- check_temperature_range ---

def test_range_check_valid():
    assert check_temperature_range(make_row(annual=1.5), 0) == []


def test_range_check_exceeds_max():
    results = check_temperature_range(make_row(annual=3.1), 0)
    assert len(results) == 1
    assert results[0].severity == 'hard_rejection'
    assert results[0].value == pytest.approx(3.1)


def test_range_check_below_min():
    results = check_temperature_range(make_row(annual=-3.5), 0)
    assert results[0].severity == 'hard_rejection'


def test_range_check_nan_annual_skipped():
    row = make_row()
    row['J-D'] = float('nan')
    assert check_temperature_range(row, 0) == []


# --- check_completeness ---

def test_completeness_ok():
    assert check_completeness(make_row(nulls=['Jan']), 0) == []


def test_completeness_hard_rejection():
    results = check_completeness(make_row(nulls=['Jan', 'Feb', 'Mar', 'Apr']), 0)
    assert results[0].severity == 'hard_rejection'
    assert results[0].rule_name == 'completeness_check'


# --- check_temporal_continuity ---

def test_temporal_continuity_no_gaps():
    df = make_df([1880, 1881, 1882])
    assert check_temporal_continuity(df) == []


def test_temporal_continuity_detects_gap():
    df = make_df([1880, 1882])  # gap at 1881
    results = check_temporal_continuity(df)
    assert len(results) == 1
    assert '1880' in results[0].reason and '1882' in results[0].reason


# --- check_baseline_integrity ---

def test_baseline_integrity_pass():
    df = make_df(list(range(1951, 1981)))
    assert check_baseline_integrity(df) == []


def test_baseline_integrity_incomplete_years():
    df = make_df(list(range(1951, 1970)))  # only 19 of 30 years
    results = check_baseline_integrity(df)
    assert len(results) == 1
    assert results[0].rule_name == 'baseline_integrity'


# --- Validator integration ---

def test_validator_get_clean_removes_hard_rejections():
    # Row 0: clean, Row 1: hard rejection (annual out of range)
    df = pd.DataFrame([
        make_row(year=1990, annual=0.5),
        make_row(year=1991, annual=9.9),
    ])
    v = Validator()
    results = v.validate(df)
    clean = v.get_clean(df, results)
    assert len(clean) == 1
    assert clean.iloc[0]['Year'] == 1990


def test_validator_rejection_summary():
    df = pd.DataFrame([make_row(year=1990, annual=9.9)])
    v = Validator()
    results = v.validate(df)
    summary = v.rejection_summary(results)
    assert 'range_check' in summary
    assert summary['range_check'] >= 1


def test_validator_warning_summary():
    df = pd.DataFrame([make_row(year=1990, nulls=['Jan'])])
    v = Validator()
    results = v.validate(df)
    summary = v.warning_summary(results)
    assert 'null_check' in summary
