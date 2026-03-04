import os
import tempfile
import textwrap
import pandas as pd
import pytest
from src.provenanceflow.ingestion.nasa_gistemp import parse_gistemp, MONTHLY_COLS


FIXTURE_CSV = textwrap.dedent("""\
    Global-mean monthly, seasonal, and annual means, 1880-present, updated through most recent month
    Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D,D-N,DJF,MAM,JJA,SON
    1880,-.15,-.21,-.16,-.10,-.11,-.22,-.18,-.26,-.20,-.24,-.19,-.17,-.18,****,****,-.12,-.22,-.21
    1881,-.19,-.14,  .02,  .05,  .06,-.20,  .00,-.03,-.14,-.22,-.17,-.07,-.08,-.09,-.17,  .04,-.08,-.18
    2024,  .51,  .90,****,  .75,  .93,  .77,  .67,  .83,  .88,  .96,  .86,  .79,****,  .78,  .77,  .72,  .76,  .90
""")


@pytest.fixture
def gistemp_csv(tmp_path):
    p = tmp_path / "gistemp.csv"
    p.write_text(FIXTURE_CSV)
    return str(p)


def test_parse_returns_dataframe(gistemp_csv):
    df = parse_gistemp(gistemp_csv)
    assert isinstance(df, pd.DataFrame)


def test_parse_row_count(gistemp_csv):
    df = parse_gistemp(gistemp_csv)
    assert len(df) == 3


def test_parse_year_column_is_int(gistemp_csv):
    df = parse_gistemp(gistemp_csv)
    assert df['Year'].dtype == int


def test_parse_missing_values_are_nan(gistemp_csv):
    df = parse_gistemp(gistemp_csv)
    # 1880 has **** in Mar position is fine; 2024 has **** in Mar and J-D
    assert pd.isna(df.loc[df['Year'] == 2024, 'Mar'].values[0])


def test_parse_has_monthly_cols(gistemp_csv):
    df = parse_gistemp(gistemp_csv)
    for col in MONTHLY_COLS:
        assert col in df.columns
