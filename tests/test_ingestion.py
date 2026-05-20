import os
import tempfile
import textwrap
import pandas as pd
import pytest
import requests as requests_lib
from src.provenanceflow.ingestion.nasa_gistemp import parse_gistemp, MONTHLY_COLS, download_gistemp


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


# ── parse_gistemp missing-value handling ─────────────────────────────────────

def test_parse_three_asterisk_missing_values_are_nan(tmp_path):
    """Live NASA CSV uses *** (3 asterisks) for seasonal columns — must parse as NaN."""
    csv = tmp_path / "gistemp_live.csv"
    csv.write_text(
        "Global-mean monthly, seasonal, and annual means\n"
        "Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D,D-N,DJF,MAM,JJA,SON\n"
        "1880,-.19,-.25,-.10,-.17,-.10,-.21,-.19,-.11,-.15,-.24,-.22,-.19,-.18,***,***,-.12,-.17,-.20\n"
    )
    df = parse_gistemp(str(csv))
    assert pd.isna(df.loc[df['Year'] == 1880, 'D-N'].values[0]), \
        "*** (3-asterisk) values must be parsed as NaN, not string"
    assert pd.isna(df.loc[df['Year'] == 1880, 'DJF'].values[0])


# ── download_gistemp HTTP behaviour ──────────────────────────────────────────

def test_download_gistemp_sends_user_agent(tmp_path, monkeypatch):
    """download_gistemp must include a User-Agent header to avoid NASA 403."""
    captured = {}

    def fake_get(url, timeout, headers=None):
        captured["headers"] = headers or {}

        class FakeResponse:
            content = FIXTURE_CSV.encode()
            def raise_for_status(self): pass

        return FakeResponse()

    monkeypatch.setattr(requests_lib, "get", fake_get)
    local = str(tmp_path / "gistemp.csv")
    download_gistemp("https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv", local)

    assert "User-Agent" in captured["headers"]
    assert "ProvenanceFlow" in captured["headers"]["User-Agent"]


def test_download_gistemp_raises_on_http_error(tmp_path, monkeypatch):
    """HTTPError from NASA propagates cleanly — not swallowed silently."""
    def fake_get(url, timeout, headers=None):
        class FakeResponse:
            content = b""
            def raise_for_status(self):
                raise requests_lib.HTTPError("403 Forbidden")

        return FakeResponse()

    monkeypatch.setattr(requests_lib, "get", fake_get)
    local = str(tmp_path / "gistemp.csv")
    with pytest.raises(requests_lib.HTTPError):
        download_gistemp("https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv", local)
