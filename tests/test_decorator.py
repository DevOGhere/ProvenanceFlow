"""Tests for the @track decorator."""
import pytest
import pandas as pd

from src.provenanceflow.decorator import track, tracked_runs
from src.provenanceflow.provenance.store import ProvenanceStore


@pytest.fixture
def sample_df():
    return pd.DataFrame({"a": [1, 2, None, 4], "b": [10, 20, 30, 40]})


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "track_test.db")


# ── Basic transparency ────────────────────────────────────────────────────────

def test_track_bare_returns_dataframe(sample_df, tmp_db):
    @track(db_path=tmp_db)
    def clean(df):
        return df.dropna()

    result = clean(sample_df)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3   # one null row dropped


def test_track_result_is_correct(sample_df, tmp_db):
    @track(db_path=tmp_db)
    def double_b(df):
        df = df.copy()
        df["b"] = df["b"] * 2
        return df

    result = double_b(sample_df)
    assert result["b"].iloc[0] == 20


def test_track_non_dataframe_function_is_transparent(tmp_db):
    @track(db_path=tmp_db)
    def add(x, y):
        return x + y

    assert add(3, 4) == 7


# ── Provenance recording ──────────────────────────────────────────────────────

def test_track_attaches_run_id_to_result(sample_df, tmp_db):
    @track(db_path=tmp_db)
    def clean(df):
        return df.dropna()

    result = clean(sample_df)
    assert "_prov_run_id" in result.attrs
    assert isinstance(result.attrs["_prov_run_id"], str)


def test_track_stores_provenance_in_db(sample_df, tmp_db):
    @track(db_path=tmp_db)
    def clean(df):
        return df.dropna()

    result = clean(sample_df)
    run_id = result.attrs["_prov_run_id"]

    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(run_id)
    assert doc is not None
    assert "entity" in doc
    assert "activity" in doc


def test_track_prov_contains_transform_activity(sample_df, tmp_db):
    @track(db_path=tmp_db)
    def clean(df):
        return df.dropna()

    result = clean(sample_df)
    run_id = result.attrs["_prov_run_id"]
    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(run_id)

    transform_activities = [k for k in doc.get("activity", {})
                            if "transform_" in k]
    assert len(transform_activities) == 1


def test_track_with_title_kwarg(sample_df, tmp_db):
    from src.provenanceflow.utils.prov_helpers import get_ingestion_entity

    @track(title="My cleaning step", db_path=tmp_db)
    def clean(df):
        return df.dropna()

    result = clean(sample_df)
    run_id = result.attrs["_prov_run_id"]
    store = ProvenanceStore(db_path=tmp_db)
    doc = store.get(run_id)
    _, ing = get_ingestion_entity(doc)
    assert "My cleaning step" in ing.get("dc:title", "")


def test_track_appends_to_tracked_runs(sample_df, tmp_db):
    initial_count = len(tracked_runs)

    @track(db_path=tmp_db)
    def passthrough(df):
        return df.copy()

    passthrough(sample_df)
    assert len(tracked_runs) == initial_count + 1


# ── Bare @track (no args) ─────────────────────────────────────────────────────

def test_track_bare_decorator_no_parens(tmp_db, monkeypatch):
    """@track (no parentheses) should work identically to @track()."""
    # monkeypatch settings to use tmp_db
    import src.provenanceflow.decorator as dec_mod
    original_settings_fn = dec_mod.get_settings

    class FakeSettings:
        prov_db_path = tmp_db

    monkeypatch.setattr(dec_mod, "get_settings", lambda: FakeSettings())

    @track
    def clean(df):
        return df.dropna()

    df = pd.DataFrame({"x": [1, None, 3]})
    result = clean(df)
    assert isinstance(result, pd.DataFrame)
    assert "_prov_run_id" in result.attrs
