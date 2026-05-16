"""
Tests for the Pipeline context manager (Phase 3 — track chaining).

The contract: multiple @track calls inside a Pipeline produce ONE PROV
document with a correctly chained wasDerivedFrom graph, not disconnected
isolated records.
"""
import pytest
import pandas as pd

from src.provenanceflow import track, Pipeline
from src.provenanceflow.provenance.store import ProvenanceStore


@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / 'test.db')


@pytest.fixture
def df():
    return pd.DataFrame({'value': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})


# ── Core chain contract ───────────────────────────────────────────────────────

def test_pipeline_produces_one_run_id(tmp_db, df):
    @track
    def step1(df): return df.iloc[:7]

    @track
    def step2(df): return df.iloc[:5]

    with Pipeline(db_path=tmp_db) as p:
        out = step2(step1(df))

    assert p.run_id is not None
    assert p.run_id.startswith('run_')


def test_pipeline_stores_one_prov_doc(tmp_db, df):
    @track
    def step1(df): return df.iloc[:7]

    @track
    def step2(df): return df.iloc[:5]

    with Pipeline(db_path=tmp_db) as p:
        step2(step1(df))

    store = ProvenanceStore(db_path=tmp_db)
    runs = store.list_runs()
    assert len(runs) == 1
    assert runs[0]['run_id'] == p.run_id


def test_pipeline_chains_two_steps(tmp_db, df):
    """The chain test — if this passes, the graph is wired correctly."""
    @track
    def step1(df): return df.iloc[:7]

    @track
    def step2(df): return df.iloc[:5]

    with Pipeline(db_path=tmp_db) as p:
        step2(step1(df))

    doc = ProvenanceStore(db_path=tmp_db).get(p.run_id)

    # Two transform_ activities
    transform_ids = [a for a in doc.get('activity', {}) if 'transform_' in a]
    assert len(transform_ids) == 2, f"Expected 2 transform activities, got {transform_ids}"

    # Two wasDerivedFrom edges
    derivations = [
        (r.get('prov:generatedEntity'), r.get('prov:usedEntity'))
        for r in doc.get('wasDerivedFrom', {}).values()
        if isinstance(r, dict)
    ]
    assert len(derivations) == 2, f"Expected 2 derivations, got {derivations}"

    # Chain: one entity must appear as both a generated entity and a used entity
    generated = {g for g, _ in derivations}
    used = {u for _, u in derivations}
    assert generated & used, (
        "No entity is both produced by one step and consumed by the next — "
        "chain is broken"
    )


def test_pipeline_three_steps(tmp_db, df):
    @track
    def drop_last(df): return df.iloc[:-2]

    @track
    def drop_negatives(df): return df[df['value'] > 0]

    @track
    def take_top5(df): return df.iloc[:5]

    with Pipeline(db_path=tmp_db) as p:
        take_top5(drop_negatives(drop_last(df)))

    doc = ProvenanceStore(db_path=tmp_db).get(p.run_id)
    transforms = [a for a in doc.get('activity', {}) if 'transform_' in a]
    derivations = list(doc.get('wasDerivedFrom', {}).values())
    assert len(transforms) == 3
    assert len(derivations) == 3


def test_pipeline_run_id_on_result_attrs(tmp_db, df):
    @track
    def step1(df): return df.iloc[:7]

    @track
    def step2(df): return df.iloc[:5]

    with Pipeline(db_path=tmp_db) as p:
        r1 = step1(df)
        r2 = step2(r1)

    # Both intermediate and final results carry the pipeline run_id
    assert r1.attrs.get('_prov_run_id') == p.run_id
    assert r2.attrs.get('_prov_run_id') == p.run_id


# ── Standalone @track still works outside Pipeline ────────────────────────────

def test_standalone_track_unaffected_by_pipeline(tmp_db, df):
    """@track outside a Pipeline still creates its own isolated PROV doc."""
    @track(db_path=tmp_db)
    def standalone(df): return df.iloc[:5]

    result = standalone(df)
    run_id = result.attrs.get('_prov_run_id')
    assert run_id is not None

    store = ProvenanceStore(db_path=tmp_db)
    assert store.get(run_id) is not None


def test_standalone_and_pipeline_coexist(tmp_db, df):
    @track(db_path=tmp_db)
    def standalone(df): return df.iloc[:8]

    @track
    def chained_a(df): return df.iloc[:6]

    @track
    def chained_b(df): return df.iloc[:4]

    standalone_result = standalone(df)
    standalone_run_id = standalone_result.attrs['_prov_run_id']

    with Pipeline(db_path=tmp_db) as p:
        chained_b(chained_a(df))

    store = ProvenanceStore(db_path=tmp_db)
    runs = store.list_runs()
    assert len(runs) == 2
    run_ids = {r['run_id'] for r in runs}
    assert standalone_run_id in run_ids
    assert p.run_id in run_ids
    assert standalone_run_id != p.run_id


# ── Pipeline properties ───────────────────────────────────────────────────────

def test_pipeline_run_id_none_before_exit(tmp_db, df):
    @track
    def step(df): return df

    p = Pipeline(db_path=tmp_db)
    assert p.run_id is None  # not set until __exit__
    with p:
        step(df)
    assert p.run_id is not None


def test_pipeline_prov_has_one_agent(tmp_db, df):
    @track
    def step1(df): return df.iloc[:5]

    @track
    def step2(df): return df.iloc[:3]

    with Pipeline(db_path=tmp_db) as p:
        step2(step1(df))

    doc = ProvenanceStore(db_path=tmp_db).get(p.run_id)
    assert len(doc.get('agent', {})) == 1


def test_pipeline_first_entity_is_ingested(tmp_db, df):
    """First step in a Pipeline must still create an ingestion entity."""
    @track
    def step1(df): return df

    with Pipeline(db_path=tmp_db) as p:
        step1(df)

    doc = ProvenanceStore(db_path=tmp_db).get(p.run_id)
    dataset_entities = [e for e in doc.get('entity', {}) if 'dataset_' in e]
    assert len(dataset_entities) >= 1


def test_pipeline_non_dataframe_function_is_transparent(tmp_db):
    """@track on a non-DataFrame function inside Pipeline is a no-op."""
    @track
    def greet(name: str) -> str:
        return f"Hello {name}"

    with Pipeline(db_path=tmp_db) as p:
        result = greet("world")

    assert result == "Hello world"
    # No crash — pipeline finalizes cleanly even with no steps tracked
