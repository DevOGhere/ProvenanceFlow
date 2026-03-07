"""
/runs router — thin delegation to provenance query functions.
No business logic here; all logic lives in query.py and store.py.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from ...provenance.store import ProvenanceStore
from ...provenance import query as q

router = APIRouter(tags=["runs"])


def _get_store() -> ProvenanceStore:
    """Dependency: returns a ProvenanceStore using the configured DB path."""
    from ...config import get_settings
    return ProvenanceStore(db_path=str(get_settings().prov_db_path))


@router.get("")
def list_runs(store: ProvenanceStore = Depends(_get_store)) -> list[dict]:
    return q.list_runs(store)


@router.get("/search")
def search_runs(
    dataset_id: str | None = Query(default=None),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    store: ProvenanceStore = Depends(_get_store),
) -> list[dict]:
    if dataset_id is not None:
        return q.get_by_dataset_id(store, dataset_id)
    if start is not None and end is not None:
        return q.get_by_date_range(store, start, end)
    raise HTTPException(
        status_code=400,
        detail="Provide either dataset_id or both start and end query parameters.",
    )


@router.get("/{run_id}")
def get_run(run_id: str, store: ProvenanceStore = Depends(_get_store)) -> dict:
    doc = q.get_run(store, run_id)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")
    return doc


@router.get("/{run_id}/entities")
def get_entities(run_id: str, store: ProvenanceStore = Depends(_get_store)) -> list[dict]:
    entities = q.get_entities(store, run_id)
    if not entities:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found or has no entities.")
    return entities


@router.get("/{run_id}/activities")
def get_activities(run_id: str, store: ProvenanceStore = Depends(_get_store)) -> list[dict]:
    activities = q.get_activities(store, run_id)
    if not activities:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found or has no activities.")
    return activities


@router.get("/{run_id_a}/compare/{run_id_b}")
def compare(run_id_a: str, run_id_b: str,
            store: ProvenanceStore = Depends(_get_store)) -> dict:
    """Compare two pipeline runs and return a structured diff."""
    import dataclasses
    from ...provenance.compare import compare_runs
    try:
        diff = compare_runs(run_id_a, run_id_b, store)
        return dataclasses.asdict(diff)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
