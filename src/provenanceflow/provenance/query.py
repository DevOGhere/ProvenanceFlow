from .store import ProvenanceStore


def get_run(store: ProvenanceStore, run_id: str) -> dict | None:
    """Retrieve a full PROV-JSON document by run_id."""
    return store.get(run_id)


def list_runs(store: ProvenanceStore) -> list[dict]:
    """List all pipeline runs ordered by most recent first."""
    return store.list_runs()


def get_entities(store: ProvenanceStore, run_id: str) -> list[dict]:
    """Extract all PROV entities from a run's document."""
    doc = store.get(run_id)
    if not doc:
        return []
    return [
        {'id': eid, **attrs}
        for eid, attrs in doc.get('entity', {}).items()
    ]


def get_activities(store: ProvenanceStore, run_id: str) -> list[dict]:
    """Extract all PROV activities from a run's document."""
    doc = store.get(run_id)
    if not doc:
        return []
    return [
        {'id': aid, **attrs}
        for aid, attrs in doc.get('activity', {}).items()
    ]


def get_by_date_range(store: ProvenanceStore,
                      start: str, end: str) -> list[dict]:
    """
    Return all runs whose created_at falls within [start, end].
    Accepts ISO date strings (e.g. '2025-01-01' or '2025-01-01T00:00:00').
    """
    return store.query_by_date_range(start, end)


def get_by_dataset_id(store: ProvenanceStore, dataset_id: str) -> list[dict]:
    """
    Return all runs that contain an entity with the given fair:identifier value.
    Searches PROV-JSON entity attributes across all stored runs.
    """
    results = []
    for run_meta in store.list_runs():
        doc = store.get(run_meta['run_id'])
        if not doc:
            continue
        for attrs in doc.get('entity', {}).values():
            if attrs.get('fair:identifier') == dataset_id:
                results.append(run_meta)
                break
    return results
