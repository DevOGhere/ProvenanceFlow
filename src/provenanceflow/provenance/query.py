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
