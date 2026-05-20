"""
Pipeline context manager — chains multiple @track calls into one PROV graph.

Without Pipeline, each @track call creates an isolated provenance record:
    df1 = clean(df)       # → run_id_A (disconnected)
    df2 = normalize(df1)  # → run_id_B (disconnected)

With Pipeline, all @track calls share one ProvenanceTracker and the output
of each step becomes the input of the next, producing a single PROV document
with correct wasDerivedFrom edges across the full chain:
    with Pipeline() as p:
        df1 = clean(df)       # step 1 — recorded in shared tracker
        df2 = normalize(df1)  # step 2 — chained: output_1 → input_2

    p.run_id  # one ID for the complete lineage record

Implementation note:
    The active pipeline is stored in a contextvars.ContextVar so it works
    correctly in async and concurrent contexts without global state.

Do NOT nest Pipelines. The inner Pipeline will finalize and close before
the outer one sees the intermediate results, producing disconnected records.
"""
from __future__ import annotations

import contextvars
import uuid

import prov.model as prov

from ..provenance.tracker import ProvenanceTracker
from ..provenance.store import ProvenanceStore
from ..config import get_settings


# Module-level context variable — holds the currently active Pipeline, or None
_active_pipeline: contextvars.ContextVar['Pipeline | None'] = contextvars.ContextVar(
    '_active_pipeline', default=None
)


class Pipeline:
    """Context manager that chains multiple @track calls into one PROV graph.

    Args:
        db_path: SQLite store path. Defaults to the configured prov_db_path.

    Usage:
        with Pipeline(db_path="study/prov.db") as p:
            df1 = remove_nulls(df)     # @track
            df2 = normalize(df1)       # @track — chains wasDerivedFrom
            df3 = filter_range(df2)    # @track — continues chain

        print(p.run_id)          # one run_id for the entire chain
        store.get(p.run_id)      # single PROV doc with full lineage
    """

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path  = db_path or str(get_settings().prov_db_path)
        self.tracker  = ProvenanceTracker()
        self.store    = ProvenanceStore(db_path=self.db_path)
        self.run_id: str | None = None

        # The output entity of the most recently completed step.
        # None until the first @track call completes.
        # The next @track call uses this as its input entity instead of
        # creating a fresh ingestion entity.
        self._last_entity: prov.ProvEntity | None = None
        self._token = None  # contextvars reset token

    def __enter__(self) -> 'Pipeline':
        self._token = _active_pipeline.set(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # Always finalize — partial provenance on failure is more useful than none.
        self.run_id = self.tracker.finalize(self.store)
        _active_pipeline.reset(self._token)
        return False  # never suppress exceptions
