"""
@track decorator — zero-friction provenance capture for DataFrame-transforming functions.

Usage:
    from provenanceflow import track

    @track
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna()

    @track(title="Filter anomalies", db_path="study/prov.db")
    def filter_pos(df: pd.DataFrame) -> pd.DataFrame:
        return df[df["value"] > 0]

The decorator:
  - Finds the first pd.DataFrame argument (positional or keyword)
  - Computes a SHA-256 checksum of the input and output DataFrames
  - Records a W3C PROV document: input entity → transform activity → output entity
  - Stores the record in the configured provenance store
  - Attaches the run_id to the result DataFrame via result.attrs['_prov_run_id']
  - Returns the original function's return value unchanged (fully transparent)
"""
from __future__ import annotations

import functools
import hashlib
import inspect
import tempfile
from pathlib import Path
from typing import Callable

import pandas as pd

from .provenance.tracker import ProvenanceTracker
from .provenance.store import ProvenanceStore
from .config import get_settings

# All run_ids produced by @track in this process — useful for testing/inspection
tracked_runs: list[str] = []


def _df_checksum(df: pd.DataFrame) -> str:
    """SHA-256 of the DataFrame serialised as CSV (no index)."""
    return hashlib.sha256(df.to_csv(index=False).encode()).hexdigest()


def _find_dataframe(args, kwargs, func) -> pd.DataFrame | None:
    """Return the first pd.DataFrame argument, positional or keyword."""
    sig = inspect.signature(func)
    for i, (name, _) in enumerate(sig.parameters.items()):
        if i < len(args) and isinstance(args[i], pd.DataFrame):
            return args[i]
        if name in kwargs and isinstance(kwargs[name], pd.DataFrame):
            return kwargs[name]
    return None


def track(_func=None, *, title: str | None = None, db_path: str | None = None):
    """Decorator: record W3C PROV provenance for any DataFrame-transforming function.

    Works as both a bare decorator (@track) and a parametrised one
    (@track(title=..., db_path=...)).

    Args:
        title:   Human-readable label stored as dc:title in the PROV entity.
                 Defaults to func.__module__ + '.' + func.__qualname__.
        db_path: Path to the SQLite provenance store.
                 Defaults to get_settings().prov_db_path.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            df_in = _find_dataframe(args, kwargs, func)

            # No DataFrame argument — call function transparently
            if df_in is None:
                return func(*args, **kwargs)

            func_label = title or f"{func.__module__}.{func.__qualname__}"
            func_location = f"file://{inspect.getfile(func)}"
            in_checksum = _df_checksum(df_in)

            # Write df_in to a temp file so track_ingestion can sha256_file it
            tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w")
            try:
                df_in.to_csv(tmp, index=False)
                tmp.flush()
                tmp_path = tmp.name
            finally:
                tmp.close()

            db = db_path or str(get_settings().prov_db_path)
            store = ProvenanceStore(db_path=db)
            tracker = ProvenanceTracker()

            raw_entity = tracker.track_ingestion(
                source_url=func_location,
                local_path=tmp_path,
                row_count=len(df_in),
                title=func_label,
            )

            # Call the actual function
            result = func(*args, **kwargs)

            df_out = result if isinstance(result, pd.DataFrame) else None
            out_checksum = _df_checksum(df_out) if df_out is not None else ""
            rows_out = len(df_out) if df_out is not None else len(df_in)

            tracker.track_transformation(
                input_entity=raw_entity,
                rows_in=len(df_in),
                rows_out=rows_out,
                function_name=func.__qualname__,
                checksum_in=in_checksum,
                checksum_out=out_checksum,
            )

            run_id = tracker.finalize(store)
            tracked_runs.append(run_id)

            Path(tmp_path).unlink(missing_ok=True)

            # Attach run_id to result DataFrame (non-destructive)
            if isinstance(result, pd.DataFrame):
                result.attrs["_prov_run_id"] = run_id

            return result

        wrapper._is_tracked = True
        return wrapper

    return decorator(_func) if _func is not None else decorator
