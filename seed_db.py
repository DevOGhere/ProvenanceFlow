"""
Seed the provenance store with pipeline runs + a @track decorator run.

Produces a realistic demo DB with:
  - 3 pipeline runs showing actual rejections and warnings
  - 1 @track run from a DataFrame cleaning function
  - Compare Runs showing identical SHA-256 (same input data)

Run: python seed_db.py
Requires: data/raw/gistemp_global.csv (bundled)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd

from src.provenanceflow.ingestion.local_csv import LocalCSVSource
from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow import track

LOCAL_CSV = 'data/raw/gistemp_global.csv'
DB_PATH   = 'provenance_store/lineage.db'


def _print_run(label, result):
    v = result.validation
    print(f'{label}: {result.run_id}')
    print(f'  rows_in={v.rows_in}  passed={v.rows_passed}  '
          f'rejected={v.rows_rejected}  rate={v.rejection_rate:.2%}')
    if v.rejections_by_rule:
        print(f'  hard rejections: {v.rejections_by_rule}')
    if v.warnings_by_rule:
        print(f'  warnings:        {v.warnings_by_rule}')


def seed():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f'Removed existing {DB_PATH}')

    # ── 3 pipeline runs ───────────────────────────────────────────────────
    for i in range(3):
        source = LocalCSVSource(path=LOCAL_CSV)
        result = run_pipeline(source=source, db_path=DB_PATH)
        _print_run(f'Pipeline run {i + 1}', result)

    # ── 1 @track decorator run ────────────────────────────────────────────
    from src.provenanceflow.ingestion.nasa_gistemp import parse_gistemp

    @track(db_path=DB_PATH, title='remove_cold_anomalies')
    def remove_cold_anomalies(df: pd.DataFrame) -> pd.DataFrame:
        """Keep only rows where annual mean > -0.2°C."""
        return df[df['J-D'].notna() & (df['J-D'] > -0.2)].reset_index(drop=True)

    df = parse_gistemp(LOCAL_CSV)
    result_df = remove_cold_anomalies(df)
    track_run_id = result_df.attrs.get('_prov_run_id', '?')
    print(f'\n@track run: {track_run_id}')
    print(f'  rows_in={len(df)}  rows_out={len(result_df)}  '
          f'dropped={len(df) - len(result_df)}')

    store = ProvenanceStore(db_path=DB_PATH)
    total = len(store.list_runs())
    print(f'\nSeeded {total} runs into {DB_PATH}  (3 pipeline + 1 @track)')
    print('Launch with: streamlit run dashboard.py')


if __name__ == '__main__':
    seed()
