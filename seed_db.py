"""
Seed the provenance store with 3 pipeline runs for demo/dashboard use.

Run: python seed_db.py
Requires: data/raw/gistemp_global.csv (bundled)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from src.provenanceflow.ingestion.local_csv import LocalCSVSource
from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore

LOCAL_CSV = 'data/raw/gistemp_global.csv'
DB_PATH   = 'provenance_store/lineage.db'


def seed():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f'Removed existing {DB_PATH}')

    run_ids = []
    labels  = ['Run 1 — baseline', 'Run 2 — rerun same data', 'Run 3 — third pass']

    for label in labels:
        source = LocalCSVSource(path=LOCAL_CSV)
        result = run_pipeline(source=source, db_path=DB_PATH)
        run_ids.append(result.run_id)
        print(f'{label}: {result.run_id}')
        print(f'  rows_in={result.validation.rows_in}  '
              f'passed={result.validation.rows_passed}  '
              f'rejected={result.validation.rows_rejected}  '
              f'rate={result.validation.rejection_rate:.2%}')

    store = ProvenanceStore(db_path=DB_PATH)
    print(f'\nSeeded {len(store.list_runs())} runs into {DB_PATH}')
    print('Launch with: streamlit run dashboard.py')


if __name__ == '__main__':
    seed()
