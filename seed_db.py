"""
Seed the provenance store with 4 runs for demo/dashboard use.

Produces meaningful variation so Compare Runs shows real differences:
  Run 1 — full dataset (all years, 1 bad row → 3.70% rejection)
  Run 2 — early years 1880-1900 only (no bad rows → 0% rejection)
  Run 3 — full dataset again (reproducibility check: same SHA-256 as Run 1)
  Run 4 — @track decorator run (DataFrame cleaning function)

Run: python seed_db.py
Requires: data/raw/gistemp_global.csv (bundled)
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd

from src.provenanceflow.ingestion.local_csv import LocalCSVSource
from src.provenanceflow.ingestion.nasa_gistemp import parse_gistemp
from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow import track

LOCAL_CSV = 'data/raw/gistemp_global.csv'
DB_PATH   = 'provenance_store/lineage.db'


def _write_gistemp_csv(df: pd.DataFrame) -> str:
    """Write a DataFrame in GISTEMP CSV format to a tempfile."""
    tmp = tempfile.NamedTemporaryFile(
        suffix='.csv', delete=False, mode='w', newline=''
    )
    tmp.write("Global-mean monthly, seasonal, and annual means, sample\n")
    df.to_csv(tmp, index=False)
    tmp.flush()
    tmp.close()
    return tmp.name


def _print_run(label: str, result) -> None:
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

    df_full = parse_gistemp(LOCAL_CSV)

    # ── Run 1: full dataset — has bad rows, 3.70% rejection ───────────────
    tmp1 = _write_gistemp_csv(df_full)
    r1 = run_pipeline(source=LocalCSVSource(path=tmp1), db_path=DB_PATH)
    _print_run('Run 1 (all years, with bad rows)', r1)
    os.unlink(tmp1)

    # ── Run 2: early years only — clean data, 0% rejection ───────────────
    df_early = df_full[df_full['Year'] <= 1900].copy().reset_index(drop=True)
    tmp2 = _write_gistemp_csv(df_early)
    r2 = run_pipeline(source=LocalCSVSource(path=tmp2), db_path=DB_PATH)
    _print_run('Run 2 (1880-1900 only, clean)', r2)
    os.unlink(tmp2)

    # ── Run 3: full dataset again — same SHA-256 as Run 1 (reproducibility)
    tmp3 = _write_gistemp_csv(df_full)
    r3 = run_pipeline(source=LocalCSVSource(path=tmp3), db_path=DB_PATH)
    _print_run('Run 3 (all years again, reproducibility check)', r3)
    os.unlink(tmp3)

    # ── Run 4: @track decorator run ───────────────────────────────────────
    @track(db_path=DB_PATH, title='remove_cold_anomalies')
    def remove_cold_anomalies(df: pd.DataFrame) -> pd.DataFrame:
        """Keep only rows where annual mean > -0.2°C."""
        return df[df['J-D'].notna() & (df['J-D'] > -0.2)].reset_index(drop=True)

    result_df = remove_cold_anomalies(df_full)
    track_run_id = result_df.attrs.get('_prov_run_id', '?')
    print(f'Run 4 (@track remove_cold_anomalies): {track_run_id}')
    print(f'  rows_in={len(df_full)}  rows_out={len(result_df)}  '
          f'dropped={len(df_full) - len(result_df)}')

    store = ProvenanceStore(db_path=DB_PATH)
    print(f'\nSeeded {len(store.list_runs())} runs into {DB_PATH}')
    print('\nUseful Compare Runs pairs:')
    print(f'  Run 1 vs Run 2  — different data, rejection rate 3.70% vs 0.00%')
    print(f'  Run 1 vs Run 3  — same SHA-256, proves reproducibility')
    print('Launch with: streamlit run dashboard.py')


if __name__ == '__main__':
    seed()
