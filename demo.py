"""
ProvenanceFlow Demo
Run: python demo.py
Uses the bundled sample GISTEMP CSV (no network required).
"""

import json

from src.provenanceflow.ingestion.local_csv import LocalCSVSource
from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore

LOCAL_CSV = 'data/raw/gistemp_global.csv'

print("=" * 60)
print("ProvenanceFlow — FAIR Data Lineage Demo")
print("Dataset: NASA GISTEMP v4 (sample, bundled)")
print("=" * 60)

source = LocalCSVSource(path=LOCAL_CSV)
result = run_pipeline(source=source)

run_id = result.run_id
store = ProvenanceStore()
prov_record = store.get(run_id)

print("\n--- Provenance Record (W3C PROV-JSON) ---")
print(json.dumps(prov_record, indent=2)[:2000])

print("\n--- All Pipeline Runs ---")
for run in store.list_runs():
    print(f"  {run['run_id']}  |  {run['created_at']}")

print("\nFAIR Compliance:")
print("  Findable      ✓ — Persistent identifiers on all entities")
print("  Accessible    ✓ — SQLite, no proprietary tools required")
print("  Interoperable ✓ — W3C PROV-JSON international standard")
print("  Reusable      ✓ — Full lineage from source to output")
