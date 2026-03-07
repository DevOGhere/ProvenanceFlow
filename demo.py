"""
ProvenanceFlow Demo
Run: python demo.py
"""
import json

from src.provenanceflow.ingestion.nasa_gistemp import NASAGISTEMPSource
from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore

NASA_GLOBAL_URL = 'https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv'

print("=" * 60)
print("ProvenanceFlow — FAIR Data Lineage Demo")
print("Dataset: NASA GISTEMP v4 Global Surface Temperature")
print("=" * 60)

source = NASAGISTEMPSource(url=NASA_GLOBAL_URL, output_dir='data/raw')
result = run_pipeline(source=source)
run_id = result.run_id

print(f"\nRows in: {result.validation.rows_in} | "
      f"Passed: {result.validation.rows_passed} | "
      f"Rejected: {result.validation.rows_rejected} | "
      f"Rejection rate: {result.validation.rejection_rate:.1%}")

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
