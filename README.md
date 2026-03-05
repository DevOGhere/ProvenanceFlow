# ProvenanceFlow

> Lightweight FAIR-compliant data lineage tracking for scientific ML pipelines.

ProvenanceFlow automatically captures the full provenance history of scientific datasets
as they move through validation and ML training pipelines, storing lineage records in
W3C PROV-JSON format for reproducibility and FAIR compliance.

## The Problem

ML research has a reproducibility crisis. Most published results cannot be reproduced
because the full data lineage — what dataset, which version, what transformations,
what was rejected and why — is never recorded. Existing tools (MLflow, W&B) track
models. Nobody adequately tracks the **data transformation layer** at HPC scale with
FAIR compliance.

## What ProvenanceFlow Does

- Downloads scientific datasets (currently: NASA GISTEMP v4 climate data)
- Runs configurable validation rules (null checks, range checks, completeness, temporal continuity, baseline integrity)
- Records every transformation as a W3C PROV-JSON provenance document
- Persists lineage to a queryable SQLite store — no proprietary tools required
- Assigns FAIR-compliant persistent identifiers (UUID-based) to all entities
- SHA-256 fingerprints every dataset for integrity verification
- Orchestrates via Apache Airflow DAGs for HPC-scale scheduling

## Quick Start

```bash
pip install -r requirements.txt
python demo.py
```

## Running with Apache Airflow (Layer 2)

Requires Docker Desktop with ≥ 4 GB RAM.

```bash
# 1. Set your user ID so Airflow writes files with correct ownership
echo "AIRFLOW_UID=$(id -u)" > docker/.env

# 2. Initialise (first time only — creates DB and admin user)
docker compose -f docker/docker-compose.yaml up airflow-init

# 3. Start webserver + scheduler
docker compose -f docker/docker-compose.yaml up -d

# 4. Open http://localhost:8080  (airflow / airflow)
#    The 'provenanceflow_gistemp_pipeline' DAG will appear automatically.

# Stop
docker compose -f docker/docker-compose.yaml down
```

## Architecture

```
Raw Data (NASA GISTEMP v4)
       │
       ▼
  [Ingestion]  ←── SHA-256 fingerprint + PID assigned
       │
       ▼
  [Validation] ←── null_check, range_check, completeness_check,
       │             temporal_continuity, baseline_integrity
       ▼
  [PROV Tracker] ←── W3C PROV-JSON document built
       │              Entity → Activity → Agent graph
       ▼
  [SQLite Store] ←── Queryable by run_id, date range, dataset_id
       │
       ▼
  Provenance Record (FAIR-compliant)
```

## FAIR Compliance

| Principle | Implementation |
|---|---|
| **Findable** | UUID-based persistent identifiers on every entity and pipeline run |
| **Accessible** | SQLite storage — queryable without proprietary tools |
| **Interoperable** | W3C PROV-JSON — the international standard for provenance data |
| **Reusable** | Full lineage from raw download to validated output, with rejection rationale |

## Validation Rules

| Rule | Type | Description |
|---|---|---|
| `null_check` | warning / hard_rejection | Flags rows with missing monthly values (`****` in source) |
| `range_check` | hard_rejection | Annual mean outside physically plausible range [-3.0, +3.0]°C |
| `completeness_check` | hard_rejection | More than 3 monthly values missing in a single year |
| `temporal_continuity` | warning | Gaps in the year sequence |
| `baseline_integrity` | warning | Incomplete coverage of the 1951-1980 anomaly baseline period |

## Provenance Record Example

Each pipeline run produces a W3C PROV-JSON document:

```json
{
  "entity": {
    "pf:dataset_abc123": {
      "prov:label": "Raw dataset from https://data.giss.nasa.gov/...",
      "fair:identifier": "dataset_abc123def456",
      "pf:row_count": 1716,
      "pf:checksum_sha256": "e3b0c44298fc1c149afb..."
    }
  },
  "activity": {
    "pf:validate_d7f3a1b2": {
      "pf:rules_applied": ["null_check", "range_check", "completeness_check"],
      "pf:rows_in": 1716,
      "pf:rows_passed": 1698,
      "pf:rows_rejected": 18,
      "pf:rejection_rate": 0.0105
    }
  },
  "wasDerivedFrom": { ... },
  "wasGeneratedBy": { ... }
}
```

## Querying Lineage

```python
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.query import get_run, get_by_date_range, get_by_dataset_id

store = ProvenanceStore()

# Get a specific run
doc = get_run(store, run_id)

# Query by date range
runs = get_by_date_range(store, start='2025-01-01', end='2025-12-31')

# Find all runs touching a specific dataset
runs = get_by_dataset_id(store, dataset_id='dataset_abc123def456')
```

## Repository Structure

```
provenanceflow/
├── src/provenanceflow/
│   ├── ingestion/       ← NASA GISTEMP download + parsing
│   ├── validation/      ← 5 validation rules + Validator orchestrator
│   ├── provenance/      ← W3C PROV tracker, SQLite store, query API
│   ├── pipeline/        ← Full pipeline runner
│   └── utils/           ← PID generation, SHA-256 checksums
├── dags/                ← Apache Airflow DAG
├── tests/               ← pytest test suite
└── demo.py              ← Single-script end-to-end demo
```

## Standards

- **W3C PROV-DM**: https://www.w3.org/TR/prov-dm/
- **FAIR Principles**: Wilkinson et al. (2016), *Scientific Data* — https://doi.org/10.1038/sdata.2016.18
- **NFDIxCS**: National Research Data Infrastructure for Computer Science — https://nfdi4cs.org/

## Relevance to Research Data Infrastructure

This project implements the data-layer provenance infrastructure relevant to:

- Souza et al. (2022), "Workflow Provenance in the Lifecycle of Scientific ML"
- GWDG Research Data Management and FAIR infrastructure work
- HeKKSaGOn Data Science priority area ("Data Science, Digitalization and AI")
- NFDIxCS national research data infrastructure for Computer Science

The validation pipeline (hard rejection / warning severity tiers) mirrors production
data quality patterns from Databricks bronze/silver layer architecture, translated into
research-grade W3C PROV standard language.
