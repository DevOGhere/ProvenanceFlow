# CLAUDE.md — ProvenanceFlow Project Bible

## Complete Context for Claude Code

-----

## 0. READ THIS FIRST

This file is the single source of truth for building ProvenanceFlow. Do not improvise architecture. Do not add features not listed here. Do not simplify the PROV-JSON layer — it is the entire academic point of the project. Every decision in this file has a reason tied to the audience this project is built for.

The audience is **Prof. Dr. Ramin Yahyapour**, Managing Director of GWDG and Chair of Practical Computer Science at University of Göttingen. He leads the Data Science pillar of the HeKKSaGOn German-Japanese university network. His group works on: FAIR data infrastructure, research data management pipelines, workflow orchestration for HPC systems, and the NFDIxCS national research data infrastructure project. His contact: ramin.yahyapour@gwdg.de

This project must speak his exact research language on first read.

-----

## 1. What is ProvenanceFlow?

ProvenanceFlow is a **lightweight FAIR-compliant data lineage tracker for scientific ML pipelines**, built on the W3C PROV data model.

**One sentence for the README:**

> ProvenanceFlow automatically captures the full provenance history of scientific datasets as they move through validation and ML training pipelines, storing lineage records in W3C PROV-JSON format for reproducibility and FAIR compliance.

**The problem it solves:**
The ML research community has a reproducibility crisis. Most published results cannot be reproduced because the full data lineage — what dataset was used, which version, what transformations were applied, what records were rejected and why — is never recorded. Tools like MLflow and Weights & Biases track models and hyperparameters. Nobody adequately tracks the **data transformation layer** at HPC scale with FAIR compliance.

**Why this matters to Yahyapour specifically:**

- His NFDIxCS project builds research data management infrastructure for Computer Science using FAIR principles
- His group published on workflow management platforms (Flowable) for data management pipelines
- HeKKSaGOn’s Data Science priority area explicitly covers “Data Science, Digitalization and AI”
- This project is the data science implementation of exactly what his group researches at the infrastructure level

**The developer’s background:**
The developer (Ojaswi Sharma) built commercial data validation pipelines at a data engineering firm for 2 years — Databricks bronze/silver layer architecture, hard rejection logic, warning counts, record-level tracking. ProvenanceFlow is that same engineering work translated into research-grade infrastructure using academic standards (W3C PROV). This is not a student learning project. It is a practitioner formalizing existing skills into research language.

-----

## 2. FAIR Principles — Understand These Before Writing One Line of Code

FAIR = Findable, Accessible, Interoperable, Reusable. Published in *Scientific Data* (Wilkinson et al., 2016). Now the mandatory standard for research data in Germany (DFG), Europe (EOSC), and globally.

**How ProvenanceFlow implements each principle:**

|Principle        |What We Do                                                                                                                          |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------|
|**Findable**     |Every dataset and pipeline run gets a unique persistent identifier (PID) — simulated as a UUID-based DOI-style ID stored in metadata|
|**Accessible**   |All provenance records stored in open SQLite DB, queryable without proprietary tools                                                |
|**Interoperable**|W3C PROV-JSON is the international standard — readable by any PROV-compliant tool worldwide                                         |
|**Reusable**     |Full provenance graph links raw data → transformations → model, enabling exact reproduction of any run                              |

-----

## 3. W3C PROV Data Model — The Core Standard

**Install:** `pip install prov`  
**Docs:** https://prov.readthedocs.io/en/latest/  
**GitHub:** https://github.com/trungdong/prov  
**Current version:** 2.1.1 (June 2025)

### Three Core Concepts

Everything in PROV maps to three types:

```
Entity    — a thing (dataset, file, model, processed output)
Activity  — something that happened (validation, transformation, training)
Agent     — who/what was responsible (pipeline, user, software version)
```

### Key Relationships

```
wasGeneratedBy(entity, activity)     — this output was produced by this process
used(activity, entity)               — this process consumed this input
wasDerivedFrom(entity2, entity1)     — this output came from this input
wasAssociatedWith(activity, agent)   — this process was run by this agent
wasAttributedTo(entity, agent)       — this data is attributed to this agent
```

### Real Example — What a Validation Step Looks Like in PROV-JSON

```python
import prov.model as prov
from datetime import datetime
import uuid

# Create document
doc = prov.ProvDocument()
doc.set_default_namespace('http://provenanceflow.org/')
doc.add_namespace('pf', 'http://provenanceflow.org/ns#')

# Define the raw dataset (input entity)
raw_dataset = doc.entity(
    'pf:raw_nasa_gistemp_2025',
    {
        'prov:label': 'NASA GISTEMP v4 Raw CSV',
        'pf:source_url': 'https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv',
        'pf:row_count': 1716,
        'pf:download_timestamp': datetime.utcnow().isoformat(),
        'pf:checksum_sha256': '<sha256_hash_here>',
    }
)

# Define the validation activity
validation_run = doc.activity(
    f'pf:validation_{uuid.uuid4().hex[:8]}',
    startTime=datetime.utcnow(),
    other_attributes={
        'pf:validator_version': '1.0.0',
        'pf:rules_applied': 'range_check,null_check,anomaly_threshold',
    }
)

# Define the clean output entity
clean_dataset = doc.entity(
    'pf:clean_nasa_gistemp_2025',
    {
        'prov:label': 'NASA GISTEMP v4 Validated',
        'pf:row_count_in': 1716,
        'pf:rows_passed': 1698,
        'pf:rows_rejected': 18,
        'pf:rejection_reasons': 'null_value:12,out_of_range:6',
    }
)

# Define the pipeline as agent
pipeline_agent = doc.agent(
    'pf:provenanceflow_v1',
    {'prov:type': 'prov:SoftwareAgent', 'pf:version': '1.0.0'}
)

# Wire everything together
doc.wasGeneratedBy(clean_dataset, validation_run)
doc.used(validation_run, raw_dataset)
doc.wasDerivedFrom(clean_dataset, raw_dataset)
doc.wasAssociatedWith(validation_run, pipeline_agent)

# Serialize to PROV-JSON
with open('provenance_run.json', 'w') as f:
    doc.serialize(f, format='json', indent=2)
```

This is the pattern. Every pipeline step produces a PROV document. Documents are stored and queryable.

-----

## 4. Dataset — NASA GISTEMP v4

**Why this dataset:**

- Publicly available, no API key required
- Used by actual HPC research centers for climate modeling
- Large enough to demonstrate real validation (1716+ rows, 1880-present)
- Has natural data quality issues (missing values, anomalies) that make validation interesting
- Scientifically credible — immediately recognizable to any research computing audience

**Direct download URLs (no auth required):**

```
Global mean (CSV):
https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv

Northern Hemisphere (CSV):
https://data.giss.nasa.gov/gistemp/tabledata_v4/NH.Ts+dSST.csv

Southern Hemisphere (CSV):
https://data.giss.nasa.gov/gistemp/tabledata_v4/SH.Ts+dSST.csv
```

**Data structure:**

- Rows: one per year from 1880
- Columns: Year, Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec, J-D (annual mean), D-N, DJF, MAM, JJA, SON
- Missing values encoded as `****`
- Temperature anomalies in °C relative to 1951-1980 baseline

**Validation rules to implement (maps to Yahyapour’s FAIR validation work):**

1. `null_check` — flag rows where any monthly value is `****`
1. `range_check` — temperature anomalies outside [-3.0, +3.0]°C are suspect
1. `completeness_check` — rows with more than 3 missing monthly values in a year
1. `temporal_continuity` — flag gaps in year sequence
1. `baseline_integrity` — verify 1951-1980 period has full coverage (required for anomaly calculation)

**Python ingestion:**

```python
import pandas as pd
import requests

def download_gistemp(url: str, local_path: str) -> pd.DataFrame:
    response = requests.get(url)
    with open(local_path, 'wb') as f:
        f.write(response.content)
    # Skip NASA header rows, handle **** missing values
    df = pd.read_csv(local_path, skiprows=1, na_values=['****'])
    return df
```

-----

## 5. Full Repository Structure

```
provenanceflow/
│
├── CLAUDE.md                          ← this file
├── README.md                          ← plain English explanation
├── requirements.txt                   ← all dependencies pinned
├── .env.example                       ← environment variables template
├── demo.py                            ← single script anyone can run to see everything working
│
├── src/
│   └── provenanceflow/
│       ├── __init__.py
│       │
│       ├── ingestion/
│       │   ├── __init__.py
│       │   └── nasa_gistemp.py        ← downloads and parses GISTEMP dataset
│       │
│       ├── validation/
│       │   ├── __init__.py
│       │   ├── rules.py               ← validation rule definitions (null, range, completeness, etc.)
│       │   └── validator.py           ← runs rules, produces pass/reject/warn records
│       │
│       ├── provenance/
│       │   ├── __init__.py
│       │   ├── tracker.py             ← core PROV document builder — THE HEART OF THE PROJECT
│       │   ├── store.py               ← SQLite persistence for provenance records
│       │   └── query.py               ← query lineage by run_id, dataset_id, date range
│       │
│       ├── pipeline/
│       │   ├── __init__.py
│       │   └── runner.py              ← orchestrates full pipeline: ingest → validate → track
│       │
│       └── utils/
│           ├── __init__.py
│           ├── identifiers.py         ← PID generation (UUID-based DOI-style)
│           └── checksums.py           ← SHA-256 hashing for dataset fingerprinting
│
├── dags/                              ← Airflow DAGs (Layer 2)
│   └── provenanceflow_dag.py          ← main DAG wrapping the pipeline
│
├── data/
│   ├── raw/                           ← downloaded source data (gitignored)
│   └── processed/                     ← validated output (gitignored)
│
├── provenance_store/
│   └── lineage.db                     ← SQLite provenance database (gitignored)
│
├── tests/
│   ├── test_validation.py
│   ├── test_provenance.py
│   └── test_query.py
│
└── docker/
    ├── docker-compose.yaml            ← Airflow local setup
    └── .env                           ← Airflow environment variables
```

-----

## 6. Layer 1 — Data Lineage Engine (Build First)

### 6.1 Validation Rules (`src/provenanceflow/validation/rules.py`)

Each rule is a function that takes a DataFrame row or DataFrame and returns a `ValidationResult`:

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class ValidationResult:
    passed: bool
    rule_name: str
    severity: str          # 'hard_rejection' | 'warning'
    row_index: Optional[int]
    value: Optional[float]
    reason: str

def check_null_values(row, row_index: int) -> list[ValidationResult]:
    """Flag rows where any monthly temperature value is NaN."""
    results = []
    monthly_cols = ['Jan','Feb','Mar','Apr','May','Jun',
                    'Jul','Aug','Sep','Oct','Nov','Dec']
    null_cols = [c for c in monthly_cols if pd.isna(row[c])]
    if null_cols:
        results.append(ValidationResult(
            passed=False,
            rule_name='null_check',
            severity='warning' if len(null_cols) <= 3 else 'hard_rejection',
            row_index=row_index,
            value=None,
            reason=f"Missing values in columns: {null_cols}"
        ))
    return results

def check_temperature_range(row, row_index: int,
                             min_val=-3.0, max_val=3.0) -> list[ValidationResult]:
    """Flag temperature anomalies outside physically plausible range."""
    results = []
    annual_mean = row.get('J-D')
    if pd.notna(annual_mean) and not (min_val <= annual_mean <= max_val):
        results.append(ValidationResult(
            passed=False,
            rule_name='range_check',
            severity='hard_rejection',
            row_index=row_index,
            value=annual_mean,
            reason=f"Annual mean {annual_mean}°C outside range [{min_val}, {max_val}]"
        ))
    return results
```

**IMPORTANT:** The `severity` field maps directly to Ojaswi’s Veersa work — `hard_rejection` = what Databricks called hard rejections, `warning` = what he called warnings. Same concept, academic framing.

### 6.2 Provenance Tracker (`src/provenanceflow/provenance/tracker.py`)

This is the most important file. Every pipeline run produces a PROV document. The tracker builds it:

```python
import prov.model as prov
from datetime import datetime
import uuid
from .store import ProvenanceStore
from ..utils.identifiers import generate_pid
from ..utils.checksums import sha256_file

class ProvenanceTracker:
    """
    Builds W3C PROV documents for pipeline runs.
    Each call to track_*() adds assertions to the current document.
    Call finalize() to serialize and store.
    """
    
    def __init__(self, run_id: str = None):
        self.run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
        self.doc = prov.ProvDocument()
        self.doc.set_default_namespace('http://provenanceflow.org/')
        self.doc.add_namespace('pf', 'http://provenanceflow.org/ns#')
        self.doc.add_namespace('fair', 'http://provenanceflow.org/fair#')
        self._pipeline_agent = self._register_agent()
    
    def _register_agent(self):
        return self.doc.agent(
            'pf:provenanceflow_v1',
            {
                'prov:type': 'prov:SoftwareAgent',
                'pf:version': '1.0.0',
                'pf:run_id': self.run_id,
            }
        )
    
    def track_ingestion(self, source_url: str, local_path: str,
                        row_count: int) -> prov.ProvEntity:
        """Record that a dataset was downloaded from source_url."""
        dataset_pid = generate_pid('dataset')
        entity = self.doc.entity(
            f'pf:dataset_{dataset_pid}',
            {
                'prov:label': f'Raw dataset from {source_url}',
                'fair:identifier': dataset_pid,
                'fair:source_url': source_url,
                'pf:row_count': row_count,
                'pf:ingest_timestamp': datetime.utcnow().isoformat(),
                'pf:checksum_sha256': sha256_file(local_path),
            }
        )
        activity = self.doc.activity(
            f'pf:ingest_{uuid.uuid4().hex[:8]}',
            startTime=datetime.utcnow(),
        )
        self.doc.wasGeneratedBy(entity, activity)
        self.doc.wasAssociatedWith(activity, self._pipeline_agent)
        return entity
    
    def track_validation(self, input_entity: prov.ProvEntity,
                         rows_in: int, rows_passed: int,
                         rejections: dict, warnings: dict) -> prov.ProvEntity:
        """Record validation step with full rejection/warning breakdown."""
        output_pid = generate_pid('validated')
        
        validation_activity = self.doc.activity(
            f'pf:validate_{uuid.uuid4().hex[:8]}',
            startTime=datetime.utcnow(),
            other_attributes={
                'pf:rules_applied': 'null_check,range_check,completeness_check',
                'pf:rows_in': rows_in,
                'pf:rows_passed': rows_passed,
                'pf:rows_rejected': rows_in - rows_passed,
                # Rejection breakdown — same logic as Veersa hard rejections
                'pf:rejections_by_rule': str(rejections),
                'pf:warnings_by_rule': str(warnings),
                'pf:rejection_rate': round((rows_in - rows_passed) / rows_in, 4),
            }
        )
        
        output_entity = self.doc.entity(
            f'pf:validated_{output_pid}',
            {
                'prov:label': 'Validated dataset',
                'fair:identifier': output_pid,
                'pf:row_count': rows_passed,
            }
        )
        
        self.doc.used(validation_activity, input_entity)
        self.doc.wasGeneratedBy(output_entity, validation_activity)
        self.doc.wasDerivedFrom(output_entity, input_entity)
        self.doc.wasAssociatedWith(validation_activity, self._pipeline_agent)
        
        return output_entity
    
    def finalize(self, store: 'ProvenanceStore') -> str:
        """Serialize PROV document to JSON and persist to store."""
        import json
        prov_json = json.loads(self.doc.serialize(format='json'))
        store.save(self.run_id, prov_json)
        return self.run_id
```

### 6.3 Provenance Store (`src/provenanceflow/provenance/store.py`)

```python
import sqlite3
import json
from pathlib import Path
from datetime import datetime

class ProvenanceStore:
    """
    Persists PROV-JSON documents to SQLite.
    SQLite chosen for: portability, no server required,
    readable without proprietary tools (satisfies FAIR 'Accessible').
    """
    
    def __init__(self, db_path: str = 'provenance_store/lineage.db'):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self._init_schema()
    
    def _init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS provenance_runs (
                run_id      TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL,
                prov_json   TEXT NOT NULL,
                summary     TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                entity_id   TEXT NOT NULL,
                run_id      TEXT NOT NULL,
                label       TEXT,
                row_count   INTEGER,
                FOREIGN KEY (run_id) REFERENCES provenance_runs(run_id)
            )
        """)
        self.conn.commit()
    
    def save(self, run_id: str, prov_json: dict):
        self.conn.execute(
            "INSERT OR REPLACE INTO provenance_runs VALUES (?, ?, ?, ?)",
            (run_id, datetime.utcnow().isoformat(), json.dumps(prov_json, indent=2), None)
        )
        self.conn.commit()
    
    def get(self, run_id: str) -> dict:
        cursor = self.conn.execute(
            "SELECT prov_json FROM provenance_runs WHERE run_id = ?", (run_id,)
        )
        row = cursor.fetchone()
        return json.loads(row[0]) if row else None
    
    def list_runs(self) -> list:
        cursor = self.conn.execute(
            "SELECT run_id, created_at FROM provenance_runs ORDER BY created_at DESC"
        )
        return [{'run_id': r[0], 'created_at': r[1]} for r in cursor.fetchall()]
```

### 6.4 Pipeline Runner (`src/provenanceflow/pipeline/runner.py`)

```python
from ..ingestion.nasa_gistemp import download_gistemp, parse_gistemp
from ..validation.validator import Validator
from ..provenance.tracker import ProvenanceTracker
from ..provenance.store import ProvenanceStore

def run_pipeline(source_url: str, local_path: str) -> str:
    """
    Full pipeline: download → validate → track provenance → store.
    Returns run_id for querying the provenance record.
    """
    store = ProvenanceStore()
    tracker = ProvenanceTracker()
    
    # Step 1: Ingest
    df = download_gistemp(source_url, local_path)
    raw_entity = tracker.track_ingestion(
        source_url=source_url,
        local_path=local_path,
        row_count=len(df)
    )
    
    # Step 2: Validate
    validator = Validator()
    results = validator.validate(df)
    clean_df = validator.get_clean(df, results)
    rejections = validator.rejection_summary(results)
    warnings = validator.warning_summary(results)
    
    validated_entity = tracker.track_validation(
        input_entity=raw_entity,
        rows_in=len(df),
        rows_passed=len(clean_df),
        rejections=rejections,
        warnings=warnings
    )
    
    # Step 3: Finalize provenance
    run_id = tracker.finalize(store)
    
    print(f"Pipeline complete. Run ID: {run_id}")
    print(f"Rows in: {len(df)}, Rows passed: {len(clean_df)}, Rejected: {len(df) - len(clean_df)}")
    print(f"Provenance stored. Query with: store.get('{run_id}')")
    
    return run_id
```

-----

## 7. Layer 2 — Airflow DAG (Build Second)

### 7.1 Setup (Do This Once)

```bash
# Create Airflow directory
mkdir -p airflow/{dags,logs,plugins,config}

# Download official docker-compose
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# Set UID
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize (first time only)
docker compose up airflow-init

# Start
docker compose up -d

# Access UI at http://localhost:8080
# Login: airflow / airflow
```

Requires: Docker Desktop with at least 4GB RAM allocated (8GB recommended).

### 7.2 The DAG (`dags/provenanceflow_dag.py`)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, '/opt/airflow')

from src.provenanceflow.ingestion.nasa_gistemp import download_gistemp
from src.provenanceflow.validation.validator import Validator
from src.provenanceflow.provenance.tracker import ProvenanceTracker
from src.provenanceflow.provenance.store import ProvenanceStore

NASA_URL = 'https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv'
LOCAL_PATH = '/opt/airflow/data/raw/gistemp_global.csv'

default_args = {
    'owner': 'provenanceflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

def task_ingest(**context):
    df = download_gistemp(NASA_URL, LOCAL_PATH)
    context['ti'].xcom_push(key='row_count', value=len(df))
    print(f"Ingested {len(df)} rows from NASA GISTEMP")

def task_validate(**context):
    import pandas as pd
    df = pd.read_csv(LOCAL_PATH, skiprows=1, na_values=['****'])
    validator = Validator()
    results = validator.validate(df)
    clean_df = validator.get_clean(df, results)
    context['ti'].xcom_push(key='rows_passed', value=len(clean_df))
    context['ti'].xcom_push(key='rows_rejected', value=len(df) - len(clean_df))
    clean_df.to_csv('/opt/airflow/data/processed/gistemp_clean.csv', index=False)
    print(f"Validation complete: {len(clean_df)}/{len(df)} rows passed")

def task_track_provenance(**context):
    ti = context['ti']
    rows_in = ti.xcom_pull(key='row_count', task_ids='ingest')
    rows_passed = ti.xcom_pull(key='rows_passed', task_ids='validate')
    
    store = ProvenanceStore()
    tracker = ProvenanceTracker()
    
    raw_entity = tracker.track_ingestion(
        source_url=NASA_URL,
        local_path=LOCAL_PATH,
        row_count=rows_in
    )
    tracker.track_validation(
        input_entity=raw_entity,
        rows_in=rows_in,
        rows_passed=rows_passed,
        rejections={'null_check': rows_in - rows_passed},
        warnings={}
    )
    run_id = tracker.finalize(store)
    print(f"Provenance tracked. Run ID: {run_id}")

with DAG(
    dag_id='provenanceflow_gistemp_pipeline',
    default_args=default_args,
    description='FAIR-compliant data lineage tracking for NASA GISTEMP',
    schedule_interval=timedelta(weeks=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['provenanceflow', 'fair', 'climate-science'],
) as dag:
    
    ingest = PythonOperator(
        task_id='ingest',
        python_callable=task_ingest,
    )
    
    validate = PythonOperator(
        task_id='validate',
        python_callable=task_validate,
    )
    
    track = PythonOperator(
        task_id='track_provenance',
        python_callable=task_track_provenance,
    )
    
    ingest >> validate >> track
```

-----

## 8. Requirements

```txt
# requirements.txt — pin all versions

# Core provenance standard
prov==2.1.1

# Data processing
pandas==2.2.2
numpy==1.26.4
requests==2.32.3

# Checksums + identifiers
hashlib  # stdlib
uuid     # stdlib

# Storage
# sqlite3  # stdlib — no install needed

# Workflow (Layer 2)
apache-airflow==2.10.1

# Testing
pytest==8.3.2

# Optional: visualization
pydot==2.0.0
graphviz==0.20.3
```

-----

## 9. Demo Script (`demo.py`)

This is what a professor runs in 30 seconds to see the entire project work:

```python
"""
ProvenanceFlow Demo
Run: python demo.py
"""

from src.provenanceflow.pipeline.runner import run_pipeline
from src.provenanceflow.provenance.store import ProvenanceStore
import json

NASA_GLOBAL_URL = 'https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv'

print("=" * 60)
print("ProvenanceFlow — FAIR Data Lineage Demo")
print("Dataset: NASA GISTEMP v4 Global Surface Temperature")
print("=" * 60)

# Run full pipeline
run_id = run_pipeline(
    source_url=NASA_GLOBAL_URL,
    local_path='data/raw/gistemp_global.csv'
)

# Query the provenance record
store = ProvenanceStore()
prov_record = store.get(run_id)

print("\n--- Provenance Record (W3C PROV-JSON) ---")
print(json.dumps(prov_record, indent=2)[:2000])  # First 2000 chars

print("\n--- All Pipeline Runs ---")
for run in store.list_runs():
    print(f"  {run['run_id']}  |  {run['created_at']}")

print("\nFAIR Compliance:")
print("  Findable      ✓ — Persistent identifiers on all entities")
print("  Accessible    ✓ — SQLite, no proprietary tools required")
print("  Interoperable ✓ — W3C PROV-JSON international standard")
print("  Reusable      ✓ — Full lineage from source to output")
```

-----

## 10. README.md Template

```markdown
# ProvenanceFlow

> Lightweight FAIR-compliant data lineage tracking for scientific ML pipelines.

ProvenanceFlow automatically captures the full provenance history of scientific datasets 
as they move through validation and ML training pipelines, storing lineage records in 
W3C PROV-JSON format for reproducibility and FAIR compliance.

## The Problem

ML research has a reproducibility crisis. Most published results cannot be reproduced 
because the full data lineage — what dataset, which version, what transformations, 
what was rejected and why — is never recorded. Existing tools (MLflow, W&B) track 
models. Nobody adequately tracks the data transformation layer.

## What ProvenanceFlow Does

- Downloads scientific datasets (currently: NASA GISTEMP v4 climate data)
- Runs configurable validation rules (null checks, range checks, completeness)
- Records every transformation as a W3C PROV-JSON provenance document
- Persists lineage to queryable SQLite store
- Orchestrates via Apache Airflow DAGs
- Assigns FAIR-compliant persistent identifiers to all entities

## Quick Start

\`\`\`bash
pip install -r requirements.txt
python demo.py
\`\`\`

## Architecture

\`\`\`
Raw Data → Validation → PROV-JSON Tracker → SQLite Store
              ↓               ↓
          Rejections     Lineage Graph
          Warnings       (W3C Standard)
\`\`\`

## Standards

- **W3C PROV-DM**: https://www.w3.org/TR/prov-dm/
- **FAIR Principles**: Wilkinson et al. (2016), *Scientific Data*
- **NFDIxCS**: National Research Data Infrastructure for Computer Science

## Relevance

This project implements the data-layer provenance infrastructure described in:
- Souza et al. (2022), "Workflow Provenance in the Lifecycle of Scientific ML"  
- Yahyapour et al. (GWDG), Research Data Management and FAIR infrastructure
```

-----

## 11. Git Commit Conventions

Every commit message must be meaningful. No “fixed stuff” or “updates”.

```
feat: add W3C PROV tracker with ingestion recording
feat: implement null_check and range_check validation rules  
feat: add SQLite provenance store with schema initialization
feat: wire validation results into PROV wasGeneratedBy graph
feat: add SHA256 checksum fingerprinting for dataset entities
feat: add Airflow DAG wrapping full pipeline
test: add pytest tests for validation rule edge cases
docs: add PROV-JSON example to README
refactor: extract PID generation to utils/identifiers.py
```

Minimum 8 commits showing real progression before the repo is shown to anyone.

-----

## 12. What Success Looks Like

### Before Departure (Layers 1 + 2 complete)

- `python demo.py` runs end-to-end without errors
- Produces a valid W3C PROV-JSON file
- SQLite DB is created and queryable
- Airflow DAG visible in localhost:8080 UI and runs successfully
- GitHub repo: clean structure, working demo, pinned requirements, 8+ meaningful commits

### The HiWi Email Test

Read this email out loud. If it sounds like a student hoping for an opportunity, it’s wrong. If it sounds like an engineer who built something relevant arriving with a specific contribution, it’s right:

> Subject: HiWi Application — FAIR Data Lineage Tracking for Scientific ML Pipelines
> 
> Dear Prof. Yahyapour,
> 
> I am a first-semester Applied Data Science student at Göttingen with two years of professional experience building data validation pipelines at a data engineering firm. Before starting the semester, I built ProvenanceFlow — a FAIR-compliant data lineage tracker for scientific ML workflows, implementing the W3C PROV data model on top of Apache Airflow: [GitHub URL]
> 
> I read your group’s work on workflow management platforms for research data management (Doan et al., 2022) and the NFDIxCS infrastructure project. ProvenanceFlow addresses the data transformation provenance layer that sits upstream of the model tracking tools — recording validation lineage, rejection rationale, and dataset versioning in PROV-JSON.
> 
> I would welcome a brief conversation about contributing to your group’s research as a HiWi.
> 
> Best regards,  
> Ojaswi Sharma

-----

## 13. Resources

|Resource                |URL                                                                                  |
|------------------------|-------------------------------------------------------------------------------------|
|W3C PROV Overview       |https://www.w3.org/TR/prov-overview/                                                 |
|PROV Python Docs        |https://prov.readthedocs.io/                                                         |
|PROV Python GitHub      |https://github.com/trungdong/prov                                                    |
|NASA GISTEMP            |https://data.giss.nasa.gov/gistemp/                                                  |
|Airflow Docker Setup    |https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html|
|Yahyapour Research Group|https://gwdg.de/en/research-education/researchgroup_yahyapour/                       |
|HeKKSaGOn Network       |https://www.uni-goettingen.de/en/203016.html                                         |
|NFDIxCS Project         |https://nfdi4cs.org/                                                                 |
|FAIR Principles Paper   |https://www.nature.com/articles/sdata201618                                          |
|ProvBook (related work) |https://www.researchgate.net/publication/342377391                                   |

-----

## 14. Scope Control — What NOT to Build

Do not build these. They are Layer 3 and 4, done as HiWi contribution at Göttingen, not before departure:

- ❌ Streamlit dashboard (Layer 4 — HiWi work)
- ❌ Dublin Core / Schema.org metadata tagging (Layer 3 — HiWi work)
- ❌ REST API for provenance queries
- ❌ Support for datasets other than NASA GISTEMP
- ❌ ML model training step (keep the pipeline as ingest → validate → track)
- ❌ Authentication / multi-user support
- ❌ Cloud deployment

The project is deliberately scoped to be completable before departure. Scope creep = nothing working = bad repo = no HiWi.

-----

*Last updated: March 2026. This document serves as the complete specification for Claude Code. All architectural decisions are final unless explicitly revised.*
