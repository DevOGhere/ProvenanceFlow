# ProvenanceFlow

> W3C PROV-native lineage capture for pandas pipelines, with row-level rejection rationale.

ProvenanceFlow records exactly what happened to your data — which rows were filtered, which rule triggered the drop, and why — as a standard W3C PROV-JSON provenance graph. One decorator. No workflow DSL required.

---

## The Problem

When you clean a DataFrame, you make decisions: drop nulls, reject outliers, flag incomplete rows. Those decisions are almost never recorded in a reproducible, queryable, standard format.

**Existing tools cover adjacent problems:**
- **OpenLineage / Marquez** — tracks which dataset fed which dataset, but not per-row rejection rationale
- **Great Expectations** — captures validation results in detail, but produces no standard provenance format
- **DVC** — versions files and checksums, but not transformation decisions
- **MLflow / W&B** — track models and experiments, not the data cleaning layer

The gap: **linking per-row rejection rationale into a W3C PROV graph** that documents the full transformation chain from raw input to validated output — in a format research infrastructure can actually consume.

---

## Quick Start

```bash
pip install -r requirements.txt
python demo.py
```

Or add provenance to any existing function with one line:

```python
from provenanceflow import track

@track
def remove_outliers(df):
    return df[df["value"].between(-3.0, 3.0)]
```

Every call records: input checksum, output checksum, row count delta, and a W3C PROV entity-activity-agent graph — stored in SQLite, queryable by run ID.

---

## CLI

After install (`pip install -e .`):

```bash
provenanceflow run                         # run NASA GISTEMP demo pipeline
provenanceflow run --file data/my.csv      # run with a local CSV
provenanceflow run --url URL --dir DIR     # custom URL + output directory
provenanceflow runs list                   # list all stored provenance runs
provenanceflow runs show <run_id>          # detail for one run
provenanceflow runs report <run_id>        # generate Markdown reproducibility report
provenanceflow dashboard                   # launch Streamlit UI
provenanceflow serve                       # launch FastAPI REST API
```

---

## What ProvenanceFlow Does

- Wraps any `DataFrame → DataFrame` function with the `@track` decorator — zero config
- Runs configurable validation rules and records which rows were dropped and why
- Builds a **W3C PROV-JSON** provenance document: Entity → Activity → Agent graph
- SHA-256 fingerprints every dataset at ingestion and output
- Persists all records to a queryable **SQLite store** — no server, no proprietary tools
- Generates **Markdown reproducibility reports** per run, embeddable in papers
- Compares two runs (`compare_runs`) to diff rejection rates, checksums, and rules applied
- Exposes lineage via **REST API** (FastAPI) and **Streamlit dashboard**
- Integrates with **Apache Airflow** for scheduled pipeline runs

---

## Architecture

```
Raw Data (any CSV, URL, or local file)
        │
        ▼
   [Ingestion]  ─── SHA-256 fingerprint assigned
        │
        ▼
  [Validation]  ─── configurable rules → ValidationResult per row
        │             severity: warning | hard_rejection
        ▼
 [PROV Tracker] ─── W3C PROV-JSON graph built
        │             Entity → Activity → Agent
        │             wasDerivedFrom, wasGeneratedBy, used
        ▼
 [SQLite Store] ─── queryable by run_id, date range, dataset_id
        │
        ├── REST API     (GET /runs, /runs/{id}/report, /runs/{a}/compare/{b})
        ├── Dashboard    (Streamlit — Overview / Run Detail / PROV Graph)
        └── CLI          (provenanceflow runs show / report)
```

---

## FAIR-Aligned Metadata

ProvenanceFlow embeds Dublin Core (`dc:`) and Schema.org (`schema:`) vocabulary into every PROV entity, making records more interoperable with research metadata registries.

> **Note:** This is FAIR-aligned, not fully FAIR-compliant. Real FAIR compliance requires resolvable persistent identifiers (registered DOIs, not UUIDs) and deposit into a metadata registry (Zenodo, B2FIND, DataCite). ProvenanceFlow provides the structured metadata that makes that step easier — it doesn't replace it.

| Principle | What ProvenanceFlow provides |
|---|---|
| **Findable** | UUID-based run identifiers + `dc:identifier` on all entities |
| **Accessible** | SQLite — queryable without proprietary tools |
| **Interoperable** | W3C PROV-JSON + Dublin Core + Schema.org vocabulary |
| **Reusable** | Full transformation chain with per-row rejection rationale |

---

## Bundled Validation Rules (NASA GISTEMP demo)

The included demo pipeline runs against NASA GISTEMP v4 climate data and applies these rules:

| Rule | Severity | Description |
|---|---|---|
| `null_check` | warning / hard_rejection | Missing monthly temperature values |
| `range_check` | hard_rejection | Annual mean outside [-3.0, +3.0]°C |
| `completeness_check` | hard_rejection | More than 3 monthly values missing |
| `temporal_continuity` | warning | Gaps in the year sequence |
| `baseline_integrity` | warning | Incomplete 1951-1980 anomaly baseline |

These are bundled rules for GISTEMP-shaped data. Write your own rules for any dataset using the `@rule` decorator:

```python
from provenanceflow import rule, Validator

@rule(severity="hard_rejection")
def no_negative_ages(row, idx):
    age = row.get("age")
    if age is not None and float(age) < 0:
        return f"Negative age: {age}"  # return a string = failure
    # return None implicitly = pass

@rule(severity="warning")
def no_duplicate_ids(df):  # one-param signature = DataFrame-level rule
    dupes = df[df["patient_id"].duplicated()]
    return [(int(i), f"Duplicate patient_id: {df.loc[i, 'patient_id']}") for i in dupes.index]

validator = Validator(rules=[no_negative_ages, no_duplicate_ids])
results = validator.validate(df)
clean_df = validator.get_clean(df, results)
print(validator.rejection_summary(results))  # {'no_negative_ages': 3}
```

Row-level rules take `(row, idx)` and return a string (fail) or `None` (pass). Return `(string, severity)` to override severity dynamically. DataFrame-level rules take just `(df,)` and return a list of `(row_index, reason)` tuples.

---

## Provenance Record

Each pipeline run produces a W3C PROV-JSON document stored in SQLite:

```json
{
  "entity": {
    "pf:dataset_abc123": {
      "prov:label": "Raw dataset from https://data.giss.nasa.gov/...",
      "fair:identifier": "dataset_abc123def456",
      "dc:title": "NASA GISTEMP v4 Global Surface Temperature",
      "pf:row_count": 1716,
      "pf:checksum_sha256": "e3b0c44298fc1c149afb..."
    }
  },
  "activity": {
    "pf:validate_d7f3a1b2": {
      "pf:rules_applied": "null_check,range_check,completeness_check",
      "pf:rows_in": 1716,
      "pf:rows_passed": 1698,
      "pf:rows_rejected": 18,
      "pf:rejection_rate": 0.0105
    }
  },
  "wasGeneratedBy": { "...": "..." },
  "wasDerivedFrom": { "...": "..." }
}
```

---

## Querying Lineage

```python
from src.provenanceflow.provenance.store import ProvenanceStore
from src.provenanceflow.provenance.query import get_run, get_by_date_range, get_by_dataset_id
from src.provenanceflow.provenance.compare import compare_runs

store = ProvenanceStore()

prov_doc = get_run(store, run_id)
runs = get_by_date_range(store, start='2025-01-01', end='2025-12-31')
runs = get_by_dataset_id(store, dataset_id='dataset_abc123def456')

diff = compare_runs(run_id_a, run_id_b, store)
print(diff.summary)  # "Rejection rate improved by 0.24% (1.29% → 1.05%)"
```

---

## Dashboard

```bash
python demo.py                  # populate the provenance store first
streamlit run dashboard.py      # http://localhost:8501
```

Three views: **Overview** (all runs, rejection rates), **Run Detail** (per-rule stats, raw PROV-JSON), **PROV Graph** (visual entity-activity-agent graph).

---

## Running with Apache Airflow

Requires Docker Desktop with ≥ 4 GB RAM.

```bash
echo "AIRFLOW_UID=$(id -u)" > docker/.env
docker compose -f docker/docker-compose.yaml up airflow-init
docker compose -f docker/docker-compose.yaml up -d
# UI at http://localhost:8080  (airflow / airflow)
# DAG: provenanceflow_gistemp_pipeline
```

---

## Repository Structure

```
src/provenanceflow/
├── ingestion/       — data source adapters (GISTEMP, local CSV, generic CSV)
├── validation/      — rule system + Validator orchestrator
├── provenance/      — W3C PROV tracker, SQLite store, query API, run comparison
├── pipeline/        — full pipeline runner
├── api/             — FastAPI REST API (/runs, /health)
├── cli.py           — Typer CLI entry point
├── decorator.py     — @track decorator for DataFrame transforms
└── utils/           — PID generation, SHA-256 checksums, PROV helpers, reports
dags/                — Apache Airflow DAG
tests/               — pytest test suite (188 passing)
demo.py              — end-to-end demo using NASA GISTEMP v4
dashboard.py         — Streamlit dashboard
```

---

## Standards

- **W3C PROV-DM**: https://www.w3.org/TR/prov-dm/
- **FAIR Principles**: Wilkinson et al. (2016), *Scientific Data* — https://doi.org/10.1038/sdata.2016.18
- **RO-Crate**: FAIR research object packaging — https://www.researchobject.org/ro-crate/
- **NFDIxCS**: National Research Data Infrastructure for Computer Science — https://nfdi4cs.org/

---

## Related Work

| Tool | What it does | How ProvenanceFlow differs |
|---|---|---|
| [OpenLineage](https://openlineage.io/) | Dataset-level lineage events, Airflow/dbt/Spark native | Row-level rejection rationale; W3C PROV not OpenLineage JSON |
| [Great Expectations](https://greatexpectations.io/) | Rich validation with detailed results | No standard provenance format; no transformation graph |
| [DVC](https://dvc.org/) | Data versioning + checksums | No per-row rejection tracking; no PROV graph |
| [ProvLake / CWLProv](https://github.com/gems-uff/provlake) | W3C PROV for scientific workflows | Workflow-DSL based; not pandas-native |
| [RO-Crate](https://www.researchobject.org/ro-crate/) | FAIR research object packaging standard | Output format target, not a lineage tool |
