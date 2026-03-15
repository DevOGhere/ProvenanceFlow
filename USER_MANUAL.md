# ProvenanceFlow — User Manual

> FAIR-compliant data lineage tracking for scientific ML pipelines.

---

## Table of Contents

1. [What Is ProvenanceFlow?](#what-is-provenanceflow)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Running the Pipeline](#running-the-pipeline)
6. [CLI Reference](#cli-reference)
7. [Streamlit Dashboard](#streamlit-dashboard)
8. [REST API](#rest-api)
9. [Apache Airflow (Docker)](#apache-airflow-docker)
10. [Configuration & Environment Variables](#configuration--environment-variables)
11. [Understanding the Output](#understanding-the-output)
12. [Validation Rules](#validation-rules)
13. [Data & Storage](#data--storage)
14. [Troubleshooting](#troubleshooting)

---

## What Is ProvenanceFlow?

ProvenanceFlow automatically records the full history of a dataset as it moves through ingestion and validation — answering the question *"exactly what data was used, what was rejected, and why?"*

Every pipeline run produces a **W3C PROV-JSON document** that captures:

- Where the data came from (URL + SHA-256 fingerprint)
- Every validation rule applied and its outcome
- Which rows were rejected and the reason
- Persistent identifiers (UUIDs) on every entity for FAIR compliance

Records are stored in a local **SQLite database** — no cloud account or proprietary tools needed.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.11 recommended (avoid 3.13+) |
| Homebrew (macOS) | latest |
| Docker Desktop | only needed for Airflow |
| graphviz (system binary) | only needed for PROV graph view |

---

## Installation

```bash
# 1. Clone the repo and enter the directory
git clone <your-repo-url>
cd ProvenanceFlow

# 2. Install the system graphviz binary (macOS)
brew install graphviz

# 3. Create and activate a virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Install the package in editable mode (enables the `provenanceflow` CLI)
pip install -e .
```

---

## Quick Start

Run the demo using the bundled sample dataset (no internet required):

```bash
python demo.py
```

This ingests 25 rows of NASA GISTEMP data (1880–1904), runs all validation rules, writes a W3C PROV-JSON record to the local SQLite store, and prints a summary to the terminal.

> **Note:** The 25 rows are intentional — `demo.py` uses a bundled sample CSV for reproducible offline demos. See [Running the Pipeline](#running-the-pipeline) to fetch the full ~145-year dataset.

---

## Running the Pipeline

### Option A — Bundled sample (offline, 25 rows)

```bash
python demo.py
```

### Option B — Live NASA download (~145 years of data)

```bash
provenanceflow run
```

This downloads the full NASA GISTEMP v4 CSV directly and runs the pipeline on all available data.

### Option C — Your own GISTEMP-format CSV

```bash
provenanceflow run --file path/to/your_data.csv
```

### Option D — Custom URL

```bash
provenanceflow run --url https://example.com/data.csv --dir data/raw
```

---

## CLI Reference

After `pip install -e .` the `provenanceflow` command is available.

### `provenanceflow run`

Run the full ingestion → validation → provenance pipeline.

| Flag | Default | Description |
|---|---|---|
| `--file FILE` | — | Path to a local GISTEMP-format CSV |
| `--url URL` | NASA GISTEMP v4 | Custom download URL |
| `--dir DIR` | `data/raw` | Directory to save downloaded CSV |
| `--db PATH` | `provenance_store/lineage.db` | SQLite database path |

**Example output:**
```
Run ID   : a3f2c1d0-...
Rows in  : 145
Passed   : 143
Rejected : 2 (1.4%)
DB       : provenance_store/lineage.db
```

---

### `provenanceflow runs list`

List all stored pipeline runs.

```bash
provenanceflow runs list
provenanceflow runs list --limit 50
```

---

### `provenanceflow runs show <run_id>`

Show a human-readable summary of a specific run.

```bash
provenanceflow runs show a3f2c1d0-...
```

Add `--json` to get the full raw PROV-JSON document:

```bash
provenanceflow runs show a3f2c1d0-... --json
```

---

### `provenanceflow runs report <run_id>`

Generate a Markdown reproducibility report for a run.

```bash
# Print to terminal
provenanceflow runs report a3f2c1d0-...

# Save to file
provenanceflow runs report a3f2c1d0-... --output report.md
```

---

### `provenanceflow dashboard`

Launch the Streamlit visual dashboard.

```bash
provenanceflow dashboard
# or with a custom port:
provenanceflow dashboard --port 8502
```

---

### `provenanceflow serve`

Launch the FastAPI REST API server.

```bash
provenanceflow serve
# or with custom host/port:
provenanceflow serve --host 127.0.0.1 --port 9000
```

---

## Streamlit Dashboard

```bash
# Option 1 — via CLI
provenanceflow dashboard

# Option 2 — directly
streamlit run dashboard.py
```

Opens at **http://localhost:8501**

### Three views:

| View | What it shows |
|---|---|
| **Overview** | All pipeline runs, rejection rates, row counts |
| **Run Detail** | Per-run validation stats, DC/Schema.org metadata, bar charts by rule, raw PROV-JSON |
| **PROV Graph** | Visual W3C PROV entity-activity-agent graph |

> The PROV Graph view requires the `graphviz` system binary (`brew install graphviz`).

---

## REST API

```bash
provenanceflow serve
# API is now at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/runs` | List all runs |
| `GET` | `/runs/{run_id}` | Get full PROV-JSON for a run |
| `GET` | `/runs/{run_id}/entities` | List all provenance entities |
| `GET` | `/runs/{run_id}/activities` | List all provenance activities |
| `GET` | `/runs/{run_id}/rejections` | List all hard-rejected rows |
| `GET` | `/runs/{run_id}/report` | Markdown reproducibility report |
| `GET` | `/runs/{run_id_a}/compare/{run_id_b}` | Diff two runs |
| `GET` | `/runs/search?dataset_id=X` | Search by dataset ID |
| `GET` | `/runs/search?start=YYYY-MM-DD&end=YYYY-MM-DD` | Search by date range |
| `GET` | `/health` | Health check |

---

## Apache Airflow (Docker)

Airflow is optional. Use it for scheduled or HPC-scale pipeline runs.

**Requirements:** Docker Desktop with ≥ 4 GB RAM allocated.

```bash
# 1. Set your user ID (ensures correct file ownership)
echo "AIRFLOW_UID=$(id -u)" > docker/.env

# 2. First-time setup — creates the DB and admin user
docker compose -f docker/docker-compose.yaml up airflow-init

# 3. Start the webserver and scheduler
docker compose -f docker/docker-compose.yaml up -d

# 4. Open http://localhost:8080
#    Login: airflow / airflow
#    The 'provenanceflow_gistemp_pipeline' DAG will appear automatically.

# Stop
docker compose -f docker/docker-compose.yaml down
```

---

## Configuration & Environment Variables

All settings have sensible defaults and can be overridden via environment variables or a `.env` file in the project root.

| Variable | Default | Description |
|---|---|---|
| `GISTEMP_URL` | NASA GISTEMP v4 CSV URL | Source dataset URL |
| `PIPELINE_SOURCE` | `nasa_gistemp` | Source identifier |
| `RAW_DATA_PATH` | `data/raw` | Directory for downloaded CSV files |
| `PROCESSED_DATA_PATH` | `data/processed` | Directory for processed outputs |
| `PROV_DB_PATH` | `provenance_store/lineage.db` | SQLite provenance database path |
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `API_HOST` | `0.0.0.0` | FastAPI bind host |
| `API_PORT` | `8000` | FastAPI bind port |

**Example `.env` file:**
```
PROV_DB_PATH=/data/my_project/lineage.db
LOG_LEVEL=DEBUG
API_PORT=9000
```

---

## Understanding the Output

### Terminal summary (after `provenanceflow run`)

```
Run ID   : a3f2c1d0-8b1e-4f2a-...   ← unique UUID for this run
Rows in  : 145                        ← total rows ingested
Passed   : 143                        ← rows that passed all validation rules
Rejected : 2 (1.4%)                   ← hard-rejected rows (excluded from output)
DB       : provenance_store/lineage.db
```

### PROV-JSON document structure

Each run produces a document with three node types:

- **Entity** — a dataset (raw or validated). Has UUID PID, SHA-256 checksum, row count, source URL.
- **Activity** — a pipeline step (ingestion or validation). Has timestamps, rules applied, rejection counts.
- **Agent** — the software that performed the activity (ProvenanceFlow itself).

Relationships:
- `wasGeneratedBy` — entity was produced by an activity
- `used` — activity consumed an entity
- `wasAssociatedWith` — activity was performed by an agent

---

## Validation Rules

The pipeline applies five rules to NASA GISTEMP data:

| Rule | Type | What it checks |
|---|---|---|
| `null_check` | Row-level | Rejects rows where all monthly columns are null/missing |
| `range_check` | Row-level | Rejects rows with temperature anomalies outside −5.0°C to +5.0°C |
| `completeness_check` | Row-level | Warns if more than 6 of 12 monthly values are missing |
| `temporal_continuity` | Dataset-level | Warns if year gaps exist in the time series |
| `baseline_integrity` | Dataset-level | Warns if the 1951–1980 baseline period is absent |

**Severity levels:**
- `hard_rejection` — row is excluded from the validated output
- `warning` — row is flagged but kept

For generic CSV files (non-GISTEMP), a `BasicValidator` is used that checks for null rows only.

---

## Data & Storage

### File layout

```
ProvenanceFlow/
├── data/
│   └── raw/
│       ├── gistemp_global.csv      ← bundled 25-row sample (used by demo.py)
│       └── gistemp.csv             ← downloaded by `provenanceflow run`
├── provenance_store/
│   └── lineage.db                  ← SQLite provenance database
```

### SQLite database

The database lives at `provenance_store/lineage.db` by default. It stores:
- One row per pipeline run (run_id, created_at, full PROV-JSON)
- Hard-rejected rows with rule name, reason, and raw row data

You can query it directly with any SQLite tool, or use the CLI and API.

---

## Troubleshooting

### `403 Forbidden` when downloading from NASA

NASA blocks requests with no `User-Agent`. This was fixed in the codebase — make sure you have the latest code:

```bash
git pull origin uat
```

### `ModuleNotFoundError: No module named 'provenanceflow'`

Run `pip install -e .` from the project root with your virtual environment active.

### PROV Graph view is blank or errors

Install the graphviz system binary:

```bash
brew install graphviz
```

Then restart the dashboard.

### Airflow DAG not appearing

Wait ~30 seconds after `docker compose up` for the scheduler to scan for DAGs.

### `streamlit: command not found`

Your virtual environment is not active. Run:

```bash
source .venv/bin/activate
```

### Port already in use

```bash
# Change the dashboard port
provenanceflow dashboard --port 8502

# Change the API port
provenanceflow serve --port 9000
```
