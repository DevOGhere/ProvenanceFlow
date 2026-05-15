# CLAUDE.md — ProvenanceFlow

## Core Directive

Single source of truth for this codebase. Do not improvise architecture. Do not add features outside the defined scope. Do not simplify or remove the PROV-JSON layer — it is the core of the project.

---

## 1. What is ProvenanceFlow?

**W3C PROV-native lineage capture for pandas pipelines, with row-level rejection rationale.**

The niche it fills: when you clean a DataFrame, you make decisions — drop nulls, reject outliers, flag incomplete rows. Those decisions are almost never recorded in a standard, reproducible format. OpenLineage tracks which dataset fed which dataset (job-level). Great Expectations validates but produces no standard provenance format. ProvenanceFlow links per-row rejection rationale into a W3C PROV graph via a `@track` decorator — no workflow DSL, no config files.

**What it is NOT:** a replacement for OpenLineage (industry lineage standard), Great Expectations (validation platform), or DVC (data versioning). It fills the gap between them: the per-row decision record in a standard academic provenance format.

---

## 2. FAIR-Aligned Metadata

FAIR = Findable, Accessible, Interoperable, Reusable (Wilkinson et al., 2016, *Scientific Data*).

ProvenanceFlow embeds Dublin Core (`dc:`) and Schema.org (`schema:`) vocabulary into PROV entities. This is **FAIR-aligned, not fully FAIR-compliant** — real FAIR requires resolvable PIDs (registered DOIs) and deposit into a metadata registry. ProvenanceFlow provides the structured metadata layer that makes that step possible.

| Principle | What ProvenanceFlow provides |
|---|---|
| Findable | UUID run identifiers + `dc:identifier` on all entities |
| Accessible | SQLite — no proprietary tools required |
| Interoperable | W3C PROV-JSON + Dublin Core + Schema.org vocabulary |
| Reusable | Full transformation chain with per-row rejection rationale |

---

## 3. W3C PROV Data Model

**Package:** `prov==2.1.1` | **Docs:** https://prov.readthedocs.io/

Three core types: **Entity** (a dataset, file, or output), **Activity** (validation, transformation), **Agent** (pipeline software).

Key relationships used in this project:
- `wasGeneratedBy(entity, activity)` — output produced by a process
- `used(activity, entity)` — process consumed an input
- `wasDerivedFrom(entity2, entity1)` — output came from input
- `wasAssociatedWith(activity, agent)` — process run by agent

Namespaces:
- Default: `http://provenanceflow.org/`
- `pf:` → `http://provenanceflow.org/ns#`
- `fair:` → `http://provenanceflow.org/fair#`
- `dc:` → `http://purl.org/dc/terms/`
- `schema:` → `https://schema.org/`

---

## 4. Demo Dataset — NASA GISTEMP v4

Used only as a demo. The validation rules in `rules.py` are specific to this dataset's structure. The core tracker, store, decorator, and API are dataset-agnostic.

**Source URLs (no auth required):**
- Global: `https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv`
- Northern Hemisphere: `https://data.giss.nasa.gov/gistemp/tabledata_v4/NH.Ts+dSST.csv`
- Southern Hemisphere: `https://data.giss.nasa.gov/gistemp/tabledata_v4/SH.Ts+dSST.csv`

**Data structure:** One row per year from 1880. Columns: Year, Jan–Dec, J-D (annual mean), D-N, DJF, MAM, JJA, SON. Missing values encoded as `****` → parse with `na_values=['****'], skiprows=1`.

---

## 5. Repository Structure

```
src/provenanceflow/
├── ingestion/
│   ├── base.py              — DataSource ABC: fetch() → IngestionResult
│   ├── nasa_gistemp.py      — HTTP fetch of GISTEMP CSVs (demo source)
│   ├── local_csv.py         — local file ingestion adapter
│   └── generic_csv.py       — generic CSV ingestion adapter
├── validation/
│   ├── rules.py             — ValidationResult dataclass + GISTEMP-specific rule functions
│   ├── validator.py         — Validator: validate(), get_clean(), summaries
│   └── basic_validator.py   — generic validator for non-GISTEMP CSVs
├── provenance/
│   ├── tracker.py           — ProvenanceTracker: builds W3C PROV documents (CORE)
│   ├── store.py             — ProvenanceStore: SQLite persistence
│   ├── query.py             — get_run(), get_by_date_range(), get_by_dataset_id()
│   └── compare.py           — compare_runs(): diff two runs → RunDiff
├── pipeline/runner.py       — run_pipeline(): ingest → validate → track
├── api/
│   ├── app.py               — FastAPI app (uvicorn entry point)
│   └── routers/
│       ├── health.py        — GET /health
│       └── runs.py          — GET /runs, /runs/{id}, /runs/{id}/report, /runs/{a}/compare/{b}
├── cli.py                   — Typer CLI (run / runs list / runs show / runs report / serve / dashboard)
├── config.py                — Settings via pydantic-settings; get_settings() LRU-cached
├── decorator.py             — @track decorator: zero-friction PROV on DataFrame functions
├── models.py                — Pydantic contracts: IngestionResult, ValidationResult, PipelineResult
└── utils/
    ├── identifiers.py       — generate_pid(): UUID-based identifiers
    ├── checksums.py         — sha256_file(): dataset fingerprinting
    ├── prov_helpers.py      — unwrap(), get_ingestion_entity(), get_validation_activity()
    └── report.py            — Markdown reproducibility report generator

dags/provenanceflow_dag.py   — Airflow DAG (weekly, ingest→validate→track)
dashboard.py                 — Streamlit dashboard (Overview / Run Detail / PROV Graph)
demo.py                      — end-to-end demo using NASA GISTEMP v4
```

---

## 6. Validation Rules

The GISTEMP-specific rules in `rules.py` are a demo implementation. `BasicValidator` handles generic CSVs. **Phase 2 work: replace both with a user-registered rule API.**

| Rule | Severity | Logic |
|---|---|---|
| `null_check` | warning (≤3 missing) / hard_rejection (>3) | NaN in any monthly column |
| `range_check` | hard_rejection | Annual mean (J-D) outside [-3.0, +3.0]°C |
| `completeness_check` | hard_rejection | >3 monthly values missing in one year |
| `temporal_continuity` | warning | Gap in year sequence |
| `baseline_integrity` | warning | Incomplete 1951-1980 coverage |

`ValidationResult` fields: `passed`, `rule_name`, `severity` (`'hard_rejection'` or `'warning'`), `row_index`, `value`, `reason`.

---

## 7. Key Design Decisions

**ProvenanceTracker** — One `ProvDocument` per run. Call `track_ingestion()` then `track_validation()` (or `track_transformation()` for decorator path), then `finalize(store)` to serialize and persist. Agent registered once at init as `pf:provenanceflow_v1` (SoftwareAgent).

**ProvenanceStore** — SQLite at `provenance_store/lineage.db` (path overridable via `PROV_DB_PATH` env var). Three tables: `provenance_runs` (run_id PK, created_at, prov_json, summary), `entities` (entity_id, run_id FK), `rejections` (per-row rejection records). Uses `INSERT OR REPLACE` — reruns overwrite.

**Configuration** — All paths and URLs in `config.py` via `get_settings()` (pydantic-settings, `.env` aware, LRU-cached). Never hardcode paths outside this file.

**@track decorator** — Wraps any `(DataFrame) → DataFrame` function. Finds first DataFrame arg, computes SHA-256 of input/output via temp CSV, records PROV doc, stores run. Attaches `result.attrs['_prov_run_id']`. Works bare (`@track`) or parametrised (`@track(title=..., db_path=...)`).

**Ingestion abstractions** — All sources implement `DataSource` ABC (`fetch() → IngestionResult`). Adding a new data source = new subclass in `ingestion/`, no changes to tracker or pipeline.

**run_pipeline() flow:**
```
DataSource.fetch() or download_gistemp()
    → tracker.track_ingestion()
    → Validator.validate() / BasicValidator.validate()
    → tracker.track_validation()
    → tracker.finalize(store)
    → returns run_id (legacy) or PipelineResult (DataSource API)
```

---

## 8. Airflow DAG

File: `dags/provenanceflow_dag.py` | DAG ID: `provenanceflow_gistemp_pipeline`
Schedule: weekly. Tasks linked by XCom: `ingest` → `validate` → `track_provenance`.
Docker: `docker compose -f docker/docker-compose.yaml up airflow-init` then `up -d`. UI at localhost:8080.

---

## 9. Roadmap (Phase 2 onward)

**Phase 2 — Generic rule registration API** (`validation/rules.py`)
Replace hardcoded GISTEMP rules with a `@rule(severity=...)` decorator. Users register rules for their own datasets. GISTEMP rules move to `provenanceflow.contrib.gistemp`. This is the blocker for the tool being usable beyond the demo.

**Phase 3 — `@track` chaining / Pipeline context manager**
Currently each `@track` call creates an isolated PROV document. A `Pipeline` context manager should stitch multiple tracked calls into a single `wasDerivedFrom` chain. Without this, multi-step pipelines produce disconnected records, not a lineage graph.

**Phase 4 — RO-Crate export**
`provenanceflow runs export <run_id> --format ro-crate` → folder with cleaned CSV + PROV-JSON + `ro-crate-metadata.json`. Turns stored runs into emailable/depositable research artifacts compatible with Zenodo, B2FIND, and NFDI infrastructure.

---

## 10. Scope Boundaries

**In scope (built):** `@track` decorator, ingestion adapters, validation rules (GISTEMP + generic), PROV tracker, SQLite store, query API, run comparison, Markdown report generation, FastAPI REST API, Typer CLI, Streamlit dashboard, Airflow DAG.

**Not in scope:** ML model tracking, multi-user auth, distributed/cloud storage, Zenodo/DOI minting, full FAIR compliance, OpenLineage emission.

---

## 11. Standards & References

| Resource | URL |
|---|---|
| W3C PROV-DM | https://www.w3.org/TR/prov-dm/ |
| PROV Python | https://prov.readthedocs.io/ |
| FAIR Principles | https://doi.org/10.1038/sdata.2016.18 |
| RO-Crate | https://www.researchobject.org/ro-crate/ |
| OpenLineage | https://openlineage.io/ |
| NFDIxCS | https://nfdi4cs.org/ |
| NASA GISTEMP | https://data.giss.nasa.gov/gistemp/ |
| Airflow Docker | https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html |
