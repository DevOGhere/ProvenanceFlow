import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, '/opt/airflow')

from src.provenanceflow.config import get_settings, configure_logging
from src.provenanceflow.ingestion.nasa_gistemp import NASAGISTEMPSource, parse_gistemp
from src.provenanceflow.validation.validator import Validator
from src.provenanceflow.provenance.tracker import ProvenanceTracker
from src.provenanceflow.provenance.store import ProvenanceStore

configure_logging()
logger = logging.getLogger(__name__)

# Paths driven by environment variables via get_settings().
# In Docker, docker-compose sets RAW_DATA_PATH, PROCESSED_DATA_PATH, PROV_DB_PATH.
_s = get_settings()
NASA_URL   = str(_s.gistemp_url)
LOCAL_PATH = str(_s.raw_data_path / 'gistemp_global.csv')
DB_PATH    = str(_s.prov_db_path)

default_args = {
    'owner': 'provenanceflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}


def task_ingest(**context):
    source = NASAGISTEMPSource(url=NASA_URL, output_dir=Path(LOCAL_PATH).parent)
    ingestion = source.fetch()
    context['ti'].xcom_push(key='row_count', value=ingestion.row_count)
    logger.info("Ingested %d rows from NASA GISTEMP", ingestion.row_count)


def task_validate(**context):
    df = parse_gistemp(LOCAL_PATH)
    validator = Validator()
    results = validator.validate(df)
    clean_df = validator.get_clean(df, results)
    context['ti'].xcom_push(key='rows_passed', value=len(clean_df))
    context['ti'].xcom_push(key='rows_rejected', value=len(df) - len(clean_df))
    context['ti'].xcom_push(key='rejections', value=str(validator.rejection_summary(results)))
    context['ti'].xcom_push(key='warnings', value=str(validator.warning_summary(results)))
    Path(_s.processed_data_path).mkdir(parents=True, exist_ok=True)
    clean_df.to_csv(_s.processed_data_path / 'gistemp_clean.csv', index=False)
    logger.info("Validation complete: %d/%d rows passed", len(clean_df), len(df))


def task_track_provenance(**context):
    import ast
    ti = context['ti']
    rows_in = ti.xcom_pull(key='row_count', task_ids='ingest')
    rows_passed = ti.xcom_pull(key='rows_passed', task_ids='validate')
    rejections = ti.xcom_pull(key='rejections', task_ids='validate')
    warnings = ti.xcom_pull(key='warnings', task_ids='validate')

    store = ProvenanceStore(db_path=DB_PATH)
    tracker = ProvenanceTracker()

    raw_entity = tracker.track_ingestion(
        source_url=NASA_URL,
        local_path=LOCAL_PATH,
        row_count=rows_in,
    )
    tracker.track_validation(
        input_entity=raw_entity,
        rows_in=rows_in,
        rows_passed=rows_passed,
        rejections=ast.literal_eval(rejections) if rejections else {},
        warnings=ast.literal_eval(warnings) if warnings else {},
        rules_applied=Validator.RULE_NAMES,
    )
    run_id = tracker.finalize(store)
    logger.info("Provenance tracked. run_id=%s", run_id)


with DAG(
    dag_id='provenanceflow_gistemp_pipeline',
    default_args=default_args,
    description='FAIR-compliant data lineage tracking for NASA GISTEMP',
    schedule=timedelta(weeks=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['provenanceflow', 'fair', 'climate-science'],
) as dag:

    ingest = PythonOperator(task_id='ingest', python_callable=task_ingest)
    validate = PythonOperator(task_id='validate', python_callable=task_validate)
    track = PythonOperator(task_id='track_provenance', python_callable=task_track_provenance)

    ingest >> validate >> track
