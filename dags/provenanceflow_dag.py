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
DB_PATH = '/opt/airflow/provenance_store/lineage.db'

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
    df = df[pd.to_numeric(df['Year'], errors='coerce').notna()].copy()
    validator = Validator()
    results = validator.validate(df)
    clean_df = validator.get_clean(df, results)
    context['ti'].xcom_push(key='rows_passed', value=len(clean_df))
    context['ti'].xcom_push(key='rows_rejected', value=len(df) - len(clean_df))
    context['ti'].xcom_push(key='rejections', value=str(validator.rejection_summary(results)))
    context['ti'].xcom_push(key='warnings', value=str(validator.warning_summary(results)))
    clean_df.to_csv('/opt/airflow/data/processed/gistemp_clean.csv', index=False)
    print(f"Validation complete: {len(clean_df)}/{len(df)} rows passed")


def task_track_provenance(**context):
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
        rejections=eval(rejections) if rejections else {},
        warnings=eval(warnings) if warnings else {},
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

    ingest = PythonOperator(task_id='ingest', python_callable=task_ingest)
    validate = PythonOperator(task_id='validate', python_callable=task_validate)
    track = PythonOperator(task_id='track_provenance', python_callable=task_track_provenance)

    ingest >> validate >> track
