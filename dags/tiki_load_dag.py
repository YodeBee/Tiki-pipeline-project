from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
import logging
import sys

sys.path.append('/opt/airflow/scripts')
from scripts.config_loader import load_config
from scripts.schemas import SCHEMAS


logging.basicConfig(level=logging.INFO)

def load_data(**kwargs):
    config = load_config()
    bucket_name = config['gcs']['bucket_name']
    transformed_data_path = config['gcs']['transformed_data_path']
    project_id = config['bigquery']['project_id']
    dataset_id = config['bigquery']['dataset_id']
    table_names = config['bigquery']['table_names']

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    transformed_blob_names = {}
    for table_name in ['fact_sales', 'dim_product', 'dim_seller', 'dim_category', 'dim_brand']:
        blobs = list(bucket.list_blobs(prefix=f"{transformed_data_path}{table_name}_"))
        if not blobs:
            logging.error(f"No transformed data found for {table_name} in GCS.")
            raise ValueError(f"No transformed data found for {table_name} in GCS.")
        latest_blob = max(blobs, key=lambda x: x.updated)
        transformed_blob_names[table_name] = latest_blob.name
        logging.info(f"Found latest {table_name} blob: {latest_blob.name}")

    if not transformed_blob_names:
        logging.error("No transformed blob names available from GCS.")
        raise ValueError("No transformed data found in GCS.")

    bq_client = bigquery.Client(project=project_id)
    dataset_ref = bq_client.dataset(dataset_id)

    for table_name, blob_name in transformed_blob_names.items():
        table_id = table_names.get(table_name)
        if not table_id:
            logging.error(f"No BigQuery table name defined for {table_name}.")
            raise ValueError(f"No BigQuery table name defined for {table_name}.")

        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=SCHEMAS[table_name],
            write_disposition='WRITE_TRUNCATE',
            max_bad_records=10,
            autodetect=False
        )

        uri = f"gs://{bucket_name}/{blob_name}"
        load_job = bq_client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )
        logging.info(f"Starting load job for {table_id} from {uri}")
        load_job.result()
        logging.info(f"Loaded {table_id} with {load_job.output_rows} rows from {uri}")

with DAG(
    'tiki_load_dag',
    start_date=datetime(2025, 3, 1),
    schedule_interval=None,
    catchup=False,
    description='Load transformed data from GCS to BigQuery',
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
