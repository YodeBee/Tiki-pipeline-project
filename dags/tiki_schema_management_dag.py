from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import logging
import sys

sys.path.append('/opt/airflow/dags/scripts') 
from scripts.config_loader import load_config
from scripts.schemas import SCHEMAS

logging.basicConfig(level=logging.INFO)

def create_dataset_and_tables(**kwargs):
    config = load_config()
    project_id = config['bigquery']['project_id']
    dataset_id = config['bigquery']['dataset_id']
    table_names = config['bigquery']['table_names']

    bq_client = bigquery.Client(project=project_id)

    # Tạo dataset nếu chưa tồn tại
    dataset_ref = bq_client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        bq_client.get_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} already exists")
    except Exception:
        bq_client.create_dataset(dataset)
        logging.info(f"Created dataset {dataset_id}")

    # Tạo hoặc cập nhật schema của các bảng
    for table_name, schema in SCHEMAS.items():
        table_id = table_names.get(table_name)
        if not table_id:
            logging.error(f"No BigQuery table name defined for {table_name}.")
            raise ValueError(f"No BigQuery table name defined for {table_name}.")

        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        try:
            bq_client.get_table(table_ref)
            logging.info(f"Table {table_id} already exists, checking schema...")
            existing_table = bq_client.get_table(table_ref)
            existing_schema = {field.name: field for field in existing_table.schema}
            new_schema = {field.name: field for field in schema}
            if existing_schema != new_schema:
                logging.info(f"Updating schema for {table_id}")
                table.schema = schema
                bq_client.update_table(table, ["schema"])
                logging.info(f"Updated schema for {table_id}")
        except Exception:
            bq_client.create_table(table)
            logging.info(f"Created table {table_id} with schema")

with DAG(
    'tiki_schema_management_dag',
    start_date=datetime(2025, 3, 7),
    schedule_interval=None,
    catchup=False,
    description='Manage BigQuery dataset and table schemas',
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    schema_task = PythonOperator(
        task_id='create_dataset_and_tables',
        python_callable=create_dataset_and_tables,
        provide_context=True
    )