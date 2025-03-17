from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

with DAG(
    'tiki_etl_orchestration',
    start_date=datetime(2025, 3, 1),
    schedule_interval=None,
    catchup=False,
    description='Orchestrate Tiki ETL pipeline with schema management after extract/transform and before load',
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    extract_data = TriggerDagRunOperator(
        task_id='extract_data',
        trigger_dag_id='tiki_extract_dag',
        wait_for_completion=True
    )

    transform_data = TriggerDagRunOperator(
        task_id='transform_data',
        trigger_dag_id='tiki_transform_dag',
        wait_for_completion=True
    )

    manage_schema = TriggerDagRunOperator(
        task_id='manage_schema',
        trigger_dag_id='tiki_schema_management_dag',
        wait_for_completion=True
    )

    load_data = TriggerDagRunOperator(
        task_id='load_data',
        trigger_dag_id='tiki_load_dag',
        wait_for_completion=True
    )

    extract_data >> transform_data >> manage_schema >> load_data
