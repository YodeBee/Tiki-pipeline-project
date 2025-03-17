from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage
import logging
import json
import sys
import io
import csv

sys.path.append('/opt/airflow/scripts')
from scripts.config_loader import load_config
from scripts.schemas import SCHEMAS

logging.basicConfig(level=logging.INFO)

def clean_data(df):
    if df is not None and not df.empty:
        df = df.dropna(subset=['id'])  
    return df

def transform_data(**kwargs):
    config = load_config()
    bucket_name = config['gcs']['bucket_name']
    transformed_data_path = config['gcs']['transformed_data_path']

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    metadata_blobs = list(bucket.list_blobs(prefix="metadata/extract_run_metadata_"))
    if not metadata_blobs:
        logging.error("No metadata files found in GCS.")
        raise ValueError("No metadata files found in GCS.")

    metadata_by_category = {}
    for blob in metadata_blobs:
        metadata = json.loads(blob.download_as_text())
        category = metadata.get('category', 'unknown')
        if category not in metadata_by_category or blob.updated > metadata_by_category[category]['updated']:
            metadata_by_category[category] = {'blob': blob, 'updated': blob.updated, 'metadata': metadata}

    all_raw_data = []
    for category, info in metadata_by_category.items():
        raw_blob_name = info['metadata'].get('raw_blob_name')
        if not raw_blob_name:
            logging.error(f"No raw_blob_name found in metadata for category {category}")
            continue
        try:
            blob = bucket.blob(raw_blob_name)
            raw_data_json = blob.download_as_bytes().decode('utf-8')
            raw_data = pd.read_json(io.StringIO(raw_data_json))
            all_raw_data.append(raw_data)
            logging.info(f"Loaded raw data from {bucket_name}/{raw_blob_name}, {len(raw_data)} rows")
        except Exception as e:
            logging.error(f"Error reading raw data from {raw_blob_name}: {e}")
            continue

    if not all_raw_data:
        logging.error("No raw data loaded from any category.")
        raise ValueError("No raw data loaded from any category.")

    df = pd.concat(all_raw_data, ignore_index=True).drop_duplicates(subset=['id'])
    logging.info(f"After deduplication across all categories: {len(df)} rows")

    df = clean_data(df)

    if df is None or df.empty:
        logging.error("DataFrame is None or empty after clean_data.")
        raise ValueError("DataFrame is None or empty after clean_data.")

    # Định nghĩa danh sách cột từ schema
    dim_product_cols = [field.name for field in SCHEMAS['dim_product']]  # ['product_id', 'sku', 'name', ...]
    fact_cols = ['id', 'seller_id', 'price', 'original_price', 'discount',
                 'discount_rate', 'quantity_sold', 'review_count', 'rating_average']

    # Tạo fact_df
    fact_df = df[[col for col in fact_cols if col in df.columns]].rename(columns={'id': 'product_id'}).drop_duplicates()

    # Tạo dim_product_df: Đảm bảo bao gồm 'id' để rename thành 'product_id'
    dim_product_input_cols = ['id'] + [col for col in dim_product_cols if col != 'product_id' and col in df.columns]
    dim_product_df = df[dim_product_input_cols].rename(columns={'id': 'product_id'}).drop_duplicates(subset=['product_id'])

    dim_seller_cols = ['seller_id', 'seller_name']
    dim_seller_df = df[[col for col in dim_seller_cols if col in df.columns]].drop_duplicates(subset=['seller_id'])

    dim_category_df = df[['category_ids', 'primary_category_name']].explode('category_ids').drop_duplicates() if 'category_ids' in df.columns else pd.DataFrame(columns=['category_id', 'category_name'])
    dim_category_df.columns = ['category_id', 'category_name']
    dim_category_df = dim_category_df[dim_category_df['category_id'].notnull()]

    dim_brand_cols = ['brand_id', 'brand_name']
    dim_brand_df = df[[col for col in dim_brand_cols if col in df.columns]].drop_duplicates(subset=['brand_id']).dropna(subset=['brand_id'])

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    transformed_blob_names = {}
    for name, data in [
        ('fact_sales', fact_df),
        ('dim_product', dim_product_df),
        ('dim_seller', dim_seller_df),
        ('dim_category', dim_category_df),
        ('dim_brand', dim_brand_df)
    ]:
        blob_name = f"{transformed_data_path}{name}_{timestamp}.csv"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data.to_csv(index=False, quoting=csv.QUOTE_ALL, escapechar='\\'), content_type='text/csv')
        transformed_blob_names[name] = blob_name
        logging.info(f"Saved {name} to {bucket_name}/{blob_name}")

    kwargs['ti'].xcom_push(key='transformed_blob_names', value=transformed_blob_names)
    logging.info(f"Pushed transformed_blob_names to XCom: {transformed_blob_names}")

with DAG(
    'tiki_transform_dag',
    start_date=datetime(2025, 3, 1),
    schedule_interval=None,
    catchup=False,
    description='Transform raw data from multiple JSON files and save as CSV to GCS',
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )
