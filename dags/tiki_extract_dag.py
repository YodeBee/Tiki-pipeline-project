from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import json
from google.cloud import storage
import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import time

sys.path.append('/opt/airflow/scripts')
from scripts.config_loader import load_config

logging.basicConfig(level=logging.INFO)

def requests_retry_session(retries=5, backoff_factor=1, status_forcelist=(429, 500, 502, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_crawled_product_ids(bucket_name, crawled_products_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(crawled_products_path)
    try:
        if blob.exists():
            return set(blob.download_as_string().decode('utf-8').splitlines())
        return set()
    except Exception as e:
        logging.error(f"Error reading crawled products: {e}")
        return set()

def update_crawled_product_ids(bucket_name, new_ids, crawled_products_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(crawled_products_path)
    try:
        if blob.exists():
            existing_ids = set(blob.download_as_string().decode('utf-8').splitlines())
        else:
            existing_ids = set()
        updated_ids = existing_ids.union(new_ids)
        blob.upload_from_string('\n'.join(sorted(updated_ids)))
        logging.info(f"Updated crawled products at {bucket_name}/{crawled_products_path}")
    except Exception as e:
        logging.error(f"Error updating crawled products: {e}")
        raise

def parse_product(item):
    logging.debug(f"Raw product data: {json.dumps(item, ensure_ascii=False)}")
    
    # Lấy brand_name từ badges_new nếu có
    brand_id = str(item.get('brand_id', '')) if item.get('brand_id') is not None else ''
    brand_name = item.get('brand_name', '') if item.get('brand_name') is not None else ''
    badges = item.get('badges_new', [])
    for badge in badges:
        if badge.get('code') == 'brand_name' and badge.get('text'):
            brand_name = badge['text']
            break

    parsed_item = {
        'id': item.get('id'),
        'sku': item.get('sku'),
        'name': item.get('name'),
        'price': item.get('price'),
        'original_price': item.get('list_price'),
        'discount': item.get('discount'),
        'discount_rate': item.get('discount_rate'),
        'rating_average': item.get('rating_average'),
        'review_count': item.get('review_count'),
        'quantity_sold': item.get('quantity_sold', {}).get('value', 0),
        'seller_id': str(item.get('seller_id', '')),
        'seller_name': item.get('seller_name', ''),
        'brand_id': brand_id,
        'brand_name': brand_name,
        'availability': item.get('stock_item', {}).get('available', 0),
        'is_tikinow': item.get('is_tikinow', False),
        'is_authentic': item.get('is_authentic', False),
        'is_installment_available': item.get('installment_info', {}).get('available', False),
        'badges_new': json.dumps(item.get('badges_new', [])),
        'is_best_offer_available': item.get('is_best_offer_available', False),
        'is_flash_deal': item.get('is_flash_deal', False),
        'is_gift_available': item.get('is_gift_available', False),
        'is_freeship_xtra': item.get('is_freeship_xtra', False),
        'is_tikinow_delivery': item.get('is_tikinow_delivery', False),
        'freeship_campaign': item.get('freeship_campaign', ''),
        'product_url': item.get('url_path', ''),
        'product_image_url': item.get('thumbnail_url', ''),
        'category_ids': [str(cat) for cat in item.get('category_ids', [])],
        'primary_category_name': item.get('primary_category_name', ''),
        'origin': item.get('origin', ''),
        'seller_product_id': str(item.get('seller_product_id', '')),
        'seller_product_sku': item.get('seller_product_sku', '')
    }
    if not parsed_item['id']:
        logging.warning(f"Skipping product due to missing 'id': {item}")
        return None
    return parsed_item

def extract_category_data(category, bucket_name, raw_data_path, crawled_products_path, api_url, **kwargs):
    session = requests_retry_session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json'
    }
    all_raw_data = []
    crawled_ids = get_crawled_product_ids(bucket_name, crawled_products_path)
    new_ids = set()

    logging.info(f"Starting crawl for category {category['urlKey']}")
    params = {'page': 1, 'category': category['category'], 'limit': 50}
    try:
        response = session.get(api_url, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        first_page = response.json()
        total_pages = first_page['paging']['last_page']
        logging.info(f"Total pages for {category['urlKey']}: {total_pages}")

        for item in first_page['data']:
            product_id = str(item.get('id'))
            if product_id and product_id not in crawled_ids:
                parsed_item = parse_product(item)
                if parsed_item:
                    all_raw_data.append(parsed_item)
                    new_ids.add(product_id)

        for page in range(2, total_pages + 1):
            params['page'] = page
            try:
                response = session.get(api_url, headers=headers, params=params, timeout=15)
                response.raise_for_status()
                data = response.json().get('data', [])
                if not data:
                    logging.warning(f"No data found for page {page} - {category['urlKey']}")
                    break
                for item in data:
                    product_id = str(item.get('id'))
                    if product_id and product_id not in crawled_ids:
                        parsed_item = parse_product(item)
                        if parsed_item:
                            all_raw_data.append(parsed_item)
                            new_ids.add(product_id)
                logging.info(f"Extracted page {page}/{total_pages} - {category['urlKey']}, total items: {len(all_raw_data)}")
                time.sleep(random.uniform(5, 10))
            except requests.exceptions.RequestException as e:
                logging.error(f"API error at page {page} - {category['urlKey']}: {e}")
                break
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to crawl category {category['urlKey']}: {e}")
        return

    if all_raw_data:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        raw_blob_name = f"{raw_data_path}raw_{category['urlKey']}_{timestamp}.json"
        blob = bucket.blob(raw_blob_name)
        blob.upload_from_string(json.dumps(all_raw_data), content_type='application/json')
        update_crawled_product_ids(bucket_name, new_ids, crawled_products_path)
        logging.info(f"Uploaded raw data to {bucket_name}/{raw_blob_name}, total items: {len(all_raw_data)}")

        metadata = {
            "raw_blob_name": raw_blob_name,
            "category": category['urlKey'],
            "execution_date": kwargs['execution_date'].isoformat(),
            "run_id": kwargs['run_id']
        }
        metadata_blob_name = f"metadata/extract_run_metadata_{category['urlKey']}_{timestamp}.json"
        metadata_blob = bucket.blob(metadata_blob_name)
        metadata_blob.upload_from_string(json.dumps(metadata), content_type='application/json')
        logging.info(f"Saved metadata to {bucket_name}/{metadata_blob_name}")
    else:
        logging.warning(f"No data extracted for category {category['urlKey']}.")

with DAG(
    'tiki_extract_dag',
    start_date=datetime(2025, 3, 1),
    schedule_interval=None,
    catchup=False,
    description='Extract raw data from Tiki API for multiple categories in parallel and save to GCS',
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    config = load_config()
    bucket_name = config['gcs']['bucket_name']
    raw_data_path = config['gcs']['raw_data_path']
    crawled_products_path = config['gcs']['crawled_products_path']
    api_url = config['tiki']['api_url']
    categories = config['tiki']['categories']

    extract_tasks = []
    for category in categories:
        task = PythonOperator(
            task_id=f"extract_{category['urlKey']}",
            python_callable=extract_category_data,
            op_kwargs={
                'category': category,
                'bucket_name': bucket_name,
                'raw_data_path': raw_data_path,
                'crawled_products_path': crawled_products_path,
                'api_url': api_url
            },
            provide_context=True,
            retries=3,
            retry_delay=timedelta(minutes=5)
        )
        extract_tasks.append(task)
