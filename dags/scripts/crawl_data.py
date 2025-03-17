import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import random
import time
from datetime import datetime
from google.cloud import storage
import json

logging.basicConfig(level=logging.INFO)
def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
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
        'brand_id': str(item.get('brand', {}).get('id', '')),
        'brand_name': item.get('brand', {}).get('name', ''),
        'availability': item.get('stock_item', {}).get('available', 0),
        'is_tikinow': item.get('is_tikinow', False),
        'is_authentic': item.get('is_authentic', False),
        'is_installment_available': item.get('installment_info', {}).get('available', False),
        'badges_new': item.get('badges_new', ''),
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

def extract_data(categories, max_pages, bucket_name, raw_data_path, crawled_products_path):
    session = requests_retry_session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json'
    }
    api_url = "https://tiki.vn/api/v2/products"
    all_raw_data = []
    crawled_ids = get_crawled_product_ids(bucket_name, crawled_products_path)
    new_ids = set()

    for category in categories:
        logging.info(f"Crawling category {category['urlKey']} with {max_pages} pages")
        for page in range(1, max_pages + 1):
            params = {'page': page, 'category': category['category'], 'limit': 50}
            try:
                response = session.get(api_url, headers=headers, params=params, timeout=15)
                response.raise_for_status()
                data = response.json().get('data', [])
                if not data:
                    logging.warning(f"No data found for page {page} - {category['urlKey']}")
                    continue
                for item in data:
                    product_id = item.get('id')
                    if product_id is not None:
                        product_id = str(product_id)
                        if product_id and product_id not in crawled_ids:  # Kiểm tra trùng lặp
                            parsed_item = parse_product(item)
                            if parsed_item:
                                all_raw_data.append(parsed_item)
                                new_ids.add(product_id)
                            else:
                                logging.info(f"Skipped product {product_id} due to parse failure")
                        else:
                            logging.info(f"Skipped product {product_id} due to duplicate")
                    else:
                        logging.info(f"Skipped product due to missing ID: {item}")
                logging.info(f"Extracted page {page} - {category['urlKey']}, {len(data)} items, total extracted: {len(all_raw_data)}")
                time.sleep(random.uniform(2, 5))
            except requests.exceptions.RequestException as e:
                logging.error(f"API error for page {page} - {category['urlKey']}: {e}")
                break
            except Exception as e:
                logging.error(f"Unexpected error for page {page} - {category['urlKey']}: {e}")
                break

    if all_raw_data:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob_name = f"{raw_data_path}raw_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(json.dumps(all_raw_data))
        update_crawled_product_ids(bucket_name, new_ids, crawled_products_path)
        logging.info(f"Uploaded raw data to {bucket_name}/{blob_name}, total items: {len(all_raw_data)}")
        return blob_name
    logging.warning("No data extracted from any category. Returning None.")
    return None