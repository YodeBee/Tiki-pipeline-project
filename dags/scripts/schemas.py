from google.cloud import bigquery

SCHEMAS = {
    'fact_sales': [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("original_price", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("discount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("discount_rate", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("quantity_sold", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("review_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rating_average", "FLOAT", mode="NULLABLE")
    ],
    'dim_product': [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sku", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("brand_id", "STRING", mode="NULLABLE"),  
        bigquery.SchemaField("availability", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("is_tikinow", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_authentic", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_installment_available", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("badges_new", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_best_offer_available", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_flash_deal", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_gift_available", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_freeship_xtra", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_tikinow_delivery", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("freeship_campaign", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_image_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("primary_category_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("origin", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("seller_product_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("seller_product_sku", "STRING", mode="NULLABLE")
    ],
    'dim_seller': [
        bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("seller_name", "STRING", mode="NULLABLE")
    ],
    'dim_category': [
        bigquery.SchemaField("category_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("category_name", "STRING", mode="NULLABLE")
    ],
    'dim_brand': [
        bigquery.SchemaField("brand_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("brand_name", "STRING", mode="NULLABLE")
    ]
}
