# Tiki Product Analytics Pipeline

- **Author:** [Your Name]  
- **GitHub:** [https://github.com/YodeBee/Tiki-pipeline-project]  
- **Tools:** Python, Apache Airflow, Google Cloud Platform, Looker Studio  

---

## Introduction

Tiki, a leading e-commerce platform in Vietnam, offers a rich product ecosystem with services like TikiNOW for rapid delivery. The *Tiki Product Analytics Pipeline* is an automated, end-to-end data solution built to analyze Tiki’s product data. Designed as a portfolio project for a Fresher/Junior Data Analyst or Business Intelligence role, it captures detailed product attribute to uncover insights for optimizing product strategies and service performance.

### Project Purpose & Motivation
I built the *Tiki Product Analytics Pipeline* to transform raw product data into actionable insights, supporting decisions in product management, marketing, and seller performance while preparing for a career in data analytics and business intelligence. Inspired by Tiki’s e-commerce ecosystem, this project bridges theoretical knowledge with practical skills, demonstrating my ability to design an end-to-end data pipeline integrating a data warehouse with BI capabilities using tools like Python, Airflow, BigQuery, and Looker Studio to drive business value from product-level data.

### Collected Data Fields
The pipeline collects 32 fields from Tiki’s API (`https://tiki.vn/api/v2/products`), forming the foundation for analysis. Key fields include:
*(Full list in Appendix)*
These fields were selected for their analytical value and scalability, enabling future enhancements without pipeline redesign.

---

## Project Objectives

- **Business Goal:** Provide insights into product performance, seller efficiency, and service impacts to optimize Tiki’s offerings.
- **Technical Goal:** Build an automated pipeline integrating data warehousing and BI, using modern tools.
- **Key Questions:**
  1. What are the total revenue and units sold across products?
  2. Which categories and brands drive the most revenue?
  3. How does TikiNOW affect product sales and ratings?
  4. What are the top-performing products by origin?
  5. Which sellers underperform and need support?

---

## Methodology
This project follows a structured ETL pipeline orchestrated by Apache Airflow, with data stored in Google Cloud Storage (GCS) and processed in BigQuery. Below is the detailed workflow:

### 1. Data Collection
- **Source:** Tiki API (`https://tiki.vn/api/v2/products`).
- **Method:** Crawled unique products across 11 categories (I only selected 11 categories because I just want to simulate the project and am limited by the free account on Google Cloud Platform. If you wish to collect more categories, you can absolutely add them.).
- **DAG:** `tiki_extract_dag` - Saves raw JSON to GCS (`tiki_raw_data/`).

### 2. ETL Process
- **Extract:** `tiki_extract_dag` - Fetches data with retry logic, avoids duplicates via `crawled_products.json`.
- **Transform:** `tiki_transform_dag` - Cleans, deduplicates, and splits data into 5 tables, saved as CSV in GCS (`tiki_transformed_data/`).
- **Load:** `tiki_load_dag` - Loads CSV into BigQuery with a Star Schema.
- **Schema Management:** `tiki_schema_management_dag` - Ensures dataset and table consistency.
- **Orchestration:** `tiki_etl_orchestration` - Runs tasks sequentially.

### 3. Data Warehouse
- **Platform:** Google BigQuery (`tiki_dataset`).
- **Star Schema:**
  - **Fact Table:** `Fact_Sales` - Metrics.
  - **Dimension Tables:**
    - `Dim_Product`: Product details.
    - `Dim_Seller`: Seller info.
    - `Dim_Category`: Category mappings.
    - `Dim_Brand`: Brand details.
- **Diagram:** ![Star Schema](star_schema.png)

### 4. Analysis & Visualization
- **SQL Queries:** Defined in `sql/` 
- **Tool:** Looker Studio:
  - **Overview:** KPIs (revenue, units sold), category/brand breakdowns.
  - **Details:** Seller performance, top products by origin.

---

## Results

- **Metrics:** 
  - Total Revenue: 2,524,673,426,420 VND.
  - Units Sold: 10,398,482.
  - Unique Products: 20,357.
- **Insights:**
  - TikiNOW products (18.7%, 3,808) significantly boost sales.
  - "China" dominates revenue by origin.
  - Top sellers drive most revenue; some high-rated sellers underperform in sales.

---

## Limitations & Future Work

- **Limitations:** No time-series data; limited service metrics (e.g., Flash Deals).
- **Improvements:** Add daily crawls, expand service data capture.

---

## Conclusion

The *Tiki Product Analytics Pipeline* showcases my ability to build an automated data solution—from crawling product data to delivering BI insights—using Python, Airflow, BigQuery, and Looker Studio. 
This project successfully implemented an automated ETL pipeline for Tiki product data, culminating in a Looker Studio dashboard with actionable insights. It demonstrates my ability to:
- Collect real-world data using Python and APIs.
- Automate workflows with Airflow.
- Design a data warehouse in BigQuery.
- Visualize insights for business decisions.
---

## Appendix

- **Full Collected Data Fields:**
  ```markdown
| Field Name                | Data Type    | Description                                                                 |
|---------------------------|--------------|-----------------------------------------------------------------------------|
| `id`                      | String       | Unique identifier of the product (Product ID).                              |
| `sku`                     | String       | Stock Keeping Unit code of the product.                                     |
| `name`                    | String       | Name of the product.                                                        |
| `price`                   | Float        | Current selling price of the product (VND).                                 |
| `original_price`          | Float        | Original price before discount (list price, VND).                           |
| `discount`                | Float        | Discount amount applied to the product (VND).                               |
| `discount_rate`           | Float        | Discount percentage (%).                                                    |
| `rating_average`          | Float        | Average rating of the product (0-5 scale).                                  |
| `review_count`            | Integer      | Number of user reviews for the product.                                     |
| `quantity_sold`           | Integer      | Total units of the product sold.                                            |
| `seller_id`               | String       | Unique identifier of the seller.                                            |
| `seller_name`             | String       | Name of the seller.                                                         |
| `brand_id`                | String       | Unique identifier of the brand.                                             |
| `brand_name`              | String       | Name of the brand (prioritized from `badges_new` if available).             |
| `availability`            | Integer      | Number of units available in stock.                                         |
| `is_tikinow`              | Boolean      | Indicates if the product is part of the TikiNOW program.                    |
| `is_authentic`            | Boolean      | Indicates if the product is certified authentic.                            |
| `is_installment_available`| Boolean      | Indicates if installment payment is available for the product.              |
| `badges_new`              | String (JSON)| List of new badges associated with the product (e.g., "official_store").    |
| `is_best_offer_available` | Boolean      | Indicates if the product has the best offer available.                      |
| `is_flash_deal`           | Boolean      | Indicates if the product is part of a Flash Deal promotion.                 |
| `is_gift_available`       | Boolean      | Indicates if the product includes a gift.                                   |
| `is_freeship_xtra`        | Boolean      | Indicates if the product qualifies for Freeship Xtra.                       |
| `is_tikinow_delivery`     | Boolean      | Indicates if the product offers TikiNOW delivery (fast shipping service).   |
| `freeship_campaign`       | String       | Details of any free shipping campaign (if applicable).                      |
| `product_url`             | String       | URL to the product page on Tiki.                                            |
| `product_image_url`       | String       | URL to the product’s thumbnail image.                                       |
| `category_ids`            | String (List)| List of category IDs associated with the product.                           |
| `primary_category_name`   | String       | Name of the primary category for the product.                               |
| `origin`                  | String       | Origin or manufacturing country of the product (e.g., "China", "Vietnam").  |
| `seller_product_id`       | String       | Product ID defined by the seller.                                           |
| `seller_product_sku`      | String       | SKU code defined by the seller for the product.                             |
