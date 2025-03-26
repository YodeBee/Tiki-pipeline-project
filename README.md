# Tiki Product Analytics Pipeline

- **Author:** Phi Hai Viet
- **Tools:** Python, Apache Airflow, Google Cloud Platform, Looker Studio  
# Data Pipeline Achitecture 
  
![ETL design](https://github.com/YodeBee/Tiki-pipeline-project/blob/6f8cf0c63a2fb80493826f14f8576e17bae20b8c/Tiki%20Data%20Pipeline%20Architecture.png)

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
  **Key Questions:**
  
**Descriptive (Hindsight)**
- What is the overall performance of Tiki’s product portfolio?
- Which categories, brands, and sellers contribute most to revenue?
**Diagnostic (Insight)**
- What factors drive product sales and customer satisfaction?
- Why do some products or sellers underperform despite high ratings?
**Predictive (Foresight)**
- Which product attributes are likely to boost future sales?
**Prescriptive (Hành động)**
- How can Tiki optimize its product strategy and seller support?

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
- **Diagram:** 

![Star schema.png](https://github.com/YodeBee/Tiki-pipeline-project/blob/5f8a33d36135cb529c97f1f2c39cf455c982ea6b/Star%20schema.png)

### 4. Analysis & Visualization
- **SQL Queries:** Defined in `Tiki_ query/` 
- **Tool:** Looker Studio: https://lookerstudio.google.com/reporting/f781db5e-890e-4b57-b732-9960467678d7 

---

# Key Insights and Strategic Recommendations 

## 1. Maximizing Revenue Through Quality and Trust
- **Insight**: Electronics (smartphones) and Self-help Books are the primary drivers of Tiki’s revenue, accounting for a significant portion of the total **2.58T VND** revenue. Tiki’s **4.6 rating** is central to its reputation, offering **authenticity** and **speed** (TikiNOW), crucial for customer loyalty.
- **Impact**: Maintaining quality is critical—any decline in product quality or delivery service risks undermining Tiki’s market position.

## 2. Optimal Discounting Strategy
- **Insight**: Discounts in the range of **20%-30%** maintain a strong sales volume (10.7M units) and protect Tiki’s **premium image**. Discounts beyond **40%** spike sales but negatively affect product ratings, indicating customer concerns about quality.
- **Recommendation**: Implement a **tiered discount strategy**—**10%-15%** for new customers, **25%-30%** for loyal customers, and **40%-44%** for seasonal clearance—while keeping the **4.6 rating** intact.

## 3. Growth Opportunities in Wellness and Beauty Products
- **Insight**: Categories like **Self-help Books**, **Face Wash**, and **Shampoos** show significant growth potential. These products address the **wellness** and **self-care** needs of Vietnam's **urban consumers**, with a focus on skincare and mental well-being.
- **Recommendation**: Invest **50B VND** into targeted campaigns to increase sales in these categories and introduce **300 new SKUs** over the next year, aiming for **+15% revenue growth** (~200B VND).

## 4. Enhancing Customer Service as a Competitive Advantage
- **Insight**: Superior customer service, especially with **TikiNOW** (1-hour delivery), can drive customer loyalty and repeat sales. Small sellers like **HappyLive** show that exceptional service can compensate for lower sales volumes.
- **Recommendation**: Pilot **1-hour delivery** in **Ho Chi Minh City** and incentivize reviews with points, aiming to improve the **rating to 4.7** and increase **repeat sales by +10%** (1.07M units).

---

## Impactful Results and Strategic Achievements:
- Optimized pricing and discount strategy resulting in a **+10% revenue increase** (2.838T VND).
- Tapped into emerging trends like **wellness** and **beauty** with a **15% growth** in related product categories.
- Improved **customer service** with **TikiNOW**, boosting customer satisfaction and loyalty by enhancing delivery speed and reviews management.

For more check Insights and Recommendations.pdf

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
