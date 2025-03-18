-- create_tiki_full_view
CREATE OR REPLACE VIEW `vietadc.tiki_dataset.tiki_full_view` AS
SELECT
    fs.product_id,
    fs.seller_id,
    fs.brand_id,
    fs.category_id,
    fs.price,
    fs.original_price,
    fs.discount,
    fs.discount_rate,
    fs.quantity_sold,
    fs.review_count,
    fs.rating_average,
    dp.sku,
    dp.name AS product_name,
    dp.availability,
    dp.is_tikinow,
    dp.is_authentic,
    dp.is_installment_available,
    dp.is_tikinow_delivery,
    dp.origin,
    ds.seller_name,
    ds.seller_product_id,
    dc.category_name,
    dc.primary_category_name,
    db.brand_name,
    (fs.price * fs.quantity_sold) AS revenue
FROM
    `vietadc.tiki_dataset.Fact_Sales` fs
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Product` dp ON fs.product_id = dp.product_id
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Seller` ds ON fs.seller_id = ds.seller_id
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Category` dc ON fs.category_id = dc.category_id
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Brand` db ON fs.brand_id = db.brand_id;

-- total_revenue_and_units_sold (Tổng doanh thu và số lượng bán)
SELECT
    SUM(fs.price * fs.quantity_sold) AS total_revenue,
    SUM(fs.quantity_sold) AS total_units_sold,
    COUNT(DISTINCT fs.product_id) AS unique_products
FROM
    `vietadc.tiki_dataset.Fact_Sales` fs;


-- revenue_by_category (Doanh thu theo danh mục)
SELECT
    dc.category_name,
    dc.primary_category_name,
    SUM(fs.price * fs.quantity_sold) AS category_revenue,
    SUM(fs.quantity_sold) AS units_sold,
    AVG(fs.rating_average) AS avg_rating
FROM
    `vietadc.tiki_dataset.Fact_Sales` fs
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Category` dc ON fs.category_id = dc.category_id
GROUP BY
    dc.category_name,
    dc.primary_category_name
ORDER BY
    category_revenue DESC;

--  revenue_by_brand (Doanh thu theo thương hiệu)
SELECT
    db.brand_name,
    SUM(fs.price * fs.quantity_sold) AS brand_revenue,
    SUM(fs.quantity_sold) AS units_sold,
    COUNT(DISTINCT fs.product_id) AS product_count
FROM
    `vietadc.tiki_dataset.Fact_Sales` fs
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Brand` db ON fs.brand_id = db.brand_id
GROUP BY
    db.brand_name
ORDER BY
    brand_revenue DESC;

-- tikinow_impact (Hiệu quả TikiNOW)
SELECT
    dp.is_tikinow_delivery,
    COUNT(DISTINCT dp.product_id) AS product_count,
    SUM(fs.price * fs.quantity_sold) AS revenue,
    SUM(fs.quantity_sold) AS units_sold,
    AVG(fs.rating_average) AS avg_rating
FROM
    `vietadc.tiki_dataset.Fact_Sales` fs
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Product` dp ON fs.product_id = dp.product_id
GROUP BY
    dp.is_tikinow_delivery;

-- seller_performance (Hiệu suất nhà bán hàng)
SELECT
    ds.seller_name,
    SUM(fs.price * fs.quantity_sold) AS seller_revenue,
    SUM(fs.quantity_sold) AS units_sold,
    COUNT(DISTINCT fs.product_id) AS product_count,
    AVG(fs.rating_average) AS avg_rating
FROM
    `vietadc.tiki_dataset.Fact_Sales` fs
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Seller` ds ON fs.seller_id = ds.seller_id
GROUP BY
    ds.seller_name
HAVING
    seller_revenue IS NOT NULL
ORDER BY
    seller_revenue DESC;

-- top_products_by_origin (Top sản phẩm bán chạy theo xuất xứ)
SELECT
    dp.origin,
    dp.name AS product_name,
    fs.price,
    fs.quantity_sold,
    (fs.price * fs.quantity_sold) AS revenue
FROM
    `vietadc.tiki_dataset.Fact_Sales` fs
LEFT JOIN
    `vietadc.tiki_dataset.Dim_Product` dp ON fs.product_id = dp.product_id
WHERE
    fs.quantity_sold IS NOT NULL
ORDER BY
    revenue DESC
LIMIT 10;
