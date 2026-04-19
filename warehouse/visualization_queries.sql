-- ==============================================================================
-- BIGQUERY ANALYTICAL QUERIES FOR OLIST GOLD LAYER
-- Dataset: Brazilian E-Commerce Public Dataset by Olist
-- ==============================================================================

-- 📊 1. TOp-Line Performance: Revenue & Order Volume Over Time
-- This query helps visualize sales trends and seasonality (e.g., Black Friday peaks).
SELECT 
    d.year,
    d.month_name,
    d.month,
    ROUND(SUM(f.item_price), 2) as total_revenue,
    COUNT(DISTINCT f.order_id) as total_orders,
    ROUND(AVG(f.item_price), 2) as avg_order_value
FROM `<your-dataset-name>.fact_sales` f
JOIN `<your-dataset-name>.dim_date` d 
  ON DATE(TIMESTAMP_MICROS(DIV(f.order_purchase_timestamp, 1000))) = d.date_key
WHERE f.order_status = 'delivered'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 3 DESC;


-- 📊 2. Product Category Analysis: High-Value vs. High-Volume
-- Identifies which categories drive the most revenue for the platform.
SELECT 
    p.product_category_name_english,
    COUNT(f.sales_sk) as units_sold,
    ROUND(SUM(f.item_price), 2) as total_revenue,
    ROUND(AVG(f.item_price), 2) as avg_unit_price
FROM `<your-dataset-name>.fact_sales` f
JOIN `<your-dataset-name>.dim_products` p ON f.product_id = p.product_id
WHERE p.is_current = true -- Only latest product info
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 10;


-- 📊 3. Customer RFM Segmentation
-- Segments customers based on Recency, Frequency, and Monetary values.
-- This can be used to create dashboards for "Champions", "At Risk", and "New" customers.
WITH rfm_scores AS (
    SELECT 
        customer_unique_id,
        monetary,
        frequency,
        recency,
        -- Simple scoring (1-5) using quantiles
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) as m_score
    FROM `<your-dataset-name>.feature_customer_rfm`
)
SELECT 
    customer_unique_id,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score) as total_rfm_score,
    CASE 
        WHEN (r_score + f_score + m_score) >= 13 THEN 'Champion'
        WHEN (r_score + f_score + m_score) >= 9 THEN 'Loyal'
        WHEN (r_score + f_score + m_score) >= 5 THEN 'Potential'
        ELSE 'At Risk'
    END as customer_segment
FROM rfm_scores
ORDER BY total_rfm_score DESC;


-- 📊 4. Logistics & Delivery Performance by State
-- Analyzes which regions experience the most delivery delays.
-- Corrected: Joins on customer_unique_id after Gold model refactor.
SELECT 
    c.customer_state,
    ROUND(AVG(f.delivery_delay_days), 1) as avg_delivery_time,
    COUNT(f.order_id) as total_shipments,
    ROUND(SUM(CASE WHEN f.delivery_delay_days > 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delayed_shipment_pct
FROM `<your-dataset-name>.fact_sales` f
JOIN `<your-dataset-name>.dim_customers` c 
  ON f.customer_unique_id = c.customer_unique_id
WHERE c.is_current = true
GROUP BY 1
ORDER BY avg_delivery_time DESC;


-- 📊 5. Seller Reliability vs. Customer Satisfaction
-- Correlations between delivery speed and review scores.
SELECT
  s.seller_id,
  r.total_orders,
  r.avg_delivery_time_days,
  ROUND(AVG(rev.review_score), 2) AS avg_customer_rating
FROM `<your-dataset-name>.feature_seller_reliability` r
JOIN `<your-dataset-name>.fact_sales` fs
  ON r.seller_id = fs.seller_id
JOIN `<your-dataset-name>.fact_reviews` rev
  ON fs.order_id = rev.order_id
JOIN `<your-dataset-name>.dim_sellers` s
  ON r.seller_id = s.seller_id
WHERE s.is_current = TRUE
GROUP BY 1, 2, 3
HAVING total_orders > 10
ORDER BY avg_customer_rating ASC
LIMIT 20;
