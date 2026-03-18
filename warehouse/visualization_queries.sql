-- ==============================================================================
-- Medallion Architecture: Data Warehouse Visualization Queries
-- Dataset: Olist E-commerce (Brazilian Ecommerce)
-- Schema: gold
-- ==============================================================================

-- 🚀 [SECTION 1] EXECUTIVE DASHBOARD (GLOBAL KPIs)
-- Goal: High-level overview of business performance.

-- 1.1 Total Business Metrics
SELECT 
    COUNT(order_id) AS total_orders,
    SUM(total_payment_value) AS total_revenue,
    AVG(total_payment_value) AS avg_order_value,
    SUM(total_items) AS total_items_sold,
    AVG(avg_review_score) AS global_avg_satisfaction
FROM gold.fact_orders;

-- 1.2 Revenue by Order Status
SELECT 
    order_status,
    COUNT(*) AS order_count,
    ROUND(SUM(total_payment_value)::numeric, 2) AS revenue
FROM gold.fact_orders
GROUP BY order_status
ORDER BY revenue DESC;


-- 📈 [SECTION 2] GROWTH & TREND ANALYSIS
-- Goal: Understand sales cycles and performance over time.

-- 2.1 Monthly Revenue Growth Trend
SELECT 
    d.year,
    d.month,
    ROUND(SUM(f.total_payment_value)::numeric, 2) AS monthly_revenue,
    COUNT(f.order_id) AS monthly_orders
FROM gold.fact_orders f
JOIN gold.dim_date d ON f.order_purchase_timestamp = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2.2 Weekday vs Weekend Sales Performance
SELECT 
    d.weekday_name,
    COUNT(f.order_id) AS total_orders,
    ROUND(AVG(f.total_payment_value)::numeric, 2) AS avg_order_value
FROM gold.fact_orders f
JOIN gold.dim_date d ON f.order_purchase_timestamp = d.date_key
GROUP BY d.weekday_name, d.day_of_week
ORDER BY d.day_of_week;


-- 📦 [SECTION 3] PRODUCT & CATEGORY ANALYTICS
-- Goal: Identify top-selling categories and low-performing products.

-- 3.1 Top 10 Categories by Revenue (Optimized)
-- Goal: Identify top-selling categories using aggregated product performance metrics.
SELECT 
    p.product_category_name_english AS category,
    SUM(pp.total_sales) AS total_units_sold,
    ROUND(SUM(pp.total_revenue)::numeric, 2) AS total_revenue
FROM gold.feature_product_performance pp
JOIN gold.dim_products p ON pp.product_id = p.product_id
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 10;

-- 3.2 Product Performance Matrix (Sales vs. Rating)
SELECT 
    product_id,
    total_sales,
    total_revenue,
    avg_rating
FROM gold.feature_product_performance
WHERE total_sales > 10
ORDER BY avg_rating DESC, total_revenue DESC
LIMIT 20;


-- 👥 [SECTION 4] CUSTOMER & GEOSPATIAL INSIGHTS
-- Goal: Where are our customers and how do they behave?

-- 4.1 Top States by Revenue & Customer Density
SELECT 
    customer_state,
    COUNT(DISTINCT customer_unique_id) AS unique_customers,
    ROUND(SUM(monetary)::numeric, 2) AS lifetime_value
FROM gold.feature_customer_profile cp
JOIN gold.dim_customers c ON cp.customer_id = c.customer_id
GROUP BY customer_state
ORDER BY lifetime_value DESC;

-- 4.2 Customer RFM Segments (Recency, Frequency, Monetary)
SELECT 
    customer_id,
    CASE 
        WHEN recency < 30 AND frequency > 5 THEN 'Champions'
        WHEN recency < 60 THEN 'Active'
        WHEN recency > 180 THEN 'At Risk'
        ELSE 'Neutral'
    END AS customer_segment,
    frequency,
    monetary,
    recency
FROM gold.feature_customer_profile
ORDER BY monetary DESC
LIMIT 50;


-- 🚚 [SECTION 5] LOGISTICS & RELIABILITY
-- Goal: Monitor shipping performance and seller quality.

-- 5.1 Seller Reliability Leaderboard
SELECT 
    seller_id,
    total_orders,
    ROUND(total_sales_value::numeric, 2) AS total_sales,
    ROUND(avg_delivery_time_days::numeric, 2) AS avg_days_to_deliver
FROM gold.feature_seller_reliability
WHERE avg_delivery_time_days > 0
ORDER BY total_orders DESC, avg_days_to_deliver ASC
LIMIT 20;

-- 5.2 Delivery Time Distribution
SELECT 
    CASE 
        WHEN avg_delivery_time_days < 5 THEN 'Over-speed (<5 days)'
        WHEN avg_delivery_time_days BETWEEN 5 AND 10 THEN 'Normal (5-10 days)'
        WHEN avg_delivery_time_days BETWEEN 10 AND 20 THEN 'Slow (10-20 days)'
        ELSE 'Very Slow (>20 days)'
    END AS delivery_speed_category,
    COUNT(*) AS seller_count
FROM gold.feature_seller_reliability
WHERE avg_delivery_time_days > 0
GROUP BY 1;

-- 5.3 Seller Distribution by Geolocation (Audit)
-- Goal: Map seller density using official geolocation city and state names.
SELECT 
    l.geolocation_state,
    l.geolocation_city,
    COUNT(DISTINCT s.seller_id) AS seller_count
FROM gold.dim_sellers s
JOIN gold.dim_location l ON s.seller_zip_code_prefix = l.geolocation_zip_code_prefix
GROUP BY 1, 2
ORDER BY seller_count DESC;
