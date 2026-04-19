# Phase 4: Gold Layer - Star Schema & SCD Type 2

The Gold layer transforms clean Silver data into a business-optimized Star Schema. This layer is designed to be the high-performance back-end for BI tools and ML models.

## 1. Dimensional Modeling Architecture
To minimize the number of joins for analytical queries, we transitioned from the normalized Silver structure to a **Star Schema**.

- **Stable Identity**: Natural keys are transformed into **SHA-256 surrogate keys** (e.g., `sales_sk`) using a deterministic concatenation of the grain attributes. This ensures identities remain stable even if source natural keys are recycled or changed.
- **Precision Typing**: Financial fields (price, freight, payments) are strictly cast to `Decimal(10,2)` to avoid floating-point errors in revenue reports.
- **Schema Optimization**: Standardized measurements are cast to `Short` or `Integer` types, significantly reducing the shuffle payload during complex join operations.

## 2. Slowly Changing Dimensions (SCD Type 2)
To track historical changes, we implemented **SCD Type 2** for Customer and Product data.

- **Tracking History**: Instead of overwriting a customer's city when they move, we use `start_date`, `end_date`, and an `is_current` flag.
- **Hashing Logic**: We use `F.hash` on non-key attributes to identify changes. If a mismatch is detected, the current record is expired and a new version is inserted.
- **Analytical Value**: This allows for "Point-in-Time" reporting—seeing where a customer was located *at the time of a specific sale* years ago.

## 3. Advanced Memory Management (OOM Prevention)
Gold transformations involve complex, multi-way joins that can easily crash a Spark Driver.

- **Broadcast Joins**: We explicitly hint Spark to broadcast smaller dimensions to all executors, avoiding expensive shuffles.
- **Cache & Unpersist Strategy**:
    - We `.cache()` the intermediate results of the Fact table joins.
    - Immediately after the final table is written to MinIO, we call `.unpersist()` to clear the cluster's RAM.
- **Localized Casting**: We cast columns to their final types *late* in the process to keep the shuffle payload light.

## 4. Star Schema Validation
Unlike Silver (which checks for data types), the Gold validator checks for **Structural Integrity**. It ensures that the Fact table has no orphaned keys and that the PK/FK relationships are consistent before exposing the data to the cloud.

## 5. Feature Engineering Strategy
Beyond standard reporting, the Gold layer computes behavioral features for downstream ML applications. By pre-aggregating metrics at the `customer_unique_id` grain, we reduce the training pipeline latency for churn and loyalty models.

## 6. Table Dictionary & Business Impact

### 6.1 Dimensional Tables (Context)
Dimensions provide descriptive attributes. Most use **SCD Type 2** to track history.

| Table Name | Logic / Purpose | Business Impact |
| :--- | :--- | :--- |
| **`dim_customers`** | Uses Geo-Medoid (most frequent GPS) to resolve spatial conflicts. | Enables **accurate geospatial analysis** regardless of customer moves. |
| **`dim_products`** | Joins English translations and uses standardized measurements (`Short`/`Int`). | Supports **international reporting** and reduces storage/shuffle footprint. |
| **`dim_sellers`** | Standardizes seller locations using Geo-Medoid logic. | Allows **logistics optimization** by calculating seller-to-customer distance. |
| **`dim_date`** | Dynamic calendar enriched with **Brazilian Holidays API** data. | Enables **Seasonality Analysis** and business-day logistics tracking. |

### 6.2 Fact Tables (Metrics)
Facts store quantitative measurements and link to dimensions via surrogate keys.

| Table Name | Grain / Logic | Business Impact |
| :--- | :--- | :--- |
| **`fact_sales`** | Order Item / High-precision Decimal types. | The source of truth for **Revenue Reporting** with 100% accuracy. |
| **`fact_payments`** | Order Payment / Captures sequential transaction IDs. | Enables **Financial Risk Analysis** and installment tracking. |
| **`fact_reviews`** | Review Event / Calculates response latency in seconds. | Monitors **Customer Satisfaction (CSAT)** and seller response efficiency. |

### 6.3 Feature Tables (Intelligence)
Aggregated datasets for Machine Learning and Executive dashboards.

| Table Name | Purpose | Application |
| :--- | :--- | :--- |
| **`feature_customer_rfm`**| Recency, Frequency, Monetary metrics per `customer_unique_id`. | **Customer Segmentation** (Identifying "Champions" vs "At-Risk"). |
| **`feature_product_performance`** | Sales volume and avg ratings per product. | **Inventory Strategy** (Expansion vs. delisting products). |
| **`feature_seller_reliability`** | Delivery delays and order counts per seller. | **Operational Governance** (Ranking and auditing seller performance). |

