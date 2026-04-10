# Gold Layer Modeling Logic: Star Schema & Feature Engineering

This document provides a comprehensive technical walkthrough of the Gold Layer architecture, which transforms Silver-tier data into a production-ready Star Schema and Analytical Feature sets.

---

## 1. Core Utilities & Infrastructure

The script utilizes several high-level utility functions to ensure idempotency, performance, and data quality.

### [A] Diagnostic Reporting: `explore_table`
- **Purpose**: Generates a professional data health report after every table update.
- **Metrics**: 
    - **Transition Metrics**: Tracks record counts and delta percentages from Source to Gold.
    - **Null Analysis**: Identifies missing values specifically in the Gold tier.
    - **Stats & Samples**: Provides Mean, Min, Max, and a "Top 5 Records" preview for visual validation.

### [B] Idempotent Writing: `write_gold`
- **Mechanism**: Handles simple upserts. If a Primary Key exists, it performs a `unionByName` and `dropDuplicates`. 
- **Critical Guard**: Caches existing data before overwriting to prevent `FILE_NOT_FOUND` errors during Spark's lazy evaluation.

### [C] History Tracking: `write_gold_scd2`
- **Pattern**: **Slowly Changing Dimension Type 2**.
- **Logic**:
    1.  Uses hashing (`F.hash`) on attribute columns to detect changes between active Gold records and incoming Silver records.
    2.  **Expiry**: If an existing record hash != new record hash, the old record is marked as `is_current = False` and `end_date = current_timestamp`.
    3.  **Versioning**: A new version is inserted with `is_current = True` and `start_date = current_timestamp`.
    4.  Preserves historical context for sellers, customers, and products.

### [D] Stable Identity: `add_surrogate_key`
- **Mechanism**: Generates an SHA-256 hash (e.g., `sales_sk`) from natural keys (e.g., `order_id` + `order_item_id`).

---

## 2. Advanced Observability & DQ Controls

Unlike basic pipelines, this Gold layer integrates "Senior-level" monitoring directly into the Spark jobs.

### [A] Centralized Audit Logging (`JobTracker`)
Every execution logs its metadata to the `audit.run_logs` table in PostgreSQL:
*   **Run Context**: `run_id` (from Airflow), `job_name`, and `table_name`.
*   **Performance Metrics**: `start_time`, `end_time`, `input_rows`, and `output_rows`.
*   **Health Status**: `status` (SUCCESS/FAILED) and `error_msg` for debugging.

### [B] Row-Count Drift Detection
The `DataQuality.check_row_count_drift` function compares the current row count against the last successful run in the audit logs.
*   **Alerting**: If the data volume deviates by more than 20% (configurable), a drift alert is logged.
*   **Why**: Helps detect upstream source failures or malformed incremental batches before they impact BI dashboards.

---

## 3. Dimension Builders (The "Who", "What", "Where")

### `build_dim_customers` / `build_dim_sellers`
- **Spatial Resolution**: Integrates `geolocation` data using a **Mode Logic**. Since a zip code can have multiple GPS coordinates, the script picks the most frequent coordinate (Medoid) for accuracy.
- **History**: Implements SCD Type 2 to track movements (e.g., a customer moving to a new city).

### `build_dim_products`
- **Translation**: Joins with the English category translation table.
- **Normalization**: Standardizes measurements into `Integer` and `Short` types for storage efficiency.

### `build_dim_date` (Universal Calendar)
- **Dynamic Range**: Calculates the date boundaries across all business events.
- **API Enrichment**: Merges with **Brazilian National Holidays** fetched via external REST API, allowing for better "Seasonality" analysis.

---

## 4. Fact Builders & Feature Sets

### `build_fact_sales` (Grain: Order Item)
- **Primary Fact**: The core revenue table with surrogate keys.
- **Precision**: Uses `Decimal(10,2)` for financial fields to ensure 100% currency accuracy.

### `feature_customer_rfm`
- **Recency, Frequency, Monetary**: Standardized features calculated at the `customer_unique_id` grain.
- **Application**: Direct input for marketing segmentation and churn prediction models.

---

## 5. Industrial Performance & Hybrid Cloud

- **Parallel Stages**: Uses three distinct stages (Dimensions -> Facts -> Features) executed via `ThreadPoolExecutor` to minimize total runtime.
- **Precise Typing**: Explicitly uses `Short`, `Integer`, and `Decimal` to optimize Parquet storage and memory footprint.
- **Hybrid Cloud Integration**: After modeling is complete, the `export_gold_to_bigquery.py` script synchronizes the final tables to **Google BigQuery** for cloud-scale analytics and BI integration.

---
*Technical Specification for the Medallion Data Platform.*
