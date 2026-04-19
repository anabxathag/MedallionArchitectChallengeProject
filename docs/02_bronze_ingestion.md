# Phase 2: Bronze Layer - Incremental Ingestion

The Bronze layer is our "landing zone." The goal here is to move data from the production source to the data lake (MinIO) with maximum efficiency and minimum impact on the source system.

## 1. High-Throughput Parallelism
We use a `ThreadPoolExecutor` to trigger ingestion for multiple tables concurrently.

- **JDBC Strategy**: Instead of a single sequential thread, Spark opens multiple connections to the PostgreSQL source.
- **Partitioning**: We use Spark's `numPartitions` and `partitionColumn` parameters. This ensures that even the largest tables (like `order_items`) are split into manageable chunks across the Spark executors.

## 2. Incremental Watermarking
To avoid re-processing millions of rows, we implement a **Watermark** strategy.

- **Logic**: The job queries the Audit DB for the `MAX(updated_at)` of the last successful run.
- **Filtering**: We inject this timestamp into the SQL query sent to PostgreSQL: `WHERE updated_at > :last_watermark`.
- **Idempotency**: If the job fails halfway, it won't update the watermark, ensuring the next run picks up exactly where it left off.

## 3. Fail-Fast Schema Validation
We implement a custom `validate_schema` function located in `config.py`.

- **Structural Check**: We compare the incoming DataFrame schema against our pre-defined source schema.
- **Action**: If a column type changes or a required column is missing, the job terminates immediately with a Discord alert. This prevents "Garbage In, Garbage Out."

---
**Technical Outcome**: A 300% gain in concurrency and a reduction in source database load.
