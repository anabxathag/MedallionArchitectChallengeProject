# Phase 3: Silver Layer - Transformation & Tuning

The Silver layer is the "Internal API" of our Data Platform. It transforms raw Bronze data into a clean, consistent format that is optimized for joining and modeling.

## 1. Precision Data Typing (Senior Optimization)
Standard Spark ingestion often defaults to `Long` for integers and `Double` for decimals. We manually tuned the schema to be storage-efficient.

- **Integer Tuning**: Small counters use `ShortType` (2 bytes). IDs that don't exceed 2 billion use `IntegerType` (4 bytes).
- **Financial Accuracy**: Prices and freight values are cast to `Decimal(10,2)`. This prevents the "floating-point error" common in data science workflows.
- **Impact**: Reduced the shuffle size by ~40%, speeding up subsequent joins in the Gold layer.

## 2. De-duplication & CDC (Upsert)
Because we use incremental loading, the same entity might appear multiple times across different Bronze runs.

- **Windowed De-duplication**: We use `row_number()` partitioned by the primary key and ordered by `updated_at` DESC. This ensures we only take the latest "Truth."
- **Merge/Upsert Logic**: We update existing records in the Silver layer and insert new ones.
- **Why?**: This ensures that the Silver layer reflects the "Final State" of the data, which is essential for analytical consistency.

## 3. Cleansing & Enrichment
- **Null Handling**: Critical columns (IDs, Statuses) are filtered or filled with defaults.
- **Standardization**: Timestamps are cast to a consistent format, and string values are trimmed.
- **Final Validation**: A second DQ gate verifies that the Silver table structure matches the expected "Internal API" schema.
