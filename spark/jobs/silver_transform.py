from pyspark.sql.window import Window
from pyspark.sql.functions import col, trim, split, lower, upper, avg, first, expr, count, when, isnull, current_timestamp, max, row_number

from pyspark.sql.types import DecimalType, ShortType, IntegerType, TimestampType

from config import get_spark_session, get_run_id, JobTracker, get_last_success_timestamp, PRIMARY_KEYS, DataQuality, MAX_WORKERS
import sys
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
BRONZE_PATH = "s3a://bronze"
SILVER_PATH = "s3a://silver"


# Initialize Spark Session and Run ID
spark = get_spark_session("Silver Transformation")
run_id = get_run_id()

# --- PRE-INITIALIZE AUDIT ---
JobTracker.initialize_audit_table(spark)

# --- Generic Helper Functions ---

def clean_dataframe(df, pks=None):
    """
    Standard cleaning: 
    1. If pks provided, deduplicate by PK (keeping newest by _ingested_at).
    2. drops duplicates (exact row-wise).
    3. trims string columns.
    """
    if pks:
        df = df.withColumn("_rank", expr(f"row_number() OVER (PARTITION BY {', '.join(pks)} ORDER BY _ingested_at DESC)")) \
               .filter(col("_rank") == 1) \
               .drop("_rank")
               
    df = df.dropDuplicates()
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, trim(col(column)))
    return df

def validate_required_columns(df, cols):
    """Filters out records where any of the specified columns are null."""
    for c in cols:
        df = df.filter(col(c).isNotNull())
    return df

def upsert_to_silver(df_new, table_name, tracker=None):
    """
    Performs an idempotent upsert by combining new data with existing data 
    and deduplicating by Primary Key.
    """
    target_path = f"{SILVER_PATH}/{table_name}/"
    pks = PRIMARY_KEYS.get(table_name)
    
    log_fn = tracker.log if tracker else print

    if not pks:
        log_fn(f"SILVER: No Primary Key defined for {table_name}. Always Overwriting.")
        df_new.coalesce(1).write.mode("overwrite").parquet(target_path)
        return

    try:
        # Check if existing data exists
        df_existing = spark.read.parquet(target_path)
        
        # Combine existing and new, keep newest record per PK
        # We put df_new first so it is prioritized in ties (rank logic)
        df_final = df_new.unionByName(df_existing, allowMissingColumns=True) \
            .withColumn("_rank", expr(f"row_number() OVER (PARTITION BY {', '.join(pks)} ORDER BY _ingested_at DESC)")) \
            .filter(col("_rank") == 1) \
            .drop("_rank")
            
        df_final.coalesce(1).write.mode("overwrite").parquet(target_path)
        log_fn(f"SILVER: Successfully upserted incremental batch into {table_name}")
        
    except Exception as e:
        # Initial load
        log_fn(f"SILVER: Initial load for {table_name} (Reason: {e})")
        df_new.coalesce(1).write.mode("overwrite").parquet(target_path)

def explore_table(table_name, tracker=None):
    """
    Enhanced exploration with Schema Analysis, Statistics, and Data Sampling.
    Reports on the final state of the Silver table.
    """
    log_fn = tracker.log if tracker else print
    log_fn(f"\n{'='*60}")
    log_fn(f"📊 DATA EXPLORATION REPORT: {table_name}")
    log_fn(f"{'='*60}")
    
    try:
        df_bronze = spark.read.parquet(f"{BRONZE_PATH}/{table_name}/").cache()
        df_silver = spark.read.parquet(f"{SILVER_PATH}/{table_name}/").cache()
        
        # 1. Schema Analysis
        log_fn("\n[1] Schema Analysis (Bronze Layer)")
        log_fn(f"Columns: {', '.join(df_bronze.columns)}")
        log_fn("\n[1] Schema Analysis (Silver Layer)")
        log_fn(f"Columns: {', '.join(df_silver.columns)}")
        
        # 2. CDC / Change Tracking Logic
        bronze_count = df_bronze.count()
        silver_count = df_silver.count()
        dropped = bronze_count - silver_count
        drop_pct = (dropped / bronze_count) * 100 if bronze_count > 0 else 0
        
        log_fn("\n[2] Change Data Capture (CDC) Metrics")
        log_fn(f"  • Bronze Records : {bronze_count:,}")
        log_fn(f"  • Silver Records : {silver_count:,}")
        log_fn(f"  • Records Dropped: {dropped:,} ({drop_pct:.2f}%)")
        log_fn(f"    (Effect of deduplication, null-filtering, and quarantine)")

        # 3. Null Data Analysis
        log_fn("\n[3] Null Data Analysis (Missing Values)")
        
        # Calculate nulls for Bronze
        bronze_nulls = df_bronze.select([count(when(isnull(c), c)).alias(c) for c in df_bronze.columns]).collect()[0].asDict()
        # Calculate nulls for Silver
        silver_nulls = df_silver.select([count(when(isnull(c), c)).alias(c) for c in df_silver.columns]).collect()[0].asDict()
        
        log_fn(f"{'Column':<35} | {'Bronze Nulls':<15} | {'Silver Nulls':<15}")
        log_fn("-" * 70)
        for col_name in df_silver.columns:
            b_null = bronze_nulls.get(col_name, "N/A")
            s_null = silver_nulls.get(col_name, 0)
            log_fn(f"{col_name:<35} | {str(b_null):<15} | {str(s_null):<15}")

        # 4. Statistical Summary (Native Python collection to avoid JVM stdout bypass)
        log_fn("\n[4] Statistical Summary (Silver Layer)")
        try:
            stats_df = df_silver.describe()
            columns = stats_df.columns
            rows = stats_df.collect()
            
            # Formatted table header
            log_fn(f"{'Summary':<15} | " + " | ".join([f"{c:<20}" for c in columns[1:]]))
            log_fn("-" * (15 + 3 + len(columns[1:]) * 23))
            
            for row in rows:
                row_dict = row.asDict()
                log_fn(f"{row_dict['summary']:<15} | " + " | ".join([f"{str(row_dict[c]):<20}" for c in columns[1:]]))
        except Exception as se:
            log_fn(f"⚠️ Stats collection failed: {se}")

        # 5. Data Sample
        log_fn("\n[5] Data Sample (Top 5 Records)")
        samples = df_silver.limit(5).collect()
        if samples:
            cols = df_silver.columns
            log_fn(" | ".join([f"{c:<20}" for c in cols]))
            log_fn("-" * (len(cols) * 23))
            for row in samples:
                log_fn(" | ".join([f"{str(row[c])[:20]:<20}" for c in cols]))
        else:
            log_fn("No data found to sample.")

    except Exception as e:
        log_fn(f"❌ Exploration failed for {table_name}: {e}")
    finally:
        if 'df_silver' in locals():
            df_silver.unpersist()
        if 'df_bronze' in locals():
            df_bronze.unpersist()
    log_fn(f"{'='*60}\n")

# --- Table-Specific Transformations ---

def transform_customers(df):
    df = df.withColumn("customer_id", col("customer_id").cast("string")) \
           .withColumn("customer_unique_id", col("customer_unique_id").cast("string")) \
           .withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("string")) \
           .withColumn("customer_city", col("customer_city").cast("string")) \
           .withColumn("customer_state", col("customer_state").cast("string"))
    return validate_required_columns(df, ["customer_id", "customer_unique_id"])

def transform_geolocation(df):
    # Aggregation effectively deduplicates the multiple lat/lng readings per zip code
    return df.groupBy("geolocation_zip_code_prefix").agg(
        avg(col("geolocation_lat")).cast(DecimalType(11, 8)).alias("geolocation_lat"),
        avg(col("geolocation_lng")).cast(DecimalType(11, 8)).alias("geolocation_lng"),
        first(col("geolocation_city")).cast("string").alias("geolocation_city"),
        first(col("geolocation_state")).cast("string").alias("geolocation_state"),
        max(col("source_updated_at")).alias("source_updated_at"),
        first(col("_ingested_at")).alias("_ingested_at")
    ).withColumn("geolocation_zip_code_prefix", col("geolocation_zip_code_prefix").cast("string"))

def transform_order_items(df):
    df = df.withColumn("price", col("price").cast(DecimalType(10, 2))) \
           .withColumn("freight_value", col("freight_value").cast(DecimalType(10, 2)))
    return validate_required_columns(df, ["order_id", "order_item_id", "product_id", "seller_id"])

def transform_order_payments(df):
    df = df.withColumn("payment_value", col("payment_value").cast(DecimalType(10, 2))) \
           .withColumn("payment_sequential", col("payment_sequential").cast(ShortType())) \
           .withColumn("payment_installments", col("payment_installments").cast(ShortType()))
    return validate_required_columns(df, ["order_id", "payment_sequential"])

def transform_order_reviews(df):
    """
    Fixes for reviews:
    1. Deduplicate by review_id (ensure one review event grain).
    2. Filter out null creation dates.
    3. Precise types for scores.
    """
    # Deduplicate by review_id, keeping latest answer
    df = df.withColumn("_rn", row_number().over(
        Window.partitionBy("review_id").orderBy(col("review_answer_timestamp").desc())
    )).filter(col("_rn") == 1).drop("_rn")

    # Filter invalid dates
    df = df.filter(col("review_creation_date").isNotNull() & col("review_answer_timestamp").isNotNull())
    
    # Precise types
    df = df.withColumn("review_score", col("review_score").cast(ShortType()))
    
    df = df.fillna({"review_comment_title": "no_title", "review_comment_message": "no_message"})
    return validate_required_columns(df, ["review_id", "order_id"])

def transform_orders(df):
    df = df.withColumn("order_id", col("order_id").cast("string")) \
           .withColumn("customer_id", col("customer_id").cast("string")) \
           .withColumn("order_status", col("order_status").cast("string")) \
           .withColumn("order_purchase_timestamp", col("order_purchase_timestamp").cast(TimestampType())) \
           .withColumn("order_approved_at", col("order_approved_at").cast(TimestampType())) \
           .withColumn("order_delivered_carrier_date", col("order_delivered_carrier_date").cast(TimestampType())) \
           .withColumn("order_delivered_customer_date", col("order_delivered_customer_date").cast(TimestampType())) \
           .withColumn("order_estimated_delivery_date", col("order_estimated_delivery_date").cast(TimestampType()))
    
    df = validate_required_columns(df, ["order_id", "customer_id"])
    
    # Quarantine logic for inconsistent dates
    df = df.filter(~((col("order_status") == "delivered") & col("order_delivered_customer_date").isNull())) \
           .filter(~((col("order_status").isin("shipped", "delivered")) & col("order_delivered_carrier_date").isNull()))
    return df

def transform_products(df):
    # Reference translations
    translation_df = spark.read.parquet(f"{BRONZE_PATH}/product_category_name_translation/") \
        .select("product_category_name", "product_category_name_english").dropDuplicates()
        
    df = df.fillna({"product_category_name": "unknown"}) \
        .join(translation_df, on="product_category_name", how="left") \
        .fillna({"product_category_name_english": "unknown"}) \
        .fillna({"product_name_lenght": 0, "product_description_lenght": 0, "product_photos_qty": 0})

    # Precision types for product metadata
    df = df.withColumn("product_id", col("product_id").cast("string")) \
           .withColumn("product_category_name", col("product_category_name").cast("string")) \
           .withColumn("product_category_name_english", col("product_category_name_english").cast("string")) \
           .withColumn("product_weight_g", col("product_weight_g").cast(IntegerType())) \
           .withColumn("product_length_cm", col("product_length_cm").cast(ShortType())) \
           .withColumn("product_height_cm", col("product_height_cm").cast(ShortType())) \
           .withColumn("product_width_cm", col("product_width_cm").cast(ShortType())) \
           .withColumn("product_name_lenght", col("product_name_lenght").cast(ShortType())) \
           .withColumn("product_description_lenght", col("product_description_lenght").cast(ShortType())) \
           .withColumn("product_photos_qty", col("product_photos_qty").cast(ShortType()))

    numeric_floats = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]  
    df = validate_required_columns(df, ["product_id"] + numeric_floats) # numeric_floats use for filter product that null
    return df

def transform_sellers(df):
    df = df.withColumn("seller_id", col("seller_id").cast("string")) \
           .withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix").cast("string")) \
           .withColumn("seller_city", split(col("seller_city"), r"[/\-,]")[0])
    df = df.withColumn("seller_city", trim(lower(col("seller_city"))).cast("string"))
    df = df.withColumn("seller_state", upper(col("seller_state")).cast("string"))
    df = df.filter(~col("seller_city").rlike(r"\d"))
    return validate_required_columns(df, ["seller_id"])

# --- Orchestration widef process_table(table_name):
def process_table(table_name):
    tracker = JobTracker(spark, "silver_transformation", run_id)
    tracker.start_job(table_name)
    try:
        # 1. Fetch Watermark
        watermark = get_last_success_timestamp(spark, "silver_transformation", table_name)
        
        # 2. Read INCREMENTAL slice from Bronze (Now benefiting from native types)
        df_bronze = spark.read.parquet(f"{BRONZE_PATH}/{table_name}/") \
            .filter(col("_ingested_at") > watermark).cache()
            
        input_count = df_bronze.count()
        
        if input_count == 0:
            tracker.log(f"INCREMENTAL: No new data for {table_name} since {watermark}")
            
            # --- Always run report to show current state ---
            explore_table(table_name, tracker=tracker)
            
            tracker.end_job("SUCCESS", input_rows=0, output_rows=0)
            return True

        # 3. Perform transformations on the new slice
        pks = PRIMARY_KEYS.get(table_name, [])
        df = clean_dataframe(df_bronze, pks)
        
        transform_fn = globals().get(f"transform_{table_name}")
        if transform_fn:
            df = transform_fn(df)
        
        # 4. Data Quality Checks
        dq_results = {
            "null_pks": DataQuality.check_nulls(df, pks) if pks else "N/A",
            "is_unique": DataQuality.check_uniques(df, pks) if pks else "N/A"
        }
        
        tracker.log(f"SILVER DQ [{table_name}]: {dq_results}")
        
        # 5. Stop if PK uniqueness is violated in the silver batch
        if dq_results["is_unique"] == False:
            raise ValueError(f"DQ FAILURE: Primary Key uniqueness violated for {table_name}")

        # 6. Upsert to Silver
        df = df.withColumn("_silver_processed_at", current_timestamp())
        upsert_to_silver(df, table_name, tracker=tracker)
        output_count = df.count()
        
        # 7. Run exploration report BEFORE ending job
        explore_table(table_name, tracker=tracker)
        
        # 8. Optimization: Clear cache for this table to free executor memory
        df_bronze.unpersist()
        
        tracker.end_job("SUCCESS", input_rows=input_count, output_rows=output_count, dq_metrics=dq_results)
        return True
        
    except Exception as e:
        tracker.end_job("FAILED", error_msg=str(e))
        tracker.log(f"FAILED to process {table_name}: {e}")
        return False

if __name__ == "__main__":
    TABLES = ["customers", "geolocation", "order_items", "order_payments", 
              "order_reviews", "orders", "products", "sellers"]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_table, TABLES))
    
    success_count = sum(1 for r in results if r)
    if success_count < len(TABLES):
        print(f"ERROR: Only {success_count}/{len(TABLES)} tables processed. Check logs.")
        sys.exit(1)
        
    print(f"\nSilver incremental transformation completed for {success_count} tables.")