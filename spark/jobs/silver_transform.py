from pyspark.sql.functions import col, try_to_timestamp, trim, split, lower, upper, regexp_replace, avg, first, expr
from config import get_spark_session

# --- Configuration ---
BRONZE_PATH = "s3a://bronze"
SILVER_PATH = "s3a://silver"

# Initialize Spark Session
spark = get_spark_session("Silver Transformation")

# --- Generic Helper Functions ---

def get_base_df(table_name):
    """Reads from Bronze, drops duplicates, and trims all string columns."""
    df = spark.read.parquet(f"{BRONZE_PATH}/{table_name}/").dropDuplicates()
    for column, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(column, trim(col(column)))
    return df

def standardize_timestamps(df, cols):
    """Batch converts columns to timestamp format."""
    for c in cols:
        df = df.withColumn(c, try_to_timestamp(col(c)))
    return df

def validate_required_columns(df, cols):
    """Filters out records where any of the specified columns are null."""
    for c in cols:
        df = df.filter(col(c).isNotNull())
    return df

# --- Table-Specific Transformations ---

def transform_customers(df):
    return validate_required_columns(df, ["customer_id", "customer_unique_id"])

def transform_geolocation(df):
    return df.groupBy("geolocation_zip_code_prefix").agg(
        avg("geolocation_lat").alias("geolocation_lat"),
        avg("geolocation_lng").alias("geolocation_lng"),
        first("geolocation_city").alias("geolocation_city"),
        first("geolocation_state").alias("geolocation_state")
    )

def transform_order_items(df):
    df = standardize_timestamps(df, ["shipping_limit_date"])
    return validate_required_columns(df, ["order_id", "product_id"])

def transform_order_payments(df):
    return validate_required_columns(df, ["order_id"])

def transform_order_reviews(df):
    df = standardize_timestamps(df, ["review_creation_date", "review_answer_timestamp"])
    df = df.fillna({"review_comment_title": "no_title", "review_comment_message": "no_message"})
    # Type casting: review_score to integer using try_cast to avoid crash on malformed input (ANSI mode)
    df = df.withColumn("review_score", expr("try_cast(review_score as integer)"))
    df = validate_required_columns(df, ["review_id", "order_id"])
    return df.filter((col("review_score") >= 1) & (col("review_score") <= 5))

def transform_orders(df):
    ts_cols = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", 
               "order_delivered_customer_date", "order_estimated_delivery_date"]
    df = standardize_timestamps(df, ts_cols)
    df = validate_required_columns(df, ["order_id", "customer_id"])
    
    # Quarantine logic for inconsistent dates
    df = df.filter(~((col("order_status") == "delivered") & col("order_delivered_customer_date").isNull())) \
           .filter(~((col("order_status").isin("shipped", "delivered")) & col("order_delivered_carrier_date").isNull()))
    return df

def transform_products(df):
    # Type casting
    numeric_ints = ["product_name_lenght", "product_description_lenght", "product_photos_qty"]
    numeric_floats = ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    
    for c in numeric_ints: df = df.withColumn(c, expr(f"try_cast({c} as integer)"))
    for c in numeric_floats: df = df.withColumn(c, expr(f"try_cast({c} as float)"))

    # Enrichment
    translation_df = spark.read.parquet(f"{BRONZE_PATH}/product_category_name_translation/").dropDuplicates()
    df = df.fillna({"product_category_name": "unknown"}) \
           .join(translation_df, on="product_category_name", how="left") \
           .fillna({"product_category_name_english": "unknown"}) \
           .fillna(0, subset=numeric_ints)
    
    # Filtering null physical dimensions to ensure data quality
    df = validate_required_columns(df, ["product_id"] + numeric_floats)
    
    return df

def transform_sellers(df):
    # City extraction logic
    df = df.withColumn("seller_city", split(col("seller_city"), r"[/\-,]")[0])
    df = df.withColumn("seller_city", trim(lower(col("seller_city"))))
    df = df.withColumn("seller_state", upper(col("seller_state")))
    
    # Error handling for corrupted data
    df = df.filter(~col("seller_city").rlike(r"\d"))
    return validate_required_columns(df, ["seller_id"])

# --- Orchestration ---

def explore_table(table_name):
    """
    Enhanced exploration with Schema Analysis, Statistics, and CDC tracking.
    """
    print(f"\n{'='*60}")
    print(f"📊 DATA EXPLORATION REPORT: {table_name}")
    print(f"{'='*60}")
    
    try:
        df_bronze = spark.read.parquet(f"{BRONZE_PATH}/{table_name}/")
        df_silver = spark.read.parquet(f"{SILVER_PATH}/{table_name}/")
        
        # 1. Schema Analysis
        print("\n[1] Schema Analysis (Bronze Layer)")
        print(f"Columns: {', '.join(df_bronze.columns)}")
        print("\n[1] Schema Analysis (Silver Layer)")
        print(f"Columns: {', '.join(df_silver.columns)}")
        
        # 2. CDC / Change Tracking Logic
        bronze_count = df_bronze.count()
        silver_count = df_silver.count()
        dropped = bronze_count - silver_count
        drop_pct = (dropped / bronze_count) * 100 if bronze_count > 0 else 0
        
        print("\n[2] Change Data Capture (CDC) Metrics")
        print(f"  • Bronze Records : {bronze_count:,}")
        print(f"  • Silver Records : {silver_count:,}")
        print(f"  • Records Dropped: {dropped:,} ({drop_pct:.2f}%)")
        print(f"    (Effect of deduplication, null-filtering, and quarantine)")

        # 3. Null Data Analysis
        from pyspark.sql.functions import count, when, isnull
        print("\n[3] Null Data Analysis (Missing Values)")
        
        # Calculate nulls for Bronze
        bronze_nulls = df_bronze.select([count(when(isnull(c), c)).alias(c) for c in df_bronze.columns]).collect()[0].asDict()
        # Calculate nulls for Silver
        silver_nulls = df_silver.select([count(when(isnull(c), c)).alias(c) for c in df_silver.columns]).collect()[0].asDict()
        
        print(f"{'Column':<35} | {'Bronze Nulls':<15} | {'Silver Nulls':<15}")
        print("-" * 70)
        for col_name in df_silver.columns:
            b_null = bronze_nulls.get(col_name, "N/A")
            s_null = silver_nulls.get(col_name, 0)
            print(f"{col_name:<35} | {str(b_null):<15} | {str(s_null):<15}")

        # 4. Statistical Summary
        print("\n[4] Statistical Summary (Bronze Layer)")
        df_bronze.describe().show()
        print("\n[4] Statistical Summary (Silver Layer)")
        df_silver.describe().show()

        # 5. Data Sample
        print("[5] Data Sample (Top 5 Rows)")
        df_silver.show(5, truncate=True)

    except Exception as e:
        print(f"❌ Exploration failed for {table_name}: {e}")
    print(f"{'='*60}\n")

def process_table(table_name):
    """Centralized read-transform-write flow."""
    print(f"Starting transformation for: {table_name}")
    try:
        df = get_base_df(table_name)
        
        # Dispatch to specific transformation
        transform_fn = globals().get(f"transform_{table_name}")
        if transform_fn:
            df = transform_fn(df)
        
        # Write to Silver
        df.coalesce(1).write.mode("overwrite").parquet(f"{SILVER_PATH}/{table_name}/")
        explore_table(table_name)
        
    except Exception as e:
        print(f"FAILED to process {table_name}: {e}")

if __name__ == "__main__":
    TABLES = ["customers", "geolocation", "order_items", "order_payments", 
              "order_reviews", "orders", "products", "sellers"]
    
    for table in TABLES:
        process_table(table)
    
    print("\nSilver transformation completed for all tables.")