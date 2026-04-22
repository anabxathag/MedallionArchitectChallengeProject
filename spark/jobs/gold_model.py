from pyspark.sql import functions as F, Window
from config import get_spark_session, get_run_id, JobTracker, get_last_success_timestamp, GOLD_PRIMARY_KEYS, GOLD_TABLE_SCHEMAS, DataQuality, MAX_WORKERS, BRAZILIAN_HOLIDAYS

from pyspark.sql.types import DecimalType, ShortType, IntegerType, TimestampType, LongType


from concurrent.futures import ThreadPoolExecutor

SILVER_PATH = "s3a://silver"
GOLD_PATH = "s3a://gold"

# Initialize Spark Session and Run ID
spark = get_spark_session("Gold Modeling")
run_id = get_run_id()

# --- PRE-INITIALIZE AUDIT ---
JobTracker.initialize_audit_table(spark)

def explore_table(table_name, source_df=None, tracker=None):
    """
    Enhanced exploration for Gold tables. 
    Matches Silver layer diagnostic depth and adds data sampling.
    """
    log_fn = tracker.log if tracker else print
    log_fn(f"\n{'='*60}")
    log_fn(f"📊 GOLD DATA EXPLORATION REPORT: {table_name}")
    log_fn(f"{'='*60}")
    
    try:
        df_gold = spark.read.parquet(f"{GOLD_PATH}/{table_name}/").cache()
        
        # 1. Schema Analysis
        log_fn(f"\n[1] Schema Analysis")
        if source_df:
            log_fn(f"Source Columns: {', '.join(source_df.columns)}")
        log_fn(f"Gold Columns  : {', '.join(df_gold.columns)}")

        # 2. Volume Metrics
        gold_count = df_gold.count()
        if source_df:
            source_count = source_df.count()
            diff = source_count - gold_count
            diff_pct = (diff / source_count) * 100 if source_count > 0 else 0
            log_fn("\n[2] Transition Metrics (Source -> Gold)")
            log_fn(f"  • Source Records : {source_count:,}")
            log_fn(f"  • Gold Records   : {gold_count:,}")
            log_fn(f"  • Record Diff    : {diff:,} ({diff_pct:.2f}%)")
        else:
            log_fn(f"\n[2] Record Count (Gold): {gold_count:,}")

        # 3. Null Data Analysis
        log_fn("\n[3] Null Data Analysis (Missing Values in Gold)")
        gold_nulls = df_gold.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_gold.columns]).collect()[0].asDict()
        
        log_fn(f"{'Column':<35} | {'Gold Nulls':<15}")
        log_fn("-" * 55)
        for col_name, n_count in gold_nulls.items():
            log_fn(f"{col_name:<35} | {str(n_count):<15}")

        # 4. Statistical Summary
        log_fn("\n[4] Statistical Summary (Gold Layer)")
        try:
            stats_df = df_gold.describe()
            columns = stats_df.columns
            rows = stats_df.collect()
            log_fn(f"{'Summary':<15} | " + " | ".join([f"{c:<20}" for c in columns[1:]]))
            log_fn("-" * (15 + 3 + len(columns[1:]) * 23))
            for row in rows:
                row_dict = row.asDict()
                log_fn(f"{row_dict['summary']:<15} | " + " | ".join([f"{str(row_dict[c]):<20}" for c in columns[1:]]))
        except Exception as se:
            log_fn(f"⚠️ Stats collection failed: {se}")

        # 5. Data Sample (New)
        log_fn("\n[5] Data Sample (Top 5 Records)")
        samples = df_gold.limit(5).collect()
        if samples:
            cols = df_gold.columns
            log_fn(" | ".join([f"{c:<20}" for c in cols]))
            log_fn("-" * (len(cols) * 23))
            for row in samples:
                log_fn(" | ".join([f"{str(row[c])[:20]:<20}" for c in cols]))
        else:
            log_fn("No data found to sample.")

    except Exception as e:
        log_fn(f"❌ Exploration failed for {table_name}: {e}")
    finally:
        if 'df_gold' in locals():
            df_gold.unpersist()
    log_fn(f"{'='*60}\n")

def write_gold(df, table_name, source_df=None):
    """Writes to Gold layer with incremental audit tracking and idempotent upserts."""
    tracker = JobTracker(spark, "gold_modeling", run_id)
    tracker.start_job(table_name)
    try:
        input_count = source_df.count() if source_df else 0
        target_path = f"{GOLD_PATH}/{table_name}/"
        pks = GOLD_PRIMARY_KEYS.get(table_name, [])
        
        # --- Data Quality Checks ---
        expected_gold_schema = GOLD_TABLE_SCHEMAS.get(table_name)
        if not DataQuality.check_schema_mismatch(df, expected_gold_schema):
            raise ValueError(f"SCHEMA MISMATCH: Gold output for '{table_name}' does not match expected schema.")
            
        drift_val, is_drift_alert = DataQuality.check_row_count_drift(df, table_name, spark)
        
        dq_results = {
            "null_pks": DataQuality.check_nulls(df, pks) if pks else "N/A",
            "is_unique": DataQuality.check_uniques(df, pks) if pks else "N/A",
            "drift_pct": drift_val,
            "drift_alert": is_drift_alert
        }
        tracker.log(f"GOLD DQ [{table_name}]: {dq_results}")

        if not pks:
            tracker.log(f"GOLD: Overwriting {table_name} (no Primary Key)")
            df.coalesce(1).write.mode("overwrite").parquet(target_path)
        else:
            try:
                # Attempt incremental upsert
                df_existing = spark.read.parquet(target_path)
                # CRITICAL: Cache existing to avoid FILE_NOT_EXIST during overwrite
                df_existing.cache().count() 
                
                # Combine existing and new, but make NEW data take precedence 
                # (union order matters because dropDuplicates keeps the first encounter)
                df_final = df.unionByName(df_existing, allowMissingColumns=True).dropDuplicates(pks)
                df_final.coalesce(1).write.mode("overwrite").parquet(target_path)
                tracker.log(f"GOLD: Successfully upserted into {table_name} (New data prioritized)")
                df_existing.unpersist()
            except Exception as e:
                # Fallback to initial write
                tracker.log(f"GOLD: Initial write for {table_name} (Reason: {e})")
                df.coalesce(1).write.mode("overwrite").parquet(target_path)
        
        output_count = df.count()
        # 10. Run exploration report BEFORE ending job
        explore_table(table_name, source_df, tracker=tracker)
        
        tracker.end_job("SUCCESS", input_rows=input_count, output_rows=output_count, dq_metrics=dq_results)
    except Exception as e:
        tracker.end_job("FAILED", error_msg=str(e))
        tracker.log(f"FAILED to save {table_name}: {e}")
    finally:
        pass

def add_surrogate_key(df, natural_keys, sk_name):
    """Generates a deterministic SHA-256 surrogate key from natural key columns."""
    concat_col = F.concat_ws("||", *[F.col(k) for k in natural_keys])
    return df.withColumn(sk_name, F.sha2(concat_col, 256))

def get_mode_geolocation(df_geo):
    """Returns the most frequent lat/lng coordinate per zip code prefix (Medoid/Mode)."""
    window_spec = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.col("count").desc(), F.col("geolocation_lat"), F.col("geolocation_lng"))
    return df_geo.groupBy("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng") \
        .count() \
        .withColumn("rn", F.row_number().over(window_spec)) \
        .filter(F.col("rn") == 1) \
        .drop("rn", "count")

def calculate_scd2(df_existing, df_new, pks, attr_cols):
    """Pure logic for SCD Type 2. Returns the final unioned dataframe."""
    import pyspark.sql.functions as F
    current_ts = F.current_timestamp()
    
    # Identify records
    df_active = df_existing.filter(F.col("is_current") == True)
    
    # Hash attributes to detect changes
    df_new_hashed = df_new.withColumn("_new_hash", F.hash(*attr_cols)).alias("new")
    df_active_hashed = df_active.withColumn("_old_hash", F.hash(*attr_cols)).alias("old")
    
    join_df = df_active_hashed.join(df_new_hashed, on=pks, how="full_outer")
    
    # New Records
    new_records = join_df.filter(F.col("_old_hash").isNull()) \
        .select(*(pks + [F.col(f"new.{c}").alias(c) for c in attr_cols])) \
        .withColumn("start_date", current_ts) \
        .withColumn("end_date", F.lit(None).cast("timestamp")) \
        .withColumn("is_current", F.lit(True))
        
    # Records to Expire
    records_to_expire = join_df.filter(F.col("_old_hash").isNotNull() & F.col("_new_hash").isNotNull() & (F.col("_old_hash") != F.col("_new_hash"))) \
        .select(*(pks + [F.col(f"old.{c}").alias(c) for c in df_existing.columns if c not in pks + ["end_date", "is_current"]])) \
        .withColumn("end_date", current_ts) \
        .withColumn("is_current", F.lit(False))
        
    # New Versions
    new_versions = join_df.filter(F.col("_old_hash").isNotNull() & F.col("_new_hash").isNotNull() & (F.col("_old_hash") != F.col("_new_hash"))) \
        .select(*(pks + [F.col(f"new.{c}").alias(c) for c in attr_cols])) \
        .withColumn("start_date", current_ts) \
        .withColumn("end_date", F.lit(None).cast("timestamp")) \
        .withColumn("is_current", F.lit(True))
        
    # Unchanged Active
    unchanged_active = join_df.filter(F.col("_old_hash").isNotNull() & F.col("_new_hash").isNotNull() & (F.col("_old_hash") == F.col("_new_hash"))) \
        .select(*(pks + [F.col(f"old.{c}").alias(c) for c in df_existing.columns if c not in pks]))
        
    inactive_history = df_existing.filter(F.col("is_current") == False)
    
    df_final = inactive_history.unionByName(unchanged_active) \
        .unionByName(records_to_expire) \
        .unionByName(new_versions) \
        .unionByName(new_records)
    
    return df_final

def write_gold_scd2(df_new, table_name, pks, attr_cols):
    """SCD Type 2 implementation using Spark Join-and-Union."""
    tracker = JobTracker(spark, "gold_modeling", run_id)
    tracker.start_job(f"{table_name}_scd2")
    target_path = f"{GOLD_PATH}/{table_name}/"
    current_ts = F.current_timestamp()
    
    try:
        # 1. Load Existing Gold Dimension
        try:
            df_existing = spark.read.parquet(target_path)
            # CRITICAL: Cache existing to avoid FILE_NOT_EXIST during lazy evaluation of joins + overwrite
            df_existing.cache().count()
        except:
            raise ValueError(f"No existing data for {table_name}")

        # 2. Calculate Final State
        df_final = calculate_scd2(df_existing, df_new, pks, attr_cols)
        
        # 3. Data Quality: Schema Validation
        expected_gold_schema = GOLD_TABLE_SCHEMAS.get(table_name)
        if not DataQuality.check_schema_mismatch(df_final, expected_gold_schema):
            raise ValueError(f"SCHEMA MISMATCH: Gold output for '{table_name}' does not match expected schema.")
            
        df_final.coalesce(1).write.mode("overwrite").parquet(target_path)
        df_existing.unpersist()
        
        explore_table(table_name, df_new, tracker=tracker)
        tracker.end_job("SUCCESS", input_rows=df_new.count(), output_rows=df_final.count())
        
    except Exception as e:
        tracker.log(f"GOLD: Initial SCD2 load for {table_name} (Reason: {e})")
        df_final = df_new.withColumn("start_date", current_ts) \
            .withColumn("end_date", F.lit(None).cast("timestamp")) \
            .withColumn("is_current", F.lit(True))
            
        # Data Quality: Initial Schema Validation
        expected_gold_schema = GOLD_TABLE_SCHEMAS.get(table_name)
        if not DataQuality.check_schema_mismatch(df_final, expected_gold_schema):
             raise ValueError(f"SCHEMA MISMATCH: Gold initial output for '{table_name}' does not match expected schema.")
             
        df_final.coalesce(1).write.mode("overwrite").parquet(target_path)
        explore_table(table_name, df_new, tracker=tracker)
        tracker.end_job("SUCCESS", input_rows=df_new.count(), output_rows=df_final.count())

    finally:
        pass

# --- Table Builders ---

def build_dim_customers():
    """Builds dim_customers at customer_unique_id grain with SCD2 and Geo-Mode."""
    df_geo_mode = get_mode_geolocation(df_geolocation)
    
    # 1. Deduplicate customers to unique_id grain for the dimension
    # (Keeping one record per unique_id, typically the latest)
    df_customers_unique = df_customers.withColumn("rn", F.row_number().over(Window.partitionBy("customer_unique_id").orderBy(F.col("source_updated_at").desc()))) \
        .filter(F.col("rn") == 1).drop("rn")

    dim_customers_new = df_customers_unique.join(
        df_geo_mode,
        df_customers_unique.customer_zip_code_prefix == df_geo_mode.geolocation_zip_code_prefix,
        "left"
    ).select(
        "customer_unique_id", "customer_zip_code_prefix", 
        "customer_city", "customer_state", 
        F.col("geolocation_lat").alias("customer_lat"), 
        F.col("geolocation_lng").alias("customer_lng")
    )
    
    # Using customer_unique_id as business PK for SCD2
    write_gold_scd2(dim_customers_new, "dim_customers", 
                    pks=["customer_unique_id"], 
                    attr_cols=dim_customers_new.columns)

def build_dim_products():
    """Builds dim_products with SCD2."""
    dim_products = df_products.select(
        "product_id", "product_category_name", "product_category_name_english",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    )
    write_gold_scd2(dim_products, "dim_products", pks=["product_id"], attr_cols=dim_products.columns)

def build_dim_sellers():
    """Builds dim_sellers with SCD2 and Geo-Mode."""
    df_geo_mode = get_mode_geolocation(df_geolocation)
    
    dim_sellers = df_sellers.join(
        df_geo_mode,
        df_sellers.seller_zip_code_prefix == df_geo_mode.geolocation_zip_code_prefix,
        "left"
    ).select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state",
        F.col("geolocation_lat").alias("seller_lat"), 
        F.col("geolocation_lng").alias("seller_lng")
    )
    write_gold_scd2(dim_sellers, "dim_sellers", pks=["seller_id"], attr_cols=dim_sellers.columns)


def build_dim_date():
    """Builds a universal calendar dimension enriched with Brazilian holidays."""
    date_cols = [
        (df_orders, "order_purchase_timestamp"),
        (df_orders, "order_delivered_customer_date"),
        (df_order_items, "shipping_limit_date"),
        (df_order_reviews, "review_creation_date")
    ]
    
    # Extract distinct dates and find global boundaries
    dates_union = None
    for df, col_name in date_cols:
        temp_df = df.filter(F.col(col_name).isNotNull()).select(F.to_date(F.col(col_name)).alias("dt")).distinct()
        dates_union = temp_df if dates_union is None else dates_union.union(temp_df)
    
    range_df = dates_union.select(F.min("dt").alias("min_dt"), F.max("dt").alias("max_dt")).collect()[0]
    min_date_str = str(range_df["min_dt"])
    max_date_str = str(range_df["max_dt"])
    
    # Holiday reference Dataframe
    holidays_df = spark.createDataFrame([(d,) for d in BRAZILIAN_HOLIDAYS], ["holiday_date"])
    holidays_df = holidays_df.withColumn("holiday_date", F.to_date("holiday_date"))
    
    # Generate continuous daily sequence
    dim_date = spark.sql(f"SELECT explode(sequence(to_date('{min_date_str}'), to_date('{max_date_str}'), interval 1 day)) as date_key") \
        .withColumn("year", F.year("date_key")) \
        .withColumn("month", F.month("date_key")) \
        .withColumn("day", F.dayofmonth("date_key")) \
        .withColumn("quarter", F.quarter("date_key")) \
        .withColumn("day_of_week", F.dayofweek("date_key")) \
        .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False)) \
        .withColumn("month_name", F.date_format("date_key", "MMMM")) \
        .join(holidays_df, F.col("date_key") == holidays_df.holiday_date, "left") \
        .withColumn("is_br_holiday", F.when(F.col("holiday_date").isNotNull(), True).otherwise(False)) \
        .drop("holiday_date")
    
    write_gold(dim_date, "dim_date", None)


def build_fact_sales(dim_cust, dim_prod, dim_sell, dim_date):
    """Grain: Order Item. Primary revenue fact table with SCD2 Point-in-Time lookups."""
    # 1. Join Items with Orders (Source of truth for purchase time)
    fact_sales = df_order_items.alias("items") \
        .join(df_orders.select("order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_delivered_customer_date").alias("orders"), "order_id", "left") \
        .join(df_customers.select("customer_id", "customer_unique_id").alias("cust_bridge"), "customer_id", "left")
    
    # 2. Perform SCD2 Range Joins (Point-In-Time Lookups)
    # We join with the version of the dimension that was active at purchase time
    
    # Customer Lookup
    fact_sales = fact_sales.join(
        dim_cust.alias("dc"),
        (F.col("cust_bridge.customer_unique_id") == F.col("dc.customer_unique_id")) & 
        (F.col("orders.order_purchase_timestamp") >= F.col("dc.start_date")) & 
        (F.col("orders.order_purchase_timestamp") < F.coalesce(F.col("dc.end_date"), F.lit("9999-12-31").cast("timestamp"))),
        "left"
    )
    
    # Product Lookup
    fact_sales = fact_sales.join(
        dim_prod.alias("dp"),
        (F.col("items.product_id") == F.col("dp.product_id")) & 
        (F.col("orders.order_purchase_timestamp") >= F.col("dp.start_date")) & 
        (F.col("orders.order_purchase_timestamp") < F.coalesce(F.col("dp.end_date"), F.lit("9999-12-31").cast("timestamp"))),
        "left"
    )

    # Seller Lookup
    fact_sales = fact_sales.join(
        dim_sell.alias("ds"),
        (F.col("items.seller_id") == F.col("ds.seller_id")) & 
        (F.col("orders.order_purchase_timestamp") >= F.col("ds.start_date")) & 
        (F.col("orders.order_purchase_timestamp") < F.coalesce(F.col("ds.end_date"), F.lit("9999-12-31").cast("timestamp"))),
        "left"
    )

    # Date Lookup (Classic Star Schema DateKey)
    fact_sales = fact_sales.withColumn("order_date_only", F.to_date("order_purchase_timestamp")) \
        .join(dim_date.alias("dd"), F.col("order_date_only") == F.col("dd.date_key"), "left")

    # 3. Final Selection & Formatting
    fact_sales = fact_sales.select(
        "order_id", "order_item_id", "customer_id", "dc.customer_unique_id", "dp.product_id", "ds.seller_id",
        F.col("dd.date_key").alias("order_date_key"), # Historical/Star Schema reference
        "order_status", "order_purchase_timestamp", "order_delivered_customer_date",
        F.col("price").cast(DecimalType(10, 2)).alias("item_price"),
        F.col("freight_value").cast(DecimalType(10, 2)).alias("item_freight"),
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp").cast(ShortType()).alias("delivery_delay_days")
    )
    
    # Add surrogate key for the grain
    fact_sales = add_surrogate_key(fact_sales, ["order_id", "order_item_id"], "sales_sk")
    
    write_gold(fact_sales, "fact_sales", df_order_items)
    return fact_sales

def build_fact_payments(dim_date):
    """Grain: Order Payment. Financial analysis fact table with Date lookup."""
    fact_payments = df_order_payments.join(df_orders.select("order_id", "order_purchase_timestamp"), "order_id", "left") \
        .withColumn("order_date_only", F.to_date("order_purchase_timestamp")) \
        .join(dim_date.alias("dd"), F.col("order_date_only") == F.col("dd.date_key"), "left") \
        .select(
            "order_id", "payment_sequential", "payment_type", 
            "payment_installments", "payment_value",
            F.col("dd.date_key").alias("order_date_key")
        )
    
    # Add surrogate key
    fact_payments = add_surrogate_key(fact_payments, ["order_id", "payment_sequential"], "payment_sk")
    
    write_gold(fact_payments, "fact_payments", df_order_payments)

def build_fact_reviews(dim_date):
    """Grain: Review. Customer satisfaction fact table with Date lookup."""
    fact_reviews = df_order_reviews.select(
        "review_id", "order_id", "review_score", 
        "review_creation_date", "review_answer_timestamp",
    ).withColumn("response_time_seconds", 
                 (F.unix_timestamp("review_answer_timestamp") - F.unix_timestamp("review_creation_date")).cast(LongType()))
    
    # Deduplicate by review_id (Grain: One review per event)
    # Keeping latest by answer timestamp
    fact_reviews = fact_reviews.withColumn("rn", F.row_number().over(Window.partitionBy("review_id").orderBy(F.col("review_answer_timestamp").desc()))) \
        .filter(F.col("rn") == 1).drop("rn")

    # Date Lookup
    fact_reviews = fact_reviews.withColumn("review_date_only", F.to_date("review_creation_date")) \
        .join(dim_date.alias("dd"), F.col("review_date_only") == F.col("dd.date_key"), "left") \
        .select(
            "review_id", "order_id", "review_score", 
            "review_creation_date", "review_answer_timestamp", "response_time_seconds",
            F.col("dd.date_key").alias("review_date_key")
        )

    write_gold(fact_reviews, "fact_reviews", df_order_reviews)


def build_feature_customer_rfm(fact_sales):
    """Calculates R, F, M features at the customer_unique_id grain."""
    # fact_sales already contains customer_unique_id from our bridge join
    max_date_row = df_orders.select(F.max("order_purchase_timestamp")).collect()[0][0]
    
    feature_customer_rfm = fact_sales.groupBy("customer_unique_id").agg(
        F.countDistinct("order_id").cast(IntegerType()).alias("frequency"),
        F.sum(F.col("item_price") + F.col("item_freight")).cast(DecimalType(12, 2)).alias("monetary"),
        F.max("order_purchase_timestamp").alias("last_purchase")
    ).withColumn("recency", F.datediff(F.lit(max_date_row), F.col("last_purchase")).cast(IntegerType())) \
    .filter(F.col("customer_unique_id").isNotNull()) \
    .fillna({"monetary": 0.0})

    write_gold(feature_customer_rfm, "feature_customer_rfm", df_orders)

def build_feature_product_performance():
    feature_product_performance = df_order_items.join(df_order_reviews, "order_id", "left") \
        .groupBy("product_id").agg(
            F.count("order_id").cast(LongType()).alias("total_sales"),
            F.sum("price").cast(DecimalType(15, 2)).alias("total_revenue"),
            F.avg("review_score").cast(DecimalType(3, 2)).alias("avg_rating")
        ).fillna({"avg_rating": 3.0})
    write_gold(feature_product_performance, "feature_product_performance", df_order_items)

def build_feature_seller_reliability():
    feature_seller_reliability = df_order_items.join(df_orders, "order_id") \
        .groupBy("seller_id").agg(
            F.count("order_id").cast(LongType()).alias("total_orders"),
            F.sum("price").cast(DecimalType(15, 2)).alias("total_sales_value"),
            F.avg(F.datediff("order_delivered_customer_date", "order_purchase_timestamp")).cast(DecimalType(10, 2)).alias("avg_delivery_time_days")
        ).fillna({"avg_delivery_time_days": -1.0})
    write_gold(feature_seller_reliability, "feature_seller_reliability", df_order_items)


# --- Main Logic ---

# Initialize global variables for tables (sharing across threads)
df_customers = None
df_geolocation = None
df_order_items = None
df_order_payments = None
df_order_reviews = None
df_orders = None
df_products = None
df_sellers = None

if __name__ == "__main__":
    print("Loading Silver tables...")
    
    df_customers = spark.read.parquet(f"{SILVER_PATH}/customers/").cache()
    df_geolocation = spark.read.parquet(f"{SILVER_PATH}/geolocation/").cache()
    df_order_items = spark.read.parquet(f"{SILVER_PATH}/order_items/").cache()
    df_order_payments = spark.read.parquet(f"{SILVER_PATH}/order_payments/").cache()
    
    # --- Unified Reviews (Batch + Stream) ---
    df_batch_reviews = spark.read.parquet(f"{SILVER_PATH}/order_reviews/").cache()
    try:
        df_stream_reviews = spark.read.parquet(f"{SILVER_PATH}/reviews_stream/")
        df_order_reviews = df_batch_reviews.unionByName(df_stream_reviews, allowMissingColumns=True).cache()
        print("  [SUCCESS] Unified Batch + Kafka Streaming reviews loaded.")
    except:
        df_order_reviews = df_batch_reviews.cache()
        print("  [INFO] No streaming reviews found. Proceeding with Batch only.")
    df_orders = spark.read.parquet(f"{SILVER_PATH}/orders/").cache()
    df_products = spark.read.parquet(f"{SILVER_PATH}/products/").cache()
    df_sellers = spark.read.parquet(f"{SILVER_PATH}/sellers/").cache()

    print("🚀 Starting Parallel Gold Modeling...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Stage 1: Dimensions (Parallel)
        print("\n--- Stage 1: Dimensions (Parallel) ---")
        dim_tasks = [
            executor.submit(build_dim_customers),
            executor.submit(build_dim_products),
            executor.submit(build_dim_sellers),
            executor.submit(build_dim_date)
        ]
        for future in dim_tasks:
            future.result()

        # --- OPTIMIZATION: Load & Cache Gold Dimensions for lookups ---
        print("\n--- Pre-Caching Gold Dimensions for lookups ---")
        dim_cust = spark.read.parquet(f"{GOLD_PATH}/dim_customers/").cache()
        dim_prod = spark.read.parquet(f"{GOLD_PATH}/dim_products/").cache()
        dim_sell = spark.read.parquet(f"{GOLD_PATH}/dim_sellers/").cache()
        dim_date = spark.read.parquet(f"{GOLD_PATH}/dim_date/").cache()

        # Stage 2: Facts (Parallel)
        print("\n--- Stage 2: Facts (Parallel) ---")
        fact_sales_future = executor.submit(build_fact_sales, dim_cust, dim_prod, dim_sell, dim_date)
        fact_tasks = [
            executor.submit(build_fact_payments, dim_date),
            executor.submit(build_fact_reviews, dim_date)
        ]
        
        # Wait for facts
        fact_sales_df = fact_sales_future.result()
        for future in fact_tasks:
            future.result()
        
        # Stage 3: Feature Tables (Parallel)
        print("\n--- Stage 3: Feature Tables (Parallel) ---")
        feature_tasks = [
            executor.submit(build_feature_customer_rfm, fact_sales_df),
            executor.submit(build_feature_product_performance),
            executor.submit(build_feature_seller_reliability)
        ]
        for future in feature_tasks:
            future.result()

    # --- CLEANUP ---
    print("\nUnpersisting tables...")
    for df in [df_customers, df_geolocation, df_order_items, df_order_payments, 
               df_order_reviews, df_orders, df_products, df_sellers,
               dim_cust, dim_prod, dim_sell, dim_date]:
        if df: df.unpersist()

    print("✅ Gold modeling completed successfully.")