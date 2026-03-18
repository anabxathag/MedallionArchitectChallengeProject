from pyspark.sql.functions import (
    count, sum, avg, col, year, month, dayofmonth, hour, 
    dayofweek, date_format, datediff, max as spark_max, 
    min as spark_min, countDistinct, lit, when, isnull
)
from config import get_spark_session

SILVER_PATH = "s3a://silver"
GOLD_PATH = "s3a://gold"

# Initialize Spark Session
spark = get_spark_session("Gold Modeling")

def explore_table(table_name, source_df=None):
    """
    Enhanced exploration for Gold tables. 
    If source_df is provided, compares Silver (source) vs Gold metrics.
    """
    print(f"\n{'='*60}")
    print(f"📊 GOLD DATA EXPLORATION REPORT: {table_name}")
    print(f"{'='*60}")
    
    try:
        df_gold = spark.read.parquet(f"{GOLD_PATH}/{table_name}/")
        
        # 1. Schema Analysis
        # 2. CDC / Change Tracking Logic (if source provided)
        if source_df:
            print(f"\n[1] Schema Analysis (Silver -> Gold): {table_name}")
            print(f"Silver Columns: {', '.join(source_df.columns)}")
            print(f"Gold Columns: {', '.join(df_gold.columns)}")

            source_count = source_df.count()
            gold_count = df_gold.count()
            diff = source_count - gold_count
            diff_pct = (diff / source_count) * 100 if source_count > 0 else 0
            
            print("\n[2] Transition Metrics (Silver -> Gold)")
            print(f"  • Silver Records : {source_count:,}")
            print(f"  • Gold Records   : {gold_count:,}")
            print(f"  • Record Diff    : {diff:,} ({diff_pct:.2f}%)")
        else:
            print(f"\n[1] Schema Analysis (Gold Layer: {table_name})")
            print(f"Columns: {', '.join(df_gold.columns)}")

            print(f"\n[2] Record Count (Gold): {df_gold.count():,}")

        # 3. Null Data Analysis
        from pyspark.sql.functions import isnull
        print("\n[3] Null Data Analysis (Missing Values in Gold)")
        gold_nulls = df_gold.select([count(when(isnull(c), c)).alias(c) for c in df_gold.columns]).collect()[0].asDict()
        
        print(f"{'Column':<35} | {'Gold Nulls':<15}")
        print("-" * 55)
        for col_name, n_count in gold_nulls.items():
            print(f"{col_name:<35} | {str(n_count):<15}")

        # 4. Statistical Summary
        print("\n[4] Statistical Summary (Gold Layer)")
        df_gold.describe().show()

        # 5. Data Sample
        print("[5] Data Sample (Top 5 Rows)")
        df_gold.show(5, truncate=True)

    except Exception as e:
        print(f"❌ Exploration failed for {table_name}: {e}")
    print(f"{'='*60}\n")

def write_gold(df, table_name, source_df=None):
    print(f"Saving gold.{table_name}...")
    df.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH}/{table_name}/")
    explore_table(table_name, source_df)

# --- Load Silver Tables ---
print("Loading Silver tables...")
df_customers = spark.read.parquet(f"{SILVER_PATH}/customers/")
df_geolocation = spark.read.parquet(f"{SILVER_PATH}/geolocation/")
df_order_items = spark.read.parquet(f"{SILVER_PATH}/order_items/")
df_order_payments = spark.read.parquet(f"{SILVER_PATH}/order_payments/")
df_order_reviews = spark.read.parquet(f"{SILVER_PATH}/order_reviews/")
df_orders = spark.read.parquet(f"{SILVER_PATH}/orders/")
df_products = spark.read.parquet(f"{SILVER_PATH}/products/")
df_sellers = spark.read.parquet(f"{SILVER_PATH}/sellers/")

# --- Star Schema: Dimensions ---

# 1. dim_customers
dim_customers = df_customers.select(
    "customer_id", "customer_unique_id", "customer_zip_code_prefix", 
    "customer_city", "customer_state"
)
write_gold(dim_customers, "dim_customers", df_customers)

# 2. dim_products
dim_products = df_products.select(
    "product_id", "product_category_name", "product_category_name_english",
    "product_name_lenght", "product_description_lenght", "product_photos_qty",
    "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
)
write_gold(dim_products, "dim_products", df_products)

# 3. dim_sellers
dim_sellers = df_sellers.select(
    "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
)
write_gold(dim_sellers, "dim_sellers", df_sellers)

# 4. dim_date
dim_date = df_orders.select("order_purchase_timestamp").distinct() \
    .withColumn("date_key", col("order_purchase_timestamp")) \
    .withColumn("year", year("order_purchase_timestamp")) \
    .withColumn("month", month("order_purchase_timestamp")) \
    .withColumn("day", dayofmonth("order_purchase_timestamp")) \
    .withColumn("hour", hour("order_purchase_timestamp")) \
    .withColumn("day_of_week", dayofweek("order_purchase_timestamp")) \
    .withColumn("weekday_name", date_format("order_purchase_timestamp", "EEEE"))
write_gold(dim_date, "dim_date")

# 5. dim_location
dim_location = df_geolocation.select(
    "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", 
    "geolocation_city", "geolocation_state"
)
write_gold(dim_location, "dim_location", df_geolocation)

# --- Star Schema: Fact ---

# Aggregating items, payments, and reviews per order
order_items_agg = df_order_items.groupBy("order_id").agg(
    sum("price").alias("total_price"),
    sum("freight_value").alias("total_freight"),
    count("order_item_id").alias("total_items"),
    countDistinct("product_id").alias("unique_products"),
    countDistinct("seller_id").alias("unique_sellers")
)

order_payments_agg = df_order_payments.groupBy("order_id").agg(
    sum("payment_value").alias("total_payment_value"),
    spark_max("payment_installments").alias("max_installments"),
    count("payment_sequential").alias("payment_count")
)

order_reviews_agg = df_order_reviews.groupBy("order_id").agg(
    avg("review_score").alias("avg_review_score"),
    count("review_id").alias("review_count")
)

fact_orders = df_orders.join(order_items_agg, "order_id", "left") \
    .join(order_payments_agg, "order_id", "left") \
    .join(order_reviews_agg, "order_id", "left") \
    .select(
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_customer_date", "order_estimated_delivery_date",
        col("total_price").cast("float").alias("total_price"),
        col("total_freight").cast("float").alias("total_freight"),
        col("total_items").cast("integer").alias("total_items"),
        col("unique_products").cast("integer").alias("unique_products"),
        col("unique_sellers").cast("integer").alias("unique_sellers"),
        col("total_payment_value").cast("float").alias("total_payment_value"),
        col("max_installments").cast("integer").alias("max_installments"),
        col("payment_count").cast("integer").alias("payment_count"),
        col("avg_review_score").cast("float").alias("avg_review_score"),
        col("review_count").cast("integer").alias("review_count")
    ).fillna({
        "total_price": 0.0, "total_freight": 0.0, "total_items": 0,
        "unique_products": 0, "unique_sellers": 0, "total_payment_value": 0.0,
        "max_installments": 0, "payment_count": 0, "review_count": 0,
        "avg_review_score": 0.0 # Using 0.0 for no review
    })
write_gold(fact_orders, "fact_orders", df_orders)

# --- ML Feature Tables ---

# 1. feature_customer_profile
# RFM and Satisfaction
max_date = df_orders.select(spark_max("order_purchase_timestamp")).collect()[0][0]

feature_customer_profile = fact_orders.groupBy("customer_id").agg(
    count("order_id").alias("frequency"),
    sum("total_payment_value").alias("monetary"),
    avg(when(col("review_count") > 0, col("avg_review_score"))).alias("avg_satisfaction"),
    spark_max("order_purchase_timestamp").alias("last_purchase")
).withColumn("recency", datediff(lit(max_date), col("last_purchase"))) \
 .fillna({"monetary": 0.0, "avg_satisfaction": 3.0}) # Default to neutral satisfaction if no reviews

write_gold(feature_customer_profile, "feature_customer_profile")

# 2. feature_product_performance
feature_product_performance = df_order_items.join(df_order_reviews, "order_id", "left") \
    .groupBy("product_id").agg(
        count("order_id").alias("total_sales"),
        sum("price").alias("total_revenue"),
        avg("review_score").alias("avg_rating")
    ).fillna({"avg_rating": 3.0}) # Default to neutral satisfaction if no reviews
write_gold(feature_product_performance, "feature_product_performance")

# 3. feature_seller_reliability
feature_seller_reliability = df_order_items.join(df_orders, "order_id") \
    .groupBy("seller_id").agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("total_sales_value"),
        avg(datediff("order_delivered_customer_date", "order_purchase_timestamp")).alias("avg_delivery_time_days")
    ).fillna({"avg_delivery_time_days": -1.0}) # Use -1 logic for unknown delivery time
write_gold(feature_seller_reliability, "feature_seller_reliability")

print("Gold modeling completed.")