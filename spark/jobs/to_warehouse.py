import os
from config import get_spark_session

GOLD_PATH = "s3a://gold"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/retail_dw"
POSTGRES_PROPERTIES = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Initialize Spark Session
spark = get_spark_session("Gold to Postgres Ingestion")

# List of Gold tables to ingest
GOLD_TABLES = [
    "fact_orders",
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "dim_date",
    "dim_location",
    "feature_customer_profile",
    "feature_product_performance",
    "feature_seller_reliability"
]

def ingest_to_postgres(table_name):
    print(f"🚀 Ingesting {table_name} to Postgres gold schema...")
    try:
        # Read from MinIO (Gold Bucket)
        df = spark.read.parquet(f"{GOLD_PATH}/{table_name}/")
        
        # Write to Postgres (Gold Schema)
        # Using overwrite mode to refresh data in the warehouse
        df.write.jdbc(
            url=POSTGRES_URL,
            table=f"gold.{table_name}",
            mode="overwrite",
            properties=POSTGRES_PROPERTIES
        )
        print(f"✅ Successfully ingested {table_name} ({df.count()} rows)")
    except Exception as e:
        print(f"❌ Failed to ingest {table_name}: {e}")

if __name__ == "__main__":
    for table in GOLD_TABLES:
        ingest_to_postgres(table)
    
    print("\n🌟 Gold to Postgres ingestion completed.")
    spark.stop()
