import os
import re
from config import get_spark_session

# Initialize Spark Session with global configuration
spark = get_spark_session("Bronze Ingestion")

RAW_PATH = "/raw"

def clean_table_name(filename):
    # Remove extension and olist_ prefix/suffix to get a cleaner table name
    name = filename.replace(".csv", "")
    name = re.sub(r"olist_|_dataset", "", name)
    return name

# List all CSV files in the raw directory
try:
    files = [f for f in os.listdir(RAW_PATH) if f.endswith(".csv")]
except FileNotFoundError:
    print(f"Error: Raw path {RAW_PATH} not found.")
    files = []

for file in files:
    table_name = clean_table_name(file)
    print(f"Ingesting {file} into bronze.{table_name}...")
    
    # Read CSV with header and infer schema
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file://{RAW_PATH}/{file}")
    
    # Write to Bronze layer in MinIO as Parquet
    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"s3a://bronze/{table_name}/")

print("Bronze ingestion completed for all files.")