import os
import boto3
import polars as pl
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor
from config import get_run_id, JobTracker, MAX_WORKERS, S3A_CONF
from logger import ParallelLogger

# MinIO Connection for Polars / boto3
# Note: Polars can use boto3-style environment variables or storage_options
os.environ["AWS_ACCESS_KEY_ID"] = S3A_CONF["spark.hadoop.fs.s3a.access.key"]
os.environ["AWS_SECRET_ACCESS_KEY"] = S3A_CONF["spark.hadoop.fs.s3a.secret.key"]
os.environ["AWS_ENDPOINT_URL"] = "http://minio:9000"
os.environ["AWS_REGION"] = "us-east-1" # Default dummy region for MinIO

run_id = get_run_id()

GOLD_TABLES = [
    "fact_sales", 
    "fact_payments", 
    "fact_reviews",
    "dim_customers", 
    "dim_products", 
    "dim_sellers", 
    "dim_date",
    "feature_customer_rfm", 
    "feature_product_performance", 
    "feature_seller_reliability"
]

def export_table_to_bq(table_name, gcp_project, bq_dataset):
    """Exports a single Gold table from MinIO to Google BigQuery using Polars and Python SDK."""
    ParallelLogger.get_logger(f"BQ Sidecar: {table_name}")
    # Initialize JobTracker WITHOUT Spark
    tracker = JobTracker(spark=None, job_name="bigquery_export", run_id=run_id)
    tracker.start_job(table_name)
    
    local_path = f"/tmp/{table_name}_{run_id}.parquet"
    
    try:
        tracker.log(f"📥 Listing files in MinIO: gold/{table_name}...")
        
        # Use boto3 to list files explicitly (avoiding flaky glob expansion)
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.environ["AWS_ENDPOINT_URL"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
        )
        
        objs = s3_client.list_objects_v2(Bucket="gold", Prefix=f"{table_name}/")
        parquet_files = [
            f"s3://gold/{obj['Key']}" 
            for obj in objs.get("Contents", []) 
            if obj["Key"].endswith(".parquet")
        ]
        
        if not parquet_files:
            raise Exception(f"No parquet files found in gold/{table_name}/")
            
        tracker.log(f"📥 Reading {len(parquet_files)} files from MinIO (Polars)...")
        
        # 1. Read files individually and concat (Decisively robust for S3 + storage_options)
        storage_options = {
            "endpoint_url": os.environ["AWS_ENDPOINT_URL"],
            "key": os.environ["AWS_ACCESS_KEY_ID"],
            "secret": os.environ["AWS_SECRET_ACCESS_KEY"]
        }
        
        dfs = [
            pl.read_parquet(f, storage_options=storage_options) 
            for f in parquet_files
        ]
        
        if not dfs:
            raise Exception("No data could be read from the parquet files.")
            
        df = pl.concat(dfs)
        input_count = len(df)
        
        tracker.log(f"📦 Loaded {input_count} rows. Saving to local temp...")
        
        # 2. Save to local temp parquet (single file)
        df.write_parquet(local_path)
        
        # 3. Push to BigQuery using Python SDK
        tracker.log(f"🚀 Uploading {table_name} to BigQuery: {gcp_project}.{bq_dataset}")
        
        # FIX: Pass project to the client!
        client = bigquery.Client(project=gcp_project)
        table_id = f"{gcp_project}.{bq_dataset}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with open(local_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
            job.result()  # Wait for completion
            
        tracker.end_job("SUCCESS", input_rows=input_count, output_rows=input_count)
        tracker.log(f"✅ Successfully exported {table_name} to BigQuery")
        return True
        
    except Exception as e:
        tracker.end_job("FAILED", error_msg=str(e))
        tracker.log(f"❌ Failed to export {table_name} to BQ: {e}")
        return False
    finally:
        # Cleanup
        if os.path.exists(local_path):
            os.remove(local_path)
        ParallelLogger.flush()

if __name__ == "__main__":
    # 1. Parse Arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", help="Airflow run_id", required=True)
    parser.add_argument("--gcp_project", help="GCP Project ID")
    parser.add_argument("--bq_dataset", help="BigQuery Dataset")
    # bq_temp_bucket is no longer strictly needed for Option 1, but we keep the arg for compatibility
    parser.add_argument("--bq_temp_bucket", help="GCS Temp Bucket", required=False)
    args, unknown = parser.parse_known_args()

    # Use env vars as fallback for GCP project/dataset
    gcp_project = args.gcp_project or os.getenv("GCP_PROJECT_ID")
    bq_dataset = args.bq_dataset or os.getenv("BQ_DATASET")

    if not gcp_project or not bq_dataset:
        print("❌ ERROR: Missing GCP configuration (GCP_PROJECT_ID or BQ_DATASET).")
        exit(1)

    print(f"🚀 Starting Lightweight Polars export to BigQuery Project: {gcp_project}...")
    
    # Initialize the audit schema if needed (using non-spark method)
    JobTracker.initialize_audit_table(spark=None)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(
            lambda t: export_table_to_bq(t, gcp_project, bq_dataset), 
            GOLD_TABLES
        ))
    
    success_count = sum(1 for r in results if r)
    print(f"\n🌟 BigQuery sidecar export completed: {success_count}/{len(GOLD_TABLES)} successful.")
    
    if success_count < len(GOLD_TABLES):
        exit(1)
