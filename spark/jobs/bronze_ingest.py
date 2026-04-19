import sys
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import current_timestamp
from config import get_spark_session, get_run_id, JobTracker, get_last_success_timestamp, SOURCE_DB_CONF, PRIMARY_KEYS, TABLE_SCHEMAS, DataQuality, MAX_WORKERS

from logger import ParallelLogger

# Initialize Spark Session with global configuration
spark = get_spark_session("Bronze Ingestion")
run_id = get_run_id()

# --- PRE-INITIALIZE AUDIT ---
# Do this once in the main process to avoid concurrency race conditions in Postgres
JobTracker.initialize_audit_table(spark)

# Iterate through defined tables in our schema
tables = list(PRIMARY_KEYS.keys())

def ingest_table(table_name):
    """Function to ingest a single table, designed for parallel execution."""
    ParallelLogger.get_logger(f"Bronze Ingest: {table_name}")
    tracker = JobTracker(spark, "bronze_ingestion", run_id)
    tracker.start_job(table_name)
    
    try:
        # 1. Determine last successful watermark
        last_ts = get_last_success_timestamp(spark, "bronze_ingestion", table_name)
        
        # 2. Logic for Table Name in Source DB
        raw_table_name = f"olist_{table_name}_dataset" if table_name != "product_category_name_translation" else table_name
        db_table = f"olist.{raw_table_name}"
        
        # 3. Incremental Query
        query = f"(SELECT * FROM {db_table} WHERE updated_at > '{last_ts}') as incremental_df"
        
        tracker.log(f"BRONZE: Fetching incremental data for {table_name} [Watermark: {last_ts}]")
        
        df = spark.read \
            .format("jdbc") \
            .option("url", SOURCE_DB_CONF["url"]) \
            .option("dbtable", query) \
            .option("user", SOURCE_DB_CONF["user"]) \
            .option("password", SOURCE_DB_CONF["password"]) \
            .option("driver", SOURCE_DB_CONF["driver"]) \
            .load().cache()
        
        # 4. Data Quality: Schema Validation
        expected_schema = TABLE_SCHEMAS.get(table_name)
        if not DataQuality.check_schema_mismatch(df, expected_schema):
            raise ValueError(f"SCHEMA MISMATCH: Source table '{table_name}' structure has changed.")

        input_count = df.count()
        tracker.log(f"BRONZE: Found {input_count} new records for {table_name}")
        
        if input_count > 0:
            df = df.withColumn("_ingested_at", current_timestamp()) \
                   .withColumnRenamed("updated_at", "source_updated_at")
            
            write_mode = "overwrite" if table_name == "product_category_name_translation" else "append"
            target_path = f"s3a://bronze/{table_name}/"
            
            df.coalesce(1) \
                .write \
                .mode(write_mode) \
                .parquet(target_path)
            
            tracker.log(f"BRONZE: Successfully {write_mode}ed {input_count} records to {target_path}")
            df.unpersist() # Optimization: Free memory after write
        else:
            tracker.log(f"BRONZE: No new data for {table_name}. Skipping write.")
        tracker.end_job("SUCCESS", input_rows=input_count, output_rows=input_count)
        return True
    except Exception as e:
        tracker.end_job("FAILED", error_msg=str(e))
        tracker.log(f"FAILED to ingest {table_name}: {e}")
        return False
    finally:
        ParallelLogger.flush()

if __name__ == "__main__":
    # Execute parallel ingestion
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(ingest_table, tables))

    success_count = sum(1 for r in results if r)
    if success_count < len(tables):
        print(f"ERROR: Ingested only {success_count}/{len(tables)} tables. Failing job.")
        sys.exit(1)

    print(f"Bronze ingestion completed successfully for {success_count} tables.")