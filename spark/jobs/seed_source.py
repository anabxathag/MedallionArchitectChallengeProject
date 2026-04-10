import os
import sys
import psycopg2
from config import get_spark_session, SOURCE_DB_CONF, METADATA_DB_CONF, TABLE_SCHEMAS, PRIMARY_KEYS

def clean_filename(filename):
    """Map filename to the short keys used in TABLE_SCHEMAS/PRIMARY_KEYS."""
    name = filename.replace(".csv", "")
    if "customers" in name: return "customers"
    if "geolocation" in name: return "geolocation"
    if "order_items" in name: return "order_items"
    if "order_payments" in name: return "order_payments"
    if "order_reviews" in name: return "order_reviews"
    if "orders" in name: return "orders"
    if "products" in name: return "products"
    if "sellers" in name: return "sellers"
    if "translation" in name: return "product_category_name_translation"
    return name

def execute_ddl(target_conf, ddl):
    """Connect via psycopg2 and execute DDL directly."""
    # Parse host/port dynamically from JDBC URL (e.g., source-db or logging_db)
    import re
    match = re.search(r"//([^:/]+)", target_conf["url"])
    host = match.group(1) if match else "localhost"
    
    conn = psycopg2.connect(
        host=host,
        port=5432,
        database=target_conf["url"].split("/")[-1],
        user=target_conf["user"],
        password=target_conf["password"]
    )
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        print(f"DDL [{host}]: Executing: {ddl}")
        cur.execute(ddl)
    conn.close()

def seed():
    spark = get_spark_session("Source DB Seeding")
    RAW_PATH = "/raw"
    
    # --- Infrastructure Cleanup and Prep ---
    print("Preparing schemas and cleaning public tables...")
    try:
        # 1. Prepare Source DB
        execute_ddl(SOURCE_DB_CONF, "CREATE SCHEMA IF NOT EXISTS olist;")
        
        # 2. Prepare Metadata DB (Audit)
        execute_ddl(METADATA_DB_CONF, "CREATE SCHEMA IF NOT EXISTS audit;")
        execute_ddl(METADATA_DB_CONF, "DROP TABLE IF EXISTS audit.run_logs CASCADE;")
    except Exception as e:
        print(f"WARNING during cleanup/prep: {e}")

    try:
        files = [f for f in os.listdir(RAW_PATH) if f.endswith(".csv")]
    except FileNotFoundError:
        print(f"ERROR: Raw path {RAW_PATH} not found.")
        sys.exit(1)

    if not files:
        print("WARNING: No CSV files found to seed.")
        return

    print(f"Found {len(files)} files to seed into {SOURCE_DB_CONF['url']} (schema: olist)")

    for file in files:
        # Full filename (without .csv) is used for the TABLE NAME in DB
        db_table_name = file.replace(".csv", "")
        # Short name is used to look up the SCHEMA
        schema_key = clean_filename(file)
        
        file_path = f"file://{RAW_PATH}/{file}"
        schema = TABLE_SCHEMAS.get(schema_key)
        
        if not schema:
            print(f"WARNING: No explicit schema defined for {schema_key}. Skipping safe load.")
            continue

        print(f"Seeding table: olist.{db_table_name} from {file} with EXPLICIT SCHEMA [{schema_key}]...")
        
        try:
            # Read with explicit schema
            df = spark.read \
                .option("header", "true") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .schema(schema) \
                .csv(file_path)
            
            # Simulated CDC: Add 'updated_at' column to all records
            from pyspark.sql.functions import current_timestamp
            df = df.withColumn("updated_at", current_timestamp())
            
            # Write to Source DB in 'olist' schema
            df.write \
                .format("jdbc") \
                .option("url", SOURCE_DB_CONF["url"]) \
                .option("dbtable", f"olist.{db_table_name}") \
                .option("user", SOURCE_DB_CONF["user"]) \
                .option("password", SOURCE_DB_CONF["password"]) \
                .option("driver", SOURCE_DB_CONF["driver"]) \
                .mode("overwrite") \
                .save()
            
            print(f"SUCCESS: Seeded olist.{db_table_name} ({df.count()} rows) with native types.")

            # --- Physical PK Enforcement ---
            pks = PRIMARY_KEYS.get(schema_key)
            if pks: # <--- Skips if list is empty []
                pk_stmt = f"ALTER TABLE olist.{db_table_name} ADD PRIMARY KEY ({', '.join(pks)});"
                try:
                    execute_ddl(SOURCE_DB_CONF, pk_stmt)
                except Exception as pk_err:
                    # Prints warning but DOES NOT stop the pipeline
                    print(f"DDL WARNING: Could not apply PK to {db_table_name}: {pk_err}")

                    
        except Exception as e:
            print(f"FAILED to seed {db_table_name}: {e}")
            sys.exit(1)

    print("Source database seeding into 'olist' schema completed successfully with native types.")

if __name__ == "__main__":
    seed()
