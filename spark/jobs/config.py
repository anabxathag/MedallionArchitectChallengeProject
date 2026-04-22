import argparse
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, DecimalType, ShortType, LongType, BooleanType

# --- Global Configs ---
MAX_WORKERS = 4

# --- Global Paths ---
EXTRA_JARS_PATH = "/opt/spark/extra_jars"
POSTGRES_JAR = f"{EXTRA_JARS_PATH}/postgresql-42.7.4.jar"
HADOOP_AWS_JAR = f"{EXTRA_JARS_PATH}/hadoop-aws-3.4.1.jar"
AWS_SDK_JAR = f"{EXTRA_JARS_PATH}/bundle-2.23.19.jar"

# Kafka Streaming JARs
KAFKA_SQL_JAR = f"{EXTRA_JARS_PATH}/spark-sql-kafka-0-10_2.13-4.0.2.jar"
KAFKA_CLIENTS_JAR = f"{EXTRA_JARS_PATH}/kafka-clients-3.9.0.jar"
KAFKA_TOKEN_JAR = f"{EXTRA_JARS_PATH}/spark-token-provider-kafka-0-10_2.13-4.0.2.jar"
COMMONS_POOL_JAR = f"{EXTRA_JARS_PATH}/commons-pool2-2.12.0.jar"

# --- JAR Inclusion Strategy ---
# Spark 4.0.2 requires jars for the session, plus extraClassPath for the driver/executors
# to ensure S3AFileSystem classes are available during initial Hadoop FS initialization.
BASE_JARS = f"{POSTGRES_JAR},{HADOOP_AWS_JAR},{AWS_SDK_JAR}"
KAFKA_JARS = f"{KAFKA_SQL_JAR},{KAFKA_CLIENTS_JAR},{KAFKA_TOKEN_JAR},{COMMONS_POOL_JAR}"

BASE_CP = f"{POSTGRES_JAR}:{HADOOP_AWS_JAR}:{AWS_SDK_JAR}"
KAFKA_CP = f"{KAFKA_SQL_JAR}:{KAFKA_CLIENTS_JAR}:{KAFKA_TOKEN_JAR}:{COMMONS_POOL_JAR}"

# --- S3A / MinIO Configuration ---
S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.multipart.size": "104857600",
}

# --- Postgres Metadata & Audit Store ---
METADATA_DB_CONF = {
    "url": "jdbc:postgresql://logging_db:5432/audit_db",
    "driver": "org.postgresql.Driver",
    "user": "admin",
    "password": "admin",
}

# --- Postgres Source Configuration (Simulated Production DB) ---
SOURCE_DB_CONF = {
    "url": "jdbc:postgresql://source-db:5432/production_db",
    "driver": "org.postgresql.Driver",
    "user": "admin",
    "password": "admin",
}

# --- Kafka Streaming Configuration ---
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
REVIEWS_TOPIC = "olist_reviews_stream"

# Schema for incoming JSON reviews via Kafka
REVIEWS_STREAM_SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("review_score", IntegerType(), True),
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", TimestampType(), True),
    StructField("review_answer_timestamp", TimestampType(), True)
])

# --- High-Fidelity Source Schemas ---
TABLE_SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), False),
        StructField("customer_zip_code_prefix", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "geolocation": StructType([
        StructField("geolocation_zip_code_prefix", IntegerType(), True),
        StructField("geolocation_lat", DoubleType(), True),
        StructField("geolocation_lng", DoubleType(), True),
        StructField("geolocation_city", StringType(), True),
        StructField("geolocation_state", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "order_items": StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "order_payments": StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", IntegerType(), False),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "order_reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("review_score", IntegerType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", TimestampType(), True),
        StructField("review_answer_timestamp", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "products": StructType([
        StructField("product_id", StringType(), False),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", IntegerType(), True),
        StructField("product_description_lenght", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", IntegerType(), True),
        StructField("product_height_cm", IntegerType(), True),
        StructField("product_width_cm", IntegerType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType(), False),
        StructField("seller_zip_code_prefix", IntegerType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ]),
    "product_category_name_translation": StructType([
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ])
}

# --- Silver Layer Schemas (Post-Transformation) ---
SILVER_TABLE_SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), False),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ]),
    "geolocation": StructType([
        StructField("geolocation_zip_code_prefix", StringType(), True),
        StructField("geolocation_lat", DecimalType(11, 8), True),
        StructField("geolocation_lng", DecimalType(11, 8), True),
        StructField("geolocation_city", StringType(), True),
        StructField("geolocation_state", StringType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ]),
    "order_items": StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("freight_value", DecimalType(10, 2), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ]),
    "order_payments": StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", ShortType(), False),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", ShortType(), True),
        StructField("payment_value", DecimalType(10, 2), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ]),
    "order_reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("review_score", ShortType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", TimestampType(), True),
        StructField("review_answer_timestamp", TimestampType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ]),
    "orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ]),
    "products": StructType([
        StructField("product_category_name", StringType(), True),
        StructField("product_id", StringType(), False),
        StructField("product_name_lenght", ShortType(), True),
        StructField("product_description_lenght", ShortType(), True),
        StructField("product_photos_qty", ShortType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", ShortType(), True),
        StructField("product_height_cm", ShortType(), True),
        StructField("product_width_cm", ShortType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("product_category_name_english", StringType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType(), False),
        StructField("seller_zip_code_prefix", StringType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
        StructField("source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_silver_processed_at", TimestampType(), True)
    ])
}

# --- Gold Layer Schemas (Modeling Output) ---
GOLD_TABLE_SCHEMAS = {
    "dim_products": StructType([
        StructField("product_id", StringType(), False),
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True),
        StructField("product_weight_g", IntegerType(), True),
        StructField("product_length_cm", ShortType(), True),
        StructField("product_height_cm", ShortType(), True),
        StructField("product_width_cm", ShortType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("is_current", BooleanType(), True)
    ]),
    "dim_sellers": StructType([
        StructField("seller_id", StringType(), False),
        StructField("seller_zip_code_prefix", StringType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True),
        StructField("seller_lat", DecimalType(11, 8), True),
        StructField("seller_lng", DecimalType(11, 8), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("is_current", BooleanType(), True)
    ]),
    "dim_date": StructType([
        StructField("date_key", TimestampType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("month_name", StringType(), True),
        StructField("is_br_holiday", BooleanType(), True)
    ]),
    "dim_customers": StructType([
        StructField("customer_unique_id", StringType(), False),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("customer_lat", DecimalType(11, 8), True),
        StructField("customer_lng", DecimalType(11, 8), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("is_current", BooleanType(), True)
    ]),
    "fact_reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("review_score", ShortType(), True),
        StructField("review_creation_date", TimestampType(), True),
        StructField("review_answer_timestamp", TimestampType(), True),
        StructField("response_time_seconds", LongType(), True),
        StructField("review_date_key", TimestampType(), True)
    ]),
    "fact_payments": StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", ShortType(), False),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", ShortType(), True),
        StructField("payment_value", DecimalType(10, 2), True),
        StructField("order_date_key", TimestampType(), True),
        StructField("payment_sk", StringType(), True)
    ]),
    "fact_sales": StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("order_date_key", TimestampType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("item_price", DecimalType(10, 2), True),
        StructField("item_freight", DecimalType(10, 2), True),
        StructField("delivery_delay_days", ShortType(), True),
        StructField("sales_sk", StringType(), True)
    ]),
    "feature_seller_reliability": StructType([
        StructField("seller_id", StringType(), False),
        StructField("total_orders", LongType(), True),
        StructField("total_sales_value", DecimalType(15, 2), True),
        StructField("avg_delivery_time_days", DecimalType(10, 2), True)
    ]),
    "feature_product_performance": StructType([
        StructField("product_id", StringType(), False),
        StructField("total_sales", LongType(), True),
        StructField("total_revenue", DecimalType(15, 2), True),
        StructField("avg_rating", DecimalType(3, 2), True)
    ]),
    "feature_customer_rfm": StructType([
        StructField("customer_unique_id", StringType(), False),
        StructField("frequency", IntegerType(), True),
        StructField("monetary", DecimalType(12, 2), True),
        StructField("last_purchase", TimestampType(), True),
        StructField("recency", IntegerType(), True)
    ])
}

def get_job_args():
    """Parses command line arguments for Spark jobs."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", help="Airflow run_id", default=f"manual_run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")
    parser.add_argument("--holidays", help="JSON string of Brazilian holidays", default=None)
    parser.add_argument("--discord_webhook", help="Discord Webhook URL for alerting", default=None)
    args, unknown = parser.parse_known_args()
    return args

def get_run_id():
    return get_job_args().run_id

def get_discord_webhook():
    return get_job_args().discord_webhook

def get_holidays_arg():
    import json
    args = get_job_args()
    if args.holidays:
        try:
            return json.loads(args.holidays)
        except Exception:
            print("WARNING: Failed to parse holidays argument. Falling back.")
    return None

def get_spark_session(app_name, include_kafka=False):
    """Creates a SparkSession with selective JARs and S3A configurations."""
    all_jars = f"{BASE_JARS},{KAFKA_JARS}" if include_kafka else BASE_JARS
    cp_jars = f"{BASE_CP}:{KAFKA_CP}" if include_kafka else BASE_CP

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", all_jars) \
        .config("spark.driver.extraClassPath", cp_jars) \
        .config("spark.executor.extraClassPath", cp_jars) \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "1024m") \
        .config("spark.executor.memory", "1024m")
    
    for key, value in S3A_CONF.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# --- Table Metrics & Primary Keys ---
PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
    "orders": ["order_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_name_translation": ["product_category_name"]
}

GOLD_PRIMARY_KEYS = {
    "dim_customers": ["customer_unique_id"],
    "dim_products": ["product_id"],
    "dim_sellers": ["seller_id"],
    "dim_date": ["date_key"],
    "fact_sales": ["order_id", "order_item_id"],
    "fact_payments": ["order_id", "payment_sequential"],
    "fact_reviews": ["review_id"],
    "feature_customer_rfm": ["customer_unique_id"],
    "feature_product_performance": ["product_id"],
    "feature_seller_reliability": ["seller_id"]
}

# --- Reference Data ---
# Primary Brazilian National Holidays (Static Fallback)
BRAZILIAN_HOLIDAYS_STATIC = [
    ## Fixed National Holidays
    "2016-01-01", "2017-01-01", "2018-01-01", "2019-01-01", "2020-01-01",    #New Year's Day
    "2016-04-21", "2017-04-21", "2018-04-21", "2019-04-21", "2020-04-21",    #Tiradentes Day
    "2016-05-01", "2017-05-01", "2018-05-01", "2019-05-01", "2020-05-01",    #Labour Day
    "2016-09-07", "2017-09-07", "2018-09-07", "2019-09-07", "2020-09-07",    #Independence Day
    "2016-10-12", "2017-10-12", "2018-10-12", "2019-10-12", "2020-10-12",    #Our Lady of Aparecida
    "2016-11-02", "2017-11-02", "2018-11-02", "2019-11-02", "2020-11-02",    #All Souls' Day
    "2016-11-15", "2017-11-15", "2018-11-15", "2019-11-15", "2020-11-15",    #Republic Day
    "2016-12-25", "2017-12-25", "2018-12-25", "2019-12-25", "2020-12-25",    #Christmas Day
    ## Mobile Holidays (Lunar-Based)
    "2016-02-09", "2017-02-28", "2018-02-13", "2019-03-05", "2020-02-25",    #Carnival
    "2016-03-25", "2017-04-14", "2018-03-30", "2019-04-19", "2020-04-10",    #Good Friday
    "2016-05-26", "2017-06-15", "2018-05-31", "2019-06-20", "2020-06-11"    #Corpus Christi
]

# Initialize Holidays (Passed via argument or static fallback)
# Note: In a production DAG, Airflow fetches these and passes them to Spark.
BRAZILIAN_HOLIDAYS = get_holidays_arg() or BRAZILIAN_HOLIDAYS_STATIC

def get_last_success_timestamp(spark, job_name, table_name):
    """
    Queries the pipeline_audit.run_logs in Postgres to find the last successful finish time 
    for a specific table in a specific job. Returns a datetime object or epoch start.
    """
    query = f"""
        (SELECT MAX(start_time) as last_run
         FROM audit.run_logs
         WHERE job_name = '{job_name}'
           AND table_name = '{table_name}'
           AND status = 'SUCCESS') AS subquery
    """
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", METADATA_DB_CONF["url"]) \
            .option("dbtable", query) \
            .option("user", METADATA_DB_CONF["user"]) \
            .option("password", METADATA_DB_CONF["password"]) \
            .option("driver", METADATA_DB_CONF["driver"]) \
            .load()
        
        last_run = df.collect()[0]["last_run"]
        if last_run:
            print(f"WATERMARK: Found last successful run for {table_name}: {last_run}")
            return last_run
    except Exception as e:
        print(f"WATERMARK: No previous successful run found for {table_name} or error: {e}")
    
    # Default to 1970 if no previous run found
    return datetime.datetime(1970, 1, 1)

class JobTracker:
    """Helper class to track and log pipeline job metrics to PostgreSQL."""
    def __init__(self, spark, job_name, run_id):
        self.spark = spark  # Can be None for sidecar scripts
        self.job_name = job_name
        self.run_id = run_id
        self.start_time = None
        self.table_name = None

    @staticmethod
    def _get_psycopg2_conn():
        """Helper to create a psycopg2 connection from METADATA_DB_CONF."""
        import psycopg2
        # Parse jdbc:postgresql://host:port/dbname
        url = METADATA_DB_CONF["url"].replace("jdbc:postgresql://", "")
        host_port, dbname = url.split("/")
        host, port = host_port.split(":")
        
        return psycopg2.connect(
            host=host,
            port=port,
            database=dbname,
            user=METADATA_DB_CONF["user"],
            password=METADATA_DB_CONF["password"]
        )

    @staticmethod
    def initialize_audit_table(spark=None):
        """
        Safely initializes the audit table. Supporting both Spark and psycopg2.
        """
        if spark:
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
            schema = StructType([
                StructField("run_id", StringType(), False),
                StructField("job_name", StringType(), False),
                StructField("table_name", StringType(), False),
                StructField("start_time", TimestampType(), False),
                StructField("end_time", TimestampType(), False),
                StructField("input_rows", IntegerType(), False),
                StructField("output_rows", IntegerType(), False),
                StructField("status", StringType(), False),
                StructField("error_msg", StringType(), True),
                StructField("dq_metrics", StringType(), True)
            ])
            empty_df = spark.createDataFrame([], schema)
            try:
                empty_df.write \
                    .format("jdbc") \
                    .option("url", METADATA_DB_CONF["url"]) \
                    .option("dbtable", "audit.run_logs") \
                    .option("user", METADATA_DB_CONF["user"]) \
                    .option("password", METADATA_DB_CONF["password"]) \
                    .option("driver", METADATA_DB_CONF["driver"]) \
                    .mode("append") \
                    .save()
            except Exception as e:
                if "already exists" not in str(e).lower():
                    print(f"AUDIT WARNING: Spark-based Init failed: {e}")
        else:
            # psycopg2 fallback
            try:
                conn = JobTracker._get_psycopg2_conn()
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE SCHEMA IF NOT EXISTS audit;
                        CREATE TABLE IF NOT EXISTS audit.run_logs (
                            run_id TEXT,
                            job_name TEXT,
                            table_name TEXT,
                            start_time TIMESTAMP,
                            end_time TIMESTAMP,
                            input_rows INTEGER,
                            output_rows INTEGER,
                            status TEXT,
                            error_msg TEXT,
                            dq_metrics TEXT
                        );
                    """)
                conn.commit()
                conn.close()
            except Exception as e:
                print(f"AUDIT WARNING: Psycopg2-based Init failed: {e}")

    def log(self, message):
        """Helper to log with component context."""
        try:
            from logger import ParallelLogger
            ParallelLogger.log(message)
        except ImportError:
            print(f"[{self.table_name or 'SYSTEM'}] {message}")

    def start_job(self, table_name):
        self.table_name = table_name
        self.start_time = datetime.datetime.now()
        
        # Initialize thread-local logger if available
        try:
            from logger import ParallelLogger
            ParallelLogger.get_logger(f"{self.job_name}: {self.table_name}")
        except Exception:
            pass

        self.log(f"AUDIT: Starting {self.job_name} for table {self.table_name} [Run: {self.run_id}]")

    def end_job(self, status, input_rows=0, output_rows=0, error_msg=None, dq_metrics=None):
        try:
            end_time = datetime.datetime.now()
            start_time = self.start_time if self.start_time else end_time
            
            if self.spark:
                # Spark JDBC write logic
                audit_data = [(
                    str(self.run_id),
                    str(self.job_name),
                    str(self.table_name) if self.table_name else "N/A",
                    start_time,
                    end_time,
                    int(input_rows or 0),
                    int(output_rows or 0),
                    str(status),
                    str(error_msg) if error_msg else None,
                    str(dq_metrics) if dq_metrics else None
                )]
                
                from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
                schema = StructType([
                    StructField("run_id", StringType(), False),
                    StructField("job_name", StringType(), False),
                    StructField("table_name", StringType(), False),
                    StructField("start_time", TimestampType(), False),
                    StructField("end_time", TimestampType(), False),
                    StructField("input_rows", IntegerType(), False),
                    StructField("output_rows", IntegerType(), False),
                    StructField("status", StringType(), False),
                    StructField("error_msg", StringType(), True),
                    StructField("dq_metrics", StringType(), True)
                ])
                audit_df = self.spark.createDataFrame(audit_data, schema)
                audit_df.write \
                    .format("jdbc") \
                    .option("url", METADATA_DB_CONF["url"]) \
                    .option("dbtable", "audit.run_logs") \
                    .option("user", METADATA_DB_CONF["user"]) \
                    .option("password", METADATA_DB_CONF["password"]) \
                    .option("driver", METADATA_DB_CONF["driver"]) \
                    .mode("append") \
                    .save()
            else:
                # psycopg2 write logic
                conn = self._get_psycopg2_conn()
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO audit.run_logs (
                            run_id, job_name, table_name, start_time, end_time, 
                            input_rows, output_rows, status, error_msg, dq_metrics
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        str(self.run_id), str(self.job_name), str(self.table_name), 
                        start_time, end_time, int(input_rows), int(output_rows), 
                        str(status), str(error_msg), str(dq_metrics)
                    ))
                conn.commit()
                conn.close()
                
            self.log(f"AUDIT: Success logging {status} for {self.table_name}")
        except Exception as e:
            self.log(f"AUDIT ERROR: Failed to log {status} for {self.table_name}: {e}")
        finally:
            # Guaranteed flush of buffered logs
            from logger import ParallelLogger
            ParallelLogger.flush()

class DataQuality:
    """Utility class for performing PySpark DQ checks."""
    @staticmethod
    def check_nulls(df, columns):
        """Returns counts of nulls in specified columns."""
        from pyspark.sql import functions as F
        null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in columns]).collect()[0].asDict()
        return null_counts

    @staticmethod
    def check_uniques(df, columns):
        """Returns True if columns form a unique key, else False."""
        if not columns: return True
        total_count = df.count()
        unique_count = df.select(*columns).distinct().count()
        return total_count == unique_count

    @staticmethod
    def check_referential_integrity(child_df, parent_df, join_col):
        """Returns count of orphaned records in child table."""
        orphans = child_df.join(parent_df, join_col, "left_anti").count()
        return orphans

    @staticmethod
    def check_row_count_drift(df, table_name, spark, threshold=0.2):
        """
        Checks if the current row count has drifted significantly from the last successful run.
        Returns (drift_percentage, is_alert).
        """
        current_count = df.count()
        query = f"(SELECT output_rows FROM audit.run_logs WHERE table_name = '{table_name}' AND status = 'SUCCESS' ORDER BY end_time DESC LIMIT 1) AS subquery"
        try:
            prev_df = spark.read \
                .format("jdbc") \
                .option("url", METADATA_DB_CONF["url"]) \
                .option("dbtable", query) \
                .option("user", METADATA_DB_CONF["user"]) \
                .option("password", METADATA_DB_CONF["password"]) \
                .option("driver", METADATA_DB_CONF["driver"]) \
                .load()
            
            prev_count = prev_df.collect()[0]["output_rows"]
            if not prev_count or prev_count == 0: 
                return 0.0, False
                
            drift = abs(current_count - prev_count) / prev_count
            return round(drift, 4), drift > threshold
        except Exception:
            return 0.0, False

    @staticmethod
    def check_schema_mismatch(df, expected_schema):
        """Returns True if schemas match, else False."""
        if not expected_schema: return True
        # Simple comparison of field names and types
        actual_fields = [(f.name, f.dataType) for f in df.schema]
        expected_fields = [(f.name, f.dataType) for f in expected_schema]
        return actual_fields == expected_fields
