from pyspark.sql import SparkSession

# --- Global Paths ---
EXTRA_JARS_PATH = "/opt/spark/extra_jars"
POSTGRES_JAR = f"{EXTRA_JARS_PATH}/postgresql-42.7.4.jar"
HADOOP_AWS_JAR = f"{EXTRA_JARS_PATH}/hadoop-aws-3.4.1.jar"
AWS_SDK_JAR = f"{EXTRA_JARS_PATH}/bundle-2.23.19.jar"

# --- JAR Inclusion Strategy ---
# Spark 4.0.2 requires jars for the session, plus extraClassPath for the driver/executors
# to ensure S3AFileSystem classes are available during initial Hadoop FS initialization.
ALL_JARS = f"{POSTGRES_JAR},{HADOOP_AWS_JAR},{AWS_SDK_JAR}"
CP_JARS = f"{POSTGRES_JAR}:{HADOOP_AWS_JAR}:{AWS_SDK_JAR}"

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

def get_spark_session(app_name):
    """
    Creates a SparkSession with global JARs and S3A configurations.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", ALL_JARS) \
        .config("spark.driver.extraClassPath", CP_JARS) \
        .config("spark.executor.extraClassPath", CP_JARS) \
        .config("spark.sql.shuffle.partitions", "1")
    
    for key, value in S3A_CONF.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
