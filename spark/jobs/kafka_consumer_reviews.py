from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.streaming import StreamingQueryListener
from config import get_spark_session, get_run_id, get_discord_webhook, KAFKA_BOOTSTRAP_SERVERS, REVIEWS_TOPIC, REVIEWS_STREAM_SCHEMA, JobTracker
import sys
import requests
import signal
import time
import os

# Configuration
SILVER_PATH = "s3a://silver/reviews_stream"
CHECKPOINT_PATH = "s3a://bronze/checkpoints/reviews"
STOP_SIGNAL_FILE = "/opt/airflow/logs/stop_simulation.signal"

# Global counter for the listener
class ReviewMetricsListener(StreamingQueryListener):
    def __init__(self):
        self.total_input = 0
        self.total_output = 0

    def onQueryStarted(self, event):
        print(f"📡 [LISTENER] Query started: {event.id}")

    def onQueryProgress(self, event):
        self.total_input += event.progress.numInputRows
        self.total_output += event.progress.numInputRows
        if event.progress.numInputRows > 0:
            print(f"📊 [LISTENER] Micro-batch processed: {event.progress.numInputRows} rows (Total: {self.total_input})")

    def onQueryTerminated(self, event):
        print(f"🛑 [LISTENER] Query terminated: {event.id}")

def send_discord_alert(run_id, error_msg):
    """Sends a failure alert directly from the Spark job."""
    webhook_url = get_discord_webhook()
    if not webhook_url:
        return

    message = {
        "embeds": [{
            "title": "🚨 Spark Streaming Job Failed",
            "color": 15158332,
            "fields": [
                {"name": "Job", "value": "kafka_consumer_reviews", "inline": True},
                {"name": "Run ID", "value": run_id, "inline": True},
                {"name": "Error", "value": f"```{error_msg[:1000]}```", "inline": False}
            ],
            "footer": {"text": "Medallion Data Platform | Spark side alert"}
        }]
    }
    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception as e:
        print(f"Failed to send Discord alert: {e}")

def main():
    """
    Spark Structured Streaming Job:
    Subscribes to Kafka 'olist_reviews_stream' and writes to Silver-tier Parquet.
    """
    run_id = get_run_id()
    spark = get_spark_session(f"Kafka-Consumer-{run_id}", include_kafka=True)
    tracker = JobTracker(spark, "kafka_consumer_reviews", run_id)
    
    # Register the Metrics Listener
    metrics = ReviewMetricsListener()
    spark.streams.addListener(metrics)

    # Graceful Shutdown Handling (Soft Stop)
    def handle_shutdown(signum, frame):
        print(f"🛑 [SHUTDOWN] Signal {signum} received. Closing audit logs...")
        tracker.end_job("SUCCESS", metrics.total_input, metrics.total_output)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    print(f"🚀 Starting Structured Streaming from Kafka...")
    print(f"  Topic: {REVIEWS_TOPIC}")
    
    tracker.start_job("olist_reviews_stream")

    try:
        # 1. Read Stream from Kafka
        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", REVIEWS_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # 2. Extract and Parse JSON value
        df_parsed = df_raw.select(
            from_json(col("value").cast("string"), REVIEWS_STREAM_SCHEMA).alias("data"),
            col("timestamp").alias("_kafka_received_at")
        ).select("data.*", "_kafka_received_at")

        # 3. Bronze-to-Silver Transformations (Cleaning)
        df_cleansed = df_parsed \
            .withColumn("_silver_processed_at", current_timestamp()) \
            .fillna({
                "review_comment_title": "no_title", 
                "review_comment_message": "no_message"
            })

        # 4. Write to S3A (MinIO Silver Layer)
        query = df_cleansed.writeStream \
            .format("parquet") \
            .option("path", SILVER_PATH) \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .outputMode("append") \
            .queryName("reviews_stream_to_silver") \
            .start()

        print("✅ [CONSUMER] Consumer is live. Monitoring for stop signal...")
        
        # Monitor for the shutdown signal file from the producer
        last_progress_time = time.time()
        last_input_count = 0
        patience_timeout = 30 # seconds to wait without progress after signal is found
        
        while True:
            current_input = metrics.total_input
            
            # Check if we made progress
            if current_input > last_input_count:
                last_input_count = current_input
                last_progress_time = time.time()

            if os.path.exists(STOP_SIGNAL_FILE):
                try:
                    with open(STOP_SIGNAL_FILE, 'r') as f:
                        target_rows = int(f.read().strip())
                except Exception:
                    target_rows = 100 # Fallback if file is unreadable or non-integer
                
                if current_input >= target_rows:
                    print(f"🚩 [CONSUMER] Target reached ({current_input}/{target_rows}). Shutting down gracefully...")
                    time.sleep(5) # Final safety buffer
                    query.stop()
                    break
                
                # If signal found but target not reached, check for timeout
                time_since_last_progress = time.time() - last_progress_time
                if time_since_last_progress > patience_timeout:
                    print(f"⚠️ [CONSUMER] Signal found but no progress for {patience_timeout}s. "
                          f"Forcing shutdown (Processed {current_input}/{target_rows}).")
                    query.stop()
                    break
                else:
                    print(f"⏳ [CONSUMER] Signal found. Progress: {current_input}/{target_rows}. "
                          f"Waiting for remaining records ({int(patience_timeout - time_since_last_progress)}s patience remaining)...")
            
            # Check if query itself crashed
            if not query.isActive:
                print("⚠️ [CONSUMER] Query is no longer active.")
                break
                
            time.sleep(5) # Check every 5 seconds
            
        tracker.end_job("SUCCESS", metrics.total_input, metrics.total_output)
        print(f"✅ [CONSUMER] Shutdown Complete. Total Processed: {metrics.total_input}")

    except Exception as e:
        error_msg = str(e)
        print(f"❌ [CONSUMER] Job Failed: {error_msg}")
        tracker.end_job("FAILED", metrics.total_input, metrics.total_output, error_msg=error_msg)
        send_discord_alert(run_id, error_msg)
        sys.exit(1)

if __name__ == "__main__":
    main()
