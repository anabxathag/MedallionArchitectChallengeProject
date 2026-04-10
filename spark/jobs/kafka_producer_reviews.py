import time
import json
import polars as pl
from confluent_kafka import Producer
import os
import sys

# Ensure we can import config relative to the job directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import JobTracker, get_run_id

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "olist_reviews_stream"
RAW_DATA_PATH = "/raw/olist_order_reviews_dataset.csv"
STOP_SIGNAL_FILE = "/opt/airflow/logs/stop_simulation.signal"

def delivery_report(err, msg):
    if err is not None:
        print(f"  ❌ Message delivery failed: {err}")

def main():
    run_id = get_run_id()
    # We use None for spark session since this is a sidecar Python script
    tracker = JobTracker(None, "streaming_producer", run_id)
    
    print(f"🚀 Starting Kafka Producer (Review Simulator) | Run ID: {run_id}")
    
    # Initialize Producer
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'review_simulator'
    }
    
    try:
        p = Producer(conf)
    except Exception as e:
        print(f"❌ Failed to create producer: {e}")
        tracker.end_job("FAILED", error_msg=str(e))
        sys.exit(1)
    
    # Load sample data
    if not os.path.exists(RAW_DATA_PATH):
        err = f"Raw data not found at {RAW_DATA_PATH}"
        print(f"❌ ERROR: {err}")
        tracker.end_job("FAILED", error_msg=err)
        return

    print(f"Reading sample reviews from {RAW_DATA_PATH}...")
    try:
        # Use Polars to read 100 random rows
        df = pl.read_csv(RAW_DATA_PATH).sample(n=100)
        reviews = df.to_dicts()
    except Exception as e:
        print(f"❌ Failed to read data: {e}")
        tracker.end_job("FAILED", error_msg=str(e))
        return
    
    total_records = len(reviews)
    tracker.start_job("olist_reviews_stream")
    print(f"Feeding {total_records} reviews to topic '{TOPIC}'...")
    
    sent_count = 0
    for i, row in enumerate(reviews):
        try:
            # Serialize and send
            p.produce(TOPIC, json.dumps(row).encode('utf-8'), callback=delivery_report)
            p.poll(0)
            
            sent_count += 1
            if sent_count % 10 == 0:
                print(f"  [PRODUCER] Progress: {sent_count}/{total_records} sent ({(sent_count/total_records)*100:.0f}%)")
            
            time.sleep(0.5) # Speed up simulation to 0.5s delay
            
        except KeyboardInterrupt:
            print("\n🚨 [PRODUCER] Stopped by user.")
            break
        except Exception as e:
            print(f"  [ERROR] Failed to send {row.get('review_id')}: {e}")
            
    p.flush()
    print(f"✅ [PRODUCER] Simulation Complete. Total sent: {sent_count}")
    
    # Create the shutdown signal file for the consumer
    try:
        with open(STOP_SIGNAL_FILE, 'w') as f:
            f.write(str(sent_count))
        print(f"🚩 [PRODUCER] Shutdown signal created at {STOP_SIGNAL_FILE}")
    except Exception as e:
        print(f"⚠️ [PRODUCER] Could not create signal file: {e}")

    tracker.end_job("SUCCESS", input_rows=sent_count, output_rows=sent_count)

if __name__ == "__main__":
    main()
