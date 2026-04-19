import json
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import sys

def send_discord_notification(context, status="FAILED"):
    """Sends a notification to Discord using a webhook stored in Airflow Variables."""
    webhook_url = Variable.get("DISCORD_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        return

    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = context.get('run_id')
    log_url = ti.log_url

    color = 15158332 if status == "FAILED" else 3066993 # Red vs Green
    emoji = "🚨" if status == "FAILED" else "✅"

    message = {
        "embeds": [{
            "title": f"{emoji} Airflow Alert: {status}",
            "color": color,
            "fields": [
                {"name": "DAG", "value": dag_id, "inline": True},
                {"name": "Task", "value": task_id, "inline": True},
                {"name": "Run ID", "value": run_id, "inline": False},
                {"name": "Logs", "value": f"[View URL]({log_url})", "inline": False}
            ],
            "footer": {"text": "Medallion Data Platform"}
        }]
    }

    try:
        requests.post(webhook_url, json=message, timeout=10)
    except Exception as e:
        print(f"Failed to send notification: {e}")

def on_failure_callback(context):
    send_discord_notification(context, status="FAILED")

def on_success_callback(context):
    send_discord_notification(context, status="SUCCESS")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 17),
    'retries': 1,
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback
}

def reset_streaming_data():
    """Clears previous simulation data and checkpoints for a fresh run."""
    import boto3
    import os
    try:
        s3 = boto3.resource('s3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='password'
        )
        # 1. Clear MinIO data
        bucket_silver = s3.Bucket('silver')
        bucket_silver.objects.filter(Prefix="reviews_stream/").delete()
        
        # 2. Clear checkpoints
        bucket_bronze = s3.Bucket('bronze')
        bucket_bronze.objects.filter(Prefix="checkpoints/reviews/").delete()
        
        # 3. Clear the Shutdown Signal File
        signal_file = "/opt/airflow/logs/stop_simulation.signal"
        if os.path.exists(signal_file):
            os.remove(signal_file)
            print(f"🗑️ Removed old shutdown signal: {signal_file}")
            
        print("🧹 Successfully cleared previous simulation state.")
    except Exception as e:
        print(f"⚠️ Warning: Failed to reset data: {e}")

def create_kafka_topic():
    from confluent_kafka.admin import AdminClient, NewTopic
    
    admin = AdminClient({'bootstrap.servers': 'kafka:9092'})
    topic_name = 'olist_reviews_stream'
    
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    fs = admin.create_topics([new_topic])
    
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"Topic {topic} already exists")
            else:
                print(f"Failed to create topic {topic}: {e}")
                sys.exit(1)


with DAG(
    'streaming_pipeline',
    default_args=default_args,
    description='Real-time Review Streaming Pipeline (Kafka + Spark)',
    schedule_interval=None,
    catchup=False,
    tags=['olist', 'streaming', 'kafka'],
) as dag:

    # 0. Reset previous simulation
    reset_sim = PythonOperator(
        task_id='reset_streaming_data',
        python_callable=reset_streaming_data
    )

    # 1. Initialize Kafka Topic via Python Admin Client
    init_topic = PythonOperator(
        task_id='init_kafka_topic',
        python_callable=create_kafka_topic
    )

    # 2. Start Spark Consumer (The Listener)
    # This job reads from Kafka and writes Parquet to Silver
    consumer_task = BashOperator(
        task_id='review_consumer_task',
        bash_command="""
            cd /opt/airflow && \
            nohup spark-submit \
                /opt/spark/jobs/kafka_consumer_reviews.py \
                --run_id {{ run_id }} \
                --discord_webhook '{{ var.value.DISCORD_WEBHOOK_URL }}' > /opt/airflow/logs/spark_streaming.log 2>&1 &
        """
    )

    # 3. Start Kafka Producer (The Simulator)
    # This job sends 100 random reviews to the Kafka topic
    producer_task = BashOperator(
        task_id='review_producer_task',
        bash_command="python3 /opt/spark/jobs/kafka_producer_reviews.py"
    )

    # Step-by-Step Execution
    # Note: Consumer and Producer run together. Consumer stops itself 
    # when Producer creates the stop signal file.
    reset_sim >> init_topic

    # Step 2 & 3: Run Producer and Consumer in Parallel
    init_topic >> consumer_task
    init_topic >> producer_task
