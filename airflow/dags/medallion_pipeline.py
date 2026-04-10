import json
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

def send_discord_notification(context, status="FAILED"):
    """Sends a notification to Discord using a webhook stored in Airflow Variables."""
    webhook_url = Variable.get("DISCORD_WEBHOOK_URL", default_var=None)
    print(f"DEBUG: Discord Webhook Found: {'Yes' if webhook_url else 'No'}")
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

# --- Holiday Fetching Logic (Real-World API Enrichment) ---
def fetch_br_holidays(**context):
    """Fetches holidays from API and returns them as a JSON string for Spark."""
    import requests
    import json
    years = [2016, 2017, 2018, 2019, 2020]
    all_holidays = []
    for year in years:
        try:
            url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/BR"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                all_holidays.extend([h['date'] for h in resp.json()])
        except Exception as e:
            print(f"Warning: Failed to fetch holidays for {year}: {e}")
    
    holidays_json = json.dumps(sorted(list(set(all_holidays))))
    return holidays_json

def on_failure_callback(context):
    send_discord_notification(context, status="FAILED")

def on_success_callback(context):
    # Optional: only notify on success for critical tasks if desired
    send_discord_notification(context, status="SUCCESS")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=default_args,
    description="E-commerce Medallion Pipeline",
    schedule_interval=None,
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=['olist', 'medallion'],
) as dag:

    fetch_holidays = PythonOperator(
        task_id="fetch_holidays",
        python_callable=fetch_br_holidays
    )

    # bronze = BashOperator(
    #     task_id="bronze_ingestion",
    #     bash_command="spark-submit /opt/spark/jobs/bronze_ingest.py --run_id {{ run_id }}"
    # )

    # silver = BashOperator(
    #     task_id="silver_processing",
    #     bash_command="spark-submit /opt/spark/jobs/silver_transform.py --run_id {{ run_id }}"
    # )

    gold = BashOperator(
        task_id="gold_modeling",
        bash_command="spark-submit /opt/spark/jobs/gold_model.py --run_id {{ run_id }} --holidays '{{ ti.xcom_pull(task_ids=\"fetch_holidays\") }}'"
    )

    export_to_bq = BashOperator(
        task_id="export_to_bigquery",
        bash_command="""
            python3 \
                /opt/spark/jobs/export_gold_to_bigquery.py \
                --run_id {{ run_id }} \
                --gcp_project {{ var.value.GCP_PROJECT_ID }} \
                --bq_dataset {{ var.value.BQ_DATASET }}
        """
    )

    # fetch_holidays >> bronze >> silver >> gold >> export_to_bq
    fetch_holidays >> gold >> export_to_bq
