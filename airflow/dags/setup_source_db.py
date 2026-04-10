import json
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'on_failure_callback': on_failure_callback,
    'on_success_callback': on_success_callback
}

with DAG(
    dag_id="setup_source_db",
    default_args=default_args,
    description="One-time or Manual Seeding and Schema Prep for Source DB",
    schedule_interval=None,
    start_date=datetime(2026, 3, 17),
    catchup=False,
    tags=['olist', 'setup'],
) as dag:

    setup_source = BashOperator(
        task_id="setup_source_db",
        bash_command="spark-submit /opt/spark/jobs/seed_source.py"
    )

    setup_source
