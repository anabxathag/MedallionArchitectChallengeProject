from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
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
