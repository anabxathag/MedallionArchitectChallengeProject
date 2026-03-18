from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="spark-submit /opt/spark/jobs/bronze_ingest.py"
    )

    silver = BashOperator(
        task_id="silver_processing",
        bash_command="spark-submit /opt/spark/jobs/silver_transform.py"
    )

    gold = BashOperator(
        task_id="gold_modeling",
        bash_command="spark-submit /opt/spark/jobs/gold_model.py"
    )

    to_postgres = BashOperator(
        task_id="to_postgres",
        bash_command="spark-submit /opt/spark/jobs/to_warehouse.py"
    )

    bronze >> silver >> gold >> to_postgres