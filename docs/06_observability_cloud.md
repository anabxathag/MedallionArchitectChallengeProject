# Phase 6: Cloud Export & Observability

The final phase bridges our local Data Platform to the cloud and ensures that the entire system is "Observable" for the operations team.

## 1. Hybrid Cloud Strategy (BigQuery)
We use a **Hybrid Cloud** approach to optimize costs and performance.

- **Local Processing**: Bronze and Silver (the "Dirty Work") stay in the inexpensive local MinIO data lake.
- **Cloud Analytics**: Only the final, high-value Gold Star Schema is synced to **Google BigQuery**.
- **BigQuery Write Logic**: We use the `indirect` write method via the GCS connector. This allows Spark to upload Parquet blocks to Google Cloud Storage as a buffer, ensuring high-speed transfers without hitting BigQuery's API limits.

## 2. Advanced Observability & Alerting
In production, a pipeline that fails silently is worse than no pipeline at all.

- **Discord Integration**: We built a custom Failure Handler in Airflow.
- **Payload**: Every failure triggers a Discord webhook that sends:
    - **DAG/Task ID**: Exactly what broke.
    - **Run ID**: Helps find the logs quickly.
    - **Error Context**: A snippet of the Python exception.
- **Success Notifications**: We also log successes to provide a "Heartbeat" for the system, confirming the daily run was healthy.

## 3. Data Drift Monitoring
Our "Deep Dive" logic doesn't just check for errors; it checks for **Drift**.

- **Row Count Analysis**: The final task in the Gold layer compares the number of orders in Gold vs the number of orders in Bronze.
- **Alert Trigger**: If the drift exceeds a 1% threshold (which could indicate a join logic error or data loss), the pipeline sends a "High Priority" alert to the engineering channel.
