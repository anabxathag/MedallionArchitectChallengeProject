# Streaming Lifecycle & Coordination Logic

This document details the coordinated shutdown and resilience mechanisms used in the real-time review streaming pipeline.

---

## 🏗️ The Problem: Streaming in a Simulation
In a production environment, Spark Streaming jobs typically run infinitely. However, in this project's simulation mode (Kaggle-to-Kafka), we need the Spark Consumer to finish gracefully once the Kafka Producer has finished sending its sample batch.

To solve this without manually killing processes, we implemented a **Self-Managing Lifecycle**.

---

## 📡 1. Signal-File Orchestration
We use a shared filesystem signal to communicate process state between the Python Producer and the Spark Consumer.

- **Mechanism**: A file located at `/opt/airflow/logs/stop_simulation.signal`.
- **Workflow**:
    1.  **Airflow** starts both the Producer and Consumer tasks in parallel.
    2.  The **Producer** sends its records to Kafka.
    3.  Once the Producer is done, it writes the **actual number of records sent** into the signal file (e.g., `100`).
    4.  The **Consumer** constantly monitors for the existence of this file.

---

## 📊 2. Dynamic Batch Verification
The consumer doesn't just stop when it sees the file; it verifies the data integrity first.

- **Logic**: The consumer reads the integer value inside the signal file.
- **Matching**: It compares its `total_input` count (tracked via a `StreamingQueryListener`) against the target number in the file.
- **Graceful Stop**: If `total_input >= target`, the consumer calls `query.stop()` and breaks its monitoring loop.

---

## 🛡️ 3. Resilient Patience Timeout
A critical challenge arises when using `.option("startingOffsets", "latest")`. If the consumer starts slightly after the producer, it may miss the first few records and never reach the target count.

- **The Solution**: A **30-second Patience Timeout**.
- **Implementation**:
    - If the signal file is found but the target count isn't met, the consumer enters a "Patience" phase.
    - It tracks the time since it last processed a record (`last_progress_time`).
    - If **30 seconds** pass with no new records arriving, the consumer assumes the remaining records are inaccessible/missed.
    - It logs a warning and performs a **forced graceful shutdown** to ensure the Airflow task completes rather than hanging forever.

---

## 📈 4. Audit & Observability
This coordination ensures that the `job_audit` table in PostgreSQL always contains accurate final metrics:
- **Total Input/Output**: Correctly reflects what Spark actually processed.
- **Job Status**: Properly transitions from `RUNNING` to `SUCCESS` instead of staying stuck or being marked as `FAILED` by an Airflow timeout.

---
*Technical Architecture for the Medallion Data Platform Streaming Layer.*
