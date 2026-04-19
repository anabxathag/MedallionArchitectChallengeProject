# Phase 1: Source Simulation & Governance

This phase establishes the "Real World" baseline for the platform. It ensures that the data doesn't just come from a static file, but behaves like a live production environment.

## 1. Production Database Simulation
Instead of reading directly from Kaggle CSVs, we seed a **PostgreSQL** instance to act as our "Source of Truth."

- **Watermarking Logic**: Every table is injected with an `updated_at` column.
- **Realistic Data Drift**: By using a database source, we can simulate record updates and inserts, which is critical for testing the incremental logic in later stages.

## 2. Audit & Governance Logic
Before any data moves, the **Audit Engine** must be online. We use a dedicated schema `audit` in PostgreSQL with a table `run_logs`.

### Key Technical Features:
- **Centralized Logging**: A utility function in `config.py` captures the metadata for every job.
- **Lineage Tracking**: Every row in the audit log stores the `run_id`, `layer`, and `table_name`.
- **Row Count Verification**: We capture `rows_read` and `rows_written`. If these numbers drift unexpectedly, the pipeline flags a warning.

### Why this matters:
In a production environment, "Silent Failures" are the biggest risk. By implementing a Governance layer first, we ensure that if a job succeeds with 0 rows, we know about it immediately.
