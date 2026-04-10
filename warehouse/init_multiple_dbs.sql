-- Initializing three logical databases inside a single container for professional separation
CREATE DATABASE airflow_db;
CREATE DATABASE audit_db;

-- Initialize Audit Schema (Used by Spark for watermarks/logs)
\c audit_db
CREATE SCHEMA IF NOT EXISTS audit;