# Databricks notebook source
# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC -- 1. Create Catalog and Dedicated Schemas
# MAGIC CREATE CATALOG IF NOT EXISTS lakehouse_healing;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS lakehouse_healing.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS lakehouse_healing.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS lakehouse_healing.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS lakehouse_healing.ops;
# MAGIC
# MAGIC -- 2. Create Volumes (Data Landing & Isolated Checkpoints)
# MAGIC CREATE VOLUME IF NOT EXISTS lakehouse_healing.bronze.cdc_landing;
# MAGIC CREATE VOLUME IF NOT EXISTS lakehouse_healing.bronze.checkpoints;
# MAGIC -- NOTE: checkpoint subfolders are created at runtime by each stream, e.g.
# MAGIC --   /Volumes/lakehouse_healing/bronze/checkpoints/bronze_ingest/
# MAGIC --   /Volumes/lakehouse_healing/bronze/checkpoints/silver_merge/
# MAGIC -- Do not point two streaming queries at the same checkpoint path.
# MAGIC
# MAGIC -- 3. Bronze Raw Ingestion Table (Append-only immutable log)
# MAGIC CREATE TABLE IF NOT EXISTS lakehouse_healing.bronze.customers_cdc_raw (
# MAGIC     raw_payload STRING,
# MAGIC     file_name STRING,
# MAGIC     ingested_at TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY (ingested_at);
# MAGIC
# MAGIC -- 4. Silver Customer SCD Type 2 Target Table
# MAGIC CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.customers (
# MAGIC     customer_id INT,
# MAGIC     email STRING,
# MAGIC     user_address STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current BOOLEAN
# MAGIC );
# MAGIC
# MAGIC -- 5. Silver Quarantine Dead-Letter Queue
# MAGIC CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.quarantine (
# MAGIC     quarantine_id STRING,
# MAGIC     raw_payload STRING,
# MAGIC     detected_drift_columns STRING,
# MAGIC     quarantined_at TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY (quarantined_at);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS lakehouse_healing.ops.schema_remediation_audit (
# MAGIC     incident_id STRING,
# MAGIC     quarantine_id STRING,
# MAGIC     target_table STRING,
# MAGIC     drift_type STRING,
# MAGIC     proposed_ddl_patch STRING,
# MAGIC     confidence_score DOUBLE,
# MAGIC     llm_reasoning STRING,
# MAGIC     incident_status STRING DEFAULT 'OPEN',
# MAGIC     is_applied BOOLEAN DEFAULT FALSE,
# MAGIC     assigned_to STRING,
# MAGIC     resolved_by STRING,
# MAGIC     created_at TIMESTAMP,
# MAGIC     resolved_at TIMESTAMP,
# MAGIC     action_history ARRAY<STRUCT<
# MAGIC         action_type: STRING,
# MAGIC         performed_by: STRING,
# MAGIC         action_timestamp: TIMESTAMP,
# MAGIC         notes: STRING
# MAGIC     >>
# MAGIC )
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC
# MAGIC -- 7. Gold Curated Dimension Table
# MAGIC CREATE TABLE IF NOT EXISTS lakehouse_healing.gold.dim_customers (
# MAGIC     customer_id INT,
# MAGIC     email STRING,
# MAGIC     user_address STRING,
# MAGIC     account_created_at TIMESTAMP,
# MAGIC     last_updated_at TIMESTAMP
# MAGIC );