# Lakehouse Setup Notebook

This repo's setup logic is implemented in the actual Databricks notebook `01_setup_lakehouse_catalog.py`. The notebook creates the catalog, schemas, volumes, and initial tables used by the rest of the pipeline.

## Catalog and schemas

```sql
CREATE CATALOG IF NOT EXISTS lakehouse_healing;

CREATE SCHEMA IF NOT EXISTS lakehouse_healing.bronze;
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.silver;
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.gold;
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.ops;
```

## Volumes

The actual notebook creates the landing and checkpoint volumes under the Bronze schema:

```sql
CREATE VOLUME IF NOT EXISTS lakehouse_healing.bronze.cdc_landing;
CREATE VOLUME IF NOT EXISTS lakehouse_healing.bronze.checkpoints;
```

The notebook also notes that checkpoint subfolders are created at runtime, for example:

```text
/Volumes/lakehouse_healing/bronze/checkpoints/bronze_ingest/
/Volumes/lakehouse_healing/bronze/checkpoints/silver_stream/
```

## Tables created by the setup notebook

### Bronze raw table

```sql
CREATE TABLE IF NOT EXISTS lakehouse_healing.bronze.customers_cdc_raw (
    raw_payload STRING,
    file_name STRING,
    ingested_at TIMESTAMP
)
CLUSTER BY (ingested_at);
```

### Silver customer table

```sql
CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.customers (
    customer_id INT,
    email STRING,
    user_address STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);
```

### Silver quarantine table

```sql
CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.quarantine (
    quarantine_id STRING,
    raw_payload STRING,
    detected_drift_columns STRING,
    quarantined_at TIMESTAMP
)
CLUSTER BY (quarantined_at);
```

### Ops audit table

```sql
CREATE TABLE IF NOT EXISTS lakehouse_healing.ops.schema_remediation_audit (
    incident_id STRING,
    quarantine_id STRING,
    target_table STRING,
    drift_type STRING,
    proposed_ddl_patch STRING,
    confidence_score DOUBLE,
    llm_reasoning STRING,
    incident_status STRING DEFAULT 'OPEN',
    is_applied BOOLEAN DEFAULT FALSE,
    assigned_to STRING,
    resolved_by STRING,
    created_at TIMESTAMP,
    resolved_at TIMESTAMP,
    action_history ARRAY<STRUCT<
        action_type: STRING,
        performed_by: STRING,
        action_timestamp: TIMESTAMP,
        notes: STRING
    >>
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
```

### Gold dimension table

```sql
CREATE TABLE IF NOT EXISTS lakehouse_healing.gold.dim_customers (
    customer_id INT,
    email STRING,
    user_address STRING,
    account_created_at TIMESTAMP,
    last_updated_at TIMESTAMP
);
```

## Deployment notes

1. Run the setup notebook in a Databricks SQL or Python notebook attached to a cluster with Unity Catalog enabled.
2. The landing path used by the simulator is: `/Volumes/lakehouse_healing/bronze/cdc_landing`
3. The checkpoint path used by streaming jobs is: `/Volumes/lakehouse_healing/bronze/checkpoints`
4. The rest of the project assumes the tables and volumes above already exist.

## Related files

- [config/uc_structure.md](../config/uc_structure.md)
- [docs/architecture.md](architecture.md)
- [docs/healing_workflow.md](healing_workflow.md)
