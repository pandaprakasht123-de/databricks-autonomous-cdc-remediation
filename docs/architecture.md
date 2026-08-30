# Lakehouse Healing CDC Pipeline Architecture

## Overview

This repository implements a Databricks-based CDC remediation pattern built around real notebooks that are already in the repo. The flow is intentionally practical and traceable:

- the catalog is created in `01_setup_lakehouse_catalog.py`
- synthetic CDC files are generated in `02_cdc_stream_simulator.py`
- raw and Silver logic is processed in `03_bronze_ingestion_and_silver_scd2.py`
- schema drift is diagnosed in `04_langchain_schema_healing_agent.py`
- the Gold layer is refreshed in `05_gold_curated_and_verification.py`
- human approval happens in `06_incident_review_applier.py`

## Current architecture in the repo

```text
01_setup_lakehouse_catalog
    -> creates catalog + schemas + volumes + base tables

02_cdc_stream_simulator
    -> writes JSON payloads to /Volumes/lakehouse_healing/bronze/cdc_landing

03_bronze_ingestion_and_silver_scd2
    -> reads raw files
    -> writes bronze.customers_cdc_raw
    -> quarantine mismatched records
    -> merges valid records into silver.customers (SCD2)

04_langchain_schema_healing_agent
    -> queries open quarantine records
    -> sends target schema + payload to Databricks LLM
    -> writes incident details to ops.schema_remediation_audit

06_incident_review_applier
    -> lets reviewer choose REVIEW_ONLY / REJECT / APPROVE_AND_EXECUTE
    -> runs DDL when approved
    -> reprocesses quarantine payload
    -> marks incident resolved or failed

05_gold_curated_and_verification
    -> refreshes gold.dim_customers from is_current=true customers
    -> validates row counts against silver
```

## Tables and volumes used by the project

### Catalog

```sql
CREATE CATALOG IF NOT EXISTS lakehouse_healing;
```

### Schemas

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.bronze;
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.silver;
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.gold;
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.ops;
```

### Volumes

The actual notebooks use:

```text
/Volumes/lakehouse_healing/bronze/cdc_landing
/Volumes/lakehouse_healing/bronze/checkpoints
```

Not the earlier `/ops/landing` and `/ops/checkpoints` layout.

## Bronze layer

The raw ingestion notebook reads the landing volume as text files and writes a row per file into `lakehouse_healing.bronze.customers_cdc_raw` with:

- `raw_payload`
- `file_name`
- `ingested_at`

This is a simple append-only raw layer, not a Debezium-envelope layer. The payload is shaped like a JSON record with:

```json
{
  "op": "I",
  "source_ts": "2024-08-30T12:00:00Z",
  "after": {
    "customer_id": 101,
    "email": "user_101@example.com",
    "user_address": "123 Tech Boulevard"
  }
}
```

## Silver layer

The Silver table is created as an SCD2-style dimension with:

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

The stream processing logic does the following:

1. Reads each bronze file payload
2. Parses `after` as a map-like object
3. Checks whether incoming keys match the current Silver schema
4. If keys differ or `customer_id` is missing, writes the payload to `silver.quarantine`
5. Otherwise merges the valid row into Silver using `valid_from`, `valid_to`, and `is_current`

The actual implementation includes out-of-order protection so stale updates do not overwrite the latest active record.

## Quarantine logic

The quarantine table is created as:

```sql
CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.quarantine (
    quarantine_id STRING,
    raw_payload STRING,
    detected_drift_columns STRING,
    quarantined_at TIMESTAMP
)
CLUSTER BY (quarantined_at);
```

The actual logic is intentionally simple:

- if incoming keys differ from the expected Silver columns, quarantine it
- if the payload is malformed JSON, quarantine it
- if `customer_id` is missing, quarantine it

This is the exact drive-in for the AI healing step.

## AI healing and audit

`04_langchain_schema_healing_agent.py` does the following:

- selects quarantine rows with a `LEFT ANTI JOIN` against the audit table
- builds a prompt using the current target schema and the sample payload
- calls a Databricks foundation model using `ChatDatabricks`
- uses `with_structured_output(...)` to enforce a schema-constrained result
- writes rows into `lakehouse_healing.ops.schema_remediation_audit`

The actual audit schema is:

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
);
```

This is the repository’s actual audit model, not the earlier more elaborate nested `ai_diagnosis` and `human_decision` structure.

## Human review

The review notebook, `06_incident_review_applier.py`, exposes the reviewer workflow directly through widgets:

- `select_incident_id`
- `action` with `REVIEW_ONLY`, `APPROVE_AND_EXECUTE`, and `REJECT`
- `user_email`
- `notes`

The action logic is:

- `REVIEW_ONLY` → no writes, just review
- `REJECT` → marks the incident `REJECTED`
- `APPROVE_AND_EXECUTE` → executes the DDL patch, reprocesses the quarantine record, and marks the incident `RESOLVED`

The actual notebook also checks for already-applied incidents and prevents redundant re-execution.

## Gold layer

The Gold refresh notebook merges only the active Silver rows:

```sql
MERGE INTO lakehouse_healing.gold.dim_customers AS target
USING (
    SELECT cur.customer_id,
           cur.email,
           cur.user_address,
           first_seen.account_created_at,
           cur.valid_from AS last_updated_at
    FROM lakehouse_healing.silver.customers cur
    JOIN (
        SELECT customer_id, MIN(valid_from) AS account_created_at
        FROM lakehouse_healing.silver.customers
        GROUP BY customer_id
    ) first_seen
      ON cur.customer_id = first_seen.customer_id
    WHERE cur.is_current = true
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...;
```

Then it compares the row count in Gold to the count of active Silver rows and prints a warning if there is a mismatch.

## Why this differs from the earlier generic template

The real repo is intentionally simpler and more operational than the placeholder architecture:

- the simulator directly writes landing JSON files in a Bronze volume
- drift detection is based on schema-key mismatch rather than a full Debezium schema registry pattern
- human review is a notebook-driven workflow, not a separate production app
- the source of truth is the notebooks themselves, especially `01_setup_lakehouse_catalog.py` and `02_cdc_stream_simulator.py`

## Related docs

- [setup.md](setup.md)
- [healing_workflow.md](healing_workflow.md)
- [../config/uc_structure.md](../config/uc_structure.md)

