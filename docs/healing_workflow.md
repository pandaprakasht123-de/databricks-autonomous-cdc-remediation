# AI Healing Workflow and Human Review

## Overview

The real workflow in this repo is driven by the actual notebooks and data model currently in the project:

1. `02_cdc_stream_simulator.py` writes drifted and clean payloads to the Bronze landing volume
2. `03_bronze_ingestion_and_silver_scd2.py` quarantines mismatched rows
3. `04_langchain_schema_healing_agent.py` diagnoses the records and writes them to the audit log
4. `06_incident_review_applier.py` applies human review decisions
5. `05_gold_curated_and_verification.py` confirms the Gold layer still matches active Silver rows

## How drift is detected

The actual ingestion notebook reviews payload keys against the current Silver schema. It computes:

```python
current_silver_cols = set(spark.table("lakehouse_healing.silver.customers").columns) - {"valid_from", "valid_to", "is_current"}
expected_keys = current_silver_cols if current_silver_cols else {"customer_id", "email", "user_address"}
incoming_keys = set(after_struct.keys())
drifted_keys = incoming_keys - expected_keys
```

Then it performs the following decision logic:

- if `drifted_keys` is not empty, quarantine
- if `customer_id` is missing or null, quarantine
- if the payload is malformed JSON, quarantine
- otherwise, treat the record as valid and insert/update Silver

This is the actual guardrail that drives the AI healing workflow in the repository.

## Quarantine queue behavior

The quarantine table stores the raw payload and the exact drift columns, for example:

```sql
CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.quarantine (
    quarantine_id STRING,
    raw_payload STRING,
    detected_drift_columns STRING,
    quarantined_at TIMESTAMP
);
```

A quarantined row is therefore traceable back to the original source payload and can be reprocessed later after a human-approved schema change.

## AI triage flow

The AI notebook is intentionally narrow and robust:

- it selects only quarantine rows that are not already present in the audit table
- it queries the target schema from the current Silver table
- it sends the payload and schema to Databricks LLM using `ChatDatabricks`
- it requires a structured response through `with_structured_output(...)`
- it writes one audit record per quarantined incident

The exact fields written are:

- `incident_id`
- `quarantine_id`
- `target_table`
- `drift_type`
- `proposed_ddl_patch`
- `confidence_score`
- `llm_reasoning`
- `incident_status`
- `is_applied`
- `assigned_to`
- `resolved_by`
- `created_at`
- `resolved_at`
- `action_history`

This is the actual implementation, and it matches the repo’s real notebook behavior.

## Human-in-the-loop review

The review notebook exposes three actionable states:

### 1. REVIEW_ONLY

No writes to the audit table or Silver layer. This lets a reviewer inspect the incident without changing any data.

### 2. REJECT

The notebook updates the incident status to `REJECTED` and records the reviewer’s decision in `action_history`.

### 3. APPROVE_AND_EXECUTE

This path does the actual remediation:

1. Executes the DDL patch returned by the model
2. Reads the quarantined raw payload
3. Reprocesses the payload into Silver using the updated schema
4. Marks the incident `RESOLVED`
5. Stores the action in `action_history`

The notebook also prevents a second approval from re-running the same already-applied incident.

## Example end-to-end flow

```text
Record arrives with extra field such as shipping_address or country
    -> 03_bronze_ingestion_and_silver_scd2 detects mismatch
    -> raw payload goes to silver.quarantine
    -> 04_langchain_schema_healing_agent analyzes payload and schema
    -> audit row is created with drift_type, patch, and confidence score
    -> reviewer opens 06_incident_review_applier
    -> reviewer chooses APPROVE_AND_EXECUTE
    -> DDL is executed
    -> quarantined record is reprocessed into Silver
    -> 05_gold_curated_and_verification confirms Gold remains aligned
```

## Why the review is manual

The notebooks intentionally make this a human-controlled decision. DDL can change the structure of the data lake and should not be applied automatically in production without review. The repo implements that by requiring a reviewer to choose the action and writing the decision into the audit trail.

## Audit trail and traceability

The audit table is not just a log; it is the operational control plane for the pipeline. It records:

- which quarantine record triggered the issue
- what drift the model believed existed
- the exact SQL patch proposed
- the confidence score returned by the model
- who reviewed the incident
- when it was approved or rejected
- what action history was added

This gives you a full operational trail for troubleshooting and data governance.

## Operational notes from the real notebooks

- The generated payloads in `02_cdc_stream_simulator.py` are intentionally simple JSON objects, not full Debezium envelope payloads.
- The simulator supports `INITIAL_LOAD` and `CONTINUOUS_GROWTH` modes and writes to `/Volumes/lakehouse_healing/bronze/cdc_landing`.
- The stream job writes raw payloads to bronze and uses Delta transaction metadata to protect idempotent writes.
- Gold refresh logic validates the active Silver count against the Gold count and emits a warning on mismatch.

## Related docs

- [architecture.md](architecture.md)
- [setup.md](setup.md)
- [../config/uc_structure.md](../config/uc_structure.md)
