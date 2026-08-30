# Unity Catalog Structure Reference

## Catalog: `lakehouse_healing`

Complete Unity Catalog structure for the Lakehouse Healing CDC Pipeline.

### Schema: `bronze`

Raw ingestion layer for CDC events.

**Table: `customers_cdc_raw`**
- **Format**: Delta
- **Source**: Auto Loader from landing volume
- **Schema**:
  - `op` (string): CDC operation type (C=Create, U=Update, D=Delete)
  - `before` (map): Previous row values (for updates)
  - `after` (map): New/current row values
  - `timestamp_ms` (long): Event timestamp in milliseconds
  - `txn_id` (string): Transaction ID (idempotency key)
  - `source_ts` (timestamp): When record was captured
  - `_metadata.ingestion_time` (timestamp): When Auto Loader ingested it

**Partitioning**: None (raw layer)

---

### Schema: `silver`

Conformed layer with SCD2 history and quality filtering.

**Table: `customers`**
- **Format**: Delta
- **Type**: Slowly Changing Dimension Type 2 (SCD2)
- **Key**: `customer_id` (business key)
- **Schema**:
  - `customer_id` (long): Customer identifier
  - `customer_name` (string): Customer name
  - `account_created_at` (timestamp): Account creation date (fixed attribute)
  - `address` (string): Customer address
  - `is_current` (boolean): SCD2 current flag (1 = active, 0 = historical)
  - `valid_from` (timestamp): When this record version became active
  - `valid_to` (timestamp): When this record version expires (null if current)
  - `last_updated_at` (timestamp): Last transaction timestamp
  - `dbt_updated_at` (timestamp): When Silver merge executed
  - `_txn_id` (string): Last transaction ID applied

**Clustering**: `customer_id` (Liquid Clustering for SCD2 lookups)

**Guarantees**:
- No duplicate active rows per `customer_id`
- Full change history preserved
- Idempotent with transaction IDs

**Table: `quarantine`**
- **Format**: Delta
- **Purpose**: Holds records that failed schema validation or contain drift
- **Schema**:
  - `quarantine_id` (string): Unique identifier
  - `source_json` (string): Original CDC JSON payload
  - `error_type` (string): Type of error (schema_drift, malformed, validation_failed)
  - `error_message` (string): Detailed error description
  - `quarantined_at` (timestamp): When record was quarantined
  - `status` (string): pending, healed, rejected, review_only
  - `ai_diagnosis_id` (string): Reference to ops.schema_remediation_audit

**Partitioning**: `quarantined_at` (date)

---

### Schema: `gold`

Curated analytics layer with active dimension data.

**Table: `dim_customers`**
- **Format**: Delta
- **Purpose**: Production-ready customer dimension for BI/analytics
- **Source**: Silver `customers` (filtered to `is_current = true`)
- **Refresh**: Fully recomputed on each pipeline run (idempotent)
- **Schema**:
  - `customer_id` (long): Customer key
  - `customer_name` (string): Customer name
  - `account_created_at` (timestamp): Account creation date
  - `address` (string): Current address
  - `last_updated_at` (timestamp): Last change to customer record
  - `row_count_check` (long): Row count for validation (debug field)

**Reconciliation**:
- Row count logged before and after MERGE in `ops.schema_remediation_audit`
- Any mismatch triggers alert in job logs

---

### Schema: `ops`

Operational and audit layer for monitoring and human review.

**Table: `schema_remediation_audit`**
- **Format**: Delta
- **Purpose**: Comprehensive log of all schema drift incidents, AI diagnoses, and human decisions
- **Schema**:
  - `incident_id` (string): Unique identifier
  - `quarantine_id` (string): Reference to the quarantined record
  - `error_type` (string): drift, malformed, validation_failed
  - `error_message` (string): Description of the issue
  - `discovered_at` (timestamp): When error was detected
  - `ai_diagnosis` (struct): AI-generated diagnosis
    - `drift_type` (string): Type of schema change detected
    - `confidence_score` (double): Confidence (0.0-1.0)
    - `proposed_ddl` (string): SQL statement to fix the schema
    - `reasoning` (string): LLM reasoning for the proposal
    - `diagnosed_at` (timestamp): When diagnosis was generated
  - `human_decision` (struct): Reviewer's action
    - `decision` (string): APPROVE_AND_EXECUTE, REJECT, REVIEW_ONLY, or NULL (pending)
    - `reviewer` (string): Databricks user email
    - `reviewed_at` (timestamp): When decision was made
    - `review_notes` (string): Additional comments from reviewer
  - `execution_result` (struct): DDL execution details (if approved)
    - `status` (string): SUCCESS, FAILED, SKIPPED
    - `executed_at` (timestamp): When DDL ran
    - `error_log` (string): Any DDL errors
  - `reprocessing_result` (struct): Quarantine record reprocessing
    - `status` (string): SUCCESS, FAILED
    - `records_moved` (long): How many rows moved from quarantine to silver
    - `reprocessed_at` (timestamp): When reprocessing occurred
  - `status` (string): pending, healed, rejected, review_only, failed
  - `action_history` (array): All actions taken (for full audit trail)
  - `created_at` (timestamp): Record creation
  - `updated_at` (timestamp): Last update

**Partitioning**: `discovered_at` (date, for performance on large audits)

---

## Volumes

### `/Volumes/lakehouse_healing/ops/landing`

**Purpose**: External landing zone for CDC JSON files

**Format**: Debezium-style JSON with envelope:
```json
{
  "schema": { /* schema info */ },
  "payload": {
    "before": { /* previous values */ },
    "after": { /* new values */ },
    "source": { "version": "...", "connector": "..." },
    "op": "u|c|d",
    "ts_ms": 1234567890000
  }
}
```

**File Format**: JSONL (one event per line)

**Directory Structure**:
```
/Volumes/lakehouse_healing/ops/landing/
├── customers/
│   ├── 2024-08-30/
│   │   ├── customers_001.jsonl
│   │   ├── customers_002.jsonl
│   │   └── ...
│   └── 2024-08-31/
│       └── ...
```

### `/Volumes/lakehouse_healing/ops/checkpoints`

**Purpose**: Spark Structured Streaming checkpoints

**Directory Structure**:
```
/Volumes/lakehouse_healing/ops/checkpoints/
├── bronze_ingestion/
│   ├── offsets
│   ├── sources
│   └── ...
└── silver_scd2_merge/
    ├── offsets
    ├── sources
    └── ...
```

---

## Setup SQL Commands

Create the entire structure:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS lakehouse_healing
COMMENT "Lakehouse for CDC remediation with AI healing";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS lakehouse_healing.bronze
COMMENT "Raw CDC data ingested via Auto Loader";

CREATE SCHEMA IF NOT EXISTS lakehouse_healing.silver
COMMENT "Conformed CDC data with SCD2 history";

CREATE SCHEMA IF NOT EXISTS lakehouse_healing.gold
COMMENT "Curated analytics tables";

CREATE SCHEMA IF NOT EXISTS lakehouse_healing.ops
COMMENT "Operational and audit tables";

-- Create volumes
CREATE VOLUME IF NOT EXISTS lakehouse_healing.ops.landing
COMMENT "Landing zone for CDC JSON files";

CREATE VOLUME IF NOT EXISTS lakehouse_healing.ops.checkpoints
COMMENT "Spark Structured Streaming checkpoints";

-- Create bronze.customers_cdc_raw
CREATE TABLE IF NOT EXISTS lakehouse_healing.bronze.customers_cdc_raw (
  op STRING,
  before MAP<STRING, STRING>,
  after MAP<STRING, STRING>,
  timestamp_ms LONG,
  txn_id STRING,
  source_ts TIMESTAMP,
  _metadata STRUCT<ingestion_time: TIMESTAMP>
)
USING DELTA
COMMENT "Raw Debezium-style CDC events from Auto Loader"
LOCATION '/user/hive/warehouse/lakehouse_healing.db/bronze/customers_cdc_raw';

-- Create silver.customers (SCD2)
CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.customers (
  customer_id LONG,
  customer_name STRING,
  account_created_at TIMESTAMP,
  address STRING,
  is_current BOOLEAN DEFAULT true,
  valid_from TIMESTAMP DEFAULT current_timestamp(),
  valid_to TIMESTAMP,
  last_updated_at TIMESTAMP,
  dbt_updated_at TIMESTAMP DEFAULT current_timestamp(),
  _txn_id STRING
)
USING DELTA
CLUSTER BY customer_id
COMMENT "Customer dimension with SCD2 history";

-- Create silver.quarantine
CREATE TABLE IF NOT EXISTS lakehouse_healing.silver.quarantine (
  quarantine_id STRING,
  source_json STRING,
  error_type STRING,
  error_message STRING,
  quarantined_at TIMESTAMP DEFAULT current_timestamp(),
  status STRING DEFAULT 'pending',
  ai_diagnosis_id STRING
)
USING DELTA
PARTITIONED BY (DATE(quarantined_at))
COMMENT "Records that failed schema validation or contain drift";

-- Create gold.dim_customers
CREATE TABLE IF NOT EXISTS lakehouse_healing.gold.dim_customers (
  customer_id LONG,
  customer_name STRING,
  account_created_at TIMESTAMP,
  address STRING,
  last_updated_at TIMESTAMP,
  row_count_check LONG
)
USING DELTA
COMMENT "Production-ready active customer dimension";

-- Create ops.schema_remediation_audit
CREATE TABLE IF NOT EXISTS lakehouse_healing.ops.schema_remediation_audit (
  incident_id STRING,
  quarantine_id STRING,
  error_type STRING,
  error_message STRING,
  discovered_at TIMESTAMP,
  ai_diagnosis STRUCT<
    drift_type: STRING,
    confidence_score: DOUBLE,
    proposed_ddl: STRING,
    reasoning: STRING,
    diagnosed_at: TIMESTAMP
  >,
  human_decision STRUCT<
    decision: STRING,
    reviewer: STRING,
    reviewed_at: TIMESTAMP,
    review_notes: STRING
  >,
  execution_result STRUCT<
    status: STRING,
    executed_at: TIMESTAMP,
    error_log: STRING
  >,
  reprocessing_result STRUCT<
    status: STRING,
    records_moved: LONG,
    reprocessed_at: TIMESTAMP
  >,
  status STRING DEFAULT 'pending',
  action_history ARRAY<STRUCT<
    action: STRING,
    actor: STRING,
    timestamp: TIMESTAMP,
    details: STRING
  >>,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP DEFAULT current_timestamp()
)
USING DELTA
PARTITIONED BY (DATE(discovered_at))
COMMENT "Complete audit log of schema remediation incidents, diagnoses, and decisions";
```

---

## Key Design Decisions

1. **SCD2 in Silver**: Tracks all customer changes over time for historical analysis
2. **Quarantine Pattern**: Separate table for drift/malformed records prevents bad data in Silver
3. **Audit Structure**: Nested structs and arrays preserve complete decision history
4. **Idempotency**: `_txn_id` prevents duplicate processing after crashes
5. **Partitioning**: By date for query performance on audit/quarantine at scale
6. **Liquid Clustering**: On `customer_id` for efficient SCD2 lookups
7. **Gold Refresh**: Full recompute (idempotent) rather than incremental
