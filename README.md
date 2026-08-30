# Lakehouse Healing CDC Pipeline

An end-to-end Databricks lakehouse remediation flow for handling CDC drift, quarantining malformed records, healing schema issues with LLM guidance, and allowing human approval before schema changes are applied.

## What this repository contains

The project is based on the actual Databricks notebooks in this repo:

- `01_setup_lakehouse_catalog.py` creates the catalog, schemas, volumes, and initial tables
- `02_cdc_stream_simulator.py` creates synthetic CDC payloads and writes drifted records into the landing volume
- `03_bronze_ingestion_and_silver_scd2.py` ingests raw files and routes invalid payloads into quarantine while merging valid rows into Silver SCD2
- `04_langchain_schema_healing_agent.py` diagnoses quarantined records with LangChain + Databricks LLM structured output
- `05_gold_curated_and_verification.py` refreshes the curated Gold dimension and checks active row counts
- `06_incident_review_applier.py` lets a reviewer approve, reject, or defer schema fixes

## Real pipeline flow

```text
01_setup_lakehouse_catalog
   ├─ create catalog + schemas
   ├─ create landing and checkpoint volumes
   ├─ create bronze.raw and silver/quarantine tables
   └─ create gold.dim_customers and ops audit table

02_cdc_stream_simulator
   ├─ writes records to /Volumes/lakehouse_healing/bronze/cdc_landing
   ├─ supports INITIAL_LOAD and CONTINUOUS_GROWTH modes
   └─ adds drift at configurable drift_rate

03_bronze_ingestion_and_silver_scd2
   ├─ reads files with Auto Loader (text format)
   ├─ stores raw payloads in bronze.customers_cdc_raw
   ├─ quarantines records whose keys do not match silver schema
   └─ updates silver.customers with SCD2 valid_from/valid_to logic

04_langchain_schema_healing_agent
   ├─ anti-joins quarantine against ops.audit
   ├─ calls foundation model with target schema + payload
   └─ logs incident rows to lakehouse_healing.ops.schema_remediation_audit

06_incident_review_applier
   ├─ REVIEW_ONLY, REJECT, or APPROVE_AND_EXECUTE
   ├─ executes DDL when approved
   ├─ reprocesses the matching quarantine record
   └─ marks the incident as RESOLVED or FAILED

05_gold_curated_and_verification
   ├─ refreshes gold.dim_customers from active silver rows
   └─ checks row counts against the active silver set
```

## Project structure

```text
lakehouse-healing-cdc-pipeline/
├── notebooks/
│   ├── 01_setup_lakehouse_catalog.py
│   ├── 02_cdc_stream_simulator.py
│   ├── 03_bronze_ingestion_and_silver_scd2.py
│   ├── 04_langchain_schema_healing_agent.py
│   ├── 05_gold_curated_and_verification.py
│   └── 06_incident_review_applier.py
├── config/
│   ├── databricks.yml
│   └── uc_structure.md
├── docs/
│   ├── architecture.md
│   ├── setup.md
│   └── healing_workflow.md
├── requirements.txt
├── .env.example
├── .editorconfig
├── .gitignore
├── LICENSE
├── README.md
└── config/uc_structure.md
```

## Setup and run flow

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks cluster with Python and Delta support
- Databricks PAT configured in your environment
- Access to create catalog, schemas, volumes, and tables

### 1. Create the lakehouse structure

Run the notebook `01_setup_lakehouse_catalog.py` in Databricks. It creates:

- `lakehouse_healing` catalog
- `bronze`, `silver`, `gold`, and `ops` schemas
- landing and checkpoint volumes under the Bronze schema
- the raw, quarantine, audit, and gold tables

### 2. Generate CDC traffic

Run `02_cdc_stream_simulator.py` to emit drifted and clean payloads into the landing volume. This notebook supports:

- `INITIAL_LOAD`
- `CONTINUOUS_GROWTH`
- configurable `num_records`
- configurable `drift_rate`

### 3. Process the stream

Run `03_bronze_ingestion_and_silver_scd2.py` to read the landing volume, write raw records to Bronze, and merge valid rows into Silver using SCD2 logic.

### 4. Run AI triage

Run `04_langchain_schema_healing_agent.py` to inspect the quarantine table, use a structured LLM response, and log remediation incidents into `ops.schema_remediation_audit`.

### 5. Review incidents

Run `06_incident_review_applier.py` to inspect pending incidents and choose among:

- `REVIEW_ONLY`
- `REJECT`
- `APPROVE_AND_EXECUTE`

### 6. Refresh the Gold layer

Run `05_gold_curated_and_verification.py` after the Silver layer is updated to refresh the curated customer dimension and confirm row counts match the active Silver set.

## Key design choices from the notebooks

- The landing volume is `/Volumes/lakehouse_healing/bronze/cdc_landing`
- The checkpoint volume is `/Volumes/lakehouse_healing/bronze/checkpoints`
- Quarantine logic compares incoming JSON keys with the current Silver schema and routes mismatches to quarantine
- The AI notebook uses `LEFT ANTI JOIN` to avoid reprocessing already-audited quarantine IDs
- The review notebook updates the audit table using Delta `update(...)` and writes a structured `action_history`
- Gold refresh is based on the active `silver.customers` rows where `is_current = true`

## Documentation

- [Architecture guide](docs/architecture.md)
- [Setup guide](docs/setup.md)
- [Healing workflow guide](docs/healing_workflow.md)

## License

MIT License
