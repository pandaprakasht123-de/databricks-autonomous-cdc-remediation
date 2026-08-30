# Databricks notebook source
# Databricks notebook source
%pip install -U langchain langchain-core pydantic databricks-langchain
%restart_python

# COMMAND ----------

# COMMAND ----------
import json
import uuid
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from databricks_langchain import ChatDatabricks
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType, ArrayType
)

# 1. Parameters
dbutils.widgets.text("batch_limit", "5", "1. Max Incidents to Triage This Run")
BATCH_LIMIT = int(dbutils.widgets.get("batch_limit"))

# 2. Fetch unaddressed incidents via LEFT ANTI JOIN
quarantine_df = spark.sql(f"""
    SELECT q.quarantine_id, q.raw_payload, q.detected_drift_columns 
    FROM lakehouse_healing.silver.quarantine q
    LEFT ANTI JOIN lakehouse_healing.ops.schema_remediation_audit a
      ON q.quarantine_id = a.quarantine_id
    LIMIT {BATCH_LIMIT}
""")

quarantine_records = quarantine_df.collect()

if not quarantine_records:
    print("✅ No unaddressed quarantine records found. Lakehouse is healthy!")
    dbutils.notebook.exit("NO_NEW_DRIFT_FOUND")

print(f"Discovered {len(quarantine_records)} unaddressed quarantine records for AI triage.")

# 3. Extract current target schema
target_table_name = "lakehouse_healing.silver.customers"
target_schema = spark.table(target_table_name).schema.json()

# 4. Pydantic Output Model — range-constrained so a hallucinated confidence
# value (e.g. 1.4 or -0.2) is rejected rather than silently written to the
# audit table.
class SchemaRemediationPlan(BaseModel):
    drift_type: str = Field(description="Column Rename, New Column Addition, or Type Mismatch")
    confidence_score: float = Field(ge=0.0, le=1.0, description="Confidence between 0.0 and 1.0")
    root_cause_explanation: str = Field(description="1-2 sentence explanation of why the record failed schema validation")
    proposed_ddl_patch: str = Field(description="Exact executable SQL ALTER TABLE statement in Databricks Spark SQL syntax")

# 5. Strict Databricks SQL Prompt
prompt = ChatPromptTemplate.from_template(
    """You are an Autonomous Lakehouse Schema Healing Agent for Databricks Delta Lake.
Analyze the target Delta table schema and the quarantined CDC payloads that caused schema drift.

CRITICAL DATABRICKS DELTA SQL SYNTAX RULES:
- Always use the valid Databricks SQL syntax: `ALTER TABLE <table_name> ADD COLUMNS (col1 TYPE, col2 TYPE)`
- DO NOT use repeated `ADD COLUMN` keywords.
- Enclose all added columns inside parentheses, e.g.:
  `ALTER TABLE lakehouse_healing.silver.customers ADD COLUMNS (shipping_address STRING, country STRING)`

TARGET TABLE: {target_table}
CURRENT TARGET SCHEMA:
{target_schema}

SAMPLE QUARANTINED PAYLOAD & DRIFTED COLUMNS:
{quarantine_samples}
"""
)

# 6. Initialize Foundation Model (Zero-Egress Serving) with true structured
# output — this uses Llama 3.3's native tool-calling to force a
# schema-conformant response, unlike JsonOutputParser which only uses the
# Pydantic model to write prompt instructions and does not validate the
# response against it.
llm = ChatDatabricks(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    temperature=0.0
)

structured_llm = llm.with_structured_output(SchemaRemediationPlan)
healing_chain = prompt | structured_llm

# 7. Execute Diagnosis
audit_entries = []
now_utc = datetime.now(timezone.utc)

for row in quarantine_records:
    print(f"\nTriaging Quarantine ID: {row.quarantine_id}...")
    sample_info = f"Drift Columns: {row.detected_drift_columns}\nRaw Payload: {row.raw_payload}"

    # One bad response (parsing failure, API hiccup, validation error)
    # should not lose every diagnosis already completed in this run — since
    # the write happens once at the end, an unhandled exception here would
    # otherwise discard the whole batch.
    try:
        diagnosis = healing_chain.invoke({
            "target_table": target_table_name,
            "target_schema": target_schema,
            "quarantine_samples": sample_info
        })
    except Exception as e:
        print(f"⚠️ Failed to triage {row.quarantine_id}: {e}")
        continue

    incident_id = f"INC_{uuid.uuid4().hex[:8].upper()}"
    confidence = diagnosis.confidence_score
    drift_type = diagnosis.drift_type
    patch = diagnosis.proposed_ddl_patch.strip()
    reason = diagnosis.root_cause_explanation

    audit_entries.append({
        "incident_id": incident_id,
        "quarantine_id": row.quarantine_id,
        "target_table": target_table_name,
        "drift_type": drift_type,
        "proposed_ddl_patch": patch,
        "confidence_score": confidence,
        "llm_reasoning": reason,
        "incident_status": "OPEN",
        "is_applied": False,
        "assigned_to": "unassigned@company.com",
        "resolved_by": None,
        "created_at": now_utc,
        "resolved_at": None,
        "action_history": [{
            "action_type": "AI_DIAGNOSED",
            "performed_by": "Databricks Foundation Model (Llama 3.3 70B)",
            "action_timestamp": now_utc,
            "notes": f"Automated diagnosis completed with confidence {confidence}"
        }]
    })

    print(f"🤖 INCIDENT LOGGED [{incident_id}]: {drift_type}")
    print(f"• Reasoning: {reason}")
    print(f"• Patch: {patch}")

# 8. Write to Ops Audit Table
if audit_entries:
    action_event_schema = StructType([
        StructField("action_type", StringType(), True),
        StructField("performed_by", StringType(), True),
        StructField("action_timestamp", TimestampType(), True),
        StructField("notes", StringType(), True)
    ])

    audit_table_schema = StructType([
        StructField("incident_id", StringType(), False),
        StructField("quarantine_id", StringType(), False),
        StructField("target_table", StringType(), True),
        StructField("drift_type", StringType(), True),
        StructField("proposed_ddl_patch", StringType(), True),
        StructField("confidence_score", DoubleType(), True),
        StructField("llm_reasoning", StringType(), True),
        StructField("incident_status", StringType(), True),
        StructField("is_applied", BooleanType(), True),
        StructField("assigned_to", StringType(), True),
        StructField("resolved_by", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("resolved_at", TimestampType(), True),
        StructField("action_history", ArrayType(action_event_schema), True)
    ])

    spark.createDataFrame(audit_entries, schema=audit_table_schema) \
        .write.format("delta").mode("append") \
        .saveAsTable("lakehouse_healing.ops.schema_remediation_audit")
    print(f"\n✅ Logged {len(audit_entries)} incidents to lakehouse_healing.ops.schema_remediation_audit.")
else:
    print("\nℹ️ No incidents were successfully diagnosed this run.")