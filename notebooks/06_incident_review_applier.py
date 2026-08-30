# Databricks notebook source
# COMMAND ----------
import json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from delta.tables import DeltaTable

# COMMAND ----------

# 1. Fetch Open Incidents
pending_incidents_df = spark.sql("""
    SELECT incident_id, quarantine_id, drift_type, proposed_ddl_patch, llm_reasoning, created_at
    FROM lakehouse_healing.ops.schema_remediation_audit
    WHERE is_applied = false AND incident_status NOT IN ('REJECTED', 'FAILED')
    ORDER BY created_at DESC
""")

display(pending_incidents_df)
pending_records = pending_incidents_df.collect()

if not pending_records:
    print("✅ No pending incidents. All schemas are healthy.")
    dbutils.notebook.exit("ALL_RESOLVED")

pending_ids = [row["incident_id"] for row in pending_records]

# COMMAND ----------

# 2. Interactive Reviewer Widgets
dbutils.widgets.dropdown("select_incident_id", pending_ids[0], pending_ids, "1. Incident ID")
dbutils.widgets.dropdown("action", "REVIEW_ONLY", ["REVIEW_ONLY", "APPROVE_AND_EXECUTE", "REJECT"], "2. Action")
dbutils.widgets.text("user_email", "lead_de@company.com", "3. Reviewer Email")
dbutils.widgets.text("notes", "DDL patch approved and quarantined data re-ingested", "4. Notes")

selected_id = dbutils.widgets.get("select_incident_id")
action = dbutils.widgets.get("action")
actor_email = dbutils.widgets.get("user_email")
notes = dbutils.widgets.get("notes")

# COMMAND ----------

# 3. Retrieve Target Incident Record
incident_row = spark.sql(f"""
    SELECT incident_id, quarantine_id, proposed_ddl_patch, is_applied, incident_status
    FROM lakehouse_healing.ops.schema_remediation_audit
    WHERE incident_id = '{selected_id}'
""").collect()[0]

quarantine_id = incident_row.quarantine_id
ddl_patch = incident_row.proposed_ddl_patch.strip()
audit_table = DeltaTable.forName(spark, "lakehouse_healing.ops.schema_remediation_audit")

# Guard: don't reprocess an incident that's already been resolved. Prevents
# an accidental rerun from re-merging the same quarantined payload a second
# time (which, combined with the out-of-order fix below, would otherwise be
# safe — but there's no reason to do redundant work or risk it).
if action == "APPROVE_AND_EXECUTE" and incident_row.is_applied:
    print(f"ℹ️ Incident {selected_id} is already applied (status: {incident_row.incident_status}). "
          f"Nothing to do — skipping re-execution.")
    dbutils.notebook.exit("ALREADY_APPLIED")

# COMMAND ----------
# 4. Action Execution

if action == "REVIEW_ONLY":
    # True no-op by design: this is a "look before you leap" mode. No writes
    # to the audit table, no writes to Silver. The reviewer just wants to
    # inspect the incident details (shown in the display() above) before
    # deciding on APPROVE_AND_EXECUTE or REJECT on a subsequent run.
    print(f"👀 REVIEW_ONLY selected for {selected_id}. No changes made.")
    print(f"Proposed DDL:\n{ddl_patch}")

elif action == "REJECT":
    new_event_array = F.array(
        F.struct(
            F.lit("REJECTED").alias("action_type"),
            F.lit(actor_email).alias("performed_by"),
            F.current_timestamp().alias("action_timestamp"),
            F.lit(notes).alias("notes")
        )
    )
    audit_table.update(
        condition=f"incident_id = '{selected_id}'",
        set={
            "incident_status": F.lit("REJECTED"),
            "resolved_by": F.lit(actor_email),
            "resolved_at": F.current_timestamp(),
            "action_history": F.concat(F.col("action_history"), new_event_array)
        }
    )
    print(f"🚫 Incident {selected_id} marked REJECTED. Silver and quarantine tables untouched.")

elif action == "APPROVE_AND_EXECUTE":
    print(f"🚀 Step A: Checking target table schema for {selected_id}...")

    ddl_failed = False
    ddl_error_message = None

    try:
        spark.sql(ddl_patch)
        print("✅ DDL patch executed successfully.")
    except Exception as e:
        if "FIELD_ALREADY_EXISTS" in str(e):
            print("ℹ️ Columns already exist in target table. Skipping DDL alteration.")
        else:
            # Genuine failure — abort the whole flow. Reprocessing quarantined
            # data against a table that doesn't have the columns it needs
            # would just fail downstream anyway, and silently marking the
            # incident RESOLVED here would be actively misleading.
            ddl_failed = True
            ddl_error_message = str(e)
            print(f"❌ DDL execution failed: {ddl_error_message}")

    if ddl_failed:
        new_event_array = F.array(
            F.struct(
                F.lit("DDL_EXECUTION_FAILED").alias("action_type"),
                F.lit(actor_email).alias("performed_by"),
                F.current_timestamp().alias("action_timestamp"),
                F.lit(f"DDL failed, incident NOT resolved: {ddl_error_message}").alias("notes")
            )
        )
        audit_table.update(
            condition=f"incident_id = '{selected_id}'",
            set={
                "incident_status": F.lit("FAILED"),
                "action_history": F.concat(F.col("action_history"), new_event_array)
            }
        )
        print(f"⚠️ Incident {selected_id} marked FAILED. is_applied remains false. "
              f"Silver and quarantine tables untouched — review the DDL patch and retry.")
        dbutils.notebook.exit("DDL_FAILED")

    # Step B: Reprocess the Quarantined Payload
    print(f"\n🔄 Step B: Reprocessing quarantine payload for {quarantine_id}...")
    q_data = spark.sql(f"""
        SELECT raw_payload FROM lakehouse_healing.silver.quarantine
        WHERE quarantine_id = '{quarantine_id}'
    """).collect()

    reprocessed_count = 0

    if q_data:
        reprocessed_records = []
        for row in q_data:
            payload = json.loads(row.raw_payload)
            after = payload.get("after", {})
            source_ts_str = payload.get("source_ts")

            source_ts = datetime.fromisoformat(source_ts_str.replace("Z", "+00:00")) if source_ts_str else datetime.now(timezone.utc)
            address = after.get("user_address") or after.get("shipping_address")

            reprocessed_records.append({
                "customer_id": int(after.get("customer_id")),
                "email": str(after.get("email", "")),
                "user_address": str(address or ""),
                "shipping_address": str(after.get("shipping_address", "")),
                "country": str(after.get("country", "")),
                "source_ts": source_ts
            })

        reprocessed_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("email", StringType(), True),
            StructField("user_address", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("country", StringType(), True),
            StructField("source_ts", TimestampType(), False)
        ])

        updates_df = spark.createDataFrame(reprocessed_records, schema=reprocessed_schema)
        silver_table = DeltaTable.forName(spark, "lakehouse_healing.silver.customers")

        # Step A (SCD2): Expire old active record ONLY if this event is
        # strictly newer than the row currently in effect.
        silver_table.alias("target").merge(
            updates_df.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true"
        ).whenMatchedUpdate(
            condition="source.source_ts > target.valid_from",
            set={
                "is_current": "false",
                "valid_to": "source.source_ts"
            }
        ).execute()

        # Step B (SCD2): Insert ONLY if the customer is new, or this event is
        # strictly newer than whatever is currently active. This is the same
        # out-of-order fix applied in Step 3 — without it, a stale reprocessed
        # payload that Step A correctly declined to expire would still get
        # inserted here as a duplicate is_current=true row.
        current_active = spark.table("lakehouse_healing.silver.customers") \
            .filter("is_current = true") \
            .select("customer_id", F.col("valid_from").alias("current_valid_from"))

        joined = updates_df.join(current_active, on="customer_id", how="left")

        records_to_insert = joined.filter(
            F.col("current_valid_from").isNull() |
            (F.col("source_ts") > F.col("current_valid_from"))
        ).drop("current_valid_from")

        skipped = updates_df.count() - records_to_insert.count()
        if skipped > 0:
            print(f"⚠️ Skipped {skipped} stale/out-of-order reprocessed record(s).")

        if records_to_insert.count() > 0:
            new_rows = records_to_insert.select(
                F.col("customer_id"),
                F.col("email"),
                F.col("user_address"),
                F.col("shipping_address"),
                F.col("country"),
                F.col("source_ts").alias("valid_from"),
                F.lit(None).cast("timestamp").alias("valid_to"),
                F.lit(True).alias("is_current")
            )
            reprocessed_count = records_to_insert.count()
            new_rows.write.format("delta").mode("append").saveAsTable("lakehouse_healing.silver.customers")
            print(f"✅ Merged {reprocessed_count} quarantined record(s) into silver.customers.")
        else:
            print("ℹ️ No records inserted — reprocessed payload was stale relative to current Silver state.")

        # Mark the quarantine record itself as resolved, so silver.quarantine
        # is self-describing and doesn't require joining against the ops
        # audit table just to know whether a given row was handled.
        spark.sql(f"""
            UPDATE lakehouse_healing.silver.quarantine
            SET resolved = true, resolved_at = current_timestamp()
            WHERE quarantine_id = '{quarantine_id}'
        """)
    else:
        print(f"⚠️ No quarantine record found for {quarantine_id} — nothing to reprocess.")

    # Step C: Log Resolution Event in Ops Audit Table
    new_event_array = F.array(
        F.struct(
            F.lit("DDL_APPLIED_AND_REPROCESSED").alias("action_type"),
            F.lit(actor_email).alias("performed_by"),
            F.current_timestamp().alias("action_timestamp"),
            F.lit(f"Applied patch & reprocessed ({reprocessed_count} record(s)): {notes}").alias("notes")
        )
    )

    audit_table.update(
        condition=f"incident_id = '{selected_id}'",
        set={
            "is_applied": F.lit(True),
            "incident_status": F.lit("RESOLVED"),
            "resolved_by": F.lit(actor_email),
            "resolved_at": F.current_timestamp(),
            "action_history": F.concat(F.col("action_history"), new_event_array)
        }
    )
    print(f"\n🎉 Incident {selected_id} is marked RESOLVED.")

    # # Step D: Immediately refresh Gold so the reviewer sees the fix reflected
    # # right away, rather than waiting for the next scheduled Gold refresh run.
    # print("\n🔁 Step D: Triggering Gold layer refresh...")
    # try:
    #     dbutils.notebook.run("05_gold_refresh", timeout_seconds=120)
    #     print("✅ Gold refresh complete — dim_customers now reflects this resolution.")
    # except Exception as e:
    #     # Don't fail the whole incident resolution over a Gold refresh hiccup —
    #     # Silver is already correct, and the next scheduled Task 4 run will
    #     # reconcile Gold regardless.
    #     print(f"⚠️ Gold refresh trigger failed: {e}")
    #     print("   Silver was updated successfully. Gold will reconcile on the next scheduled run.")