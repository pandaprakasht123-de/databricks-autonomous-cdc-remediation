# Databricks notebook source
# Databricks notebook source
import json
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

VOLUME_LANDING = "/Volumes/lakehouse_healing/bronze/cdc_landing"
VOLUME_CHECKPOINTS = "/Volumes/lakehouse_healing/bronze/checkpoints"

CHECKPOINT_BRONZE = f"{VOLUME_CHECKPOINTS}/bronze_ingest"
CHECKPOINT_SILVER = f"{VOLUME_CHECKPOINTS}/silver_stream"

# ==============================================================================
# 1. Ingest Landing Zone -> Bronze Raw Delta Table
# ==============================================================================
bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "text")
    .load(VOLUME_LANDING)
    .filter(~F.col("_metadata.file_path").contains("/checkpoints/"))
    .select(
        F.col("value").alias("raw_payload"),
        F.col("_metadata.file_path").alias("file_name"),
        F.current_timestamp().alias("ingested_at")
    )
)

query_bronze = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_BRONZE)
    .trigger(availableNow=True)
    .toTable("lakehouse_healing.bronze.customers_cdc_raw")
)
query_bronze.awaitTermination()
print("✅ Bronze ingestion batch complete.")

# ==============================================================================
# 2. Ingest Bronze -> Silver SCD-2 & Quarantine Routing
# ==============================================================================
bronze_delta_stream = (
    spark.readStream
    .format("delta")
    .table("lakehouse_healing.bronze.customers_cdc_raw")
)

def process_silver_microbatch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    current_silver_cols = set(spark.table("lakehouse_healing.silver.customers").columns) - {"valid_from", "valid_to", "is_current"}
    expected_keys = current_silver_cols if current_silver_cols else {"customer_id", "email", "user_address"}

    valid_records = []
    quarantine_records = []
    now_utc = datetime.now(timezone.utc)

    for row in batch_df.collect():
        raw_text = row.raw_payload
        if not raw_text or not raw_text.strip().startswith("{"):
            continue

        try:
            payload = json.loads(raw_text)
            after_struct = payload.get("after") or {}
            source_ts_str = payload.get("source_ts")

            source_ts = datetime.fromisoformat(source_ts_str.replace("Z", "+00:00")) if source_ts_str else now_utc

            incoming_keys = set(after_struct.keys())
            drifted_keys = incoming_keys - expected_keys

            if drifted_keys or "customer_id" not in after_struct or after_struct.get("customer_id") is None:
                quarantine_records.append({
                    "quarantine_id": f"QR_{uuid.uuid4().hex[:8].upper()}",
                    "raw_payload": json.dumps(payload),
                    "detected_drift_columns": str(list(drifted_keys)) if drifted_keys else "['MISSING_PRIMARY_KEY']",
                    "quarantined_at": now_utc
                })
            else:
                valid_records.append({
                    "customer_id": int(after_struct.get("customer_id")),
                    "email": str(after_struct.get("email", "")),
                    "user_address": str(after_struct.get("user_address", "")),
                    "source_ts": source_ts
                })
        except Exception:
            quarantine_records.append({
                "quarantine_id": f"QR_{uuid.uuid4().hex[:8].upper()}",
                "raw_payload": str(raw_text),
                "detected_drift_columns": "['MALFORMED_JSON']",
                "quarantined_at": now_utc
            })

    # Write to Quarantine DLQ — idempotent write keyed on this stream's batch_id.
    # If this batch is ever retried (e.g. after a mid-batch crash), Delta
    # detects the same (txnAppId, txnVersion) pair was already committed
    # and skips the write instead of re-inserting duplicate rows.
    if quarantine_records:
        quarantine_schema = StructType([
            StructField("quarantine_id", StringType(), False),
            StructField("raw_payload", StringType(), True),
            StructField("detected_drift_columns", StringType(), True),
            StructField("quarantined_at", TimestampType(), True)
        ])
        spark.createDataFrame(quarantine_records, schema=quarantine_schema) \
            .write.format("delta") \
            .option("txnAppId", "silver_quarantine_writer") \
            .option("txnVersion", batch_id) \
            .mode("append") \
            .saveAsTable("lakehouse_healing.silver.quarantine")
        print(f"📦 Quarantined {len(quarantine_records)} drifted records.")

    # Process Valid SCD-2 Records
    if valid_records:
        valid_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("email", StringType(), True),
            StructField("user_address", StringType(), True),
            StructField("source_ts", TimestampType(), False)
        ])

        updates_df = spark.createDataFrame(valid_records, schema=valid_schema)

        # Deduplicate source for MERGE to prevent DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
        window_spec = Window.partitionBy("customer_id").orderBy(F.col("source_ts").desc())
        latest_updates_df = (
            updates_df
            .withColumn("rn", F.row_number().over(window_spec))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )
        # NOTE: no .cache() here — persist()/cache() is not supported on
        # serverless compute (NOT_SUPPORTED_WITH_SERVERLESS). latest_updates_df
        # is recomputed a few times below (merge, join, counts), which is fine
        # at this batch size.

        silver_table = DeltaTable.forName(spark, "lakehouse_healing.silver.customers")

        # Step A: Expire the existing active record ONLY if the new event is
        # strictly newer (> not >=) than the row currently in effect.
        # A replayed event with an identical timestamp must not expire the
        # current row or create a zero-duration validity window.
        silver_table.alias("target").merge(
            latest_updates_df.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true"
        ).whenMatchedUpdate(
            condition="source.source_ts > target.valid_from",
            set={
                "is_current": "false",
                "valid_to": "source.source_ts"
            }
        ).execute()

        # Step B: Insert ONLY records that are brand-new customers, or that
        # are strictly newer than whatever is currently active. Without this
        # check, a stale/out-of-order event that Step A correctly declined to
        # expire would still get inserted here as a second is_current=true
        # row, corrupting SCD2 history.
        current_active = spark.table("lakehouse_healing.silver.customers") \
            .filter("is_current = true") \
            .select("customer_id", F.col("valid_from").alias("current_valid_from"))

        joined = latest_updates_df.join(current_active, on="customer_id", how="left")

        records_to_insert = joined.filter(
            F.col("current_valid_from").isNull() |
            (F.col("source_ts") > F.col("current_valid_from"))
        ).drop("current_valid_from")

        total_candidates = latest_updates_df.count()
        insert_count = records_to_insert.count()
        stale_count = total_candidates - insert_count

        if stale_count > 0:
            print(f"⚠️ Dropped {stale_count} stale/out-of-order record(s) — "
                  f"older than or equal to the currently active row.")

        if insert_count > 0:
            new_inserts = records_to_insert.select(
                F.col("customer_id"),
                F.col("email"),
                F.col("user_address"),
                F.col("source_ts").alias("valid_from"),
                F.lit(None).cast("timestamp").alias("valid_to"),
                F.lit(True).alias("is_current")
            )
            # Idempotent write — same rationale as the quarantine write above.
            new_inserts.write.format("delta") \
                .option("txnAppId", "silver_customers_writer") \
                .option("txnVersion", batch_id) \
                .mode("append") \
                .saveAsTable("lakehouse_healing.silver.customers")
            print(f"✅ Processed {insert_count} clean records into silver.customers.")
        else:
            print("ℹ️ No new/updated records to insert this batch (all stale or none present).")

query_silver = (
    bronze_delta_stream.writeStream
    .foreachBatch(process_silver_microbatch)
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .trigger(availableNow=True)
    .start()
)
query_silver.awaitTermination()
print("✅ Silver processing batch complete.")