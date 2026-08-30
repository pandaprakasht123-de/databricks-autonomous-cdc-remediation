# Databricks notebook source
# Databricks notebook source
import json
import random
import time
import uuid
from datetime import datetime, timezone

# COMMAND ----------

# 1. Define Parameters
dbutils.widgets.text("num_records", "20", "1. Number of Records")
dbutils.widgets.text("drift_rate", "0.20", "2. Drift Rate (e.g. 0.20 = 20%)")
dbutils.widgets.dropdown("mode", "INITIAL_LOAD", ["INITIAL_LOAD", "CONTINUOUS_GROWTH"], "3. Generation Mode")

NUM_RECORDS = int(dbutils.widgets.get("num_records"))
DRIFT_RATE = float(dbutils.widgets.get("drift_rate"))
MODE = dbutils.widgets.get("mode")
VOLUME_PATH = "/Volumes/lakehouse_healing/bronze/cdc_landing"

# COMMAND ----------

# 2. Dynamic Primary Key Range Detection
try:
    max_id_row = spark.sql(
        "SELECT COALESCE(MAX(customer_id), 100) AS max_id FROM lakehouse_healing.silver.customers"
    ).collect()
    current_max_id = max_id_row[0]["max_id"]
except Exception:
    current_max_id = 100

# For CONTINUOUS_GROWTH, pull the actual pool of existing IDs so updates
# only ever target customers that really exist — no more guessing a range.
existing_ids = []
if MODE == "CONTINUOUS_GROWTH":
    try:
        rows = spark.sql(
            "SELECT DISTINCT customer_id FROM lakehouse_healing.silver.customers WHERE is_current = true"
        ).collect()
        existing_ids = [r["customer_id"] for r in rows]
    except Exception:
        existing_ids = []
    if not existing_ids:
        # No customers yet — CONTINUOUS_GROWTH has nothing to update against.
        # Fall back to INITIAL_LOAD behavior so the run doesn't collapse to a
        # narrow ID range like before.
        MODE = "INITIAL_LOAD"
        print("⚠️ No existing customers found in silver.customers — falling back to INITIAL_LOAD.")


def generate_cdc_payload(op: str, customer_id: int, drift: bool = False):
    """Generates a Debezium-style JSON CDC record for a given op/id."""
    if drift:
        after_payload = {
            "customer_id": customer_id,
            "email": f"user_{customer_id}@example.com",
            "shipping_address": f"{random.randint(100, 999)} Industrial Way",
            "country": "IN"
        }
    else:
        after_payload = {
            "customer_id": customer_id,
            "email": f"user_{customer_id}@example.com",
            "user_address": f"{random.randint(100, 999)} Tech Boulevard"
        }

    return {
        "op": op,
        "source_ts": datetime.now(timezone.utc).isoformat(),
        "after": after_payload
    }


# 3. Build the record plan up front (id + op decided before any file is written)
records_plan = []

if MODE == "INITIAL_LOAD":
    # One unique, sequential customer per record — no collisions, easy to trace.
    for i in range(1, NUM_RECORDS + 1):
        records_plan.append(("I", current_max_id + i))
else:
    # CONTINUOUS_GROWTH: mix of brand-new inserts and updates to real existing IDs.
    next_new_id = current_max_id + 1
    for _ in range(NUM_RECORDS):
        if random.random() < 0.40 or not existing_ids:
            records_plan.append(("I", next_new_id))
            existing_ids.append(next_new_id)
            next_new_id += 1
        else:
            records_plan.append(("U", random.choice(existing_ids)))

# 4. Emit files to the Volume landing zone
clean_count, drift_count = 0, 0
print(f"Emitting {NUM_RECORDS} records to {VOLUME_PATH} (mode={MODE})...")

for op, customer_id in records_plan:
    has_drift = random.random() < DRIFT_RATE
    # Filename encodes op + customer_id so you can trace a specific record
    # in the volume listing without opening every file.
    file_id = f"cdc_{op}_{customer_id}_{uuid.uuid4().hex[:6]}.json"
    file_path = f"{VOLUME_PATH}/{file_id}"

    payload = generate_cdc_payload(op=op, customer_id=customer_id, drift=has_drift)
    with open(file_path, "w") as f:
        json.dump(payload, f)

    if has_drift:
        drift_count += 1
    else:
        clean_count += 1
    time.sleep(0.05)

print(f"✅ Finished CDC simulation: {NUM_RECORDS} events generated "
      f"({clean_count} Clean, {drift_count} Drifted, mode={MODE}).")
print(f"   Customer ID range this run: {min(r[1] for r in records_plan)}–{max(r[1] for r in records_plan)}")