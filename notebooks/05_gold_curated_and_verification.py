# Databricks notebook source
# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC -- 1. Refresh Gold Dimension Table (Only Active Customer Records)
# MAGIC -- account_created_at is computed from FULL customer history (not just the
# MAGIC -- current row) so it reflects the true first-ever valid_from for that
# MAGIC -- customer, not just the timestamp of their latest update.
# MAGIC MERGE INTO lakehouse_healing.gold.dim_customers AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         cur.customer_id,
# MAGIC         cur.email,
# MAGIC         cur.user_address,
# MAGIC         first_seen.account_created_at,
# MAGIC         cur.valid_from AS last_updated_at
# MAGIC     FROM lakehouse_healing.silver.customers cur
# MAGIC     JOIN (
# MAGIC         SELECT customer_id, MIN(valid_from) AS account_created_at
# MAGIC         FROM lakehouse_healing.silver.customers
# MAGIC         GROUP BY customer_id
# MAGIC     ) first_seen
# MAGIC       ON cur.customer_id = first_seen.customer_id
# MAGIC     WHERE cur.is_current = true
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET
# MAGIC         target.email = source.email,
# MAGIC         target.user_address = source.user_address,
# MAGIC         target.last_updated_at = source.last_updated_at
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (customer_id, email, user_address, account_created_at, last_updated_at)
# MAGIC     VALUES (source.customer_id, source.email, source.user_address, source.account_created_at, source.last_updated_at);

# COMMAND ----------

# COMMAND ----------
# Logging: row counts before/after, so each Gold refresh run leaves a
# readable trail in the notebook output / Job run history.
gold_count = spark.table("lakehouse_healing.gold.dim_customers").count()
silver_active_count = spark.sql(
    "SELECT COUNT(*) AS c FROM lakehouse_healing.silver.customers WHERE is_current = true"
).collect()[0]["c"]

print(f"📊 Gold refresh complete.")
print(f"   silver.customers (is_current=true): {silver_active_count} rows")
print(f"   gold.dim_customers (after merge):   {gold_count} rows")

if gold_count != silver_active_count:
    print(f"⚠️ Row count mismatch — expected gold.dim_customers to match the "
          f"count of currently-active silver customers. Investigate before "
          f"trusting downstream BI numbers.")
else:
    print("✅ Row counts reconciled — Gold matches Silver's active customer set.")