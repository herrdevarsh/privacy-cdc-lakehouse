from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object, when, coalesce,
    from_unixtime, max as smax, row_number, current_timestamp, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

CATALOG = "iceberg"          # IMPORTANT: match Trino catalog you query (iceberg.silver..., iceberg.monitoring...)
PIPELINE = "orders"

spark = (
    SparkSession.builder
    .appName("merge_orders_silver_incremental")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}.uri", "http://iceberg-rest:8181")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://warehouse/wh/")
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{CATALOG}.s3.region", "us-east-1")
    .getOrCreate()
)

# Ensure namespaces exist
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.monitoring")

# Target table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.orders_current (
  order_id INT,
  user_id INT,
  amount_eur DOUBLE,
  status STRING,
  last_change_ts TIMESTAMP
) USING iceberg
""")

# Checkpoint table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.monitoring.cdc_checkpoints (
  pipeline STRING,
  last_offset BIGINT,
  updated_at TIMESTAMP
) USING iceberg
""")

# Read last checkpoint (correct + simple)
cp = (
    spark.table(f"{CATALOG}.monitoring.cdc_checkpoints")
    .filter(col("pipeline") == lit(PIPELINE))
)
lo_row = cp.agg(smax(col("last_offset")).alias("lo")).collect()[0]
lo = lo_row["lo"] if lo_row["lo"] is not None else -1

bronze = (
    spark.table(f"{CATALOG}.bronze.orders_cdc_raw")
    .filter(col("offset") > lit(lo))
)

# If nothing new, exit cleanly
if bronze.rdd.isEmpty():
    print(f"No new rows in bronze after offset {lo}.")
    spark.stop()
    raise SystemExit(0)

# Debezium value is {"schema":..., "payload":...}
payload = get_json_object(col("v"), "$.payload")
cdc_json = when(payload.isNotNull(), payload).otherwise(col("v"))

row_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount_eur", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
])

cdc_schema = StructType([
    StructField("before", row_schema, True),
    StructField("after", row_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
])

parsed = (
    bronze.select(
        col("offset").alias("kafka_offset"),
        from_json(cdc_json, cdc_schema).alias("cdc"),
    )
    .select(
        col("cdc.op").alias("op"),
        col("cdc.ts_ms").alias("ts_ms"),
        col("kafka_offset"),
        col("cdc.before").alias("before"),
        col("cdc.after").alias("after"),
    )
)

events = parsed.select(
    coalesce(col("after.order_id"), col("before.order_id")).alias("order_id"),
    coalesce(col("after.user_id"), col("before.user_id")).alias("user_id"),
    coalesce(col("after.amount_eur"), col("before.amount_eur")).alias("amount_eur_raw"),
    coalesce(col("after.status"), col("before.status")).alias("status"),
    col("op"),
    col("ts_ms"),
    col("kafka_offset"),
).filter(col("order_id").isNotNull())

# Deterministically keep only latest event per order_id within this micro-batch
w = Window.partitionBy("order_id").orderBy(col("ts_ms").desc(), col("kafka_offset").desc())

latest = (
    events
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
    .select(
        col("order_id"),
        col("user_id"),
        col("amount_eur_raw").cast(DoubleType()).alias("amount_eur"),
        col("status"),
        when(
            col("ts_ms").isNotNull(),
            from_unixtime(col("ts_ms") / 1000).cast("timestamp")
        ).otherwise(current_timestamp()).alias("last_change_ts"),
        col("op"),
    )
)

latest.createOrReplaceTempView("staging_orders")

# MERGE: upserts + deletes
spark.sql(f"""
MERGE INTO {CATALOG}.silver.orders_current t
USING staging_orders s
ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED AND s.op <> 'd' THEN UPDATE SET
  user_id = s.user_id,
  amount_eur = s.amount_eur,
  status = s.status,
  last_change_ts = s.last_change_ts
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (order_id, user_id, amount_eur, status, last_change_ts)
VALUES (s.order_id, s.user_id, s.amount_eur, s.status, s.last_change_ts)
""")

# Update checkpoint to max offset processed (use MERGE instead of delete+insert)
max_offset = bronze.agg(smax(col("offset")).alias("m")).collect()[0]["m"]
if max_offset is None:
    print("No max offset found even though bronze is non-empty. Something is wrong.")
    spark.stop()
    raise SystemExit(1)

spark.sql(f"""
MERGE INTO {CATALOG}.monitoring.cdc_checkpoints t
USING (SELECT '{PIPELINE}' AS pipeline, {int(max_offset)} AS last_offset) s
ON t.pipeline = s.pipeline
WHEN MATCHED THEN UPDATE SET
  last_offset = s.last_offset,
  updated_at = current_timestamp
WHEN NOT MATCHED THEN INSERT (pipeline, last_offset, updated_at)
VALUES (s.pipeline, s.last_offset, current_timestamp)
""")

print(f"MERGE complete. Updated checkpoint to offset {max_offset}.")
spark.stop()
