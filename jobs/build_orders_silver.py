from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object, when, coalesce,
    from_unixtime, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

CATALOG = "demo"

spark = (
    SparkSession.builder
    .appName("build_silver_orders_current_v2")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}.uri", "http://iceberg-rest:8181")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://warehouse/wh/")
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{CATALOG}.s3.region", "us-east-1")
    .getOrCreate()
)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")

bronze = spark.table(f"{CATALOG}.bronze.orders_cdc_raw")

# Debezium value is usually: {"schema":..., "payload":{before,after,op,ts_ms,...}}
payload = get_json_object(col("v"), "$.payload")
cdc_json = when(payload.isNotNull(), payload).otherwise(col("v"))

row_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    # keep as STRING to be safe; we'll cast after cleanup
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

parsed = bronze.select(
    col("offset").alias("kafka_offset"),
    from_json(cdc_json, cdc_schema).alias("cdc"),
    # fallback extraction directly from JSON (covers cases where amount isn't in parsed schema cleanly)
    get_json_object(cdc_json, "$.after.amount_eur").alias("amount_after_raw_json"),
    get_json_object(cdc_json, "$.before.amount_eur").alias("amount_before_raw_json"),
).select(
    col("cdc.op").alias("op"),
    col("cdc.ts_ms").alias("ts_ms"),
    col("kafka_offset"),
    col("cdc.before").alias("before"),
    col("cdc.after").alias("after"),
    col("amount_after_raw_json"),
    col("amount_before_raw_json"),
)

events = parsed.select(
    coalesce(col("after.order_id"), col("before.order_id")).alias("order_id"),
    coalesce(col("after.user_id"), col("before.user_id")).alias("user_id"),
    # prefer parsed field; if null use json-extracted raw
    coalesce(col("after.amount_eur"), col("amount_after_raw_json")).alias("amount_eur_raw"),
    col("after.status").alias("status"),
    col("op"),
    col("ts_ms"),
    col("kafka_offset"),
)

# latest per order_id wins
latest = (
    events
    .orderBy(col("ts_ms").desc(), col("kafka_offset").desc())
    .dropDuplicates(["order_id"])
)

# Clean numeric strings like "49.50" or "\"49.50\"" etc
clean_amount = regexp_replace(col("amount_eur_raw"), r'["\s]', '')

current = (
    latest
    .filter((col("order_id").isNotNull()) & (col("op") != "d"))
    .select(
        col("order_id"),
        col("user_id"),
        clean_amount.cast(DoubleType()).alias("amount_eur"),
        col("status"),
        from_unixtime(col("ts_ms") / 1000).cast("timestamp").alias("last_change_ts"),
    )
)

current.writeTo(f"{CATALOG}.silver.orders_current").createOrReplace()
spark.stop()
