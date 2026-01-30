from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

CATALOG = "demo"  # IMPORTANT: matches spark-defaults.conf in this image

spark = (
    SparkSession.builder
    .appName("kafka_to_iceberg_bronze_orders_raw")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # Override the baked-in defaults that point to http://rest:8181
    .config(f"spark.sql.catalog.{CATALOG}.uri", "http://iceberg-rest:8181")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://warehouse/wh/")
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{CATALOG}.s3.region", "us-east-1")

    .getOrCreate()
)

# Namespace == Trino schema iceberg.bronze
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.orders_cdc_raw (
  topic STRING,
  partition INT,
  offset BIGINT,
  kafka_ts TIMESTAMP,
  k STRING,
  v STRING,
  ingested_at TIMESTAMP
) USING iceberg
""")

df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "pg.public.orders")
    .option("startingOffsets", "earliest")
    .load()
    .select(
        col("topic").cast("string").alias("topic"),
        col("partition").cast("int").alias("partition"),
        col("offset").cast("bigint").alias("offset"),
        col("timestamp").cast("timestamp").alias("kafka_ts"),
        col("key").cast("string").alias("k"),
        col("value").cast("string").alias("v"),
        current_timestamp().alias("ingested_at"),
    )
)

df.writeTo(f"{CATALOG}.bronze.orders_cdc_raw").append()
spark.stop()
