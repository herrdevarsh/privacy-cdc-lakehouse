import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, lit

CATALOG = "demo"
salt = os.environ.get("PII_SALT", "")
if not salt:
    raise RuntimeError("PII_SALT is not set. Add it to docker-compose.yml under spark.environment")

spark = (
    SparkSession.builder
    .appName("build_privacy_table")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}.uri", "http://iceberg-rest:8181")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", "s3://warehouse/wh/")
    .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{CATALOG}.s3.region", "us-east-1")
    .getOrCreate()
)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")

base = spark.table(f"{CATALOG}.silver.orders_current")

priv = (
    base
    .select(
        col("order_id"),
        sha2(concat_ws("::", col("user_id").cast("string"), lit(salt)), 256).alias("user_key"),
        col("amount_eur"),
        col("status"),
        col("last_change_ts"),
    )
)

priv.writeTo(f"{CATALOG}.silver.orders_current_priv_table").createOrReplace()
spark.stop()
