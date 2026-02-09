import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------
# SILVER: Cleaning, Dedup & Typing
# -----------------------------
@dlt.table(
    name="crypto_silver",
    comment="Cleaned, typed and deduplicated crypto data"
)
@dlt.expect_or_drop("valid_price", "price > 0")
@dlt.expect_or_warn("valid_symbol", "symbol IS NOT NULL")
def crypto_silver():
    # Read from the Bronze table in the Bronze schema
    catalog = spark.conf.get("pipelines.catalog")
    schema_bronze = spark.conf.get("pipelines.target_schema_bronze")
    
    return (
        spark.readStream.table(f"{catalog}.{schema_bronze}.crypto_bronze")
        .withColumn("timestamp_dt", from_unixtime(col("timestamp") / 1000).cast(TimestampType()))
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("volume", col("volume").cast(DoubleType()))
        .dropDuplicates(["symbol", "timestamp"]) # Remove duplicates based on key
    )
