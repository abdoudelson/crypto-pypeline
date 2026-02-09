import dlt
from pyspark.sql.functions import *

# -----------------------------
# GOLD: Analytics & Business Metrics
# -----------------------------
@dlt.table(
    name="crypto_gold",
    comment="Volatility and price performance metrics"
)
def crypto_gold():
    # Read from the Silver table in the Silver schema
    catalog = spark.conf.get("pipelines.catalog")
    schema_silver = spark.conf.get("pipelines.target_schema_silver")
    
    return (
        spark.readStream.table(f"{catalog}.{schema_silver}.crypto_silver")
        .withWatermark("timestamp_dt", "10 minutes")
        .groupBy(
            window(col("timestamp_dt"), "5 minutes"),
            col("symbol")
        )
        .agg(
            avg("price").alias("avg_price"),
            max("price").alias("max_price"),
            min("price").alias("min_price"),
            last("price").alias("latest_price"),
            sum("volume").alias("total_volume")
        )
    )
