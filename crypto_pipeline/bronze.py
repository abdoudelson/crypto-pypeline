import dlt
from pyspark.sql.functions import *

# -----------------------------
# BRONZE: Raw Ingestion with Auto Loader
# -----------------------------
@dlt.table(
    name="crypto_bronze",
    comment="Raw streaming crypto ticker data"
)
def crypto_bronze():
    storage_account = spark.conf.get("pipelines.storage_account")
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"abfss://raw@{storage_account}.dfs.core.windows.net/")
    )
