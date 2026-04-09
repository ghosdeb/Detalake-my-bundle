from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *

# bronze : ingest orders

@dp.table(name="deltalake_catalog.default.orders_bronze")
def ingest_orders():
  return (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferSchema", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/deltalake_catalog/ingestion/raw_vol/orders_checkpoint")
    .load(f"{src_path}/orders")
    .withColumn("ingesttime", current_timestamp())
    .withColumn("filename", col("_metadata.file_path"))
  )
# silver : enrich orders with customer details

