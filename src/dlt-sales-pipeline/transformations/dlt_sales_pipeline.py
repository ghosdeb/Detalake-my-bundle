from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, sum, round
from utilities import utils
from pyspark.sql.types import *

src_path = spark.conf.get("source")
checkpointlocation = spark.conf.get("checkpointlocation")

# bronze : ingest orders

@dp.table(name="deltalake_catalog.default.orders_bronze")
def ingest_orders():
  return (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferSchema", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    #.option("cloudFiles.schemaLocation", f"{checkpointlocation}")
    .load(f"{src_path}/orders")
    .withColumn("ingesttime", current_timestamp())
    .withColumn("filename", col("_metadata.file_path"))
  )

# bronze: ingest customers
@dp.table(name="deltalake_catalog.default.customers_bronze")
def ingest_customers():
  return (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferSchema", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    #.option("cloudFiles.schemaLocation", f"{checkpointlocation}")
    .load(f"{src_path}/customers")
    .withColumn("ingesttime", current_timestamp())
    .withColumn("filename", col("_metadata.file_path"))
  )

# Silver: cleaned orders with expectation
@dp.table(name="deltalake_catalog.default.orders_silver_cleaned")
@dp.expect_or_drop("valid_order", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def clean_orders():
  return (
    spark.readStream.table("deltalake_catalog.default.orders_bronze")
    .selectExpr("orderid as order_id", "orderdate as order_date", "customerid as customer_id", "totalamount as total_amount", "status as status","filename as file_name", "ingesttime as ingest_time")
    .withColumn("total_amount_in_usd", utils.inr_to_usd(col("total_amount")))
  )

# Silver: cleaned customers with expectation
@dp.table(name="deltalake_catalog.default.customers_silver_cleaned")
@dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
def clean_customers():
  return (
    spark.readStream.table("deltalake_catalog.default.customers_bronze")
    .selectExpr("customerid as customer_id", "customername as customer_name", "contactnumber as phone_number","email as email", "address as city", "dateofbirth as dob", "registrationdate as customer_since","filename as file_name", "ingesttime as ingest_time")
  )

# Silver: SCD type 2 for customers using AUTO CDC
# Step 1: Create target table
dp.create_streaming_table(name="deltalake_catalog.default.customers_silver")

# Step 2: Define CDC flow
dp.create_auto_cdc_flow(
  target="deltalake_catalog.default.customers_silver",
  source="deltalake_catalog.default.customers_silver_cleaned",
  keys=["customer_id"],
  sequence_by="ingest_time",
  stored_as_scd_type=2
)

#Silver: SCD type 1 for orders using AUTO CDC

dp.create_streaming_table(name="deltalake_catalog.default.orders_silver")
dp.create_auto_cdc_flow(
  target="deltalake_catalog.default.orders_silver",
  source="deltalake_catalog.default.orders_silver_cleaned",
  keys=["order_id"],
  sequence_by="ingest_time"
)

# Gold: join orders and customers
@dp.materialized_view(name="deltalake_catalog.default.city_wise_sales_gold")
def city_wise_sales():
  return (
    spark.read.table("deltalake_catalog.default.orders_silver")
    .join(spark.read.table("deltalake_catalog.default.customers_silver"), on="customer_id")
    .groupBy("city")
    .agg(round(sum("total_amount_in_usd"),2).alias("total_sales"))
  )
                          
