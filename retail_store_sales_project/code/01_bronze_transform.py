# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("item", StringType(), True),
    StructField("price_per_unit", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("total_spent", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("location", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("discount_applied", StringType(), True),
])

# COMMAND ----------

path = "/Volumes/workspace/default/raw_layer/retail_store_sales.csv"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
        .schema(schema) \
            .load(path)

df_raw.write.mode("overwrite").saveAsTable("bronze_retail_store_sales")

# COMMAND ----------

display(spark.table("bronze_retail_store_sales").limit(5))

# COMMAND ----------

