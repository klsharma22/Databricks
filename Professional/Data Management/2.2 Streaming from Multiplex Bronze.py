# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC select cast(key as String), cast(value as string)
# MAGIC from bronze
# MAGIC limit 20

# COMMAND ----------

# MAGIC %sql
# MAGIC select v.*
# MAGIC from (
# MAGIC   select from_json(cast(value as string), "order_id string, oredr_timestamp timestamp, customer_id string, quantity bigint, total bigint, books array<struct<book_id string, quantity bigint, subtotal bigint>>") v
# MAGIC from bronze
# MAGIC where topic = "orders")

# COMMAND ----------

(spark.readStream
 .table("bronze")
 .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select v.*
# MAGIC from (
# MAGIC   select from_json(cast(value as string), "oreder_id string, order_timestamp timestamp, customer_id string, quantity bigint, total bigint, books array<struct<book_id string, quantity bigint, subtotal bigint>>") v
# MAGIC   from bronze_tmp
# MAGIC   where topic = "orders"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view orders_silver_tmp as select v.*
# MAGIC from (
# MAGIC   select from_json(cast(value as string), "order_id string, order_timestamp timestamp, customer_is string, quantity bigint, total bigint, books array<struct<book_id string, quantity bigint, subtotal bigint>>") v
# MAGIC   from bronze_tmp
# MAGIC   where topic = "orders"
# MAGIC )

# COMMAND ----------

query = (spark.table("orders_silver_tmp")
         .writeStream
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
         .trigger(availableNow= True)
         .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id string, order_timestamp timestamp, customer_id string, quantity bigint, total bigint, books array<struct<book_id string, quantity bigint, subtotal bigint>>"

query = (spark.readStream.table("bronze")
         .filter("topic = 'orders'")
         .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
         .select("v.*")
         .writeStream
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
         .trigger(availableNow= True)
         .table("orders_silver"))
query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_silver
