# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE hive_metastore.bookstore_eng_pro.orders_silver add CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED hive_metastore.bookstore_eng_pro.orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into hive_metastore.bookstore_eng_pro.orders_silver
# MAGIC values ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from hive_metastore.bookstore_eng_pro.orders_silver
# MAGIC where order_id in ('1', '2', '3')

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table hive_metastore.bookstore_eng_pro.orders_silver add constraint valid_quantity check (quantity > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended hive_metastore.bookstore_eng_pro.orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM hive_metastore.bookstore_eng_pro.orders_silver
# MAGIC WHERE quantity <= 0;

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = "order_id String, order_timestamp timestamp, customer_id string, quantity bigint, total bigint, books array<struct<book_id string, quantity bigint, subtotal bigint>>"

query = (
    spark.readStream.table("hive_metastore.bookstore_eng_pro.bronze")
    .filter("topic = 'orders'")
    .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
    .select("v.*")
    .filter("quantity > 0")
    .writeStream
    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
    .trigger(availableNow=True)
    .table("orders_silver")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE bookstore_eng_pro.orders_silver DROP CONSTRAINT timestamp_within_range

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bookstore_eng_pro.orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE bookstore_eng_pro.orders_silver

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)
