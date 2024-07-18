# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "parquet")
 .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_chechkpoint")
 .load(f"{dataset_bookstore}/orders-raw")
 .writeStream
 .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
 .table("orders_updates")
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_updates

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates
