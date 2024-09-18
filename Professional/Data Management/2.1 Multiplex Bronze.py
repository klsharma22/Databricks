# Databricks notebook source
# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)

# COMMAND ----------

from pyspark.sql import functions as F

def process_bronze():
    schema= "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .schema(schema)
                .load(f"{dataset_bookstore}/kafka-raw")
                .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))
                .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                .writeStream
                .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                .option("mergeSchema", True)
                .partitionBy("topic", "year_month")
                .trigger(availableNow= True)
                .table("bronze"))
    query.awaitTermination()

# COMMAND ----------

process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select Distinct(topic) from bronze

# COMMAND ----------

bookstore.load_new_data()

# COMMAND ----------

process_bronze()

# COMMAND ----------

# MAGIC %sql Select count(*) from bronze

# COMMAND ----------


