# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

load_new_json_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`${dataset.bookstore}/books-cdc/02.json`

# COMMAND ----------


