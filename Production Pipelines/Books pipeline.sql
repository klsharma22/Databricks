-- Databricks notebook source
SET datasets.path = dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Bronze Layer Table

-- COMMAND ----------

CREATE or Refresh streaming live table books_bronze
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets.path}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Silver Layer Tables

-- COMMAND ----------

CREATE or refresh streaming live table books_silver;

apply changes into live.books_silver
from stream(live.books_bronze)
keys (book_id)
apply as delete when row_status = "DELETE"
sequence by row_time
columns * Except (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Gold Layer Tables

-- COMMAND ----------

CREATE LIVE TABLE author_counts_state
COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() update_time
FROM live.books_silver
group by author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DLT

-- COMMAND ----------

CREATE LIVE VIEW books_sales
as select b.title, o.quantity
from (
  select *, explode(books) as book
  from live.orders_cleaned ) o
  inner join live.books_silver b
  on o.book.book_id = b.book_id;
