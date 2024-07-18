-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

Select * from json. `${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json. `${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json. `${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) from json. `${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT *, input_file_name() source_file
FROM json. `${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * from text. `${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT * from binaryFile. `${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * from csv. `${dataset.bookstore}/books-csv`

-- COMMAND ----------

CREATE TABLE books_csv
(book_id string, title string, author string, category string, price DOUBLE)
USING CSV
options (
  header = "true",
  delimiter = ";"
)
Location "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

describe extended books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.table("books_csv").write.mode("append").format("csv").option("header", "true").option("delimiter", ";").save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT count(*) from books_csv

-- COMMAND ----------

REFRESH table books_csv

-- COMMAND ----------

SELECT count(*) FROM books_csv

-- COMMAND ----------

Create table customers AS
SELECT * FROM JSON.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed as SELECT * from csv. `${dataset.bookstore}/books-csv`;

Select * from books_unparsed

-- COMMAND ----------

CREATE TEMPORARY VIEW books_temp_view
(book_id string, title string, author string, category string, price double)
using CSV
Options(
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
  );

Create table books as
SELECT * FROM books_temp_view;

SELECT * from books;

-- COMMAND ----------

DESCRIBE EXTENDED books
