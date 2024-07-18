-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM PARQUET.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

CREATE OR Replace TABLE orders AS
Select * from PARQUET.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM PARQUET.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM PARQUET.`${dataset.bookstore}/orders`

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM PARQUET.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

CREATE OR Replace temp view customers_updates AS
SELECT * FROM JSON.`${dataset.bookstore}/customers-json-new`;
Merge into customers c
using customers_updates u
on c.customer_id = u.customer_id
when matched and c.email is null and u.email is not null then
update set email = u.email, updated = u.updated
when not matched then insert *

-- COMMAND ----------

Create or replace temp view books_updates
(book_id string, title string, author string, category string, price double)
using csv
options (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE into books b
using books_updates u
On b.book_id = u.book_id and b.title = u.title
When not matched and u.category = 'Computer Science' then
Insert *

-- COMMAND ----------


