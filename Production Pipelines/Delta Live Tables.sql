-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Orders Bronze

-- COMMAND ----------

SET datasets.path = dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

CREATE OR REFRESH STREAMING live table orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${datasets.path}/orders-raw", "parquet", map("schema", "order_id String, order_timestamp LONG, customer_id STRING, quantity LONG"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Customers Bronze

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM JSON.`${datasets.path}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Orders Cleaned Silver Layer

-- COMMAND ----------

CREATE or refresh streaming live table orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id is NOT NULL) ON VIOLATION DROP ROW
)
comment "The cleaned books orders with valid order_id"
as 
select order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp,
c.profile:address:country as country
from stream(live.orders_raw) o
left join live.customers c
on o.customer_id = c.customer_id 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Gold table

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
from live.orders_cleaned
WHERE country = "China"
GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
