-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country 
From customers

-- COMMAND ----------

SELECT from_json(profile) as profile_struct
FROM customers;

-- COMMAND ----------

SELECT profile
FROM customers
LIMIT 1

-- COMMAND ----------

Create or Replace Temp view parsed_customers AS
SELECT customer_id, from_json(profile, schema_of_json(
'{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) AS profile_struct
FROM customers;

SELECT * FROM parsed_customers

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers

-- COMMAND ----------

Create or replace temp view customers_final AS
SELECT customer_id, profile_struct.*
FROM parsed_customers;

SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books 
FROM orders

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book
FROM orders

-- COMMAND ----------

SELECT customer_id,
collect_set(order_id) AS order_sets,
collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

SELECT customer_id,
collect_set(books.book_id) AS before_flatten,
array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

create or replace view orders_enriched as
select * 
from (
  select *, explode(books) As book
  from orders) o
inner join books b
on o.book.book_id = b.book_id;

select * from orders_enriched

-- COMMAND ----------

create or replace temp view orders_updates
as select * from parquet.`${dataset.bookstore}/orders-new`;

select * from orders
Union
select * from orders_updates 

-- COMMAND ----------

SELECT * FROM orders
Intersect
SELECT * FROM orders_updates

-- COMMAND ----------

SELECT * FROM orders
MINUS
Select * from orders_updates

-- COMMAND ----------

Create or replace table transactions as
select * from (
  select
    customer_id,
    book.book_id as book_id,
    book.quantity as quantity
  from orders_enriched
) Pivot (
  sum(quantity) for book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

Select * from transactions
