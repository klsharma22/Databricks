-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SELECT
order_id,
books,
filter(books, i -> i.quantity >= 2) As multiple_copies
FROM orders

-- COMMAND ----------

SELECT order_id, multiple_copies
FROM (
  SELECT
  order_id,
  filter(books, i -> i.quantity >= 2) As multiple_copies
  FROM orders
)
WHERE size(multiple_copies) > 0; 

-- COMMAND ----------

SELECT
order_id,
books,
transform(
  books, 
  b -> cast(b.subtotal * 0.8 as int)
  ) as subtotal_after_discount
FROM orders

-- COMMAND ----------

CREATE or replace function get_url(email string)
returns STRING

return concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

DESCRIBE FUNCTION get_url

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING

RETURN Case
          When email like "%.com" then "Commercial business"
          WHEN email like "%.org" then "Non-profits organization"
          when email like "%.edu" then "educational institution"
          ELSE concat("Unknown extension for domain:", split(email, "@")[1])
          END;

-- COMMAND ----------

select email, site_type(email) as domain_category
from customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;

-- COMMAND ----------


