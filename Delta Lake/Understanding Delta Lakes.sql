-- Databricks notebook source
create table employees
  (id int, name string, salary double);

-- COMMAND ----------

insert into employees
values
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5),
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3),
  (5, "Anna", 2500.0),
  (6, "Kim", 6200.3)

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

UPDATE employees
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employees

-- COMMAND ----------

DESCRIBE DETAIL employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employees/_delta_log

-- COMMAND ----------


