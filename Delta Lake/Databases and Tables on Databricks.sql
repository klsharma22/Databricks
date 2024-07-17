-- Databricks notebook source
CREATE TABLE managed_default
  (widht INT, length INT, heigt INT);

INSERT INTO managed_default
VALUES (3 int, 2 int, 1 int)

-- COMMAND ----------

DESCRIBE EXTENDED managed_default

-- COMMAND ----------

-- MAGIC %fs ls

-- COMMAND ----------

CREATE TABLE external_default
(width int, length int, height int)
LOCATION 'dbfs:/mnt/demo/externa_default';

INSERT into external_default
Values (3 int, 2 int, 1 int)

-- COMMAND ----------

DESCRIBE EXTENDED external_default

-- COMMAND ----------

Drop table managed_default

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/managed_default

-- COMMAND ----------

DROP TABLE external_default

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/demo/externa_default

-- COMMAND ----------

CREATE SCHEMA new_default

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED new_default

-- COMMAND ----------

use new_default;

Create table managed_new_default
(width int, length int, height int);

Insert into managed_new_default
values (3 int, 2 int, 1 int);

--------------------------------

Create table external_new_default
(widht int, length int, height int)
Location 'dbfs:/mnt/demo/external_new_default';

insert into external_new_default
values (3 int, 2 int, 1 int);

-- COMMAND ----------

describe extended managed_new_default

-- COMMAND ----------

describe extended external_new_default

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default

-- COMMAND ----------

-- MAGIC %fs ls dfs:/user/hive/warehouse/managed_new_default

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/demo/external_new_default

-- COMMAND ----------

Create schema custom
location 'dbfs:/Shared/schemas/custom.db';

-- COMMAND ----------

describe database extended custom

-- COMMAND ----------

use custom;

create table managed_custom
(widht int, length int, height int);

insert into managed_custom
values(3 int, 3 int, 1 int);

----------------

create table external_custom
(width int, length int, height int)
location 'dbfs:/mnt/demo/external_custom';

insert into external_custom
values(3 int, 2 int, 1 int);

-- COMMAND ----------

DESCRIBE EXTENDED managed_custom

-- COMMAND ----------

describe extended external_custom

-- COMMAND ----------

drop table managed_custom;
drop table external_custom;

-- COMMAND ----------

drop database custom;

-- COMMAND ----------

drop database new_default
