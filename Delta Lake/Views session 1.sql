-- Databricks notebook source
Create table if not exists smartphones
(is int, name string, brand string, year int);

insert into smartphones
values (1, 'iphone 14', 'apple', 2022),
(2, 'iphone 13', 'apple', 2021),
(3, 'iphone 6', 'apple', 2014),
(4, 'ipad Air', 'apple', 2013),
(5, 'galaxy s22', 'samsung', 2022),
(6, 'galaxy z fold', 'samsung', 2022),
(7, 'galaxy s9', 'samsung', 2016),
(8, '12 pro', 'xiomi', 2022),
(9, 'redmi 11t pro', 'xiaomi', 2022),
(10, 'redmi note 11', 'xiami', 2021)

-- COMMAND ----------

show tables

-- COMMAND ----------

create view view_apple_phones
as select * from smartphones where brand="apple";

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

show tables

-- COMMAND ----------

create temporary view temp_view_phones_brands
as select distinct brand
from smartphones;

select * from temp_view_phones_brands;

-- COMMAND ----------

show tables

-- COMMAND ----------

create global temp view global_temp_view_latest_phones
as select * from smartphones where year > 2020
order by year desc;


-- COMMAND ----------

select * from global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

show tables
