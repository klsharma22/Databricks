# Databricks notebook source
# MAGIC %run 
# MAGIC ./Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bookstore_eng_pro.books_silver
# MAGIC USING (
# MAGIC     SELECT updates.book_id as merge_key, updates.*
# MAGIC     from updates
# MAGIC
# MAGIC     UNion all
# MAGIC
# MAGIC     select null as merge_key, updates.*
# MAGIC     from updates
# MAGIC     join bookstore_eng_pro.books_silver on updates.book_id = bookstore_eng_pro.books_silver.bbok_id
# MAGIC     where bookstore_eng_pro.books_silver.current = true and updates.price <> bookstore_eng_pro.books_silver.price
# MAGIC ) staged_updates
# MAGIC on bookstore_eng_pro.books_silver.book_id = merge_key
# MAGIC when matched and bookstore_eng_pro.books_silver.current = true and bookstore_eng_pro.books_silver.price <> staged_updates.price then
# MAGIC update set current = false, end_date = staged_updates.updated
# MAGIC when not matched then
# MAGIC insert (book_id, title, author, price, current, effective_date, end_date)
# MAGIC values (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, null)

# COMMAND ----------

def type2_upsert(microBatchDF, batch):
  microBatchDF.createOrReplaceTempView("updates")

  sql_squery = """
  MERGE INTO bookstore_eng_pro.books_silver
  USING (
    SELECT updates.book_id as merge_key, updates.*
    from updates

    UNion all

    select null as merge_key, updates.*
    from updates
    join bookstore_eng_pro.books_silver on updates.book_id = bookstore_eng_pro.books_silver.book_id
    where bookstore_eng_pro.books_silver.current = true and updates.price <> bookstore_eng_pro.books_silver.price
    ) staged_updates
    on bookstore_eng_pro.books_silver.book_id = merge_key
    when matched and bookstore_eng_pro.books_silver.current = true and bookstore_eng_pro.books_silver.price <> staged_updates.price then
    update set current = false, end_date = staged_updates.updated
    when not matched then
    insert (book_id, title, author, price, current, effective_date, end_date)
    values (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, null)
  """

  microBatchDF.sparkSession.sql(sql_squery)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bookstore_eng_pro.books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

from pyspark.sql import functions as F
def process_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"

    query = (spark.readStream
              .table("bookstore_eng_pro.bronze")
              .filter("topic = 'books'")
              .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
              .select("v.*")
              .writeStream
              .foreachBatch(type2_upsert)
              .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoint/books_silver")
              .trigger(availableNow= True)
              .start())
    
    query.awaitTermination()

process_books()    

# COMMAND ----------

books_df = spark.read.table("bookstore_eng_pro.books_silver").orderBy("book_id", "effective_date")

display(books_df)

# COMMAND ----------

bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_df = spark.read.table("bookstore_eng_pro.books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_books
# MAGIC AS SELECT book_id, title, author, price
# MAGIC FROM bookstore_eng_pro.books_silver
# MAGIC where current is true

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_books
# MAGIC ORDER BY book_id
