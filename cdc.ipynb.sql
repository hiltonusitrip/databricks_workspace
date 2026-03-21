-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Bronze tables

-- COMMAND ----------

CREATE OR REPLACE STREAMING LIVE TABLE books_bronze
COMMENT "The raw cdc book data"
AS SELECT * FROM cloud_files('/Volumes/hilton_catalog/bronze/dataset/books-cdc/')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver Tables

-- COMMAND ----------

CREATE OR REPLACE STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver FROM STREAM(LIVE.books_bronze)
KEYS(book_id)
APPLY AS DELETE WHEN row_status='DELETE'
SEQUENCE BY row_time
COLUMNS * EXCEPT (row_status, row_time) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Gold Tables

-- COMMAND ----------

CREATE OR REPLACE LIVE TABLE author_count_state
COMMENT "Numbers of books per auth"
AS
SELECT author, count(*) as books_count, current_timestamp() as updated_time
FROM LIVE.books_silver
GROUP BY author
