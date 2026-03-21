-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC with open(
-- MAGIC     "/Volumes/hilton_catalog/bronze/scripts_pro/Databricks-Certified-Data-Engineer-Professional-main/Includes/Copy-Datasets.py"
-- MAGIC     ) as f:
-- MAGIC         exec(f.read())
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bookstore.load_pipeline_data()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC with open(
-- MAGIC     "/Volumes/hilton_catalog/bronze/scripts_pro/Databricks-Certified-Data-Engineer-Professional-main/Includes/Reset-Datasets.py"
-- MAGIC     ) as f:
-- MAGIC         exec(f.read())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_path = '/Volumes/hilton_hotmail/bookstore_eng_pro/dataset'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.functions import from_unixtime, col
-- MAGIC schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"
-- MAGIC def process_bronze():
-- MAGIC     query = (spark.readStream
-- MAGIC           .format('cloudFiles')
-- MAGIC           .option('cloudFiles.format','json')
-- MAGIC           .schema(schema)
-- MAGIC           .load(f'{dataset_path}/kafka-raw')
-- MAGIC           .withColumn('timestamp', (col('timestamp')/1000).cast('timestamp'))
-- MAGIC           .withColumn('year_month', F.date_format(col('timestamp'), 'yyyy-MM'))
-- MAGIC           .withWatermark('timestamp','1 minute')
-- MAGIC           #.dropDuplicates(['key','timestamp'])
-- MAGIC           .select("key","value","topic","partition","offset","timestamp","year_month")
-- MAGIC           .writeStream
-- MAGIC           .option('checkpointLocation','/Volumes/hilton_hotmail/bookstore_eng_pro/checkpoints/bronze')
-- MAGIC           .option('mergeSchema',True)
-- MAGIC           .partitionBy('topic','year_month')
-- MAGIC           .trigger(availableNow=True)
-- MAGIC           .table('bronze')
-- MAGIC     )
-- MAGIC
-- MAGIC     query.awaitTermination()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC process_bronze()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('hilton_hotmail.bookstore_eng_pro.bronze')
-- MAGIC display(df.filter('topic=="orders"'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bookstore.load_new_data()

-- COMMAND ----------

use catalog hilton_hotmail;
use schema bookstore_eng_pro;
select CAST(key AS STRING), CAST(value AS STRING) from hilton_hotmail.bookstore_eng_pro.bronze
where topic='orders'

-- COMMAND ----------

SELECT schema_of_json(cast(value as string)), cast(value as string) from bronze where topic='orders';
SELECT v.* from (
select topic, from_json(cast(value as string),('STRUCT<books: ARRAY<STRUCT<book_id: STRING, quantity: BIGINT, subtotal: BIGINT>>, customer_id: STRING, order_id: STRING, order_timestamp: STRING, quantity: BIGINT, total: BIGINT>
')) v from bronze
where topic='orders'
) where topic='orders'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.table("hilton_hotmail.bookstore_eng_pro.bronze")
-- MAGIC df.createOrReplaceTempView("bronze_tmp")
-- MAGIC
-- MAGIC display(spark.sql("SELECT * FROM bronze_tmp"))
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_silver_tmp
AS
SELECT v.* from (
select  from_json(cast(value as string),('STRUCT<order_id: STRING,books: ARRAY<STRUCT<book_id: STRING, quantity: BIGINT, subtotal: BIGINT>>, customer_id: STRING,  order_timestamp: STRING, quantity: BIGINT, total: BIGINT>')) v from bronze
where topic='orders'
) 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = ( spark.table('orders_silver_tmp')
-- MAGIC .write
-- MAGIC .format("delta")
-- MAGIC .option('checkpointLocation','/Volumes/hilton_hotmail/bookstore_eng_pro/checkpoints/silver')
-- MAGIC .mode('overwrite')
-- MAGIC .saveAsTable('hilton_hotmail.bookstore_eng_pro.orders_silver')
-- MAGIC )
-- MAGIC
-- MAGIC

-- COMMAND ----------

select * from hilton_hotmail.bookstore_eng_pro.orders_silver


-- COMMAND ----------

alter table bookstore_eng_pro.orders_silver add constraint timestamp_within_range check (order_timestamp>'2020-01-01')

-- COMMAND ----------

describe extended bookstore_eng_pro.orders_silver

-- COMMAND ----------

use schema bookstore_eng_pro;
CREATE OR REPLACE TABLE orders_silver1 AS SELECT distinct * FROM orders_silver 

-- COMMAND ----------

select * from bookstore_eng_pro.orders_silver1;
create or replace table bookstore_eng_pro.orders_silver2 as select * from orders_silver1 where 1=2;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC def upsert_batch ( microBatchDf, batch ):
-- MAGIC   microBatchDf.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("orders_microbatch")
-- MAGIC   
-- MAGIC   sql_query=spark.sql("""
-- MAGIC     MERGE INTO orders_silver2 t
-- MAGIC     USING orders_microbatch s
-- MAGIC     ON t.order_id = s.order_id and t.order_timestamp=s.order_timestamp
-- MAGIC     WHEN MATCHED THEN UPDATE SET *
-- MAGIC     WHEN NOT MATCHED THEN INSERT *
-- MAGIC   """)
-- MAGIC
-- MAGIC   microBatchDf.sparkSession.sql(sql_query)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC silver_df = spark.readStream.table("hilton_hotmail.bookstore_eng_pro.orders_silver1")
-- MAGIC
-- MAGIC query=(silver_df.writeStream
-- MAGIC           .foreachBatch(upsert_batch)
-- MAGIC           .option("checkpointLocation","/Volumes/hilton_hotmail/bookstore_eng_pro/checkpoints/silver")
-- MAGIC           .trigger(availableNow=True)
-- MAGIC           .start()
-- MAGIC )
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

select count(*) from orders_silver2

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/Volumes/hilton_catalog/bronze/scripts_pro
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC password=dbutils.secrets.get('bookstore-dev','db-secret')
-- MAGIC
-- MAGIC for i in password:
-- MAGIC     print(i)