-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, city STRING, country_code STRING, row_status STRING, row_time TIMESTAMP"
-- MAGIC
-- MAGIC customer_df = (spark.table("bookstore_eng_pro.bronze")
-- MAGIC                .filter(F.col("topic") == "customers")
-- MAGIC                .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
-- MAGIC                .filter(F.col('v.row_status').isin('insert', 'update'))
-- MAGIC                .select("v.*")
-- MAGIC )
-- MAGIC display(customer_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.window import Window
-- MAGIC from pyspark.sql.functions import rank
-- MAGIC
-- MAGIC def batch_upsert(microBatchDf, batchId):
-- MAGIC   window = Window.partitionBy('customer_id').orderBy(F.col('row_time').desc())
-- MAGIC   (microBatchDf
-- MAGIC               .withColumn('rank', rank().over(window))
-- MAGIC               .filter('rank == 1')
-- MAGIC               .drop('rank')
-- MAGIC               .createOrReplaceTempView('ranked_updates'))
-- MAGIC   
-- MAGIC
-- MAGIC   query = """
-- MAGIC        MERGE INTO customer_silver AS t
-- MAGIC        USING ranked_updates AS s
-- MAGIC        ON t.customer_id = s.customer_id
-- MAGIC        WHEN MATCHED AND t.row_time < s.row_time THEN
-- MAGIC          UPDATE SET *
-- MAGIC        WHEN NOT MATCHED THEN
-- MAGIC          INSERT *
-- MAGIC   
-- MAGIC   """
-- MAGIC
-- MAGIC   microBatchDf.sparkSession.sql(query)
-- MAGIC
-- MAGIC

-- COMMAND ----------

drop table bookstore_eng_pro.customers_silver;

/*(customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, city STRING, country STRING, row_status STRING, row_time TIMESTAMP)*/


-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_store = '/Volumes/hilton_hotmail/bookstore_eng_pro/dataset'
-- MAGIC df_country_lookup = spark.read.json(f'{dataset_store}/country_lookup')
-- MAGIC display(df_country_lookup)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import broadcast
-- MAGIC spark.sql('use schema bookstore_eng_pro')
-- MAGIC schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, city STRING, country_code STRING, row_status STRING, row_time TIMESTAMP"
-- MAGIC query = (spark.readStream.table('bronze')
-- MAGIC            .filter("topic=='customers'")
-- MAGIC            .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
-- MAGIC            .join(broadcast(df_country_lookup), F.col('v.country_code')==F.col('code'), how='inner')
-- MAGIC            .filter(F.col('v.row_status').isin('insert', 'update'))
-- MAGIC            .select ("v.customer_id", "v.email", "v.first_name", "v.last_name", "v.gender", "v.city", "country", "v.row_time")
-- MAGIC            .writeStream
-- MAGIC            .foreachBatch(batch_upsert)
-- MAGIC            .option("checkpointLocation", f"{dataset_store}/customer_silver/checkpoint")
-- MAGIC            .trigger(availableNow=True)
-- MAGIC )
-- MAGIC
-- MAGIC query.start().awaitTermination()
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC count = spark.table('customer_silver').count()
-- MAGIC expected_count = spark.table('customer_silver').select('customer_id').distinct().count()
-- MAGIC assert count == expected_count, f"Expected {expected_count} records, found {count} records"
-- MAGIC print("test passed")
-- MAGIC

-- COMMAND ----------

alter table bookstore_eng_pro.customer_silver set tblproperties(delta.enableChangeDataFeed = true);
describe history customer_silver;

-- COMMAND ----------

update bookstore_eng_pro.customer_silver set country = 'PRC' where country='China'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=(spark.readStream
-- MAGIC       .format('delta')
-- MAGIC       .option('readChangeData',True)
-- MAGIC       .option('startingVersion',5)
-- MAGIC       .table('bookstore_eng_pro.customer_silver')
-- MAGIC )
-- MAGIC
-- MAGIC (df.writeStream
-- MAGIC  .format('delta')
-- MAGIC  .trigger(availableNow=True)
-- MAGIC  .option('checkpointLocation',f'{dataset_store}/customer_silver1/checkpoint')
-- MAGIC  .toTable('customer_silver_cdc'))
-- MAGIC  

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC orders = (spark.readStream.table('orders_silver')
-- MAGIC .withColumn('order_timestamp', F.col('order_timestamp').cast('timestamp'))
-- MAGIC .withWatermark('order_timestamp','1 hour')
-- MAGIC .groupBy(F.window('order_timestamp','1 hour').alias('w'),'customer_id')
-- MAGIC .agg(F.count('customer_id').alias('order_cnt'))
-- MAGIC )
-- MAGIC
-- MAGIC customer = (spark.readStream.table('customer_silver')
-- MAGIC .withWatermark('row_time','1 hour')
-- MAGIC .groupBy(F.window('row_time','1 hour').alias('w'),'customer_id')
-- MAGIC .agg(F.count('customer_id').alias('customer_cnt'))
-- MAGIC )
-- MAGIC
-- MAGIC customer_orders = (customer.join(orders,['customer_id','w'],'inner')
-- MAGIC                    .select('customer_id','w','customer_cnt','order_cnt')
-- MAGIC )
-- MAGIC
-- MAGIC (customer_orders.writeStream
-- MAGIC .format('delta') 
-- MAGIC .option('checkpointLocation',f'{dataset_store}/customer_silver_orders3/checkpoint')
-- MAGIC .trigger(availableNow=True)
-- MAGIC .outputMode('append')
-- MAGIC .toTable('customer_silver_orders3')
-- MAGIC )   
-- MAGIC
-- MAGIC

-- COMMAND ----------

select * from customer_silver_orders3

-- COMMAND ----------

select * from bookstore_eng_pro.orders_silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC customer =(spark.readStream.table('customer_silver'))
-- MAGIC
-- MAGIC order = (spark.readStream.table('orders_silver').withColumn('order_timestamp', F.col('order_timestamp').cast('timestamp')))
-- MAGIC
-- MAGIC customer_orders = customer.alias('c').join(orders.alias('o'), on="customer_id", how="inner")
-- MAGIC
-- MAGIC (customer_orders.writeStream
-- MAGIC                 .format('delta')
-- MAGIC                 .option('checkpointLocation',f'{dataset_store}/customer_silver_orders4/checkpoint')
-- MAGIC                 .trigger(availableNow=True)
-- MAGIC                 .outputMode('append')
-- MAGIC                 .toTable('customer_silver_orders4'))    
-- MAGIC

-- COMMAND ----------

select * from customer_silver_orders4

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import DoubleType
-- MAGIC
-- MAGIC def apply_discount(price, discount):
-- MAGIC     return price * (1 - discount)
-- MAGIC
-- MAGIC apply_discount(1,0.1)
-- MAGIC
-- MAGIC spark.udf.register("apply_discount_udf", apply_discount, DoubleType())
-- MAGIC        
-- MAGIC spark.sql('use schema bookstore_eng_pro')
-- MAGIC
-- MAGIC

-- COMMAND ----------


select order_cnt, apply_discount_udf(cast(order_cnt as int),0.1D) from customer_silver_orders4

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.functions import pandas_udf
-- MAGIC from pyspark.sql.functions import lit, col
-- MAGIC from pyspark.sql.types import DoubleType
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC def apply_discount_udf(order_cnt: pd.Series, discount: pd.Series) -> pd.Series:
-- MAGIC     return order_cnt * discount
-- MAGIC
-- MAGIC pd_apply_discount_udf = pandas_udf(apply_discount_udf, returnType=DoubleType())
-- MAGIC
-- MAGIC df = (spark.table('customer_silver_orders4')
-- MAGIC       .select('customer_id', 'order_cnt', pd_apply_discount_udf('order_cnt', lit(0.1)).alias('discounted_price'))
-- MAGIC       .write
-- MAGIC       .format('delta')
-- MAGIC       .mode('overwrite')
-- MAGIC       .saveAsTable('customer_silver_orders5')
-- MAGIC )

-- COMMAND ----------

select * from customer_silver_orders5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.udf.register('sql_apply_discount',pd_apply_discount_udf)

-- COMMAND ----------

select sql_apply_discount(cast(order_cnt as int),0.1D) from customer_silver_orders4