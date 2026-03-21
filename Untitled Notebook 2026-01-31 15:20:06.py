# Databricks notebook source
# MAGIC %sql
# MAGIC  
# MAGIC DROP RECIPIENT IF EXISTS prod_recipient;
# MAGIC
# MAGIC CREATE RECIPIENT prod_recipient USING  ID 'aws:us-west-1:7c42c5ad-23f0-4274-a507-71ef2c160696';
# MAGIC
# MAGIC
# MAGIC GRANT SELECT ON SHARE customer_share TO RECIPIENT prod_recipient;
# MAGIC
# MAGIC show recipients;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe recipient prod_2

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG hilton_hotmail TO `hma68@hotmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA hilton_hotmail.default TO `hma68@hotmail.com`;
# MAGIC GRANT SELECT ON TABLE hilton_hotmail.default.books_bronze TO `hma68@hotmail.com`;
# MAGIC ALTER SHARE customer_share ADD TABLE hilton_hotmail.default.books_bronze;
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE RECIPIENT prod

# COMMAND ----------

from pyspark.sql.functions import col

df_metrics = spark.read.table('system.mlflow.run_metrics_history')
df_latest = spark.read.table('system.mlflow.runs_latest')
df = df_metrics.alias('m').join(df_latest.alias('l'), on = 'run_id', how = 'inner').select("m.metric_name", "m.metric_value", "m.metric_time","l.start_time")

# COMMAND ----------

from pyspark.sql.functions import window,count
df_window = df.groupBy(window(col('metric_time'), '5 minutes'), 'metric_name').agg(count('*')).display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
window = Window.partitionBy('metric_name').orderBy('metric_time').rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_window = df.withColumn('rolling_count', rank().over(window)).display()

# COMMAND ----------

# MAGIC %pip install databricks_genai_inference==0.2.3 evaluate torch transformers textstat
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks_genai_inference import ChatCompletion

chat_response = ChatCompletion.create(
    model="databricks-meta-llama-3-3-70b-instruct",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is the capital of France?"},
    ],
    temperature=0.5,
    stop=["\n", " Human:", " AI:"])

print(chat_response.json['choices'][0]['message']['content'])

# COMMAND ----------

from databricks_genai_inference import Completion

chat_response = Completion.create(
    model="databricks-qwen3-next-80b-a3b-instruct",
    prompt="What are the three pros of databricks",
    temperature=0.5)

print(chat_response.json['choices'][0]['message']['content'])

# COMMAND ----------

from databricks_genai_inference import Embedding

chat_response = Embedding.create(
    model="bge-large-en",
    input="What are the three pros of databricks")

print(chat_response.json['data'][0]['embedding'])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT catalog_name, schema_name, table_name
# MAGIC FROM system.data_quality_monitoring.table_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from system.billing.usage
# MAGIC where usage_date>'2026-02-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC with q as (
# MAGIC   select
# MAGIC     statement_id,
# MAGIC     endpoint_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     total_duration_ms
# MAGIC   from system.query.history
# MAGIC ),
# MAGIC
# MAGIC u as (
# MAGIC   select
# MAGIC     warehouse_id,
# MAGIC     usage_start_time,
# MAGIC     usage_end_time,
# MAGIC     usage_quantity as dbus
# MAGIC   from system.billing.usage
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   q.user_name,
# MAGIC   q.statement_id,
# MAGIC   q.start_time,
# MAGIC   q.total_duration_ms,
# MAGIC   u.dbus
# MAGIC from q
# MAGIC join u
# MAGIC   on q.endpoint_id = u.warehouse_id
# MAGIC  and q.start_time between u.usage_start_time and u.usage_end_tim
# MAGIC   where q.start_time>'2026-02-02'

# COMMAND ----------

# MAGIC %sql
# MAGIC select statement_id /*, compute_id*/ ,*
# MAGIC   from system.query.history limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC   select
# MAGIC     statement_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     total_duration_ms,
# MAGIC     *
# MAGIC   from system.query.history

# COMMAND ----------

# MAGIC %sql
# MAGIC with q as (
# MAGIC   select
# MAGIC     statement_id,
# MAGIC     warehouse_id,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     total_duration_ms
# MAGIC   from system.query.history
# MAGIC ),
# MAGIC
# MAGIC u as (
# MAGIC   select
# MAGIC     warehouse_id,
# MAGIC     usage_start_time,
# MAGIC     usage_end_time,
# MAGIC     usage_quantity as dbus
# MAGIC   from system.billing.usage
# MAGIC )
# MAGIC
# MAGIC select
# MAGIC   q.statement_id,
# MAGIC   q.user_name,
# MAGIC   q.start_time,
# MAGIC   q.total_duration_ms,
# MAGIC   u.dbus
# MAGIC from q
# MAGIC join u
# MAGIC   on q.warehouse_id = u.warehouse_id
# MAGIC  and q.start_time between u.usage_start_time and u.usage_end_time
# MAGIC  and q.start_time>'2026-02-04'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_usage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     j.run_name,
# MAGIC     SUM(u.usage_quantity) / COUNT(DISTINCT j.run_id) AS avg_dbu_per_run
# MAGIC FROM system.billing.usage u
# MAGIC JOIN system.jobs.run j
# MAGIC   ON u.usage_metadata.job_id = j.job_id
# MAGIC WHERE u.usage_date >= current_date() - INTERVAL 14 DAYS
# MAGIC GROUP BY j.run_name
# MAGIC ORDER BY avg_dbu_per_run DESC;