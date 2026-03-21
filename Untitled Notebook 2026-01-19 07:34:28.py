# Databricks notebook source
from pyspark.sql.functions import col
df = spark.sql("select * from hilton_hotmail.default.customer_dim")
from pyspark.sql import functions as F
df.select([F.sum(col(c).isNull().cast('int')).alias(c) for c in df.columns]).show()


# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE history bookstore_eng_pro.books_raw

# COMMAND ----------

from delta import DeltaTable
dt = DeltaTable.forName(spark,'bookstore_eng_pro.country_lookup1')
display(dt.history())

# COMMAND ----------

# MAGIC %sql
# MAGIC select _metadata.file_path from bookstore_eng_pro.books_raw

# COMMAND ----------

tbl = "bookstore_eng_pro.country_lookup1"  # change to your table
files = spark.table(tbl).inputFiles()
files[:20], len(files)
