# Databricks notebook source
# MAGIC %sql
# MAGIC show shares
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SHARE customer_share TO RECIPIENT prod_recipient