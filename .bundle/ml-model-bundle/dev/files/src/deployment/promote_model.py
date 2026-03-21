# Databricks notebook source
# MAGIC %md
# MAGIC # Promote Model to Production
# MAGIC Copies a model version from source catalog (e.g. dev) to target catalog (prod) using Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get job parameters (from bundle job base_parameters)

# COMMAND ----------

dbutils.widgets.text("source_catalog", "hilton_hotmail", "Source catalog")
dbutils.widgets.text("source_schema", "default", "Source schema")
dbutils.widgets.text("source_model", "wine_quality_model", "Source model name")
dbutils.widgets.text("source_version", "2", "Source model version")
dbutils.widgets.text("target_catalog", "prod_2", "Target catalog")
dbutils.widgets.text("target_schema", "default", "Target schema")
dbutils.widgets.text("target_model", "wine_quality_model", "Target model name")

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
source_model = dbutils.widgets.get("source_model")
source_version = dbutils.widgets.get("source_version")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_model = dbutils.widgets.get("target_model")

print(f"Source: {source_catalog}.{source_schema}.{source_model} version {source_version}")
print(f"Target: {target_catalog}.{target_schema}.{target_model}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy model version to target catalog (Unity Catalog)

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

# Use Databricks Unity Catalog registry
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient(registry_uri="databricks-uc")

# Source: full model URI with version
src_model_uri = f"models:/{source_catalog}.{source_schema}.{source_model}/{source_version}"

# Target: catalog.schema.model_name (new version will be created)
dst_model_name = f"{target_catalog}.{target_schema}.{target_model}"

print(f"Copying: {src_model_uri} -> {dst_model_name}")

copied = client.copy_model_version(
    src_model_uri=src_model_uri,
    dst_name=dst_model_name,
)

print(f"Promoted model version: {copied.name} version {copied.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: log result for downstream tasks

# COMMAND ----------

dbutils.jobs.taskValues.set("promoted_version", str(copied.version))
dbutils.jobs.taskValues.set("promoted_model_uri", f"models:/{dst_model_name}/{copied.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC Done. Model is now available in prod. Check the printed `promoted_model_uri` above.
