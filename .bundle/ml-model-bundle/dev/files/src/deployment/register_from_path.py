# Databricks notebook source
# MAGIC %md
# MAGIC # Register Model from Path (prod workspace)
# MAGIC Use when source model is in a different metastore (dev). Copy the model artifact
# MAGIC to a path this workspace can read (e.g. shared volume or cloud storage), then run
# MAGIC this job with that path to register it in prod_2.default.wine_quality_model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get job parameters

# COMMAND ----------

dbutils.widgets.text("artifact_path", "", "Path to model artifact (e.g. /Volumes/catalog/schema/vol/path or dbfs:/path)")
dbutils.widgets.text("target_catalog", "prod_2", "Target catalog")
dbutils.widgets.text("target_schema", "default", "Target schema")
dbutils.widgets.text("target_model", "wine_quality_model", "Target model name")

# COMMAND ----------

artifact_path = dbutils.widgets.get("artifact_path").strip()
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_model = dbutils.widgets.get("target_model")

if not artifact_path:
    raise ValueError("artifact_path is required. Set it to the path where the model artifact was copied (e.g. shared volume or DBFS).")

dst_name = f"{target_catalog}.{target_schema}.{target_model}"
print(f"Registering from: {artifact_path}")
print(f"Target: {dst_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register model to Unity Catalog (prod metastore)

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient(registry_uri="databricks-uc")

# Register the model from the artifact path (must contain MLmodel + artifact files)
version = client.create_model_version(
    name=dst_name,
    source=artifact_path,
    run_id=None,
)

print(f"Registered: {dst_name} version {version.version}")

# COMMAND ----------

dbutils.jobs.taskValues.set("registered_version", str(version.version))
dbutils.jobs.taskValues.set("registered_model_uri", f"models:/{dst_name}/{version.version}")
